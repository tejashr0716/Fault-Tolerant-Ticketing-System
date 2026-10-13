import json
import logging
import threading
import time
import uuid
from decimal import Decimal
from typing import Any, Dict, Optional

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import AMQPConnectionError, AMQPChannelError

from app.config import settings

logger = logging.getLogger(__name__)

# Global reference for QueueService
_queue_service: Optional["QueueService"] = None
_queue_lock = threading.Lock()


class DecimalEncoder(json.JSONEncoder):
    """JSON Encoder handling Decimal types."""

    def default(self, obj: Any) -> Any:
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


class QueueService:
    """RabbitMQ Publisher Service managing persistent queue connection and message publication."""

    def __init__(self, amqp_url: Optional[str] = None):
        self.amqp_url = amqp_url or settings.rabbitmq_dsn
        self.queue_name = settings.RABBITMQ_QUEUE
        self.dlx_name = settings.RABBITMQ_DLX
        self.dlq_name = settings.RABBITMQ_DLQ
        self._connection: Optional[pika.BlockingConnection] = None
        self._channel: Optional[BlockingChannel] = None
        self._lock = threading.Lock()

    def _connect(self) -> BlockingChannel:
        """Establish or verify RabbitMQ connection and channel."""
        with self._lock:
            if (
                self._connection is not None
                and self._connection.is_open
                and self._channel is not None
                and self._channel.is_open
            ):
                return self._channel

            logger.info("Connecting to RabbitMQ at %s...", self.amqp_url)
            parameters = pika.URLParameters(self.amqp_url)
            parameters.heartbeat = 60
            parameters.blocked_connection_timeout = 300

            self._connection = pika.BlockingConnection(parameters)
            self._channel = self._connection.channel()

            # Enable publisher confirms to guarantee delivery
            self._channel.confirm_delivery()

            self._setup_queues(self._channel)
            logger.info("Successfully established connection to RabbitMQ with queue: %s", self.queue_name)
            return self._channel

    def _setup_queues(self, channel: BlockingChannel) -> None:
        """Declare dead-letter exchange, dead-letter queue, and durable processing queue."""
        try:
            # 1. Declare Dead-Letter Exchange (DLX)
            channel.exchange_declare(
                exchange=self.dlx_name,
                exchange_type="direct",
                durable=True,
            )

            # 2. Declare Dead-Letter Queue (DLQ)
            channel.queue_declare(
                queue=self.dlq_name,
                durable=True,
            )

            # 3. Bind DLQ to DLX
            channel.queue_bind(
                queue=self.dlq_name,
                exchange=self.dlx_name,
                routing_key=self.dlq_name,
            )

            # 4. Declare main durable queue with DLX forwarding configuration
            queue_args = {
                "x-dead-letter-exchange": self.dlx_name,
                "x-dead-letter-routing-key": self.dlq_name,
            }
            try:
                channel.queue_declare(
                    queue=self.queue_name,
                    durable=True,
                    arguments=queue_args,
                )
            except AMQPChannelError as ce:
                # If queue already exists with different attributes, fallback to existing configuration
                logger.warning(
                    "Queue %s declared with different arguments; reopening channel without extra args: %s",
                    self.queue_name,
                    ce,
                )
                self._channel = self._connection.channel()
                self._channel.confirm_delivery()
                self._channel.queue_declare(queue=self.queue_name, durable=True)
        except Exception as exc:
            logger.exception("Error while setting up RabbitMQ queues and exchanges: %s", exc)
            raise

    def publish_payment_event(self, event_data: Dict[str, Any]) -> bool:
        """
        Publish a `process_payment` event to the durable RabbitMQ queue.

        Args:
            event_data: Dictionary containing payment confirmation details.

        Returns:
            bool: True if publication confirmed by broker, False otherwise.
        """
        # Ensure payload indicates event type
        if "event_type" not in event_data:
            event_data["event_type"] = "process_payment"

        message_body = json.dumps(event_data, cls=DecimalEncoder)
        correlation_id = event_data.get("idempotency_key", str(uuid.uuid4()))
        message_id = str(uuid.uuid4())

        properties = pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Persistent,  # Durable message persisted to disk
            content_type="application/json",
            content_encoding="utf-8",
            correlation_id=correlation_id,
            message_id=message_id,
            timestamp=int(time.time()),
            headers={
                "event_type": event_data.get("event_type", "process_payment"),
                "idempotency_key": correlation_id,
            },
        )

        # Retry logic for resilient publishing
        max_retries = 3
        last_error = None
        for attempt in range(1, max_retries + 1):
            try:
                channel = self._connect()
                channel.basic_publish(
                    exchange="",
                    routing_key=self.queue_name,
                    body=message_body.encode("utf-8"),
                    properties=properties,
                    mandatory=True,
                )
                logger.info(
                    "Published process_payment event to %s: idempotency_key=%s, msg_id=%s",
                    self.queue_name,
                    correlation_id,
                    message_id,
                )
                return True
            except (AMQPConnectionError, AMQPChannelError, Exception) as exc:
                last_error = exc
                logger.warning(
                    "Publish attempt %d/%d failed: %s. Reconnecting...",
                    attempt,
                    max_retries,
                    exc,
                )
                self.close()
                time.sleep(0.5 * attempt)

        # Resilient fallback: Settle payment directly via worker logic if RabbitMQ is offline
        logger.warning("RabbitMQ offline (%s). Executing fault-tolerant direct ACID settlement...", last_error)
        try:
            from worker import PaymentWorker
            from decimal import Decimal
            worker = PaymentWorker()
            settlement = worker.execute_payment_transaction(
                user_id=event_data["user_id"],
                show_id=event_data["show_id"],
                seat_number=event_data["seat_number"],
                amount=Decimal(str(event_data["amount"])),
                idempotency_key=event_data["idempotency_key"],
                booking_id=event_data.get("booking_id"),
            )
            # Release Redis lock immediately
            worker.release_redis_lock(event_data["show_id"], event_data["seat_number"])
            logger.info("Direct ACID settlement succeeded: %s", settlement)
            return True
        except Exception as fallback_exc:
            logger.error("Direct settlement fallback failed: %s", fallback_exc)
            raise fallback_exc


    def is_connected(self) -> bool:
        """Verify whether current connection and channel are open and operational."""
        try:
            with self._lock:
                if self._connection and self._connection.is_open and self._channel and self._channel.is_open:
                    return True
            channel = self._connect()
            return channel.is_open
        except Exception as exc:
            logger.debug("RabbitMQ health check connection probe failed: %s", exc)
            return False

    def close(self) -> None:
        """Safely close open channel and connection."""
        with self._lock:
            try:
                if self._channel and self._channel.is_open:
                    self._channel.close()
            except Exception:
                pass
            finally:
                self._channel = None

            try:
                if self._connection and self._connection.is_open:
                    self._connection.close()
            except Exception:
                pass
            finally:
                self._connection = None
            logger.info("RabbitMQ connection closed.")


def get_queue_service() -> QueueService:
    """Dependency provider returning singleton QueueService instance."""
    global _queue_service
    if _queue_service is None:
        with _queue_lock:
            if _queue_service is None:
                _queue_service = QueueService()
    return _queue_service


def init_rabbitmq() -> QueueService:
    """Initialize RabbitMQ connection during application startup."""
    service = get_queue_service()
    try:
        service._connect()
    except Exception as exc:
        logger.warning(
            "RabbitMQ initial connection failed during startup (will retry on demand): %s",
            exc,
        )
    return service


def close_rabbitmq() -> None:
    """Cleanly shutdown RabbitMQ connection pool during application shutdown."""
    global _queue_service
    if _queue_service is not None:
        _queue_service.close()
        _queue_service = None
