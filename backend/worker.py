#!/usr/bin/env python3
"""
Background Consumer Worker for Fault-Tolerant Ticketing System.

Listens to the durable RabbitMQ `ticket_payments` queue and processes payment events:
1. Idempotently checks if the transaction was already completed.
2. Executes an atomic PostgreSQL transaction:
   - Locks wallet row (SELECT ... FOR UPDATE)
   - Checks balance >= amount
   - Deducts amount from wallets table
   - Records audit log in wallet_transactions table
   - Confirms booking status in bookings and show_seats tables
3. On success:
   - Deletes Redis seat lock key (seat_lock:{show_id}:{seat_number})
   - Acknowledges message (basic_ack)
4. On failure / insufficient funds:
   - Rolls back financial transaction
   - Marks booking as FAILED
   - Deletes Redis seat lock key
   - Routes failed event to dead-letter queue (ticket_payments_dlq)
   - Acknowledges or rejects message (requeue=False)
"""

import json
import logging
import os
import signal
import sys
import time
import uuid
from decimal import Decimal
from typing import Any, Dict, Optional

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import AMQPConnectionError, AMQPChannelError
import psycopg2
from psycopg2.extras import DictCursor
import redis

# Add parent directory to sys.path to allow `app.config` imports when executed directly
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

from app.config import settings

# Configure logging
logging.basicConfig(
    level=logging.INFO if not settings.DEBUG else logging.DEBUG,
    format="%(asctime)s [%(levelname)s] [Worker] %(message)s",
)
logger = logging.getLogger("PaymentWorker")


class PaymentProcessingError(Exception):
    """Base exception for payment processing errors."""
    pass


class InsufficientFundsError(PaymentProcessingError):
    """Raised when user wallet balance is lower than ticket cost."""
    pass


class WalletNotFoundError(PaymentProcessingError):
    """Raised when no wallet is registered for the specified user."""
    pass


class DoubleBookingAttemptError(PaymentProcessingError):
    """Raised when seat has already been booked by another user."""
    pass



class PaymentWorker:
    """Consumer worker processing payment transactions from RabbitMQ."""

    def __init__(
        self,
        amqp_url: Optional[str] = None,
        postgres_dsn: Optional[str] = None,
        redis_url: Optional[str] = None,
        queue_name: Optional[str] = None,
        dlx_name: Optional[str] = None,
        dlq_name: Optional[str] = None,
    ):
        self.amqp_url = amqp_url or settings.rabbitmq_dsn
        self.postgres_dsn = postgres_dsn or settings.postgres_dsn
        self.redis_url = redis_url or settings.redis_dsn
        self.queue_name = queue_name or settings.RABBITMQ_QUEUE
        self.dlx_name = dlx_name or settings.RABBITMQ_DLX
        self.dlq_name = dlq_name or settings.RABBITMQ_DLQ

        self._rmq_connection: Optional[pika.BlockingConnection] = None
        self._rmq_channel: Optional[BlockingChannel] = None
        self._pg_conn: Optional[psycopg2.extensions.connection] = None
        self._redis_client: Optional[redis.Redis] = None
        self._is_running = False

    def init_redis(self) -> redis.Redis:
        """Initialize synchronous Redis client for lock deletion."""
        if self._redis_client is None:
            logger.info("Connecting to Redis at %s...", self.redis_url)
            self._redis_client = redis.Redis.from_url(
                self.redis_url,
                decode_responses=True,
                socket_connect_timeout=5,
            )
            self._redis_client.ping()
            logger.info("Successfully connected to Redis.")
        return self._redis_client

    def get_postgres_connection(self) -> psycopg2.extensions.connection:
        """Get or reconnect to PostgreSQL database."""
        if self._pg_conn is None or self._pg_conn.closed:
            logger.info("Connecting to PostgreSQL at %s...", self.postgres_dsn)
            self._pg_conn = psycopg2.connect(
                self.postgres_dsn,
                cursor_factory=DictCursor,
            )
            self._pg_conn.autocommit = False
            logger.info("Successfully connected to PostgreSQL.")
        return self._pg_conn

    def connect_rabbitmq(self) -> BlockingChannel:
        """Connect to RabbitMQ and declare processing queue and dead-letter queue."""
        logger.info("Connecting to RabbitMQ at %s...", self.amqp_url)
        params = pika.URLParameters(self.amqp_url)
        params.heartbeat = 60
        params.blocked_connection_timeout = 300

        self._rmq_connection = pika.BlockingConnection(params)
        self._rmq_channel = self._rmq_connection.channel()

        # Prefetch 1 message to ensure fair dispatching and sequential ACID safety
        self._rmq_channel.basic_qos(prefetch_count=1)

        # 1. Declare Dead-Letter Exchange (DLX)
        self._rmq_channel.exchange_declare(
            exchange=self.dlx_name,
            exchange_type="direct",
            durable=True,
        )

        # 2. Declare Dead-Letter Queue (DLQ)
        self._rmq_channel.queue_declare(
            queue=self.dlq_name,
            durable=True,
        )

        # 3. Bind DLQ to DLX
        self._rmq_channel.queue_bind(
            queue=self.dlq_name,
            exchange=self.dlx_name,
            routing_key=self.dlq_name,
        )

        # 4. Declare Main Queue with dead-letter configuration
        queue_args = {
            "x-dead-letter-exchange": self.dlx_name,
            "x-dead-letter-routing-key": self.dlq_name,
        }
        try:
            self._rmq_channel.queue_declare(
                queue=self.queue_name,
                durable=True,
                arguments=queue_args,
            )
        except AMQPChannelError as ce:
            logger.warning(
                "Queue %s exists with differing configuration; reopening channel: %s",
                self.queue_name,
                ce,
            )
            self._rmq_channel = self._rmq_connection.channel()
            self._rmq_channel.basic_qos(prefetch_count=1)
            self._rmq_channel.queue_declare(queue=self.queue_name, durable=True)

        logger.info(
            "RabbitMQ connected. Consuming from queue '%s' with DLQ '%s'.",
            self.queue_name,
            self.dlq_name,
        )
        return self._rmq_channel

    def release_redis_lock(self, show_id: str, seat_number: str) -> None:
        """Immediately delete the Redis lock for the confirmed/handled seat."""
        lock_key = f"seat_lock:{show_id}:{seat_number}"
        try:
            r = self.init_redis()
            deleted = r.delete(lock_key)
            if deleted:
                logger.info("Successfully deleted Redis lock key: %s", lock_key)
            else:
                logger.debug("Redis lock key %s already released or expired.", lock_key)
        except Exception as exc:
            logger.warning("Failed to delete Redis lock %s: %s", lock_key, exc)

    def send_to_dlq(
        self,
        ch: BlockingChannel,
        original_body: bytes,
        failure_reason: str,
        properties: Optional[pika.BasicProperties] = None,
    ) -> None:
        """Publish failed payment message to dead-letter queue with failure diagnostics."""
        try:
            headers = properties.headers.copy() if (properties and properties.headers) else {}
            headers.update({
                "x-failure-reason": failure_reason,
                "x-failed-at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "x-original-queue": self.queue_name,
            })

            dlq_properties = pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent,
                content_type="application/json",
                content_encoding="utf-8",
                correlation_id=properties.correlation_id if properties else None,
                message_id=str(uuid.uuid4()),
                headers=headers,
            )

            ch.basic_publish(
                exchange=self.dlx_name,
                routing_key=self.dlq_name,
                body=original_body,
                properties=dlq_properties,
                mandatory=True,
            )
            logger.info("Routed message to Dead-Letter Queue '%s'. Reason: %s", self.dlq_name, failure_reason)
        except Exception as exc:
            logger.exception("Failed to publish message directly to DLQ: %s", exc)

    def record_booking_failure(
        self,
        booking_id: Optional[str],
        show_id: str,
        seat_number: str,
        reason: str,
    ) -> None:
        """Record failure status in PostgreSQL bookings table and release any lingering seat holds."""
        try:
            conn = self.get_postgres_connection()
            with conn:
                    valid_booking_uuid = None
                    if booking_id:
                        try:
                            valid_booking_uuid = str(uuid.UUID(str(booking_id)))
                        except Exception:
                            valid_booking_uuid = None

                    if valid_booking_uuid:
                        cur.execute(
                            """
                            UPDATE bookings
                            SET status = 'FAILED',
                                updated_at = CURRENT_TIMESTAMP
                            WHERE id = %s;
                            """,
                            (valid_booking_uuid,),
                        )
                        logger.info("Marked booking %s as FAILED.", valid_booking_uuid)


                    # Release seat hold back to AVAILABLE if it was HELD
                    cur.execute(
                        """
                        UPDATE show_seats
                        SET status = 'AVAILABLE',
                            user_id = NULL,
                            held_at = NULL,
                            hold_expires_at = NULL,
                            version = version + 1,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE show_id = %s
                          AND seat_number = %s
                          AND status = 'HELD';
                        """,
                        (show_id, seat_number),
                    )
        except Exception as exc:
            logger.warning("Could not record booking failure status in PostgreSQL: %s", exc)

    def execute_payment_transaction(
        self,
        user_id: str,
        show_id: str,
        seat_number: str,
        amount: Decimal,
        idempotency_key: str,
        booking_id: Optional[str],
    ) -> Dict[str, Any]:
        """
        Execute atomic PostgreSQL transaction:
        1. Lock wallet row with SELECT ... FOR UPDATE.
        2. Deduct amount from wallets balance.
        3. Insert audit record into wallet_transactions.
        4. Confirm booking in bookings.
        5. Confirm seat allocation in show_seats.
        """
        conn = self.get_postgres_connection()

        # Python psycopg2 connection context manager:
        # `with conn:` commits on normal exit, rolls back on exception.
        with conn:
            with conn.cursor() as cur:
                # ---------------------------------------------------------
                # 1. IDEMPOTENCY CHECK
                # ---------------------------------------------------------
                cur.execute(
                    """
                    SELECT id, status, balance_before, balance_after
                    FROM wallet_transactions
                    WHERE idempotency_key = %s;
                    """,
                    (idempotency_key,),
                )
                existing_tx = cur.fetchone()
                if existing_tx:
                    logger.info(
                        "Idempotency hit: Transaction already executed for idempotency_key=%s (tx_id=%s, status=%s).",
                        idempotency_key,
                        existing_tx["id"],
                        existing_tx["status"],
                    )
                    return {
                        "status": "ALREADY_PROCESSED",
                        "transaction_id": existing_tx["id"],
                        "booking_id": booking_id,
                    }

                # ---------------------------------------------------------
                # 2. PESSIMISTIC LOCK ON WALLET ROW
                # ---------------------------------------------------------
                cur.execute(
                    """
                    SELECT id, balance, version
                    FROM wallets
                    WHERE user_id = %s
                    FOR UPDATE;
                    """,
                    (user_id,),
                )
                wallet = cur.fetchone()
                if not wallet:
                    raise WalletNotFoundError(f"No wallet found for user_id={user_id}")

                wallet_id = wallet["id"]
                current_balance = Decimal(str(wallet["balance"]))

                # ---------------------------------------------------------
                # 2b. DOUBLE-BOOKING CHECK (ACID Row Lock)
                # ---------------------------------------------------------
                if isinstance(seat_number, list):
                    seats_list = [str(s).strip() for s in seat_number if str(s).strip()]
                elif isinstance(seat_number, str):
                    seats_list = [s.strip() for s in seat_number.split(",") if s.strip()]
                else:
                    seats_list = ["A1"]

                if not seats_list:
                    seats_list = ["A1"]

                for s in seats_list:
                    cur.execute(
                        """
                        SELECT status, user_id
                        FROM show_seats
                        WHERE show_id = %s AND seat_number = %s
                        FOR UPDATE;
                        """,
                        (show_id, s),
                    )
                    seat_row = cur.fetchone()
                    if seat_row and seat_row["status"] == "BOOKED" and str(seat_row["user_id"]) != str(user_id):
                        raise DoubleBookingAttemptError(
                            f"Seat {s} is already booked by another customer."
                        )


                # ---------------------------------------------------------
                # 3. VERIFY SUFFICIENT BALANCE
                # ---------------------------------------------------------
                if current_balance < amount:
                    raise InsufficientFundsError(
                        f"Insufficient wallet funds. Required: {amount}, Available: {current_balance}"
                    )

                new_balance = current_balance - amount

                # ---------------------------------------------------------
                # 4. DEDUCT TICKET PRICE FROM WALLETS
                # ---------------------------------------------------------
                cur.execute(
                    """
                    UPDATE wallets
                    SET balance = %s,
                        version = version + 1,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s;
                    """,
                    (new_balance, wallet_id),
                )

                # ---------------------------------------------------------
                # 5. CONFIRM / CREATE BOOKING RECORD
                # ---------------------------------------------------------
                confirmed_booking_id = None
                if booking_id:
                    try:
                        valid_uuid = str(uuid.UUID(str(booking_id)))
                    except Exception:
                        valid_uuid = None

                    if valid_uuid:
                        cur.execute(
                            """
                            UPDATE bookings
                            SET status = 'CONFIRMED',
                                confirmed_at = CURRENT_TIMESTAMP,
                                payment_method = 'WALLET',
                                idempotency_key = COALESCE(bookings.idempotency_key, %s),
                                updated_at = CURRENT_TIMESTAMP
                            WHERE id = %s
                            RETURNING id;
                            """,
                            (idempotency_key, valid_uuid),
                        )
                        b_upd = cur.fetchone()
                        if b_upd:
                            confirmed_booking_id = b_upd["id"]

                if not confirmed_booking_id:
                    # Create confirmed booking record if not pre-created
                    booking_ref = f"BLR-{uuid.uuid4().hex[:10].upper()}"
                    cur.execute(
                        """
                        INSERT INTO bookings (
                            booking_reference,
                            user_id,
                            show_id,
                            seat_count,
                            ticket_amount,
                            total_amount,
                            status,
                            idempotency_key,
                            payment_method,
                            confirmed_at
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, 'CONFIRMED', %s, 'WALLET', CURRENT_TIMESTAMP
                        )
                        RETURNING id;
                        """,
                        (
                            booking_ref,
                            user_id,
                            show_id,
                            len(seats_list),
                            amount,
                            amount,
                            idempotency_key,
                        ),
                    )
                    b_row = cur.fetchone()
                    if b_row:
                        confirmed_booking_id = b_row["id"]

                # ---------------------------------------------------------
                # 6. LOG AUDIT ENTRY IN WALLET_TRANSACTIONS
                # ---------------------------------------------------------
                ref_number = f"TX-PAY-{uuid.uuid4().hex[:12].upper()}"
                seats_joined = ", ".join(seats_list)
                cur.execute(
                    """
                    INSERT INTO wallet_transactions (
                        wallet_id,
                        user_id,
                        transaction_type,
                        amount,
                        balance_before,
                        balance_after,
                        booking_id,
                        idempotency_key,
                        reference_number,
                        status,
                        description
                    ) VALUES (
                        %s, %s, 'BOOKING_PAYMENT', %s, %s, %s, %s, %s, %s, 'SUCCESS', %s
                    )
                    RETURNING id;
                    """,
                    (
                        wallet_id,
                        user_id,
                        -amount,
                        current_balance,
                        new_balance,
                        confirmed_booking_id,
                        idempotency_key,
                        ref_number,
                        f"Movie ticket payment for show {show_id}, seats {seats_joined}",
                    ),
                )
                tx_row = cur.fetchone()
                tx_id = tx_row["id"] if tx_row else None

                # ---------------------------------------------------------
                # 7. CONFIRM SEAT STATUS IN SHOW_SEATS FOR ALL SEATS
                # ---------------------------------------------------------
                unit_price = amount / Decimal(str(len(seats_list)))
                for s in seats_list:
                    cur.execute(
                        """
                        UPDATE show_seats
                        SET status = 'BOOKED',
                            booking_id = %s,
                            user_id = %s,
                            hold_expires_at = NULL,
                            version = version + 1,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE show_id = %s
                          AND seat_number = %s;
                        """,
                        (confirmed_booking_id, user_id, show_id, s),
                    )

                    if cur.rowcount == 0:
                        cur.execute(
                            """
                            INSERT INTO show_seats (
                                show_id,
                                seat_id,
                                seat_number,
                                booking_id,
                                user_id,
                                price,
                                status
                            )
                            SELECT
                                sh.id,
                                st.id,
                                %s,
                                %s,
                                %s,
                                %s,
                                'BOOKED'
                            FROM shows sh
                            JOIN screens sc ON sh.screen_id = sc.id
                            JOIN seats st ON st.screen_id = sc.id AND st.seat_number = %s
                            WHERE sh.id = %s
                            ON CONFLICT (show_id, seat_number) WHERE (status != 'CANCELLED')
                            DO UPDATE SET
                                status = 'BOOKED',
                                booking_id = EXCLUDED.booking_id,
                                user_id = EXCLUDED.user_id,
                                hold_expires_at = NULL,
                                version = show_seats.version + 1,
                                updated_at = CURRENT_TIMESTAMP;
                            """,
                            (
                                s,
                                confirmed_booking_id,
                                user_id,
                                unit_price,
                                s,
                                show_id,
                            ),
                        )

                logger.info(
                    "Successfully committed atomic payment: tx_id=%s, booking_id=%s, new_balance=%s",
                    tx_id,
                    confirmed_booking_id,
                    new_balance,
                )

                # Broadcast live event to all connected WebSocket clients
                try:
                    import urllib.request, json
                    req_payload = json.dumps({
                        "type": "SEAT_BOOKED",
                        "show_id": show_id,
                        "seats": seats_list,
                        "user_id": user_id,
                        "booking_id": str(confirmed_booking_id)
                    }).encode("utf-8")
                    req = urllib.request.Request(
                        "http://localhost:8000/api/live/broadcast",
                        data=req_payload,
                        headers={"Content-Type": "application/json"}
                    )
                    urllib.request.urlopen(req, timeout=1.0)
                except Exception:
                    pass

                return {
                    "status": "SUCCESS",
                    "transaction_id": tx_id,
                    "booking_id": confirmed_booking_id,
                    "balance_after": new_balance,
                }

    def on_message(
        self,
        ch: BlockingChannel,
        method: pika.spec.Basic.Deliver,
        properties: pika.spec.BasicProperties,
        body: bytes,
    ) -> None:
        """Callback handling consumption of payment events from RabbitMQ."""
        delivery_tag = method.delivery_tag
        logger.info("Received message delivery_tag=%s", delivery_tag)

        user_id = None
        show_id = None
        seat_number = None
        booking_id = None
        idempotency_key = None

        seats_to_release = []

        try:
            payload: Dict[str, Any] = json.loads(body.decode("utf-8"))
            user_id = payload.get("user_id")
            show_id = payload.get("show_id")
            seat_number = payload.get("seats") or payload.get("seat_number")
            amount = Decimal(str(payload.get("amount", "0")))
            idempotency_key = payload.get("idempotency_key")
            booking_id = payload.get("booking_id")

            if not (user_id and show_id and seat_number and amount > 0 and idempotency_key):
                raise ValueError(
                    f"Malformed payment event payload: {payload}. Missing required fields."
                )

            if isinstance(seat_number, list):
                seats_to_release = [str(s).strip() for s in seat_number if str(s).strip()]
            elif isinstance(seat_number, str):
                seats_to_release = [s.strip() for s in seat_number.split(",") if s.strip()]
            else:
                seats_to_release = ["A1"]

            # Execute atomic database transaction
            result = self.execute_payment_transaction(
                user_id=user_id,
                show_id=show_id,
                seat_number=payload.get("seat_number") or seats_to_release,
                amount=amount,
                idempotency_key=idempotency_key,
                booking_id=booking_id,
            )

            # On successful transaction:
            # 1. Immediately delete the Redis lock for all seats
            for s in seats_to_release:
                self.release_redis_lock(show_id=show_id, seat_number=s)

            # 2. Acknowledge message from queue
            ch.basic_ack(delivery_tag=delivery_tag)
            logger.info(
                "Awaited payment settlement complete: idempotency_key=%s, status=%s",
                idempotency_key,
                result["status"],
            )

        except (InsufficientFundsError, WalletNotFoundError, DoubleBookingAttemptError) as client_err:
            logger.error("Financial processing failed: %s", client_err)
            if show_id and seats_to_release:
                for s in seats_to_release:
                    self.record_booking_failure(
                        booking_id=booking_id,
                        show_id=show_id,
                        seat_number=s,
                        reason=str(client_err),
                    )
                    self.release_redis_lock(show_id=show_id, seat_number=s)

            # Send to dead-letter queue and acknowledge from original queue
            self.send_to_dlq(
                ch=ch,
                original_body=body,
                failure_reason=str(client_err),
                properties=properties,
            )
            ch.basic_ack(delivery_tag=delivery_tag)

        except Exception as unhandled_exc:
            logger.exception("Unexpected error processing payment event: %s", unhandled_exc)
            if show_id and seats_to_release:
                for s in seats_to_release:
                    self.record_booking_failure(
                        booking_id=booking_id,
                        show_id=show_id,
                        seat_number=s,
                        reason=str(unhandled_exc),
                    )
                    self.release_redis_lock(show_id=show_id, seat_number=s)

            # Forward to DLQ and ack or nack
            try:
                self.send_to_dlq(
                    ch=ch,
                    original_body=body,
                    failure_reason=str(unhandled_exc),
                    properties=properties,
                )
                ch.basic_ack(delivery_tag=delivery_tag)
            except Exception:
                # If DLQ forwarding fails, reject without requeue (triggers broker-level DLX)
                ch.basic_nack(delivery_tag=delivery_tag, requeue=False)

    def start(self) -> None:
        """Start consumer loop with automatic reconnection resilience."""
        self._is_running = True
        logger.info("Starting Payment Worker consumer daemon...")

        while self._is_running:
            try:
                channel = self.connect_rabbitmq()
                self.init_redis()
                self.get_postgres_connection()

                channel.basic_consume(
                    queue=self.queue_name,
                    on_message_callback=self.on_message,
                    auto_ack=False,
                )
                logger.info("Worker is actively listening for payment events on '%s'...", self.queue_name)
                channel.start_consuming()

            except (AMQPConnectionError, psycopg2.OperationalError) as conn_err:
                if not self._is_running:
                    break
                logger.warning(
                    "Connection lost (%s). Retrying in 5 seconds...",
                    conn_err,
                )
                self.stop_connections()
                time.sleep(5)
            except Exception as exc:
                if not self._is_running:
                    break
                logger.exception("Worker loop encountered unexpected exception: %s. Retrying...", exc)
                self.stop_connections()
                time.sleep(5)

    def stop_connections(self) -> None:
        """Safely close active network connections."""
        try:
            if self._rmq_channel and self._rmq_channel.is_open:
                self._rmq_channel.stop_consuming()
        except Exception:
            pass

        try:
            if self._rmq_connection and self._rmq_connection.is_open:
                self._rmq_connection.close()
        except Exception:
            pass
        finally:
            self._rmq_channel = None
            self._rmq_connection = None

        try:
            if self._pg_conn and not self._pg_conn.closed:
                self._pg_conn.close()
        except Exception:
            pass
        finally:
            self._pg_conn = None

        try:
            if self._redis_client:
                self._redis_client.close()
        except Exception:
            pass
        finally:
            self._redis_client = None

    def stop(self) -> None:
        """Signal worker to stop consuming and shut down gracefully."""
        logger.info("Stopping Payment Worker...")
        self._is_running = False
        self.stop_connections()
        logger.info("Payment Worker shut down cleanly.")


def main():
    """Main execution entrypoint for worker daemon."""
    worker = PaymentWorker()

    def handle_shutdown(signum, frame):
        logger.info("Received termination signal (%s). Shutting down worker...", signum)
        worker.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    worker.start()


if __name__ == "__main__":
    main()
