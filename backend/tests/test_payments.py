import pytest
import uuid
from unittest.mock import MagicMock
from httpx import AsyncClient, ASGITransport
from app.main import app
from app.services.queue import get_queue_service, QueueService


@pytest.mark.asyncio
async def test_post_seats_pay_success():
    """Verify that POST /seats/pay queues payment and returns 202 Accepted."""
    mock_queue = MagicMock(spec=QueueService)
    mock_queue.publish_payment_event.return_value = True

    app.dependency_overrides[get_queue_service] = lambda: mock_queue

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        payload = {
            "user_id": str(uuid.uuid4()),
            "show_id": str(uuid.uuid4()),
            "seat_number": "A1",
            "amount": 450.00,
            "idempotency_key": f"IDEM-{uuid.uuid4().hex[:8]}",
            "booking_id": str(uuid.uuid4()),
        }

        response = await client.post("/seats/pay", json=payload)
        assert response.status_code == 202
        data = response.json()
        assert data["status"] == "QUEUED"
        assert data["idempotency_key"] == payload["idempotency_key"]
        assert data["seat_number"] == "A1"
        assert float(data["amount"]) == 450.00
        assert data["queue"] == "ticket_payments"
        assert "queued_at" in data

        # Verify publisher was called with expected payload
        mock_queue.publish_payment_event.assert_called_once()
        published_event = mock_queue.publish_payment_event.call_args[0][0]
        assert published_event["event_type"] == "process_payment"
        assert published_event["user_id"] == payload["user_id"]
        assert published_event["show_id"] == payload["show_id"]
        assert published_event["seat_number"] == "A1"
        assert published_event["idempotency_key"] == payload["idempotency_key"]

    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_post_seats_pay_validation_failure():
    """Verify validation error when required payment parameters are missing."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Negative amount
        invalid_payload = {
            "user_id": str(uuid.uuid4()),
            "show_id": str(uuid.uuid4()),
            "seat_number": "A1",
            "amount": -50.0,
            "idempotency_key": "IDEM-TEST",
        }
        response = await client.post("/seats/pay", json=invalid_payload)
        assert response.status_code == 422


@pytest.mark.asyncio
async def test_post_seats_pay_queue_failure():
    """Verify 503 response if queue publisher encounters connection error."""
    mock_queue = MagicMock(spec=QueueService)
    mock_queue.publish_payment_event.side_effect = RuntimeError("RabbitMQ cluster unavailable")

    app.dependency_overrides[get_queue_service] = lambda: mock_queue

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        payload = {
            "user_id": str(uuid.uuid4()),
            "show_id": str(uuid.uuid4()),
            "seat_number": "B2",
            "amount": 350.00,
            "idempotency_key": f"IDEM-{uuid.uuid4().hex[:8]}",
        }

        response = await client.post("/seats/pay", json=payload)
        assert response.status_code == 503
        assert "temporarily unavailable" in response.json()["detail"]

    app.dependency_overrides.clear()
