import json
import uuid
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
import pika

from worker import (
    PaymentWorker,
    InsufficientFundsError,
    WalletNotFoundError,
)


@pytest.fixture
def mock_worker():
    """Instantiate a PaymentWorker with mocked networking dependencies."""
    worker = PaymentWorker(
        amqp_url="amqp://guest:guest@localhost:5672/",
        postgres_dsn="postgresql://postgres:postgres@localhost:5432/ticketing_db",
        redis_url="redis://localhost:6379/0",
    )
    worker._redis_client = MagicMock()
    mock_conn = MagicMock()
    mock_conn.closed = False
    worker._pg_conn = mock_conn
    return worker


def test_worker_idempotency(mock_worker):
    """Verify that an already processed idempotency_key skips deduction and returns ALREADY_PROCESSED."""
    mock_conn = MagicMock()
    mock_conn.closed = False
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    # Simulate existing transaction found
    mock_cursor.fetchone.return_value = {
        "id": "tx-12345",
        "status": "SUCCESS",
        "balance_before": Decimal("500.00"),
        "balance_after": Decimal("400.00"),
    }

    mock_worker._pg_conn = mock_conn

    res = mock_worker.execute_payment_transaction(
        user_id="usr-1",
        show_id="show-1",
        seat_number="A1",
        amount=Decimal("100.00"),
        idempotency_key="IDEM-ALREADY-DONE",
        booking_id="bkg-1",
    )

    assert res["status"] == "ALREADY_PROCESSED"
    assert res["transaction_id"] == "tx-12345"


def test_worker_insufficient_funds_raises(mock_worker):
    """Verify that InsufficientFundsError is raised when wallet balance < ticket price."""
    mock_conn = MagicMock()
    mock_conn.closed = False
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    # 1. First fetchone for idempotency check -> None
    # 2. Second fetchone for wallet -> balance of 50.00
    # 2b. Third fetchone for show_seats double-booking check -> AVAILABLE
    mock_cursor.fetchone.side_effect = [
        None,
        {"id": "w-1", "balance": Decimal("50.00"), "version": 1},
        {"status": "AVAILABLE", "user_id": None},
    ]

    mock_worker._pg_conn = mock_conn

    with pytest.raises(InsufficientFundsError) as exc_info:
        mock_worker.execute_payment_transaction(
            user_id="usr-1",
            show_id="show-1",
            seat_number="A1",
            amount=Decimal("150.00"),
            idempotency_key="IDEM-LOW-BALANCE",
            booking_id="bkg-1",
        )

    assert "Insufficient wallet funds" in str(exc_info.value)


def test_worker_successful_transaction(mock_worker):
    """Verify successful deduction, audit log insertion, booking confirmation."""
    mock_conn = MagicMock()
    mock_conn.closed = False
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    # 1. Idempotency -> None
    # 2. Wallet select -> balance 500.00
    # 2b. Seat check -> AVAILABLE
    # 3. Bookings insert/update -> returning id
    # 4. wallet_transactions insert -> returning id
    mock_cursor.fetchone.side_effect = [
        None,
        {"id": "w-1", "balance": Decimal("500.00"), "version": 1},
        {"status": "AVAILABLE", "user_id": None},
        {"id": "b-1"},
        {"id": "tx-new-999"},
    ]
    mock_cursor.rowcount = 1

    mock_worker._pg_conn = mock_conn

    res = mock_worker.execute_payment_transaction(
        user_id="usr-1",
        show_id="show-1",
        seat_number="A1",
        amount=Decimal("150.00"),
        idempotency_key="IDEM-SUCCESS",
        booking_id="bkg-1",
    )

    assert res["status"] == "SUCCESS"
    assert res["transaction_id"] == "tx-new-999"
    assert res["balance_after"] == Decimal("350.00")


def test_on_message_success_flow(mock_worker):
    """Verify message processing, Redis lock deletion, and basic_ack on success."""
    mock_channel = MagicMock()
    method = MagicMock()
    method.delivery_tag = 42
    properties = MagicMock()

    body = json.dumps({
        "event_type": "process_payment",
        "user_id": "usr-1",
        "show_id": "show-1",
        "seat_number": "A1",
        "amount": 250.0,
        "idempotency_key": "IDEM-TEST-ACK",
        "booking_id": "bkg-1",
    }).encode("utf-8")

    with patch.object(mock_worker, "execute_payment_transaction") as mock_exec, \
         patch.object(mock_worker, "release_redis_lock") as mock_release_lock:
        
        mock_exec.return_value = {"status": "SUCCESS"}

        mock_worker.on_message(mock_channel, method, properties, body)

        mock_exec.assert_called_once_with(
            user_id="usr-1",
            show_id="show-1",
            seat_number="A1",
            amount=Decimal("250.0"),
            idempotency_key="IDEM-TEST-ACK",
            booking_id="bkg-1",
        )
        mock_release_lock.assert_called_once_with(show_id="show-1", seat_number="A1")
        mock_channel.basic_ack.assert_called_once_with(delivery_tag=42)


def test_on_message_insufficient_funds_flow(mock_worker):
    """Verify that insufficient funds triggers DLQ forwarding, lock release, failure record, and ack."""
    mock_channel = MagicMock()
    method = MagicMock()
    method.delivery_tag = 101
    properties = MagicMock()
    properties.headers = {}

    body = json.dumps({
        "event_type": "process_payment",
        "user_id": "usr-broke",
        "show_id": "show-1",
        "seat_number": "A1",
        "amount": 5000.0,
        "idempotency_key": "IDEM-BROKE",
        "booking_id": "bkg-broke",
    }).encode("utf-8")

    with patch.object(mock_worker, "execute_payment_transaction", side_effect=InsufficientFundsError("Insufficient funds")), \
         patch.object(mock_worker, "record_booking_failure") as mock_rec_fail, \
         patch.object(mock_worker, "release_redis_lock") as mock_release_lock, \
         patch.object(mock_worker, "send_to_dlq") as mock_send_dlq:

        mock_worker.on_message(mock_channel, method, properties, body)

        mock_rec_fail.assert_called_once_with(
            booking_id="bkg-broke",
            show_id="show-1",
            seat_number="A1",
            reason="Insufficient funds",
        )
        mock_release_lock.assert_called_once_with(show_id="show-1", seat_number="A1")
        mock_send_dlq.assert_called_once()
        mock_channel.basic_ack.assert_called_once_with(delivery_tag=101)
