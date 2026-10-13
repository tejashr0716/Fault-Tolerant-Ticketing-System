from datetime import datetime
from decimal import Decimal
from typing import Optional
from pydantic import BaseModel, Field


class PaymentConfirmationRequest(BaseModel):
    """Payment confirmation payload for finalizing a seat reservation."""

    user_id: str = Field(
        ...,
        description="Unique identifier of the user issuing the payment confirmation.",
        examples=["a0000001-0000-0000-0000-000000000001"],
    )
    show_id: str = Field(
        ...,
        description="Unique identifier for the movie screening / show.",
        examples=["f0000001-0000-0000-0000-000000000001"],
    )
    seat_number: Optional[str] = Field(
        default=None,
        description="Identifier of the seat to be confirmed and booked (e.g. A1, B4) or comma-separated list.",
        examples=["A1"],
    )
    seats: Optional[list[str]] = Field(
        default=None,
        description="Optional list of seat identifiers for multi-seat bookings (e.g. ['A1', 'A2']).",
        examples=[["A1", "A2"]],
    )
    amount: Decimal = Field(
        ...,
        gt=0,
        description="Ticket price amount to be debited from user wallet.",
        examples=[450.00],
    )
    idempotency_key: str = Field(
        ...,
        min_length=1,
        max_length=128,
        description="Idempotency key guaranteeing exact-once processing of the payment transaction.",
        examples=["PAY-BLR-20261012-001"],
    )
    booking_id: Optional[str] = Field(
        default=None,
        description="Optional booking identifier associated with the reservation.",
        examples=["9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d"],
    )


class PaymentConfirmationResponse(BaseModel):
    """Response returned when a payment confirmation is accepted and queued for background processing."""

    status: str = Field(
        default="QUEUED",
        description="Current status of the payment event processing.",
    )
    message: str = Field(
        default="Payment confirmation accepted and queued for atomic settlement.",
        description="Human-readable response message.",
    )
    idempotency_key: str = Field(
        ...,
        description="Idempotency key supplied in the request.",
    )
    user_id: str = Field(..., description="User identifier.")
    show_id: str = Field(..., description="Show identifier.")
    seat_number: str = Field(..., description="Seat identifier.")
    amount: Decimal = Field(..., description="Amount submitted for payment.")
    booking_id: Optional[str] = Field(default=None, description="Booking identifier if supplied.")
    queue: str = Field(..., description="Target RabbitMQ durable queue name.")
    queued_at: datetime = Field(..., description="UTC timestamp when the event was queued.")
