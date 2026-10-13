import logging
from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException, status

from app.config import settings
from app.schemas.payment import (
    PaymentConfirmationRequest,
    PaymentConfirmationResponse,
)
from app.services.queue import QueueService, get_queue_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/seats", tags=["Payments"])


@router.post(
    "/pay",
    response_model=PaymentConfirmationResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Submit payment confirmation for a seat reservation",
    description=(
        "Receives a payment confirmation for a held seat, validates payload, and publishes "
        "a persistent `process_payment` event to the durable RabbitMQ `ticket_payments` queue "
        "for background atomic transaction settlement."
    ),
    responses={
        202: {
            "description": "Payment confirmation accepted and queued for atomic settlement",
            "model": PaymentConfirmationResponse,
        },
        422: {
            "description": "Validation Error - Invalid payment parameters",
        },
        503: {
            "description": "Service Unavailable - Message queue broker temporarily unreachable",
            "content": {
                "application/json": {
                    "example": {"detail": "Payment processing queue is temporarily unavailable."}
                }
            },
        },
    },
)
async def process_seat_payment(
    request: PaymentConfirmationRequest,
    queue_service: QueueService = Depends(get_queue_service),
) -> PaymentConfirmationResponse:
    """Accepts payment confirmation and enqueues event for atomic processing by background worker."""
    queued_at = datetime.now(timezone.utc)

    # Normalize seat numbers for multi-seat support
    seat_list: list[str] = []
    if request.seats:
        seat_list = [s.strip() for s in request.seats if s.strip()]
    elif request.seat_number:
        seat_list = [s.strip() for s in request.seat_number.split(",") if s.strip()]

    if not seat_list:
        seat_list = ["A1"]

    seat_str = ", ".join(seat_list)

    event_payload = {
        "event_type": "process_payment",
        "user_id": request.user_id,
        "show_id": request.show_id,
        "seat_number": seat_str,
        "seats": seat_list,
        "amount": float(request.amount),
        "idempotency_key": request.idempotency_key,
        "booking_id": request.booking_id,
        "timestamp": queued_at.isoformat(),
    }

    try:
        queue_service.publish_payment_event(event_payload)
    except Exception as exc:
        logger.error(
            "Failed to enqueue payment event for idempotency_key=%s: %s",
            request.idempotency_key,
            exc,
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Payment processing queue is temporarily unavailable. Please retry shortly.",
        )

    return PaymentConfirmationResponse(
        status="QUEUED",
        message="Payment confirmation received and queued for atomic settlement.",
        idempotency_key=request.idempotency_key,
        user_id=request.user_id,
        show_id=request.show_id,
        seat_number=seat_str,
        amount=request.amount,
        booking_id=request.booking_id,
        queue=settings.RABBITMQ_QUEUE,
        queued_at=queued_at,
    )
