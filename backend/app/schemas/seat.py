from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field


class SeatLockRequest(BaseModel):
    """Payload to request an atomic lock on a specific seat for a show."""

    show_id: str = Field(
        ...,
        description="Unique identifier for the movie screening / show.",
        examples=["show-101", "c0a8012e-8d54-4f2a-bcf3-d14ef9c2b9a7"],
    )
    seat_number: str = Field(
        ...,
        description="Identifier of the seat to lock (e.g. A1, B12).",
        examples=["A12", "F5"],
    )
    user_id: str = Field(
        ...,
        description="Identifier of the user attempting to lock the seat.",
        examples=["user-501", "usr_99812"],
    )


class SeatLockResponse(BaseModel):
    """Response returned upon successfully acquiring an atomic seat lock."""

    show_id: str = Field(..., description="The show identifier.")
    seat_number: str = Field(..., description="The locked seat identifier.")
    locked_by: str = Field(..., description="User ID that owns this lock.")
    is_locked: bool = Field(True, description="Lock status indicator.")
    expires_at: datetime = Field(
        ...,
        description="ISO 8601 UTC timestamp when the lock will expire.",
    )
    ttl_seconds: int = Field(
        ...,
        description="Remaining time-to-live in seconds for this lock.",
    )
    message: str = Field(
        default="Seat successfully locked.",
        description="Status message.",
    )


class SeatStatusResponse(BaseModel):
    """Status details for a specific seat lock."""

    show_id: str = Field(..., description="The show identifier.")
    seat_number: str = Field(..., description="The seat identifier.")
    is_locked: bool = Field(..., description="Whether the seat is currently locked.")
    locked_by: Optional[str] = Field(
        default=None,
        description="User ID holding the lock if currently locked.",
    )
    ttl_seconds: Optional[int] = Field(
        default=None,
        description="Remaining lock TTL in seconds if currently locked.",
    )
    expires_at: Optional[datetime] = Field(
        default=None,
        description="Lock expiration timestamp if locked.",
    )
