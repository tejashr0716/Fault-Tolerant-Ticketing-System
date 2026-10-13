from fastapi import APIRouter, Depends, HTTPException, status
from app.schemas.seat import (
    SeatLockRequest,
    SeatLockResponse,
    SeatStatusResponse,
)
from app.services.cache import CacheService, get_cache_service

router = APIRouter(prefix="/seats", tags=["Seat Reservations"])


@router.post(
    "/lock",
    response_model=SeatLockResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Atomically lock a movie seat",
    description=(
        "Locks a seat for a given show_id and user_id using Redis SET EX NX for 300 seconds (5 minutes). "
        "Returns 409 Conflict if the seat is already locked."
    ),
    responses={
        201: {
            "description": "Seat lock acquired successfully",
            "model": SeatLockResponse,
        },
        409: {
            "description": "Conflict - Seat is currently locked by another user",
            "content": {
                "application/json": {
                    "example": {"detail": "Seat is currently locked by another user"}
                }
            },
        },
    },
)
async def lock_seat(
    request: SeatLockRequest,
    cache_service: CacheService = Depends(get_cache_service),
) -> SeatLockResponse:
    """Acquires a 5-minute atomic lock on the specified seat."""
    import psycopg2
    from app.config import settings

    # 1. First check if seat is already permanently BOOKED in PostgreSQL
    try:
        conn = psycopg2.connect(settings.postgres_dsn)
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT status FROM show_seats
                    WHERE show_id = %s AND seat_number = %s;
                    """,
                    (request.show_id, request.seat_number),
                )
                row = cur.fetchone()
                if row and row[0] == "BOOKED":
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail=f"Seat {request.seat_number} is already booked and confirmed.",
                    )
        conn.close()
    except HTTPException:
        raise
    except Exception as db_err:
        # If DB check encounters error, log and proceed to Redis lock
        pass

    # 2. Acquire Redis 5-minute atomic lock (SET EX NX)
    acquired, expires_at, ttl = await cache_service.acquire_seat_lock(
        show_id=request.show_id,
        seat_number=request.seat_number,
        user_id=request.user_id,
    )

    if not acquired or expires_at is None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Seat is currently locked by another user",
        )

    # Broadcast real-time live event to all connected WebSocket clients
    try:
        from app.services.live_server import live_manager
        await live_manager.broadcast({
            "type": "SEAT_LOCKED",
            "show_id": request.show_id,
            "seat_number": request.seat_number,
            "user_id": request.user_id,
            "ttl": ttl,
            "expires_at": expires_at.isoformat() if hasattr(expires_at, "isoformat") else str(expires_at),
        })
    except Exception:
        pass

    return SeatLockResponse(
        show_id=request.show_id,
        seat_number=request.seat_number,
        locked_by=request.user_id,
        is_locked=True,
        expires_at=expires_at,
        ttl_seconds=ttl,
        message="Seat successfully locked.",
    )


@router.post("/unlock")
async def unlock_seat(
    request: SeatLockRequest,
    cache_service: CacheService = Depends(get_cache_service),
):
    """Releases a seat lock in Redis if held by the requesting user."""
    is_locked, locked_by, ttl_seconds, expires_at = await cache_service.get_seat_lock_status(
        show_id=request.show_id,
        seat_number=request.seat_number,
    )
    if is_locked and locked_by == request.user_id:
        await cache_service.release_seat_lock(
            show_id=request.show_id,
            seat_number=request.seat_number,
        )
        try:
            from app.services.live_server import live_manager
            await live_manager.broadcast({
                "type": "SEAT_UNLOCKED",
                "show_id": request.show_id,
                "seat_number": request.seat_number,
                "user_id": request.user_id,
            })
        except Exception:
            pass
        return {"status": "UNLOCKED", "seat_number": request.seat_number}
    return {"status": "IGNORED", "seat_number": request.seat_number}



@router.get(
    "/lock/{show_id}/{seat_number}",
    response_model=SeatStatusResponse,
    summary="Check seat lock status",
    description="Inspect whether a given seat for a show is locked, who holds the lock, and remaining TTL.",
)
async def get_seat_status(
    show_id: str,
    seat_number: str,
    cache_service: CacheService = Depends(get_cache_service),
) -> SeatStatusResponse:
    """Returns the locking status of the seat."""
    is_locked, locked_by, ttl_seconds, expires_at = (
        await cache_service.get_seat_lock_status(
            show_id=show_id,
            seat_number=seat_number,
        )
    )

    return SeatStatusResponse(
        show_id=show_id,
        seat_number=seat_number,
        is_locked=is_locked,
        locked_by=locked_by,
        ttl_seconds=ttl_seconds,
        expires_at=expires_at,
    )


@router.post("/reset/{show_id}")
async def reset_show_seats(
    show_id: str,
    cache_service: CacheService = Depends(get_cache_service),
):
    """Resets all seats for a show back to AVAILABLE in PostgreSQL and flushes Redis keys for concurrency re-testing."""
    import psycopg2
    from app.config import settings

    # 1. Clear Redis seat keys
    try:
        keys = await cache_service.redis.keys(f"seat_lock:{show_id}:*")
        if keys:
            await cache_service.redis.delete(*keys)
    except Exception:
        pass

    # 2. Reset DB show_seats
    try:
        conn = psycopg2.connect(settings.postgres_dsn)
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE show_seats SET status = 'AVAILABLE', user_id = NULL WHERE show_id = %s;",
                    (show_id,),
                )
        conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return {"status": "RESET_SUCCESS", "show_id": show_id}
