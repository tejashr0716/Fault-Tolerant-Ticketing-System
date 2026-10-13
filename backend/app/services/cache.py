import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple
import redis.asyncio as aioredis
from redis.asyncio import Redis

from app.config import settings

logger = logging.getLogger(__name__)

import asyncio

# Global Redis client and event loop references
_redis_client: Optional[Redis] = None
_redis_loop: Optional[asyncio.AbstractEventLoop] = None


async def init_redis() -> Redis:
    """Initialize Redis connection pool during application startup."""
    global _redis_client, _redis_loop
    try:
        current_loop = asyncio.get_running_loop()
    except RuntimeError:
        current_loop = None

    if _redis_client is None or (_redis_loop is not None and (_redis_loop.is_closed() or _redis_loop is not current_loop)):
        logger.info(f"Connecting to Redis at {settings.redis_dsn}...")
        _redis_client = aioredis.from_url(
            settings.redis_dsn,
            encoding="utf-8",
            decode_responses=True,
            socket_connect_timeout=5,
        )
        _redis_loop = current_loop
        try:
            await _redis_client.ping()
            logger.info("Successfully connected to Redis.")
        except Exception as exc:
            logger.warning(
                f"Redis ping failed during startup (service will retry on request): {exc}"
            )
    return _redis_client


async def close_redis() -> None:
    """Gracefully close Redis connections during application shutdown."""
    global _redis_client, _redis_loop
    if _redis_client is not None:
        logger.info("Closing Redis connection pool...")
        try:
            if hasattr(_redis_client, "aclose"):
                await _redis_client.aclose()
            else:
                await _redis_client.close()
        except Exception as exc:
            logger.debug(f"Error closing redis connection pool: {exc}")
        _redis_client = None
        _redis_loop = None
        logger.info("Redis connection pool closed.")


async def get_redis_client() -> Redis:
    """Dependency provider for FastAPI routes to access the Redis client."""
    global _redis_client, _redis_loop
    try:
        current_loop = asyncio.get_running_loop()
    except RuntimeError:
        current_loop = None

    if (
        _redis_client is None
        or _redis_loop is None
        or _redis_loop.is_closed()
        or _redis_loop is not current_loop
    ):
        _redis_client = aioredis.from_url(
            settings.redis_dsn,
            encoding="utf-8",
            decode_responses=True,
            socket_connect_timeout=5,
        )
        _redis_loop = current_loop
    return _redis_client


class CacheService:
    """Service class handling seat locking operations backed by Redis."""

    def __init__(self, redis: Redis):
        self.redis = redis

    @staticmethod
    def format_seat_lock_key(show_id: str, seat_number: str) -> str:
        """Construct the Redis key according to standard naming conventions: seat_lock:{show_id}:{seat_number}."""
        return f"seat_lock:{show_id}:{seat_number}"

    async def acquire_seat_lock(
        self,
        show_id: str,
        seat_number: str,
        user_id: str,
        ttl_seconds: Optional[int] = None,
    ) -> Tuple[bool, Optional[datetime], int]:
        """
        Atomically lock a seat for a specific show using Redis `SET key user_id EX <ttl> NX`.

        Returns:
            Tuple[bool, Optional[datetime], int]:
                - acquired: True if lock acquired, False if key already exists.
                - expires_at: datetime object when the lock expires (None if failed).
                - ttl_seconds: TTL duration applied or remaining.
        """
        ttl = ttl_seconds or settings.SEAT_LOCK_TTL_SECONDS
        key = self.format_seat_lock_key(show_id, seat_number)

        # Execute SET key user_id EX ttl NX atomically
        result = await self.redis.set(key, user_id, ex=ttl, nx=True)

        if not result:
            # Seat is already locked by someone else
            return False, None, 0

        # Calculate expiration timestamp in UTC
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=ttl)
        return True, expires_at, ttl

    async def get_seat_lock_status(
        self,
        show_id: str,
        seat_number: str,
    ) -> Tuple[bool, Optional[str], Optional[int], Optional[datetime]]:
        """
        Check the status of a seat lock.

        Returns:
            Tuple[bool, Optional[str], Optional[int], Optional[datetime]]:
                - is_locked: True if locked, False otherwise.
                - locked_by: user_id who holds the lock or None.
                - ttl_seconds: remaining time-to-live or None.
                - expires_at: lock expiration datetime or None.
        """
        key = self.format_seat_lock_key(show_id, seat_number)
        user_id = await self.redis.get(key)

        if user_id is None:
            return False, None, None, None

        ttl = await self.redis.ttl(key)
        # If ttl is negative (e.g. key expired or no TTL), treat as expired/zero
        ttl_seconds = max(ttl, 0) if ttl is not None else 0
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)

        return True, str(user_id), ttl_seconds, expires_at

    async def release_seat_lock(
        self,
        show_id: str,
        seat_number: str,
        user_id: Optional[str] = None,
    ) -> bool:
        """
        Release a seat lock. If user_id is supplied, atomically verifies ownership
        using a Lua script before deleting the key.
        """
        key = self.format_seat_lock_key(show_id, seat_number)

        if user_id is None:
            deleted = await self.redis.delete(key)
            return deleted > 0

        # Lua script for atomic unlock if owner matches
        unlock_script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """
        result = await self.redis.eval(unlock_script, 1, key, user_id)
        return bool(result)


async def get_cache_service() -> CacheService:
    """Dependency injection helper returning an instantiated CacheService."""
    client = await get_redis_client()
    return CacheService(redis=client)
