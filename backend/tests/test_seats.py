import asyncio
import pytest
import uuid
from httpx import AsyncClient, ASGITransport
from app.main import app, lifespan
from app.services.cache import get_redis_client, CacheService


@pytest.mark.asyncio
async def test_seat_lock_lifecycle():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        test_show_id = f"test-show-{uuid.uuid4().hex[:8]}"
        test_seat = "A15"
        user_1 = "user-alice"
        user_2 = "user-bob"

        # 1. Verify seat is initially unlocked
        status_resp = await client.get(f"/seats/lock/{test_show_id}/{test_seat}")
        assert status_resp.status_code == 200
        status_data = status_resp.json()
        assert status_data["is_locked"] is False
        assert status_data["locked_by"] is None
        assert status_data["ttl_seconds"] is None

        # 2. Lock seat for user_1
        payload = {
            "show_id": test_show_id,
            "seat_number": test_seat,
            "user_id": user_1,
        }
        lock_resp = await client.post("/seats/lock", json=payload)
        assert lock_resp.status_code == 201
        lock_data = lock_resp.json()
        assert lock_data["show_id"] == test_show_id
        assert lock_data["seat_number"] == test_seat
        assert lock_data["locked_by"] == user_1
        assert lock_data["is_locked"] is True
        assert 0 < lock_data["ttl_seconds"] <= 300
        assert "expires_at" in lock_data

        # 3. Attempt to lock same seat by user_2 -> Should return 409 Conflict
        payload_conflict = {
            "show_id": test_show_id,
            "seat_number": test_seat,
            "user_id": user_2,
        }
        conflict_resp = await client.post("/seats/lock", json=payload_conflict)
        assert conflict_resp.status_code == 409
        assert conflict_resp.json()["detail"] == "Seat is currently locked by another user"

        # 4. Check seat status endpoint -> confirms locked by user_1
        status_resp2 = await client.get(f"/seats/lock/{test_show_id}/{test_seat}")
        assert status_resp2.status_code == 200
        status_data2 = status_resp2.json()
        assert status_data2["is_locked"] is True
        assert status_data2["locked_by"] == user_1
        assert status_data2["ttl_seconds"] > 0
        assert status_data2["expires_at"] is not None

        # Cleanup: Release the lock directly using cache service
        redis = await get_redis_client()
        cache = CacheService(redis)
        await cache.release_seat_lock(test_show_id, test_seat)


@pytest.mark.asyncio
async def test_concurrent_seat_locks():
    """Verify atomicity: when multiple concurrent requests try to lock the same seat simultaneously, exactly one succeeds."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        test_show_id = f"test-show-race-{uuid.uuid4().hex[:8]}"
        test_seat = "C10"

        users = [f"user-race-{i}" for i in range(10)]

        async def attempt_lock(user_id: str):
            return await client.post(
                "/seats/lock",
                json={
                    "show_id": test_show_id,
                    "seat_number": test_seat,
                    "user_id": user_id,
                },
            )

        responses = await asyncio.gather(*(attempt_lock(u) for u in users))

        success_count = sum(1 for r in responses if r.status_code == 201)
        conflict_count = sum(1 for r in responses if r.status_code == 409)

        assert success_count == 1, "Exactly one user should successfully lock the seat"
        assert conflict_count == 9, "All other concurrent users should receive 409 Conflict"

        # Cleanup
        redis = await get_redis_client()
        cache = CacheService(redis)
        await cache.release_seat_lock(test_show_id, test_seat)


@pytest.mark.asyncio
async def test_health_check_endpoint():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "healthy"
        assert data["redis_connected"] is True


@pytest.mark.asyncio
async def test_lock_validation_error():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Missing user_id
        resp = await client.post(
            "/seats/lock",
            json={"show_id": "show-1", "seat_number": "B2"},
        )
        assert resp.status_code == 422


@pytest.mark.asyncio
async def test_app_lifespan():
    """Verify lifespan context initializes and closes Redis resources cleanly."""
    async with lifespan(app):
        redis = await get_redis_client()
        assert await redis.ping() is True
