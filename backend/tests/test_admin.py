import pytest
import uuid
from httpx import AsyncClient, ASGITransport
from app.main import app


@pytest.mark.asyncio
async def test_admin_stats_and_theaters():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. Fetch admin stats
        stats_resp = await client.get("/api/admin/stats")
        assert stats_resp.status_code == 200
        stats = stats_resp.json()
        assert "theaters_count" in stats
        assert "movies_count" in stats
        assert "active_locks" in stats
        assert "live_clients" in stats

        # 2. Fetch theaters list
        theaters_resp = await client.get("/api/theaters")
        assert theaters_resp.status_code == 200
        theaters = theaters_resp.json()["theaters"]
        assert len(theaters) >= 4


@pytest.mark.asyncio
async def test_admin_create_and_delete_theater():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. Reject invalid pincode (must match ^560[0-9]{3}$)
        invalid_payload = {
            "name": "Invalid Cinema Mumbai",
            "location": "Bandra West, Mumbai",
            "pincode": "400050",
            "area": "Bandra",
        }
        rej_resp = await client.post("/api/admin/theaters", json=invalid_payload)
        assert rej_resp.status_code == 400

        # 2. Create valid Bengaluru cinema
        unique_name = f"PVR Test Arena {uuid.uuid4().hex[:6]}"
        valid_payload = {
            "name": unique_name,
            "location": "Outer Ring Road, Bellandur, Bengaluru",
            "area": "Bellandur",
            "pincode": "560103",
            "screens": ["Audi 1 (IMAX)", "Audi 2 (4DX)"],
            "amenities": ["M-Ticket", "F&B", "Dolby Atmos"],
        }
        create_resp = await client.post("/api/admin/theaters", json=valid_payload)
        assert create_resp.status_code == 200
        data = create_resp.json()
        assert data["status"] == "success"
        theater = data["theater"]
        theater_id = theater["id"]
        assert theater["name"] == unique_name

        # 3. Verify theater is listed in catalog
        list_resp = await client.get("/api/theaters")
        assert list_resp.status_code == 200
        theaters = list_resp.json()["theaters"]
        assert any(t["id"] == theater_id for t in theaters)

        # 4. Delete the theater
        del_resp = await client.delete(f"/api/admin/theaters/{theater_id}")
        assert del_resp.status_code == 200
        assert del_resp.json()["status"] == "success"


@pytest.mark.asyncio
async def test_admin_reset_seats():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/api/admin/reset-seats")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "success"
        assert "cleared_locks" in data
