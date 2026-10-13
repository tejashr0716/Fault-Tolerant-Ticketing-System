import json
import logging
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import StreamingResponse
import asyncio

from app.services.live_server import live_manager

logger = logging.getLogger(__name__)
router = APIRouter(tags=["Live Real-Time Server"])


@router.websocket("/ws/live")
async def websocket_live_endpoint(websocket: WebSocket):
    """
    WebSocket endpoint for real-time live seat modifications and notifications.
    Clients receive instant broadcasts when seats are locked, unlocked, or booked.
    """
    await live_manager.connect(websocket)
    try:
        while True:
            # Keep connection open and accept ping/pong or client action requests
            data = await websocket.receive_text()
            try:
                msg = json.loads(data)
                action = msg.get("action")
                if action == "PING":
                    await websocket.send_json({"type": "PONG", "timestamp": msg.get("timestamp")})
                elif action == "REQUEST_SYNC":
                    await websocket.send_json({
                        "type": "SYNC_ACK",
                        "active_clients": len(live_manager.active_connections)
                    })
            except Exception:
                pass
    except WebSocketDisconnect:
        await live_manager.disconnect(websocket)
    except Exception as exc:
        logger.error(f"[WebSocket] Exception: {exc}")
        await live_manager.disconnect(websocket)


@router.post("/api/live/broadcast")
async def broadcast_live_event(event: dict):
    """Allows internal background workers or HTTP handlers to broadcast events to all WebSocket clients."""
    await live_manager.broadcast(event)
    return {"status": "broadcasted", "clients": len(live_manager.active_connections)}


@router.get("/api/live/stream")
async def sse_live_stream(request: Request):
    """Server-Sent Events (SSE) fallback stream for live seat updates."""
    async def event_generator():
        yield f"data: {json.dumps({'type': 'SSE_CONNECTED', 'message': 'Live SSE Stream Active'})}\n\n"
        while True:
            if await request.is_disconnected():
                break
            await asyncio.sleep(15)
            yield f"data: {json.dumps({'type': 'HEARTBEAT'})}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"}
    )
