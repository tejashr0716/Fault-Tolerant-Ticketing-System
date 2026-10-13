import asyncio
import json
import logging
from typing import List, Dict, Any
from fastapi import WebSocket, WebSocketDisconnect

logger = logging.getLogger(__name__)


class LiveServerManager:
    """
    Real-Time WebSocket Connection & Broadcast Hub for BookMyShow Live Updates.
    Broadcasts seat holds, release events, booking confirmations, and ACID wallet updates
    to all actively connected browser sessions instantaneously.
    """

    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        async with self._lock:
            self.active_connections.append(websocket)
        logger.info(f"[LiveServer] Client connected. Total active sessions: {len(self.active_connections)}")
        # Send welcome and current server status
        await websocket.send_json({
            "type": "LIVE_INIT",
            "message": "Connected to BookMyShow Real-Time Live Sync Server",
            "active_clients": len(self.active_connections)
        })
        # Broadcast updated client count to all
        await self.broadcast({
            "type": "CLIENT_COUNT",
            "count": len(self.active_connections)
        })

    async def disconnect(self, websocket: WebSocket):
        async with self._lock:
            if websocket in self.active_connections:
                self.active_connections.remove(websocket)
        logger.info(f"[LiveServer] Client disconnected. Remaining sessions: {len(self.active_connections)}")
        await self.broadcast({
            "type": "CLIENT_COUNT",
            "count": len(self.active_connections)
        })

    async def broadcast(self, message: Dict[str, Any]):
        """Broadcasts a JSON event payload to all active WebSocket clients safely."""
        async with self._lock:
            dead_connections = []
            for connection in self.active_connections:
                try:
                    await connection.send_json(message)
                except Exception as e:
                    logger.warning(f"[LiveServer] Failed to send message to client, marking dead: {e}")
                    dead_connections.append(connection)

            for dead in dead_connections:
                if dead in self.active_connections:
                    self.active_connections.remove(dead)


# Global singleton instance
live_manager = LiveServerManager()
