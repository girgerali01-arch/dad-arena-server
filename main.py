import asyncio
import json
import os
import time
import uuid
from typing import Dict, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

app = FastAPI(title="DAD_ARENA Online Server", version="0.1.0-alpha")

# player_id -> connection record
players: Dict[str, dict] = {}

# FIFO queue of player IDs waiting for a 1v1 match
match_queue: list[str] = []

# room_id -> {"players": [player_id_1, player_id_2], "created_at": float}
rooms: Dict[str, dict] = {}

lock = asyncio.Lock()


def now_ms() -> int:
    return int(time.time() * 1000)


async def safe_send(ws: WebSocket, payload: dict) -> None:
    try:
        await ws.send_text(json.dumps(payload, ensure_ascii=False))
    except Exception:
        pass


def public_player(player_id: str) -> dict:
    p = players.get(player_id, {})
    return {
        "player_id": player_id,
        "name": p.get("name", "Player"),
        "online": player_id in players,
    }


async def remove_from_queue(player_id: str) -> None:
    while player_id in match_queue:
        match_queue.remove(player_id)


async def close_room_for(player_id: str, reason: str) -> None:
    room_id: Optional[str] = players.get(player_id, {}).get("room_id")
    if not room_id:
        return

    room = rooms.pop(room_id, None)
    if not room:
        if player_id in players:
            players[player_id]["room_id"] = None
        return

    for pid in room["players"]:
        if pid in players:
            players[pid]["room_id"] = None

    for pid in room["players"]:
        if pid != player_id and pid in players:
            await safe_send(
                players[pid]["ws"],
                {
                    "type": "match_ended",
                    "room_id": room_id,
                    "reason": reason,
                    "time": now_ms(),
                },
            )


async def try_matchmake() -> None:
    # Clean invalid entries first.
    cleaned: list[str] = []
    seen = set()

    for pid in match_queue:
        if pid in seen:
            continue
        if pid not in players:
            continue
        if players[pid].get("room_id") is not None:
            continue
        seen.add(pid)
        cleaned.append(pid)

    match_queue[:] = cleaned

    while len(match_queue) >= 2:
        p1 = match_queue.pop(0)
        p2 = match_queue.pop(0)

        if p1 not in players or p2 not in players:
            continue

        room_id = uuid.uuid4().hex[:12]
        rooms[room_id] = {
            "players": [p1, p2],
            "created_at": time.time(),
        }

        players[p1]["room_id"] = room_id
        players[p2]["room_id"] = room_id

        await safe_send(
            players[p1]["ws"],
            {
                "type": "match_found",
                "room_id": room_id,
                "side": 1,
                "opponent": public_player(p2),
                "time": now_ms(),
            },
        )

        await safe_send(
            players[p2]["ws"],
            {
                "type": "match_found",
                "room_id": room_id,
                "side": 2,
                "opponent": public_player(p1),
                "time": now_ms(),
            },
        )


async def relay_to_opponent(sender_id: str, payload: dict) -> None:
    sender = players.get(sender_id)
    if not sender:
        return

    room_id = sender.get("room_id")
    if not room_id or room_id not in rooms:
        return

    room = rooms[room_id]
    opponent_id = next((pid for pid in room["players"] if pid != sender_id), None)

    if not opponent_id or opponent_id not in players:
        return

    outgoing = dict(payload)
    outgoing["from_player_id"] = sender_id
    outgoing["room_id"] = room_id
    outgoing["server_time"] = now_ms()

    await safe_send(players[opponent_id]["ws"], outgoing)


@app.get("/")
async def root():
    return JSONResponse(
        {
            "service": "DAD_ARENA Online Server",
            "version": "0.1.0-alpha",
            "status": "ok",
            "online_players": len(players),
            "waiting_players": len(match_queue),
            "active_rooms": len(rooms),
        }
    )


@app.get("/health")
async def health():
    return {"ok": True, "time": now_ms()}


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()

    player_id: Optional[str] = None

    try:
        # First packet must identify the player.
        raw = await asyncio.wait_for(ws.receive_text(), timeout=15)
        hello = json.loads(raw)

        if hello.get("type") != "hello":
            await safe_send(ws, {"type": "error", "message": "First packet must be hello"})
            await ws.close(code=1008)
            return

        candidate = str(hello.get("player_id", "")).strip()
        name = str(hello.get("name", "Player")).strip()[:32] or "Player"

        if not candidate or len(candidate) > 64:
            await safe_send(ws, {"type": "error", "message": "Invalid player_id"})
            await ws.close(code=1008)
            return

        player_id = candidate

        async with lock:
            old = players.get(player_id)
            if old is not None:
                await safe_send(
                    old["ws"],
                    {"type": "session_replaced", "message": "Account connected from another device"},
                )
                try:
                    await old["ws"].close(code=1000)
                except Exception:
                    pass

            players[player_id] = {
                "ws": ws,
                "name": name,
                "room_id": None,
                "connected_at": time.time(),
            }

        await safe_send(
            ws,
            {
                "type": "hello_ok",
                "player_id": player_id,
                "name": name,
                "server_version": "0.1.0-alpha",
                "time": now_ms(),
            },
        )

        while True:
            raw = await ws.receive_text()

            if len(raw) > 100_000:
                await safe_send(ws, {"type": "error", "message": "Message too large"})
                continue

            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                await safe_send(ws, {"type": "error", "message": "Invalid JSON"})
                continue

            msg_type = str(msg.get("type", ""))

            if msg_type == "ping":
                await safe_send(ws, {"type": "pong", "time": now_ms()})

            elif msg_type == "queue_join":
                async with lock:
                    if players[player_id].get("room_id") is not None:
                        await safe_send(ws, {"type": "error", "message": "Already in match"})
                        continue

                    await remove_from_queue(player_id)
                    match_queue.append(player_id)

                    await safe_send(
                        ws,
                        {
                            "type": "queue_joined",
                            "position": match_queue.index(player_id) + 1,
                            "time": now_ms(),
                        },
                    )

                    await try_matchmake()

            elif msg_type == "queue_leave":
                async with lock:
                    await remove_from_queue(player_id)

                await safe_send(ws, {"type": "queue_left", "time": now_ms()})

            elif msg_type == "leave_match":
                async with lock:
                    await close_room_for(player_id, "opponent_left")

                await safe_send(ws, {"type": "left_match", "time": now_ms()})

            elif msg_type == "player_lookup":
                # Alpha version: finds currently connected players only.
                target_id = str(msg.get("player_id", "")).strip()
                found = target_id in players

                await safe_send(
                    ws,
                    {
                        "type": "player_lookup_result",
                        "found": found,
                        "player": public_player(target_id) if found else None,
                        "time": now_ms(),
                    },
                )

            elif msg_type in {
                "ready",
                "input",
                "state",
                "action",
                "shot",
                "hit",
                "eliminated",
                "emote",
                "match_message",
            }:
                await relay_to_opponent(player_id, msg)

            else:
                await safe_send(
                    ws,
                    {
                        "type": "error",
                        "message": f"Unknown message type: {msg_type}",
                    },
                )

    except (WebSocketDisconnect, asyncio.TimeoutError):
        pass
    except Exception as exc:
        await safe_send(ws, {"type": "error", "message": "Server error"})
        print(f"WebSocket error for {player_id}: {exc}")

    finally:
        if player_id is not None:
            async with lock:
                await remove_from_queue(player_id)
                await close_room_for(player_id, "opponent_disconnected")

                current = players.get(player_id)
                if current is not None and current.get("ws") is ws:
                    players.pop(player_id, None)
