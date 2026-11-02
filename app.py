"""Lucky draw live room server (multi-room).

Each room ("空间") has its own organiser PIN, participants, prizes, history and
live sockets, all hosted under the same URL:

    /                     entry page (enter a room code / open your own room)
    /s/{code}             participant page of that room
    /s/{code}/admin       organiser back office of that room

In-memory state, FastAPI + WebSocket. Designed for a free Render web service.
"""

from __future__ import annotations

import asyncio
import csv
import io
import re
import secrets
import time
import zipfile
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal, Optional
from urllib.parse import quote
from xml.etree import ElementTree

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, RedirectResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter
from pydantic import BaseModel, Field


BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

ADMIN_SESSION_TTL_SECONDS = 12 * 60 * 60
LOGIN_MAX_ATTEMPTS = 8
LOGIN_WINDOW_SECONDS = 10 * 60

ROOM_CREATE_MAX_PER_IP = 6
ROOM_CREATE_WINDOW_SECONDS = 60 * 60
MAX_ROOMS = 500
MAX_ROSTER_ITEMS = 5000

WHEEL_REVEAL_SECONDS = 7.2
RANDOM_REVEAL_SECONDS = 1.4
MAX_PARTICIPANTS = 5000
MAX_TEXT_SHORT = 60
MAX_TEXT_MEDIUM = 120
MAX_CDKS_PER_PRIZE = 2000
MAX_SCHEDULE_DAYS = 365
MAX_IMPORT_BYTES = 5 * 1024 * 1024

app = FastAPI(title="幸运抽奖系统", version="2.0.0")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


#---------------------------------------------------------------------------
# Data model
#---------------------------------------------------------------------------


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse_iso(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def clean_text(value: str, max_len: int) -> str:
    text = "".join(ch for ch in str(value or "") if ch >= " " and ch != "\x7f")
    return " ".join(text.split()).strip()[:max_len]


def new_id() -> str:
    return secrets.token_hex(8)


def norm_name(value: str) -> str:
    return clean_text(value, MAX_TEXT_SHORT).casefold()


@dataclass
class Participant:
    id: str
    name: str
    token: str
    joined_at: str
    normalized_name: str
    source: Literal["join", "import"] = "join"

    def admin_brief(self) -> dict[str, Any]:
        return {"id": self.id, "name": self.name, "source": self.source}


@dataclass
class Prize:
    id: str
    title: str
    level: str
    reward_type: Literal["cdk", "item"]
    reward_name: str
    quantity: int
    cdks: list[str]
    assigned_cdks: list[str] = field(default_factory=list)
    created_at: str = field(default_factory=lambda: iso(utc_now()))

    def available_cdks(self) -> list[str]:
        used = set(self.assigned_cdks)
        return [code for code in self.cdks if code not in used]

    def snapshot(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "title": self.title,
            "level": self.level,
            "reward_type": self.reward_type,
            "reward_name": self.reward_name,
            "quantity": self.quantity,
        }

    def admin_view(self) -> dict[str, Any]:
        return {
            **self.snapshot(),
            "cdks": list(self.cdks),
            "assigned_cdks": list(self.assigned_cdks),
            "available_count": len(self.available_cdks()),
            "created_at": self.created_at,
        }

    def public_view(self) -> dict[str, Any]:
        return {
            **self.snapshot(),
            "is_cdk": self.reward_type == "cdk",
        }


@dataclass
class Room:
    """One organiser's independent draw room."""

    code: str
    pin: str
    name: str = "幸运抽奖活动"
    registration_open: bool = True
    pool_mode: Literal["registration", "roster"] = "registration"
    cooldown_enabled: bool = False
    cooldown_days: int = 30
    participants: dict[str, Participant] = field(default_factory=dict)
    prizes: list[Prize] = field(default_factory=list)
    history: list[dict[str, Any]] = field(default_factory=list)
    schedule: Optional[dict[str, Any]] = None
    current_draw: Optional[dict[str, Any]] = None
    created_at: str = field(default_factory=lambda: iso(utc_now()))
    login_attempts: deque[float] = field(default_factory=deque)
    participant_sockets: dict[str, set[WebSocket]] = field(default_factory=lambda: defaultdict(set))
    admin_sockets: set[WebSocket] = field(default_factory=set)
    finish_task: Optional[asyncio.Task[Any]] = None
    schedule_task: Optional[asyncio.Task[Any]] = None

    # ----- derived helpers -----

    def online_ids(self) -> set[str]:
        return {pid for pid, sockets in self.participant_sockets.items() if sockets}

    def find_name(self, name: str) -> Optional[Participant]:
        target = norm_name(name)
        if not target:
            return None
        for participant in self.participants.values():
            if participant.normalized_name == target:
                return participant
        return None

    def add_participant_name(self, raw_name: str, source: str = "import") -> tuple[Optional[Participant], str]:
        name = clean_text(raw_name, MAX_TEXT_SHORT)
        if not name:
            return None, "empty"
        if self.find_name(name):
            return None, "duplicate"
        if len(self.participants) >= MAX_PARTICIPANTS:
            return None, "full"
        participant = Participant(
            id=new_id(),
            name=name,
            token=secrets.token_urlsafe(32),
            joined_at=iso(utc_now()),
            normalized_name=norm_name(name),
            source=source,  # type: ignore[arg-type]
        )
        self.participants[participant.id] = participant
        return participant, "ok"

    # ----- status -----

    def status(self) -> str:
        if self.current_draw:
            return "drawing"
        if self.schedule:
            return "scheduled"
        if self.pool_mode == "roster":
            return "ready" if self.registration_open else "waiting"
        return "registering" if self.registration_open else "waiting"

    # ----- cooldown / eligibility -----

    def cooldown_info(self) -> tuple[dict[str, str], list[dict[str, str]]]:
        if not self.cooldown_enabled or self.cooldown_days <= 0:
            return {}, []
        cutoff = utc_now() - timedelta(days=self.cooldown_days)
        latest: dict[str, datetime] = {}
        for record in self.history:
            try:
                finished = parse_iso(record.get("finished_at", record.get("started_at", "")))
            except (ValueError, TypeError):
                continue
            if finished < cutoff:
                continue
            for winner in record.get("winners", []):
                pid = winner.get("participant_id") or ""
                if not pid:
                    continue
                if pid not in latest or finished > latest[pid]:
                    latest[pid] = finished
        excluded = []
        for pid, won_at in latest.items():
            participant = self.participants.get(pid)
            if participant:
                excluded.append({"id": pid, "name": participant.name, "last_won_at": iso(won_at)})
        excluded.sort(key=lambda item: item["name"])
        return {pid: iso(dt) for pid, dt in latest.items()}, excluded

    def eligible(self) -> tuple[list[Participant], list[dict[str, str]]]:
        _, excluded = self.cooldown_info()
        excluded_ids = {item["id"] for item in excluded}
        pool = [p for p in self.participants.values() if p.id not in excluded_ids]
        return pool, excluded

    def my_cooldown(self, participant_id: str) -> Optional[dict[str, Any]]:
        latest_iso, _ = self.cooldown_info()
        value = latest_iso.get(participant_id)
        if not value:
            return None
        return {
            "last_won_at": value,
            "until": iso(parse_iso(value) + timedelta(days=self.cooldown_days)),
        }

    # ----- public projections -----

    def event_public(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status(),
            "registration_open": self.registration_open,
            "pool_mode": self.pool_mode,
            "cooldown_enabled": self.cooldown_enabled,
            "cooldown_days": self.cooldown_days,
        }

    def schedule_public(self) -> Optional[dict[str, Any]]:
        return dict(self.schedule) if self.schedule else None

    def current_draw_public(self) -> Optional[dict[str, Any]]:
        draw = self.current_draw
        if not draw:
            return None
        return {
            "id": draw["id"],
            "title": draw["title"],
            "mode": draw["mode"],
            "started_at": draw["started_at"],
            "reveal_at": draw["reveal_at"],
            "eligible_count": draw["eligible_count"],
            "participant_count": draw["participant_count"],
            "total_winners": draw["total_winners"],
            "prizes": draw["prizes_snapshot"],
        }

    def public_state(self) -> dict[str, Any]:
        return {
            "role": "public",
            "server_time": iso(utc_now()),
            "room": {"code": self.code},
            "event": self.event_public(),
            "stats": {
                "participant_count": len(self.participants),
                "online_count": len(self.online_ids()),
                "prize_count": len(self.prizes),
                "draw_count": len(self.history),
            },
            "prizes": [p.public_view() for p in self.prizes],
            "schedule": self.schedule_public(),
            "current_draw": self.current_draw_public(),
        }

    def admin_state(self) -> dict[str, Any]:
        online_ids = self.online_ids()
        eligible, excluded = self.eligible()
        latest_iso, _ = self.cooldown_info()
        participants = []
        for participant in self.participants.values():
            participants.append(
                {
                    "id": participant.id,
                    "name": participant.name,
                    "source": participant.source,
                    "joined_at": participant.joined_at,
                    "online": participant.id in online_ids,
                    "excluded": participant.id in latest_iso,
                }
            )
        return {
            "role": "admin",
            "server_time": iso(utc_now()),
            "room": {"code": self.code},
            "event": {**self.event_public(), "created_at": self.created_at},
            "stats": {
                "participant_count": len(self.participants),
                "online_count": len(online_ids),
                "eligible_count": len(eligible),
                "excluded_count": len(excluded),
                "prize_count": len(self.prizes),
                "draw_count": len(self.history),
                "roster_count": sum(1 for p in self.participants.values() if p.source == "import"),
            },
            "participants": participants,
            "eligible_names": [p.name for p in eligible],
            "excluded": excluded,
            "prizes": [prize.admin_view() for prize in self.prizes],
            "history": [history_public(r, include_cdk=True) for r in reversed(self.history)],
            "schedule": self.schedule_public(),
            "current_draw": self.current_draw_public(),
            "drawing": bool(self.current_draw),
        }

    def participant_state(self, participant: Participant) -> dict[str, Any]:
        online_ids = self.online_ids()
        eligible, excluded = self.eligible()
        my_results = [
            result
            for record in reversed(self.history)
            if (result := my_result_for(participant.id, record)) is not None
        ]
        return {
            "role": "participant",
            "server_time": iso(utc_now()),
            "room": {"code": self.code},
            "event": self.event_public(),
            "me": {
                "id": participant.id,
                "name": participant.name,
                "joined_at": participant.joined_at,
                "online": participant.id in online_ids,
                "cooldown": self.my_cooldown(participant.id),
            },
            "stats": {
                "participant_count": len(self.participants),
                "online_count": len(online_ids),
                "eligible_count": len(eligible),
                "excluded_count": len(excluded),
                "prize_count": len(self.prizes),
                "draw_count": len(self.history),
            },
            "prizes": [p.public_view() for p in self.prizes],
            "history": [history_public(r, include_cdk=False) for r in reversed(self.history)],
            "my_results": my_results,
            "schedule": self.schedule_public(),
            "current_draw": self.current_draw_public(),
        }


#---------------------------------------------------------------------------
# Record projections (pure functions)
#---------------------------------------------------------------------------


def winner_public(winner: dict[str, Any], include_cdk: bool = False) -> dict[str, Any]:
    item = {
        "participant_id": winner["participant_id"],
        "name": winner["name"],
        "prize_id": winner["prize_id"],
        "prize_title": winner["prize_title"],
        "prize_level": winner["prize_level"],
        "reward_type": winner["reward_type"],
        "reward_name": winner["reward_name"],
    }
    if include_cdk:
        item["cdk"] = winner.get("cdk", "")
    return item


def history_public(record: dict[str, Any], include_cdk: bool = False) -> dict[str, Any]:
    return {
        "id": record["id"],
        "title": record["title"],
        "mode": record["mode"],
        "started_at": record["started_at"],
        "finished_at": record["finished_at"],
        "prizes": record["prizes"],
        "winners": [winner_public(w, include_cdk=include_cdk) for w in record["winners"]],
        "participant_count": record["participant_count"],
        "eligible_count": record["eligible_count"],
        "excluded_count": len(record.get("excluded", [])),
        "excluded": record.get("excluded", []) if include_cdk else [],
        "scheduled": bool(record.get("scheduled")),
    }


def my_result_for(participant_id: str, record: dict[str, Any]) -> Optional[dict[str, Any]]:
    if participant_id not in record.get("participant_ids", []):
        return None
    winner = next(
        (w for w in record.get("winners", []) if w.get("participant_id") == participant_id),
        None,
    )
    base = {
        "draw_id": record["id"],
        "draw_title": record["title"],
        "finished_at": record["finished_at"],
        "mode": record["mode"],
    }
    if winner:
        return {
            **base,
            "status": "won",
            "prize_id": winner["prize_id"],
            "prize_title": winner["prize_title"],
            "prize_level": winner["prize_level"],
            "reward_type": winner["reward_type"],
            "reward_name": winner["reward_name"],
            "cdk": winner.get("cdk", ""),
        }
    excluded = next(
        (e for e in record.get("excluded", []) if e.get("id") == participant_id),
        None,
    )
    if excluded:
        return {**base, "status": "excluded", "last_won_at": excluded.get("last_won_at")}
    return {**base, "status": "not_won"}


#---------------------------------------------------------------------------
# Registry
#---------------------------------------------------------------------------


rooms: dict[str, Room] = {}
state_lock = asyncio.Lock()
admin_sessions: dict[str, str] = {}
admin_sessions_expiry: dict[str, float] = {}
participant_index: dict[str, str] = {}
create_attempts: dict[str, deque[float]] = defaultdict(deque)


def is_reserved_code(value: str) -> bool:
    return value.lower() in {"admin", "api", "ws", "s", "static", "healthz", "favicon", "health", "login"}


def generate_room_code() -> str:
    alphabet = "23456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    for _ in range(64):
        code = "".join(secrets.choice(alphabet) for _ in range(6))
        if code not in rooms and not is_reserved_code(code) and not code.lower().startswith("s"):
            return code
    raise HTTPException(status_code=503, detail="暂时无法创建空间，请稍后重试")


def room_slug(raw: str) -> str:
    return (raw or "").strip()


def get_room(code: str) -> Room:
    room = rooms.get(room_slug(code))
    if not room:
        raise HTTPException(status_code=404, detail="抽奖空间不存在或已失效")
    return room


#---------------------------------------------------------------------------
# Auth helpers
#---------------------------------------------------------------------------



def client_ip(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def get_bearer(authorization: Optional[str]) -> str:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="缺少登录凭证")
    return authorization.split(" ", 1)[1].strip()


def issue_admin_session(room_code: str) -> tuple[str, int]:
    token = secrets.token_urlsafe(32)
    expires_at = time.time() + ADMIN_SESSION_TTL_SECONDS
    admin_sessions[token] = room_code
    admin_sessions_expiry[token] = expires_at
    return token, int(expires_at)


def drop_admin_session(token: str) -> None:
    admin_sessions.pop(token, None)
    admin_sessions_expiry.pop(token, None)


def drop_room_admin_sessions(room_code: str, keep: Optional[str] = None) -> None:
    for token, scope in list(admin_sessions.items()):
        if scope == room_code and token != keep:
            drop_admin_session(token)




def resolve_admin_session(token: str) -> Room:
    scope = admin_sessions.get(token)
    if not scope or admin_sessions_expiry.get(token, 0.0) <= time.time():
        drop_admin_session(token)
        raise HTTPException(status_code=401, detail="主办方登录已过期，请重新登录")
    room = rooms.get(scope)
    if not room:
        drop_admin_session(token)
        raise HTTPException(status_code=401, detail="主办方登录已过期，请重新登录")
    return room


def admin_guard(code: str, authorization: Optional[str] = Header(default=None)) -> dict[str, Any]:
    token = get_bearer(authorization)
    room = resolve_admin_session(token)
    if room.code != room_slug(code):
        raise HTTPException(status_code=403, detail="该登录凭证不属于这个抽奖空间")
    return {"room": room, "token": token}


def admin_guard_path(code: str, authorization: Optional[str] = Header(default=None)) -> dict[str, Any]:
    """FastAPI dependency used by every /api/rooms/{code}/admin route."""
    return admin_guard(code, authorization)


def participant_from_token(token: str) -> tuple[Room, Participant]:
    room_code = participant_index.get(token)
    if room_code:
        room = rooms.get(room_code)
        if room:
            for participant in room.participants.values():
                if secrets.compare_digest(participant.token, token):
                    return room, participant
    raise HTTPException(status_code=401, detail="参会凭证已失效，请重新加入活动")




#---------------------------------------------------------------------------
# WebSocket broadcast
#---------------------------------------------------------------------------



async def send_json_safe(ws: WebSocket, payload: dict[str, Any]) -> None:
    try:
        await ws.send_json(payload)
    except Exception:
        pass


async def broadcast_room(room: Room, payload: dict[str, Any]) -> None:
    targets: list[WebSocket] = list(room.admin_sockets)
    for sockets in room.participant_sockets.values():
        targets.extend(sockets)
    if targets:
        await asyncio.gather(*(send_json_safe(ws, payload) for ws in targets))


async def broadcast_refresh(room: Room) -> None:
    await broadcast_room(room, {"type": "refresh"})


async def close_participant_sockets(room: Room, participant_id: str) -> None:
    sockets = list(room.participant_sockets.get(participant_id, set()))
    for ws in sockets:
        try:
            await ws.close(code=4401, reason="participant removed")
        except Exception:
            pass


#---------------------------------------------------------------------------
# Validation and draw helpers
#---------------------------------------------------------------------------


def get_prize_or_404(room: Room, prize_id: str) -> Prize:
    prize = next((p for p in room.prizes if p.id == prize_id), None)
    if not prize:
        raise HTTPException(status_code=404, detail="奖项不存在或已被删除")
    return prize


def all_other_codes(room: Room, exclude_prize_id: Optional[str] = None) -> set[str]:
    codes: set[str] = set()
    for prize in room.prizes:
        if prize.id == exclude_prize_id:
            continue
        codes.update(prize.cdks)
        codes.update(prize.assigned_cdks)
    return codes


def validate_prize_payload(room: Room, payload: PrizeRequest, prize_id: Optional[str] = None) -> tuple[str, str, str, str, int, list[str]]:
    title = clean_text(payload.title, 80)
    level = clean_text(payload.level, 40)
    reward_name = clean_text(payload.reward_name, 120)
    if not title:
        raise HTTPException(status_code=422, detail="奖项名称不能为空")
    if payload.quantity < 1:
        raise HTTPException(status_code=422, detail="奖项名额必须至少为 1")
    if payload.reward_type == "item" and not reward_name:
        reward_name = title

    cdks: list[str] = []
    if payload.reward_type == "cdk":
        seen: set[str] = set()
        for raw in payload.cdks:
            code = clean_text(raw, 200)
            if not code or code in seen:
                continue
            seen.add(code)
            cdks.append(code)
        if len(cdks) > MAX_CDKS_PER_PRIZE:
            raise HTTPException(status_code=422, detail=f"单个奖项最多支持 {MAX_CDKS_PER_PRIZE} 条 CDK")
        if len(cdks) < payload.quantity:
            raise HTTPException(
                status_code=422,
                detail=f"CDK 数量不足：本奖项有 {payload.quantity} 个名额，需要至少 {payload.quantity} 条 CDK",
            )
        conflicts = set(cdks) & all_other_codes(room, exclude_prize_id=prize_id)
        if conflicts:
            sample = "、".join(sorted(conflicts)[:5])
            raise HTTPException(status_code=422, detail=f"CDK 与其它奖项重复：{sample}")

    return title, level, payload.reward_type, reward_name, payload.quantity, cdks


def ensure_no_active_operation(room: Room) -> None:
    if room.current_draw:
        raise HTTPException(status_code=409, detail="正在开奖中，请等待本轮结束")
    if room.schedule:
        raise HTTPException(status_code=409, detail="已设置定时开奖，请先取消后再修改奖项或立即开奖")


def draw_config_from(room: Room, request_data: DrawRequest) -> dict[str, Any]:
    title = clean_text(request_data.title, 80)
    if not title:
        raise HTTPException(status_code=422, detail="抽奖名称不能为空")
    prize_ids = list(dict.fromkeys(request_data.prize_ids))
    if not prize_ids:
        raise HTTPException(status_code=422, detail="请至少选择一个奖项")
    prizes = [get_prize_or_404(room, pid) for pid in prize_ids]
    return {"title": title, "mode": request_data.mode, "prize_ids": prize_ids, "prizes": prizes}


def prepare_draw(room: Room, config: dict[str, Any], scheduled: bool) -> tuple[dict[str, Any], dict[str, Any]]:
    """Must be called while holding state_lock."""
    if room.current_draw:
        raise HTTPException(status_code=409, detail="正在开奖中")

    prizes: list[Prize] = config["prizes"]
    for prize in prizes:
        if prize.reward_type == "cdk":
            available = len(prize.available_cdks())
            if available < prize.quantity:
                raise HTTPException(
                    status_code=409,
                    detail=f"奖项「{prize.title}」剩余 CDK 不足：需要 {prize.quantity} 条，当前只有 {available} 条",
                )

    total_winners = sum(prize.quantity for prize in prizes)
    eligible, excluded = room.eligible()
    if not eligible:
        raise HTTPException(status_code=409, detail="没有可参与抽奖的人员")
    if total_winners > len(eligible):
        raise HTTPException(
            status_code=409,
            detail=f"本轮共需抽出 {total_winners} 人，但当前可参与人数只有 {len(eligible)} 人",
        )

    # SystemRandom uses OS entropy and is suitable for a live draw.
    rng = secrets.SystemRandom()
    picked = rng.sample(eligible, total_winners)

    cursor = 0
    winners: list[dict[str, Any]] = []
    pending_cdks: dict[str, list[str]] = defaultdict(list)
    for prize in prizes:
        available_codes = prize.available_cdks()
        for offset in range(prize.quantity):
            participant = picked[cursor]
            cursor += 1
            cdk = ""
            if prize.reward_type == "cdk":
                cdk = available_codes[offset]
                pending_cdks[prize.id].append(cdk)
            winners.append(
                {
                    "participant_id": participant.id,
                    "name": participant.name,
                    "prize_id": prize.id,
                    "prize_title": prize.title,
                    "prize_level": prize.level,
                    "reward_type": prize.reward_type,
                    "reward_name": prize.reward_name,
                    "cdk": cdk,
                }
            )

    now = utc_now()
    reveal_seconds = WHEEL_REVEAL_SECONDS if config["mode"] == "wheel" else RANDOM_REVEAL_SECONDS
    reveal_at = now + timedelta(seconds=reveal_seconds)
    draw_id = new_id()
    prizes_snapshot = [prize.snapshot() for prize in prizes]

    current = {
        "id": draw_id,
        "title": config["title"],
        "mode": config["mode"],
        "started_at": iso(now),
        "reveal_at": iso(reveal_at),
        "participant_ids": list(room.participants.keys()),
        "eligible_ids": [p.id for p in eligible],
        "excluded": excluded,
        "participant_count": len(room.participants),
        "eligible_count": len(eligible),
        "total_winners": total_winners,
        "prizes_snapshot": prizes_snapshot,
        "winners": winners,
        "pending_cdks": {k: list(v) for k, v in pending_cdks.items()},
        "scheduled": scheduled,
        "reveal_seconds": reveal_seconds,
    }
    room.current_draw = current
    room.registration_open = False

    public_start = {
        "id": draw_id,
        "title": config["title"],
        "mode": config["mode"],
        "started_at": iso(now),
        "reveal_at": iso(reveal_at),
        "eligible_count": len(eligible),
        "participant_count": len(room.participants),
        "total_winners": total_winners,
        "prizes": prizes_snapshot,
        "scheduled": scheduled,
    }
    admin_payload = {
        **public_start,
        "eligible_names": [p.name for p in eligible],
        "winners": [winner_public(w, include_cdk=True) for w in winners],
    }
    return public_start, admin_payload


def finalize_current_draw(room: Room) -> dict[str, Any]:
    """Must be called while holding state_lock."""
    current = room.current_draw
    if not current:
        raise RuntimeError("No current draw")
    for prize_id, codes in current["pending_cdks"].items():
        prize = next((p for p in room.prizes if p.id == prize_id), None)
        if prize:
            for code in codes:
                if code not in prize.assigned_cdks:
                    prize.assigned_cdks.append(code)
    record = {
        "id": current["id"],
        "title": current["title"],
        "mode": current["mode"],
        "started_at": current["started_at"],
        "finished_at": iso(utc_now()),
        "prizes": current["prizes_snapshot"],
        "winners": current["winners"],
        "participant_ids": current["participant_ids"],
        "participant_count": current["participant_count"],
        "eligible_count": current["eligible_count"],
        "excluded": current["excluded"],
        "scheduled": current["scheduled"],
    }
    room.history.append(record)
    room.current_draw = None
    return record


async def finish_draw_after(room: Room, draw_id: str, delay_seconds: float) -> None:
    try:
        if delay_seconds > 0:
            await asyncio.sleep(delay_seconds)
        async with state_lock:
            if not room.current_draw or room.current_draw["id"] != draw_id:
                return
            record = finalize_current_draw(room)
        await broadcast_room(
            room,
            {
                "type": "draw_finished",
                "draw_id": draw_id,
                "draw": history_public(record, include_cdk=False),
            },
        )
        await broadcast_refresh(room)
    except asyncio.CancelledError:
        raise
    except Exception:
        pass


async def launch_draw(room: Room, config: dict[str, Any], scheduled: bool = False) -> dict[str, Any]:
    async with state_lock:
        if room.schedule and not scheduled:
            raise HTTPException(status_code=409, detail="已设置定时开奖，请先取消后再立即开奖")
        if scheduled:
            room.schedule = None
        public_start, admin_payload = prepare_draw(room, config, scheduled=scheduled)
        reveal_at = parse_iso(public_start["reveal_at"])
        delay = max(0.0, (reveal_at - utc_now()).total_seconds())
    await broadcast_room(room, {"type": "draw_started", "draw": public_start})
    await broadcast_refresh(room)
    room.finish_task = asyncio.create_task(finish_draw_after(room, public_start["id"], delay))
    return {"draw": public_start, "admin": admin_payload}


#---------------------------------------------------------------------------
# Scheduled draw
#---------------------------------------------------------------------------


def cancel_schedule_task(room: Room) -> None:
    if room.schedule_task and not room.schedule_task.done():
        room.schedule_task.cancel()
    room.schedule_task = None


async def run_scheduled_draw(room: Room, schedule_id: str) -> None:
    try:
        while True:
            async with state_lock:
                scheduled = room.schedule
                if not scheduled or scheduled["id"] != schedule_id:
                    return
                due = parse_iso(scheduled["at"])
            remaining = (due - utc_now()).total_seconds()
            if remaining <= 0:
                break
            await asyncio.sleep(min(remaining, 30.0))

        # Time has arrived. Take the schedule out of state and attempt the draw.
        async with state_lock:
            scheduled = room.schedule
            if not scheduled or scheduled["id"] != schedule_id:
                return
            try:
                config = {
                    "title": scheduled["title"],
                    "mode": scheduled["mode"],
                    "prize_ids": scheduled["prize_ids"],
                    "prizes": [get_prize_or_404(room, pid) for pid in scheduled["prize_ids"]],
                }
                room.schedule = None
                public_start, _ = prepare_draw(room, config, scheduled=True)
                delay = max(0.0, (parse_iso(public_start["reveal_at"]) - utc_now()).total_seconds())
            except HTTPException as exc:
                room.schedule = None
                error_message = exc.detail
                public_start = None
                delay = 0
            except Exception as exc:  # defensive: never leave a broken schedule spinning
                room.schedule = None
                error_message = f"定时开奖执行失败：{exc}"
                public_start = None
                delay = 0

        if public_start is None:
            await broadcast_room(room, {"type": "schedule_failed", "message": error_message})
            await broadcast_refresh(room)
            return

        await broadcast_room(room, {"type": "draw_started", "draw": public_start, "scheduled": True})
        await broadcast_refresh(room)
        room.finish_task = asyncio.create_task(finish_draw_after(room, public_start["id"], delay))
    except asyncio.CancelledError:
        raise
    except Exception:
        pass


#---------------------------------------------------------------------------
# Roster (imported name list)
#---------------------------------------------------------------------------


def strip_code_comments(raw: str, filename: str) -> str:
    data = raw.replace("\r\n", "\n").replace("\r", "\n")
    name = filename.lower()
    if name.endswith(".xml") or name.endswith(".xlsx"):
        for token in ("/**", "*/"):  # e.g. "<!-- /* ... */ -->" left in sharedStrings
            data = data.replace(token, "")
    elif name.endswith(".txt"):
        lines = []
        for line in data.split("\n"):
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            if stripped.startswith("//"):
                stripped = stripped.lstrip("/").strip()
            lines.append(stripped)
        return "\n".join(lines)
    return data


_CELL_SAFE_CHARS = re.compile(r"[^\x00-\x08\x0b\x0c\x0e-\x1f]")


def _column_letters(ref: str) -> str:
    letters = re.match(r"[A-Za-z]+", ref or "")
    return letters.group(0).upper() if letters else ""


def parse_xlsx_rows(data: bytes) -> list[list[str]]:
    try:
        found: list[list[str]] = []
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            shared: list[str] = []
            if "xl/sharedStrings.xml" in zf.namelist():
                root = ElementTree.fromstring(zf.read("xl/sharedStrings.xml"))
                ns = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
                for si in root.findall("m:si", ns):
                    shared.append("".join(t.text or "" for t in si.iter("{%s}t" % ns["m"])))
            sheets = sorted(n for n in zf.namelist() if re.match(r"xl/worksheets/sheet\d+\.xml$", n))
            for sheet_name in sheets:
                root = ElementTree.fromstring(zf.read(sheet_name))
                ns = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
                for row in root.iter("{%s}row" % ns["m"]):
                    values: dict[str, str] = {}
                    for cell in row.findall("m:c", ns):
                        col = _column_letters(cell.get("r", ""))
                        ctype = cell.get("t", "")
                        text = ""
                        if ctype == "inlineStr":
                            text = "".join(t.text or "" for t in cell.iter("{%s}t" % ns["m"]))
                        else:
                            v = cell.find("m:v", ns)
                            if v is not None and v.text is not None:
                                if ctype == "s":
                                    try:
                                        text = shared[int(v.text)]
                                    except (ValueError, IndexError):
                                        text = ""
                                else:
                                    text = v.text
                        values[col] = text
                    if values:
                        found.append(values_row(values))
        return found
    except Exception:
        return []


def values_row(values: dict[str, str]) -> list[str]:
    """Place sparse A1-style cells into an ordered list."""

    if not values:
        return []

    def index(key: str) -> int:
        total = 0
        for ch in key:
            total = total * 26 + (ord(ch) - 64)
        return total

    width = max(index(k) for k in values)
    ordered = [""] * width
    for key, text in values.items():
        idx = index(key)
        if 1 <= idx <= width:
            ordered[idx - 1] = text
    return ordered


def parse_docx_blocks(data: bytes) -> tuple[list[str], list[list[list[str]]]]:
    """Return (paragraph lines, tables) from a .docx file."""
    w = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            if "word/document.xml" not in zf.namelist():
                return [], []
            root = ElementTree.fromstring(zf.read("word/document.xml"))
    except Exception:
        return [], []
    body = root.find(w + "body")
    if body is None:
        return [], []

    def paragraph_text(node) -> str:
        parts: list[str] = []
        for child in node.iter():
            if child.tag == w + "t":
                parts.append(child.text or "")
            elif child.tag == w + "tab":
                parts.append("	")
            elif child.tag in (w + "br", w + "cr"):
                parts.append("\n")
        return "".join(parts)

    paragraphs: list[str] = []
    tables: list[list[list[str]]] = []
    for child in body:
        if child.tag == w + "p":
            paragraphs.append(paragraph_text(child))
        elif child.tag == w + "tbl":
            rows: list[list[str]] = []
            for tr in child.findall(w + "tr"):
                cells = []
                for tc in tr.findall(w + "tc"):
                    text = " ".join(t.strip() for t in (paragraph_text(p) for p in tc.findall(w + "p")) if t.strip())
                    cells.append(text)
                if any(cells):
                    rows.append(cells)
            if rows:
                tables.append(rows)
    return paragraphs, tables


def parse_csv_text(text: str) -> list[list[str]]:
    sample = text[:2048].replace("﻿", "")
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
    except csv.Error:
        lines = [line for line in sample.splitlines() if line.strip()]
        dialect = csv.excel_tab if any("\t" in line for line in lines) else csv.excel
    reader = csv.reader(io.StringIO(text), dialect)
    return [row for row in reader]


def split_line(line: str) -> list[str]:
    return [part for part in re.split(r"[\t,，;；、|]+", line) if part.strip()]


def drop_list_number(value: str) -> str:
    return re.sub(r"^\s*(?:[（(]\s*\d+\s*[)）]|\d+\s*[\.、\)）．:：])\s*", "", value).strip()


HEADING_HINTS = ("名单", "抽奖", "参与", "人员", "活动", "序号", "共计", "人数", "签到", "报名")


def _looks_like_heading(line: str) -> bool:
    text = line.strip()
    if not text or len(text) > 40:
        return False
    if re.search(r"\d+\s*人", text):
        return True
    if re.search(r"[、,，;；	]", text):
        return False
    return any(hint in text for hint in HEADING_HINTS)


NAME_HEADERS = {"姓名", "名字", "名称", "参与人", "参与者", "人员", "员工", "成员", "name", "names", "participant", "participants"}
ID_HEADERS = {"工号", "编号", "学号", "序号", "id", "no", "code", "number"}
EXTRA_HEADERS = {"部门", "班级", "单位", "备注", "联系方式", "电话", "手机", "邮箱", "性别", "年级", "dept", "department", "team", "group"}


def _header_kind(cell: str) -> Optional[str]:
    key = cell.strip().casefold()
    if not key:
        return None
    if key in NAME_HEADERS:
        return "name"
    if key in ID_HEADERS or "name" in key:
        return "id"
    return None


def extract_names_from_table(rows: list[list[str]], header_detected: bool = False) -> list[str]:
    cleaned = [[(c or "").strip() for c in row] for row in rows if any((c or "").strip() for c in row)]
    if not cleaned:
        return []
    header = cleaned[0]
    kinds = [_header_kind(c) for c in header]
    name_idx = next((i for i, kind in enumerate(kinds) if kind == "name"), None)
    id_idx = next((i for i, kind in enumerate(kinds) if kind in ("name", "id")), None)
    body = cleaned[1:] if (name_idx is not None or id_idx is not None or header_detected) else cleaned
    if name_idx is None and id_idx is None:
        id_idx = 0
    names: list[str] = []
    for row in body:
        idx = name_idx if name_idx is not None else id_idx
        value = row[idx] if idx is not None and idx < len(row) else ""
        value = drop_list_number(value)  # drop "1. " list numbering
        if value:
            names.append(value)
    return names


def names_from_plain_text(text: str) -> list[str]:
    """Parse names from free text: skip title lines, drop "1. " numbering, split on separators."""
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    while lines and _looks_like_heading(lines[0]):
        lines.pop(0)
    if not lines:
        return []
    first = split_line(lines[0])
    if any(_header_kind(cell) for cell in first):
        rows = [first] + [split_line(line) for line in lines[1:]]
        return extract_names_from_table(rows, header_detected=True)
    names: list[str] = []
    for line in lines:
        for part in split_line(drop_list_number(line)):
            part = part.strip()
            if part and not part.isdigit():
                names.append(part)
    return names


def parse_roster(filename: str, data: bytes) -> dict[str, Any]:
    name = (filename or "").lower()
    text = ""
    table: list[list[str]] = []
    names: list[str] = []
    preparsed = False
    fmt = "txt"
    if name.endswith(".xlsx"):
        fmt = "xlsx"
        table = parse_xlsx_rows(data)
        if not table:
            try:
                wb = load_workbook(io.BytesIO(data), read_only=True, data_only=True)
                for ws in wb.worksheets:
                    for row in ws.iter_rows(values_only=True):
                        table.append(["" if v is None else str(v) for v in row])
                    if table:
                        break
                wb.close()
            except Exception:
                table = []
    elif name.endswith(".docx"):
        fmt = "docx"
        paragraphs, tables = parse_docx_blocks(data)
        doc_names: list[str] = []
        for table_rows in tables:
            doc_names.extend(extract_names_from_table(table_rows))
        if not doc_names:
            # Drops title-like lines such as "2026 年会抽奖名单（共 30 人）".
            doc_names = names_from_plain_text("\n".join(paragraphs))
        names = doc_names
        preparsed = True
    elif name.endswith(".xls"):
        raise HTTPException(
            status_code=422,
            detail="不支持旧版 .xls 格式，请在 Excel / WPS 中「另存为」.xlsx 或 .csv 后再上传",
        )
    elif name.endswith(".doc"):
        raise HTTPException(
            status_code=422,
            detail="不支持旧版 .doc 格式，请在 Word / WPS 中「另存为」.docx 后再上传",
        )
    elif name.endswith(".csv"):
        fmt = "csv"
        text = data.decode("utf-8-sig", errors="replace")
    else:
        text = data.decode("utf-8-sig", errors="replace")

    if preparsed:
        pass
    elif table:
        names = extract_names_from_table(table)
    else:
        text = strip_code_comments(text, name)
        names = names_from_plain_text(text)

    seen: set[str] = set()
    unique: list[str] = []
    for raw in names:
        clean = clean_text(raw, MAX_TEXT_SHORT)
        if not clean:
            continue
        key = clean.casefold()
        if key in seen:
            continue
        seen.add(key)
        unique.append(clean)
        if len(unique) >= MAX_ROSTER_ITEMS:
            break
    return {"names": unique, "format": fmt}


#---------------------------------------------------------------------------
# Request models
#---------------------------------------------------------------------------


class CreateRoomRequest(BaseModel):
    pin: str = Field(min_length=4, max_length=200)
    name: Optional[str] = Field(default=None, max_length=120)


class AdminLoginRequest(BaseModel):
    pin: str = Field(min_length=1, max_length=200)


class AdminPinRequest(BaseModel):
    new_pin: str = Field(min_length=4, max_length=200)


class JoinRequest(BaseModel):
    name: str = Field(min_length=1, max_length=80)


class RosterReplaceRequest(BaseModel):
    names: list[str] = Field(default_factory=list)
    mode: Literal["append", "replace"] = "replace"


class RosterItemRequest(BaseModel):
    name: str = Field(min_length=1, max_length=80)


class RosterItemPatchRequest(BaseModel):
    name: str = Field(min_length=1, max_length=80)


class EventUpdateRequest(BaseModel):
    name: Optional[str] = Field(default=None, max_length=120)
    registration_open: Optional[bool] = None
    pool_mode: Optional[Literal["registration", "roster"]] = None
    cooldown_enabled: Optional[bool] = None
    cooldown_days: Optional[int] = Field(default=None, ge=0, le=3650)


class PrizeRequest(BaseModel):
    title: str = Field(min_length=1, max_length=80)
    level: str = Field(default="", max_length=40)
    reward_type: Literal["cdk", "item"] = "item"
    reward_name: str = Field(default="", max_length=120)
    quantity: int = Field(default=1, ge=1, le=2000)
    cdks: list[str] = Field(default_factory=list)


class DrawRequest(BaseModel):
    title: str = Field(min_length=1, max_length=80)
    mode: Literal["wheel", "random"] = "wheel"
    prize_ids: list[str] = Field(min_length=1, max_length=100)


class ScheduleRequest(DrawRequest):
    scheduled_at: str = Field(min_length=1, max_length=80)


#---------------------------------------------------------------------------
# Room creation
#---------------------------------------------------------------------------


@app.post("/api/rooms")
async def api_create_room(payload: CreateRoomRequest, request: Request) -> dict[str, Any]:
    pin = payload.pin.strip()
    if len(pin) < 4:
        raise HTTPException(status_code=422, detail="PIN 请设置至少 4 位（建议 6 位数字）")
    if len(rooms) >= MAX_ROOMS:
        raise HTTPException(status_code=503, detail="服务器空间数量已达上限，请稍后再试")
    if len(pin) > 32:
        raise HTTPException(status_code=422, detail="PIN 最长 32 位")

    ip = client_ip(request)
    now = time.time()
    attempts = create_attempts[ip]
    while attempts and now - attempts[0] > ROOM_CREATE_WINDOW_SECONDS:
        attempts.popleft()
    if len(attempts) >= ROOM_CREATE_MAX_PER_IP:
        raise HTTPException(status_code=429, detail="创建空间过于频繁，请稍后再试")
    attempts.append(now)

    name = clean_text(payload.name or "", MAX_TEXT_MEDIUM)
    async with state_lock:
        for existing in rooms.values():
            if secrets.compare_digest(existing.pin, pin):
                raise HTTPException(status_code=409, detail="这个 PIN 已被其他发起者使用，请换一个")
        code = generate_room_code()
        room = Room(code=code, pin=pin, name=name or f"{code} 的抽奖活动")
        rooms[code] = room
    token, expires_at = issue_admin_session(room.code)
    return {
        "ok": True,
        "room": {"code": room.code, "name": room.name},
        "token": token,
        "expires_at": expires_at,
        "state": room.admin_state(),
    }


pin_attempts: dict[str, deque[float]] = defaultdict(deque)


@app.post("/api/admin/login")
async def api_admin_login_by_pin(payload: AdminLoginRequest, request: Request) -> dict[str, Any]:
    """Log in with just the organiser PIN (PINs are unique across rooms)."""
    pin = payload.pin.strip()
    ip = client_ip(request)
    now = time.time()
    attempts = pin_attempts[ip]
    while attempts and now - attempts[0] > LOGIN_WINDOW_SECONDS:
        attempts.popleft()
    if len(attempts) >= LOGIN_MAX_ATTEMPTS:
        retry_after = int(LOGIN_WINDOW_SECONDS - (now - attempts[0]))
        raise HTTPException(status_code=429, detail=f"尝试过于频繁，请 {max(1, retry_after // 60)} 分钟后再试")

    matches = [room for room in rooms.values() if secrets.compare_digest(room.pin, pin)]
    if not matches:
        attempts.append(now)
        raise HTTPException(status_code=401, detail="PIN 不正确，或该空间已被服务器重启清除")
    if len(matches) > 1:
        raise HTTPException(status_code=409, detail="该 PIN 对应多个空间，请使用空间专属后台链接登录")
    attempts.clear()
    room = matches[0]
    token, expires_at = issue_admin_session(room.code)
    return {
        "ok": True,
        "room": {"code": room.code, "name": room.name},
        "token": token,
        "expires_at": expires_at,
        "state": room.admin_state(),
    }


#---------------------------------------------------------------------------
# Pages
#---------------------------------------------------------------------------



@app.get("/")
async def index_page() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html", headers={"Cache-Control": "no-cache"})


@app.get("/s/{code}")
async def room_page(code: str) -> Response:
    if room_slug(code) not in rooms:
        return RedirectResponse(url="/?missing=" + quote(room_slug(code), safe=""), status_code=302)
    return FileResponse(STATIC_DIR / "index.html", headers={"Cache-Control": "no-cache"})


@app.get("/s/{code}/admin")
async def room_admin_page(code: str) -> Response:
    if room_slug(code) not in rooms:
        return RedirectResponse(url="/?missing=" + quote(room_slug(code), safe=""), status_code=302)
    return FileResponse(STATIC_DIR / "index.html", headers={"Cache-Control": "no-cache"})


@app.get("/admin")
async def legacy_admin_page(code: Optional[str] = Query(default=None)) -> Response:
    if code:
        return RedirectResponse(url=f"/s/{code}/admin", status_code=302)
    return RedirectResponse(url="/", status_code=302)


@app.get("/favicon.ico")
async def favicon() -> Response:
    return Response(status_code=204)


@app.get("/healthz")
async def healthz() -> dict[str, Any]:
    return {"status": "ok", "service": "lucky-draw", "rooms": len(rooms)}


#---------------------------------------------------------------------------
# Public / participant API
#---------------------------------------------------------------------------


@app.get("/api/public")
async def api_public(room: Optional[str] = Query(default=None)) -> dict[str, Any]:
    return get_room(room or "").public_state()


@app.post("/api/rooms/{code}/join")
async def api_join(code: str, payload: JoinRequest) -> dict[str, Any]:
    target = get_room(code)
    name = clean_text(payload.name, MAX_TEXT_SHORT)
    if not name:
        raise HTTPException(status_code=422, detail="请输入你的名字")
    async with state_lock:
        if target.pool_mode == "roster":
            raise HTTPException(status_code=403, detail="本次抽奖使用主办方导入的名单，无需现场报名")
        if not target.registration_open:
            raise HTTPException(status_code=403, detail="活动报名已关闭，无法再加入")
        if target.current_draw:
            raise HTTPException(status_code=409, detail="正在开奖，暂时无法加入")
        participant, reason = target.add_participant_name(name, source="join")
        if participant is None:
            if reason == "duplicate":
                raise HTTPException(status_code=409, detail="这个名字已经有人使用，请换一个名字")
            if reason == "full":
                raise HTTPException(status_code=409, detail="参与人数已达上限")
            raise HTTPException(status_code=422, detail="请输入你的名字")
        participant_index[participant.token] = target.code
    await broadcast_refresh(target)
    return {
        "participant": {"id": participant.id, "name": participant.name, "token": participant.token},
        "state": target.participant_state(participant),
    }


@app.post("/api/rooms/{code}/claim")
async def api_claim(code: str, payload: JoinRequest) -> dict[str, Any]:
    """Reopen this page as a name already on the participant/roster list."""
    target = get_room(code)
    participant = target.find_name(payload.name)
    if not participant:
        raise HTTPException(status_code=404, detail="没有找到这个已报名的名字")
    participant_index[participant.token] = target.code
    return {
        "participant": {"id": participant.id, "name": participant.name, "token": participant.token},
        "state": target.participant_state(participant),
    }


@app.get("/api/rooms/{code}/participant/state")
async def api_participant_state(code: str, authorization: Optional[str] = Header(default=None)) -> dict[str, Any]:
    target, participant = participant_from_token(get_bearer(authorization))
    if target.code != room_slug(code):
        raise HTTPException(status_code=403, detail="该参会凭证不属于这个抽奖空间")
    return target.participant_state(participant)


#---------------------------------------------------------------------------
# Admin API
#---------------------------------------------------------------------------



@app.post("/api/rooms/{code}/admin/login")
async def api_admin_login(code: str, payload: AdminLoginRequest, request: Request) -> dict[str, Any]:
    target = get_room(code)
    ip = client_ip(request)
    now = time.time()
    attempts = target.login_attempts
    while attempts and now - attempts[0] > LOGIN_WINDOW_SECONDS:
        attempts.popleft()
    if len(attempts) >= LOGIN_MAX_ATTEMPTS:
        retry_after = int(LOGIN_WINDOW_SECONDS - (now - attempts[0]))
        raise HTTPException(status_code=429, detail=f"尝试过于频繁，请 {max(1, retry_after // 60)} 分钟后再试")

    if not secrets.compare_digest(payload.pin.strip(), target.pin):
        attempts.append(now)
        raise HTTPException(status_code=401, detail="管理 PIN 不正确")
    attempts.clear()
    token, expires_at = issue_admin_session(target.code)
    return {"token": token, "expires_at": expires_at, "state": target.admin_state()}


@app.post("/api/rooms/{code}/admin/logout")
async def api_admin_logout(code: str, authorization: Optional[str] = Header(default=None)) -> dict[str, bool]:
    token = get_bearer(authorization)
    drop_admin_session(token)
    return {"ok": True}


@app.patch("/api/rooms/{code}/admin/pin")
async def api_admin_change_pin(code: str, payload: AdminPinRequest, authorization: Optional[str] = Header(default=None)) -> dict[str, Any]:
    token = get_bearer(authorization)
    room = resolve_admin_session(token)
    if room.code != room_slug(code):
        raise HTTPException(status_code=403, detail="该登录凭证不属于这个抽奖空间")
    pin = payload.new_pin.strip()
    if len(pin) < 4:
        raise HTTPException(status_code=422, detail="PIN 请设置至少 4 位（建议 6 位数字）")
    async with state_lock:
        for other in rooms.values():
            if other.code != room.code and secrets.compare_digest(other.pin, pin):
                raise HTTPException(status_code=409, detail="这个 PIN 已被其他发起者使用，请换一个")
        room.pin = pin
        drop_room_admin_sessions(room.code, keep=token)
    return {"ok": True, "state": room.admin_state()}


@app.get("/api/rooms/{code}/admin/state")
async def api_admin_state(code: str, who: dict[str, Any] = Depends(admin_guard_path)) -> dict[str, Any]:
    room = who["room"]
    return room.admin_state()


@app.patch("/api/rooms/{code}/admin/event")
async def api_update_event(code: str, payload: EventUpdateRequest, who: dict[str, Any] = Depends(admin_guard_path)) -> dict[str, Any]:
    room = who["room"]
    async with state_lock:
        if room.current_draw:
            raise HTTPException(status_code=409, detail="开奖过程中不能修改活动设置")
        if payload.name is not None:
            name = clean_text(payload.name, MAX_TEXT_MEDIUM)
            if not name:
                raise HTTPException(status_code=422, detail="活动名称不能为空")
            room.name = name
        if payload.pool_mode is not None:
            if payload.pool_mode != room.pool_mode:
                if room.history or room.current_draw or room.schedule:
                    raise HTTPException(status_code=409, detail="已有开奖记录或定时任务时不能切换参与方式")
                room.pool_mode = payload.pool_mode
                room.registration_open = True
        if payload.registration_open is not None:
            room.registration_open = payload.registration_open
        if payload.cooldown_enabled is not None:
            room.cooldown_enabled = payload.cooldown_enabled
        if payload.cooldown_days is not None:
            room.cooldown_days = payload.cooldown_days
    await broadcast_refresh(room)
    return {"ok": True, "state": room.admin_state()}


@app.post("/api/rooms/{code}/admin/prizes")
async def api_create_prize(code: str, payload: PrizeRequest, who: dict[str, Any] = Depends(admin_guard_path)) -> dict[str, Any]:
    room = who["room"]
    ensure_no_active_operation(room)
    title, level, reward_type, reward_name, quantity, cdks = validate_prize_payload(room, payload)
    async with state_lock:
        ensure_no_active_operation(room)
        prize = Prize(
            id=new_id(),
            title=title,
            level=level,
            reward_type=reward_type,
            reward_name=reward_name,
            quantity=quantity,
            cdks=cdks,
        )
        room.prizes.append(prize)
    await broadcast_refresh(room)
    return {"ok": True, "prize": prize.admin_view(), "state": room.admin_state()}


@app.patch("/api/rooms/{code}/admin/prizes/{prize_id}")
async def api_update_prize(code: str, prize_id: str, payload: PrizeRequest, who: dict[str, Any] = Depends(admin_guard_path)) -> dict[str, Any]:
    room = who["room"]
    ensure_no_active_operation(room)
    title, level, reward_type, reward_name, quantity, cdks = validate_prize_payload(room, payload, prize_id=prize_id)
    async with state_lock:
        ensure_no_active_operation(room)
        prize = get_prize_or_404(room, prize_id)
        prize.title = title
        prize.level = level
        prize.reward_type = reward_type
        prize.reward_name = reward_name
        prize.quantity = quantity
        prize.cdks = cdks
        if reward_type != "cdk":
            prize.assigned_cdks = []
        else:
            prize.assigned_cdks = [c for c in prize.assigned_cdks if c in set(cdks)]
    await broadcast_refresh(room)
    return {"ok": True, "prize": prize.admin_view(), "state": room.admin_state()}


@app.delete("/api/rooms/{code}/admin/prizes/{prize_id}")
async def api_delete_prize(code: str, prize_id: str, who: dict[str, Any] = Depends(admin_guard_path)) -> dict[str, Any]:
    room = who["room"]
    ensure_no_active_operation(room)
    async with state_lock:
        ensure_no_active_operation(room)
        get_prize_or_404(room, prize_id)
        room.prizes = [p for p in room.prizes if p.id != prize_id]
    await broadcast_refresh(room)
    return {"ok": True, "state": room.admin_state()}


# ----- roster -----


async def read_roster_upload(request: Request) -> tuple[str, bytes]:
    content_type = request.headers.get("content-type", "")
    if "multipart/form-data" not in content_type:
        raise HTTPException(status_code=422, detail="请上传 Excel / Word / TXT / CSV 文件")
    form = await request.form()
    upload = form.get("file")
    if upload is None or not hasattr(upload, "read"):
        raise HTTPException(status_code=422, detail="没有收到文件，请选择后重试")
    filename = getattr(upload, "filename", "") or "roster.txt"
    data = await upload.read()
    if not data:
        raise HTTPException(status_code=422, detail="文件是空的")
    if len(data) > MAX_IMPORT_BYTES:
        raise HTTPException(status_code=422, detail="文件太大（上限 5MB）")
    allowed = (".xlsx", ".xls", ".docx", ".doc", ".txt", ".csv")
    if not filename.lower().endswith(allowed):
        raise HTTPException(status_code=422, detail="仅支持 Excel(.xlsx/.csv)、Word(.docx)、TXT 文件")
    return filename, data


@app.post("/api/rooms/{code}/admin/roster/parse")
async def api_parse_roster(code: str, request: Request, who: dict[str, Any] = Depends(admin_guard_path)) -> dict[str, Any]:
    """Parse a roster file without saving, so the organiser can edit the list first."""
    filename, data = await read_roster_upload(request)
    parsed = parse_roster(filename, data)
    if not parsed["names"]:
        raise HTTPException(status_code=422, detail="没有从文件中识别到任何姓名，请检查文件内容（支持一列姓名或带「姓名」表头的表格）")
    return {"ok": True, "format": parsed["format"], "names": parsed["names"]}


@app.post("/api/rooms/{code}/admin/roster/import")
async def api_import_roster(code: str, request: Request, who: dict[str, Any] = Depends(admin_guard_path)) -> dict[str, Any]:
    room = who["room"]
    if room.current_draw:
        raise HTTPException(status_code=409, detail="开奖过程中不能导入名单")
    filename, data = await read_roster_upload(request)
    parsed = parse_roster(filename, data)
    names = parsed["names"]
    if not names:
        raise HTTPException(status_code=422, detail="没有从文件中识别到任何姓名，请检查文件内容（支持一列姓名或带「姓名」表头的表格）")

    # The file carries the pool, so switch the room to an imported roster.
    switch_to_roster = room.pool_mode != "roster"

    async with state_lock:
        if room.current_draw:
            raise HTTPException(status_code=409, detail="开奖过程中不能导入名单")
        before = len(room.participants)
        if switch_to_roster:
            room.pool_mode = "roster"
        added, duplicates, skipped = 0, 0, 0
        for name in names:
            participant, reason = room.add_participant_name(name, source="import")
            if participant:
                added += 1
            elif reason == "duplicate":
                duplicates += 1
            else:
                skipped += 1
    await broadcast_refresh(room)
    return {
        "ok": True,
        "format": parsed["format"],
        "switched_to_roster": switch_to_roster,
        "parsed": len(names),
        "added": added,
        "duplicates": duplicates,
        "skipped": skipped,
        "total": len(room.participants),
        "before": before,
        "state": room.admin_state(),
    }


@app.post("/api/rooms/{code}/admin/roster")
async def api_add_roster(code: str, payload: RosterItemRequest, who: dict[str, Any] = Depends(admin_guard_path)) -> dict[str, Any]:
    room = who["room"]
    if room.current_draw:
        raise HTTPException(status_code=409, detail="开奖过程中不能修改名单")
    async with state_lock:
        if room.current_draw:
            raise HTTPException(status_code=409, detail="开奖过程中不能修改名单")
        participant, reason = room.add_participant_name(payload.name, source="import")
        if participant is None:
            raise HTTPException(status_code=409, detail="名字为空或名单中已存在同名人员")
    await broadcast_refresh(room)
    return {"ok": True, "participant": participant.admin_brief(), "state": room.admin_state()}


@app.patch("/api/rooms/{code}/admin/roster/{participant_id}")
async def api_rename_roster(code: str, participant_id: str, payload: RosterItemPatchRequest, who: dict[str, Any] = Depends(admin_guard_path)) -> dict[str, Any]:
    room = who["room"]
    if room.current_draw:
        raise HTTPException(status_code=409, detail="开奖过程中不能修改名单")
    new_name = clean_text(payload.name, MAX_TEXT_SHORT)
    if not new_name:
        raise HTTPException(status_code=422, detail="名字不能为空")
    async with state_lock:
        if room.current_draw:
            raise HTTPException(status_code=409, detail="开奖过程中不能修改名单")
        participant = room.participants.get(participant_id)
        if not participant:
            raise HTTPException(status_code=404, detail="名单中没有这个人员")
        existing = room.find_name(new_name)
        if existing and existing.id != participant_id:
            raise HTTPException(status_code=409, detail="名单中已存在同名人员")
        participant.name = new_name
        participant.normalized_name = norm_name(new_name)
    await broadcast_refresh(room)
    return {"ok": True, "state": room.admin_state()}


@app.delete("/api/rooms/{code}/admin/roster/{participant_id}")
async def api_delete_roster(code: str, participant_id: str, who: dict[str, Any] = Depends(admin_guard_path)) -> dict[str, Any]:
    room = who["room"]
    if room.current_draw:
        raise HTTPException(status_code=409, detail="开奖过程中不能修改名单")
    async with state_lock:
        if room.current_draw:
            raise HTTPException(status_code=409, detail="开奖过程中不能修改名单")
        participant = room.participants.pop(participant_id, None)
        if not participant:
            raise HTTPException(status_code=404, detail="名单中没有这个人员")
    participant_index.pop(participant.token, None)
    await close_participant_sockets(room, participant.id)
    await broadcast_refresh(room)
    return {"ok": True, "state": room.admin_state()}


@app.post("/api/rooms/{code}/admin/roster/replace")
async def api_replace_roster(code: str, payload: RosterReplaceRequest, who: dict[str, Any] = Depends(admin_guard_path)) -> dict[str, Any]:
    room = who["room"]
    if room.current_draw:
        raise HTTPException(status_code=409, detail="开奖过程中不能修改名单")
    if len(payload.names) > MAX_ROSTER_ITEMS:
        raise HTTPException(status_code=422, detail=f"名单最多 {MAX_ROSTER_ITEMS} 人")
    async with state_lock:
        if room.current_draw:
            raise HTTPException(status_code=409, detail="开奖过程中不能修改名单")
        if payload.mode == "replace":
            room.participants.clear()
        room.pool_mode = "roster"
        added, duplicates, skipped = 0, 0, 0
        for name in payload.names:
            participant, reason = room.add_participant_name(name, source="import")
            if participant:
                added += 1
            elif reason == "duplicate":
                duplicates += 1
            else:
                skipped += 1
    drop_stale_participant_index(room.code)
    await broadcast_refresh(room)
    return {
        "ok": True,
        "mode": payload.mode,
        "added": added,
        "duplicates": duplicates,
        "skipped": skipped,
        "state": room.admin_state(),
    }


@app.delete("/api/rooms/{code}/admin/roster")
async def api_clear_roster(code: str, who: dict[str, Any] = Depends(admin_guard_path)) -> dict[str, Any]:
    room = who["room"]
    if room.current_draw:
        raise HTTPException(status_code=409, detail="开奖过程中不能修改名单")
    async with state_lock:
        if room.current_draw:
            raise HTTPException(status_code=409, detail="开奖过程中不能修改名单")
        tokens = [p.token for p in room.participants.values()]
        ids = list(room.participants.keys())
        room.participants.clear()
    for token in tokens:
        participant_index.pop(token, None)
    for pid in ids:
        await close_participant_sockets(room, pid)
    await broadcast_refresh(room)
    return {"ok": True, "state": room.admin_state()}


def drop_stale_participant_index(room_code: str) -> None:
    """Forget indexed tokens that no longer point at a live participant."""
    room = rooms.get(room_code)
    if not room:
        return
    live = {p.token for p in room.participants.values()}
    for token, scope in list(participant_index.items()):
        if scope == room_code and token not in live:
            participant_index.pop(token, None)


# ----- participants (legacy join-flow removal) -----


@app.delete("/api/rooms/{code}/admin/participants/{participant_id}")
async def api_remove_participant(code: str, participant_id: str, who: dict[str, Any] = Depends(admin_guard_path)) -> dict[str, Any]:
    room = who["room"]
    async with state_lock:
        if room.current_draw:
            raise HTTPException(status_code=409, detail="开奖过程中不能移除参与者")
        participant = room.participants.pop(participant_id, None)
        if not participant:
            raise HTTPException(status_code=404, detail="参与者不存在")
    participant_index.pop(participant.token, None)
    await close_participant_sockets(room, participant.id)
    await broadcast_refresh(room)
    return {"ok": True, "state": room.admin_state()}


# ----- draws -----


@app.post("/api/rooms/{code}/admin/draws")
async def api_start_draw(code: str, payload: DrawRequest, who: dict[str, Any] = Depends(admin_guard_path)) -> dict[str, Any]:
    room = who["room"]
    config = draw_config_from(room, payload)
    result = await launch_draw(room, config, scheduled=False)
    return {"ok": True, "draw": result["draw"], "admin": result["admin"], "state": room.admin_state()}


@app.post("/api/rooms/{code}/admin/schedule")
async def api_schedule_draw(code: str, payload: ScheduleRequest, who: dict[str, Any] = Depends(admin_guard_path)) -> dict[str, Any]:
    room = who["room"]
    config = draw_config_from(room, payload)
    try:
        scheduled_at = parse_iso(payload.scheduled_at)
    except (ValueError, TypeError):
        raise HTTPException(status_code=422, detail="定时开奖时间格式不正确")
    now = utc_now()
    if scheduled_at <= now + timedelta(seconds=2):
        raise HTTPException(status_code=422, detail="定时开奖时间必须晚于当前时间")
    if scheduled_at > now + timedelta(days=MAX_SCHEDULE_DAYS):
        raise HTTPException(status_code=422, detail=f"定时开奖最多只能设置 {MAX_SCHEDULE_DAYS} 天后")

    total_winners = sum(p.quantity for p in config["prizes"])
    eligible, _ = room.eligible()
    if total_winners > len(eligible):
        raise HTTPException(
            status_code=409,
            detail=f"当前可参与人数不足以抽出 {total_winners} 人，请增加参与者或减少奖项名额",
        )
    for prize in config["prizes"]:
        if prize.reward_type == "cdk" and len(prize.available_cdks()) < prize.quantity:
            raise HTTPException(status_code=409, detail=f"奖项「{prize.title}」剩余 CDK 不足")

    schedule_item = {
        "id": new_id(),
        "title": config["title"],
        "mode": config["mode"],
        "prize_ids": config["prize_ids"],
        "prizes": [p.snapshot() for p in config["prizes"]],
        "at": iso(scheduled_at),
        "created_at": iso(now),
        "total_winners": total_winners,
        "eligible_count": len(eligible),
    }
    async with state_lock:
        if room.current_draw:
            raise HTTPException(status_code=409, detail="正在开奖中，不能设置定时开奖")
        room.schedule = schedule_item
    cancel_schedule_task(room)
    room.schedule_task = asyncio.create_task(run_scheduled_draw(room, schedule_item["id"]))
    await broadcast_refresh(room)
    return {"ok": True, "schedule": schedule_item, "state": room.admin_state()}


@app.delete("/api/rooms/{code}/admin/schedule")
async def api_cancel_schedule(code: str, who: dict[str, Any] = Depends(admin_guard_path)) -> dict[str, Any]:
    room = who["room"]
    cancel_schedule_task(room)
    async with state_lock:
        room.schedule = None
    await broadcast_refresh(room)
    return {"ok": True, "state": room.admin_state()}


@app.post("/api/rooms/{code}/admin/history/clear")
async def api_clear_history(code: str, who: dict[str, Any] = Depends(admin_guard_path)) -> dict[str, Any]:
    room = who["room"]
    async with state_lock:
        if room.current_draw:
            raise HTTPException(status_code=409, detail="开奖过程中不能清空中奖记录")
        room.history = []
    await broadcast_refresh(room)
    return {"ok": True, "state": room.admin_state()}


# ----- export -----


def export_rows(room: Room, participant_only: bool = False) -> list[list[Any]]:
    rows: list[list[Any]] = []
    index = 1
    if participant_only:
        for record in reversed(room.history):
            for winner in record.get("winners", []):
                rows.append([index, winner.get("name", ""), winner.get("prize_title", ""), winner.get("cdk", "") if winner["reward_type"] == "cdk" else winner.get("reward_name", "")])
                index += 1
        return rows
    for record in reversed(room.history):
        for winner in record.get("winners", []):
            reward_type_label = "CDK" if winner["reward_type"] == "cdk" else "奖品名称"
            rows.append(
                [
                    index,
                    record["finished_at"],
                    record["title"],
                    winner.get("prize_level", ""),
                    winner.get("prize_title", ""),
                    reward_type_label,
                    winner.get("reward_name", ""),
                    winner.get("name", ""),
                    winner.get("cdk", "") if winner["reward_type"] == "cdk" else "",
                ]
            )
            index += 1
    return rows


def _excel_header(ws, headers: list[str]) -> None:
    ws.append(headers)
    header_fill = PatternFill("solid", fgColor="6D28D9")
    header_font = Font(color="FFFFFF", bold=True)
    for cell in ws[1]:
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center", vertical="center")


@app.get("/api/rooms/{code}/admin/export.xlsx")
async def api_export_xlsx(code: str, kind: Optional[str] = Query(default=None), who: dict[str, Any] = Depends(admin_guard_path)) -> StreamingResponse:
    room = who["room"]
    wb = Workbook()
    if kind == "participant":
        ws = wb.active
        ws.title = "中奖名单"
        _excel_header(ws, ["序号", "中奖者", "奖项名称", "CDK/奖品"])
        for row in export_rows(room, participant_only=True):
            ws.append(row)
        for idx, width in enumerate([8, 18, 24, 32], start=1):
            ws.column_dimensions[get_column_letter(idx)].width = width
        ws.freeze_panes = "A2"
        filename_prefix = "winners"
    else:
        ws = wb.active
        ws.title = "中奖记录"
        _excel_header(ws, ["序号", "开奖时间（UTC）", "抽奖名称", "奖品等级", "奖项名称", "奖品类型", "奖品内容", "中奖者", "CDK"])
        for row in export_rows(room):
            ws.append(row)
            excel_time = row[1]
            try:
                parsed = parse_iso(str(excel_time))
                ws.cell(row=ws.max_row, column=2).value = parsed.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                pass
        widths = [8, 22, 24, 16, 24, 12, 28, 18, 28]
        for idx, width in enumerate(widths, start=1):
            ws.column_dimensions[get_column_letter(idx)].width = width
        ws.freeze_panes = "A2"

        summary = wb.create_sheet("开奖汇总")
        _excel_header(summary, ["序号", "开奖时间（UTC）", "抽奖名称", "方式", "奖项", "中奖人数", "参与人数", "可抽人数"])
        for idx, record in enumerate(reversed(room.history), start=1):
            prize_summary = "、".join(f"{p['title']}×{p['quantity']}" for p in record.get("prizes", []))
            try:
                time_text = parse_iso(record["finished_at"]).strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                time_text = record.get("finished_at", "")
            summary.append(
                [
                    idx,
                    time_text,
                    record.get("title", ""),
                    "转盘抽奖" if record.get("mode") == "wheel" else "随机开奖",
                    prize_summary,
                    len(record.get("winners", [])),
                    record.get("participant_count", 0),
                    record.get("eligible_count", 0),
                ]
            )
        for idx, width in enumerate([8, 22, 24, 12, 44, 12, 12, 12], start=1):
            summary.column_dimensions[get_column_letter(idx)].width = width
        summary.freeze_panes = "A2"
        filename_prefix = "lucky_draw_results"

    stream = io.BytesIO()
    wb.save(stream)
    stream.seek(0)
    filename = f"{filename_prefix}_{room.code}_{utc_now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    return StreamingResponse(
        stream,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}", "Cache-Control": "no-store"},
    )


@app.get("/api/rooms/{code}/admin/export.txt")
async def api_export_txt(code: str, kind: Optional[str] = Query(default=None), who: dict[str, Any] = Depends(admin_guard_path)) -> StreamingResponse:
    room = who["room"]
    if kind == "participant":
        lines = ["序号\t中奖者\t奖项名称\tCDK/奖品"]
        for row in export_rows(room, participant_only=True):
            lines.append("\t".join(str(v).replace("\t", " ").replace("\n", " ") for v in row))
        prefix = "winners"
    else:
        lines = ["序号\t开奖时间(UTC)\t抽奖名称\t奖品等级\t奖项名称\t奖品类型\t奖品内容\t中奖者\tCDK"]
        for row in export_rows(room):
            lines.append("\t".join(str(value).replace("\t", " ").replace("\n", " ") for value in row))
        prefix = "lucky_draw_results"
    content = "﻿" + "\n".join(lines) + "\n"
    filename = f"{prefix}_{room.code}_{utc_now().strftime('%Y%m%d_%H%M%S')}.txt"
    return StreamingResponse(
        io.BytesIO(content.encode("utf-8")),
        media_type="text/plain; charset=utf-8",
        headers={"Content-Disposition": f"attachment; filename={filename}", "Cache-Control": "no-store"},
    )


#---------------------------------------------------------------------------
# WebSocket
#---------------------------------------------------------------------------



@app.websocket("/ws")
async def websocket_endpoint(
    websocket: WebSocket,
    role: str = Query(default="participant"),
    token: str = Query(default=""),
    room: str = Query(default=""),
) -> None:
    connected: Optional[tuple[Room, Participant]] = None
    admin_room: Optional[Room] = None

    if role == "admin":
        scope = admin_sessions.get(token)
        admin_room = rooms.get(scope or "")
        if not admin_room or admin_sessions_expiry.get(token, 0.0) <= time.time():
            await websocket.close(code=4401, reason="admin session expired")
            return
        if not room or admin_room.code != room_slug(room):
            await websocket.close(code=4401, reason="admin session expired")
            return
    else:
        try:
            connected = participant_from_token(token)
        except HTTPException:
            await websocket.close(code=4401, reason="invalid participant token")
            return
        if not room or connected[0].code != room_slug(room):
            await websocket.close(code=4401, reason="invalid participant token")
            return

    await websocket.accept()
    if admin_room:
        admin_room.admin_sockets.add(websocket)
    elif connected:
        connected[0].participant_sockets[connected[1].id].add(websocket)

    await send_json_safe(websocket, {"type": "hello", "role": "admin" if admin_room else "participant"})
    target = admin_room or connected[0]  # type: ignore[index]
    await broadcast_refresh(target)

    try:
        while True:
            raw = await websocket.receive_text()
            if raw == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        if admin_room:
            admin_room.admin_sockets.discard(websocket)
        elif connected:
            sockets = connected[0].participant_sockets.get(connected[1].id)
            if sockets:
                sockets.discard(websocket)
                if not sockets:
                    connected[0].participant_sockets.pop(connected[1].id, None)
        if target:
            await broadcast_refresh(target)


#---------------------------------------------------------------------------
# Startup
#---------------------------------------------------------------------------



@app.on_event("startup")
async def on_startup() -> None:
    print("Lucky draw server starting (multi-room, in-memory state)", flush=True)
