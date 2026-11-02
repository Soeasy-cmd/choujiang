"""Lucky draw live room server.

Single active event, in-memory state, FastAPI + WebSocket.
Designed for a free Hugging Face Docker Space.
"""

from __future__ import annotations

import asyncio
import io
import os
import secrets
import time
from collections import OrderedDict, defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal, Optional
from urllib.parse import quote

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter
from pydantic import BaseModel, Field


BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

ADMIN_PIN = os.environ.get("ADMIN_PIN", "888888").strip() or "888888"
ADMIN_SESSION_TTL_SECONDS = 12 * 60 * 60
LOGIN_MAX_ATTEMPTS = 8
LOGIN_WINDOW_SECONDS = 10 * 60

WHEEL_REVEAL_SECONDS = 7.2
RANDOM_REVEAL_SECONDS = 1.4
MAX_PARTICIPANTS = 5000
MAX_TEXT_SHORT = 60
MAX_TEXT_MEDIUM = 120
MAX_CDKS_PER_PRIZE = 2000
MAX_SCHEDULE_DAYS = 365

app = FastAPI(title="幸运抽奖系统", version="1.0.0")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


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


@dataclass
class Participant:
    id: str
    name: str
    token: str
    joined_at: str
    normalized_name: str


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
class Event:
    name: str = "幸运抽奖活动"
    registration_open: bool = True
    cooldown_enabled: bool = False
    cooldown_days: int = 30
    participants: OrderedDict[str, Participant] = field(default_factory=OrderedDict)
    prizes: list[Prize] = field(default_factory=list)
    history: list[dict[str, Any]] = field(default_factory=list)
    schedule: Optional[dict[str, Any]] = None
    current_draw: Optional[dict[str, Any]] = None
    created_at: str = field(default_factory=lambda: iso(utc_now()))


event = Event()

state_lock = asyncio.Lock()
admin_sessions: dict[str, float] = {}
login_attempts: dict[str, deque[float]] = defaultdict(deque)
participant_sockets: dict[str, set[WebSocket]] = defaultdict(set)
admin_sockets: set[WebSocket] = set()
finish_task: Optional[asyncio.Task[Any]] = None
schedule_task: Optional[asyncio.Task[Any]] = None


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------


class JoinRequest(BaseModel):
    name: str = Field(min_length=1, max_length=80)


class AdminLoginRequest(BaseModel):
    pin: str = Field(min_length=1, max_length=200)


class EventUpdateRequest(BaseModel):
    name: Optional[str] = Field(default=None, max_length=120)
    registration_open: Optional[bool] = None
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


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------


def client_ip(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def issue_admin_session() -> tuple[str, int]:
    token = secrets.token_urlsafe(32)
    expires_at = time.time() + ADMIN_SESSION_TTL_SECONDS
    admin_sessions[token] = expires_at
    return token, int(expires_at)


def get_bearer(authorization: Optional[str]) -> str:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="缺少登录凭证")
    return authorization.split(" ", 1)[1].strip()


def require_admin(authorization: Optional[str] = Header(default=None)) -> str:
    token = get_bearer(authorization)
    expiry = admin_sessions.get(token)
    if not expiry or expiry <= time.time():
        admin_sessions.pop(token, None)
        raise HTTPException(status_code=401, detail="主办方登录已过期，请重新登录")
    return token


def participant_from_token(token: str) -> Participant:
    for participant in event.participants.values():
        if secrets.compare_digest(participant.token, token):
            return participant
    raise HTTPException(status_code=401, detail="参会凭证已失效，请重新加入活动")


def require_participant(authorization: Optional[str] = Header(default=None)) -> Participant:
    return participant_from_token(get_bearer(authorization))


# ---------------------------------------------------------------------------
# State helpers
# ---------------------------------------------------------------------------


def online_participant_ids() -> set[str]:
    return {pid for pid, sockets in participant_sockets.items() if sockets}


def participant_by_id(pid: str) -> Optional[Participant]:
    return event.participants.get(pid)


def cooldown_info() -> tuple[dict[str, str], list[dict[str, str]]]:
    """Return latest-win ISO by participant id, and excluded entries."""
    if not event.cooldown_enabled or event.cooldown_days <= 0:
        return {}, []
    cutoff = utc_now() - timedelta(days=event.cooldown_days)
    latest: dict[str, datetime] = {}
    for record in event.history:
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
        participant = participant_by_id(pid)
        if participant:
            excluded.append(
                {
                    "id": pid,
                    "name": participant.name,
                    "last_won_at": iso(won_at),
                }
            )
    excluded.sort(key=lambda item: item["name"])
    return {pid: iso(dt) for pid, dt in latest.items()}, excluded


def eligible_participants() -> tuple[list[Participant], list[dict[str, str]]]:
    _, excluded = cooldown_info()
    excluded_ids = {item["id"] for item in excluded}
    eligible = [p for p in event.participants.values() if p.id not in excluded_ids]
    return eligible, excluded


def event_status() -> str:
    if event.current_draw:
        return "drawing"
    if event.schedule:
        return "scheduled"
    if event.registration_open:
        return "registering"
    return "waiting"


def public_schedule() -> Optional[dict[str, Any]]:
    if not event.schedule:
        return None
    item = dict(event.schedule)
    return item


def public_current_draw() -> Optional[dict[str, Any]]:
    if not event.current_draw:
        return None
    draw = event.current_draw
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
        return {
            **base,
            "status": "excluded",
            "last_won_at": excluded.get("last_won_at"),
        }
    return {**base, "status": "not_won"}


def my_cooldown(participant_id: str) -> Optional[dict[str, Any]]:
    latest_iso, _ = cooldown_info()
    value = latest_iso.get(participant_id)
    if not value:
        return None
    return {
        "last_won_at": value,
        "until": iso(parse_iso(value) + timedelta(days=event.cooldown_days)),
    }


def prize_public(prize: Prize) -> dict[str, Any]:
    return prize.public_view()


def admin_state() -> dict[str, Any]:
    online_ids = online_participant_ids()
    eligible, excluded = eligible_participants()
    participants = []
    for participant in event.participants.values():
        participants.append(
            {
                "id": participant.id,
                "name": participant.name,
                "joined_at": participant.joined_at,
                "online": participant.id in online_ids,
            }
        )
    return {
        "role": "admin",
        "server_time": iso(utc_now()),
        "event": {
            "name": event.name,
            "status": event_status(),
            "registration_open": event.registration_open,
            "cooldown_enabled": event.cooldown_enabled,
            "cooldown_days": event.cooldown_days,
            "created_at": event.created_at,
        },
        "stats": {
            "participant_count": len(event.participants),
            "online_count": len(online_ids),
            "eligible_count": len(eligible),
            "excluded_count": len(excluded),
            "prize_count": len(event.prizes),
            "draw_count": len(event.history),
        },
        "participants": participants,
        "eligible_names": [p.name for p in eligible],
        "excluded": excluded,
        "prizes": [prize.admin_view() for prize in event.prizes],
        "history": [history_public(record, include_cdk=True) for record in reversed(event.history)],
        "schedule": public_schedule(),
        "current_draw": public_current_draw(),
        "drawing": bool(event.current_draw),
    }


def participant_state(participant: Participant) -> dict[str, Any]:
    online_ids = online_participant_ids()
    eligible, excluded = eligible_participants()
    my_results = [
        result
        for record in reversed(event.history)
        if (result := my_result_for(participant.id, record)) is not None
    ]
    return {
        "role": "participant",
        "server_time": iso(utc_now()),
        "event": {
            "name": event.name,
            "status": event_status(),
            "registration_open": event.registration_open,
            "cooldown_enabled": event.cooldown_enabled,
            "cooldown_days": event.cooldown_days,
        },
        "me": {
            "id": participant.id,
            "name": participant.name,
            "joined_at": participant.joined_at,
            "online": participant.id in online_ids,
            "cooldown": my_cooldown(participant.id),
        },
        "stats": {
            "participant_count": len(event.participants),
            "online_count": len(online_ids),
            "eligible_count": len(eligible),
            "excluded_count": len(excluded),
            "prize_count": len(event.prizes),
            "draw_count": len(event.history),
        },
        "prizes": [prize_public(p) for p in event.prizes],
        "history": [history_public(record, include_cdk=False) for record in reversed(event.history)],
        "my_results": my_results,
        "schedule": public_schedule(),
        "current_draw": public_current_draw(),
    }


def public_state() -> dict[str, Any]:
    return {
        "role": "public",
        "server_time": iso(utc_now()),
        "event": {
            "name": event.name,
            "status": event_status(),
            "registration_open": event.registration_open,
            "cooldown_enabled": event.cooldown_enabled,
            "cooldown_days": event.cooldown_days,
        },
        "stats": {
            "participant_count": len(event.participants),
            "online_count": len(online_participant_ids()),
            "prize_count": len(event.prizes),
            "draw_count": len(event.history),
        },
        "prizes": [prize_public(p) for p in event.prizes],
        "schedule": public_schedule(),
        "current_draw": public_current_draw(),
    }


# ---------------------------------------------------------------------------
# WebSocket broadcast
# ---------------------------------------------------------------------------


async def send_json_safe(ws: WebSocket, payload: dict[str, Any]) -> None:
    try:
        await ws.send_json(payload)
    except Exception:
        pass


async def broadcast_refresh() -> None:
    payload = {"type": "refresh"}
    targets: list[WebSocket] = list(admin_sockets)
    for sockets in participant_sockets.values():
        targets.extend(sockets)
    if targets:
        await asyncio.gather(*(send_json_safe(ws, payload) for ws in targets))


async def broadcast_event(payload: dict[str, Any]) -> None:
    targets: list[WebSocket] = list(admin_sockets)
    for sockets in participant_sockets.values():
        targets.extend(sockets)
    if targets:
        await asyncio.gather(*(send_json_safe(ws, payload) for ws in targets))


async def close_participant_sockets(participant_id: str) -> None:
    sockets = list(participant_sockets.get(participant_id, set()))
    for ws in sockets:
        try:
            await ws.close(code=4401, reason="participant removed")
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Validation and draw helpers
# ---------------------------------------------------------------------------


def get_prize_or_404(prize_id: str) -> Prize:
    prize = next((p for p in event.prizes if p.id == prize_id), None)
    if not prize:
        raise HTTPException(status_code=404, detail="奖项不存在或已被删除")
    return prize


def all_other_codes(exclude_prize_id: Optional[str] = None) -> set[str]:
    codes: set[str] = set()
    for prize in event.prizes:
        if prize.id == exclude_prize_id:
            continue
        codes.update(prize.cdks)
        codes.update(prize.assigned_cdks)
    return codes


def validate_prize_payload(payload: PrizeRequest, prize_id: Optional[str] = None) -> tuple[str, str, str, str, int, list[str]]:
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
        conflicts = set(cdks) & all_other_codes(exclude_prize_id=prize_id)
        if conflicts:
            sample = "、".join(sorted(conflicts)[:5])
            raise HTTPException(status_code=422, detail=f"CDK 与其它奖项重复：{sample}")

    return title, level, payload.reward_type, reward_name, payload.quantity, cdks


def ensure_no_active_operation() -> None:
    if event.current_draw:
        raise HTTPException(status_code=409, detail="正在开奖中，请等待本轮结束")
    if event.schedule:
        raise HTTPException(status_code=409, detail="已设置定时开奖，请先取消后再修改奖项或立即开奖")


def draw_config_from(request_data: DrawRequest) -> dict[str, Any]:
    title = clean_text(request_data.title, 80)
    if not title:
        raise HTTPException(status_code=422, detail="抽奖名称不能为空")
    prize_ids = list(dict.fromkeys(request_data.prize_ids))
    if not prize_ids:
        raise HTTPException(status_code=422, detail="请至少选择一个奖项")
    prizes = [get_prize_or_404(pid) for pid in prize_ids]
    return {"title": title, "mode": request_data.mode, "prize_ids": prize_ids, "prizes": prizes}


def prepare_draw(config: dict[str, Any], scheduled: bool) -> tuple[dict[str, Any], dict[str, Any]]:
    """Must be called while holding state_lock."""
    if event.current_draw:
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
    eligible, excluded = eligible_participants()
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
        "participant_ids": list(event.participants.keys()),
        "eligible_ids": [p.id for p in eligible],
        "excluded": excluded,
        "participant_count": len(event.participants),
        "eligible_count": len(eligible),
        "total_winners": total_winners,
        "prizes_snapshot": prizes_snapshot,
        "winners": winners,
        "pending_cdks": {k: list(v) for k, v in pending_cdks.items()},
        "scheduled": scheduled,
        "reveal_seconds": reveal_seconds,
    }
    event.current_draw = current
    event.registration_open = False

    public_start = {
        "id": draw_id,
        "title": config["title"],
        "mode": config["mode"],
        "started_at": iso(now),
        "reveal_at": iso(reveal_at),
        "eligible_count": len(eligible),
        "participant_count": len(event.participants),
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


def finalize_current_draw() -> dict[str, Any]:
    """Must be called while holding state_lock."""
    current = event.current_draw
    if not current:
        raise RuntimeError("No current draw")
    for prize_id, codes in current["pending_cdks"].items():
        prize = next((p for p in event.prizes if p.id == prize_id), None)
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
    event.history.append(record)
    event.current_draw = None
    return record


async def finish_draw_after(draw_id: str, delay_seconds: float) -> None:
    try:
        if delay_seconds > 0:
            await asyncio.sleep(delay_seconds)
        async with state_lock:
            if not event.current_draw or event.current_draw["id"] != draw_id:
                return
            record = finalize_current_draw()
        await broadcast_event(
            {
                "type": "draw_finished",
                "draw_id": draw_id,
                "draw": history_public(record, include_cdk=False),
            }
        )
        await broadcast_refresh()
    except asyncio.CancelledError:
        raise
    except Exception:
        pass


async def launch_draw(config: dict[str, Any], scheduled: bool = False) -> dict[str, Any]:
    global finish_task
    async with state_lock:
        if event.schedule and not scheduled:
            raise HTTPException(status_code=409, detail="已设置定时开奖，请先取消后再立即开奖")
        if scheduled:
            event.schedule = None
        public_start, admin_payload = prepare_draw(config, scheduled=scheduled)
        reveal_at = parse_iso(public_start["reveal_at"])
        delay = max(0.0, (reveal_at - utc_now()).total_seconds())
    await broadcast_event({"type": "draw_started", "draw": public_start})
    await broadcast_refresh()
    finish_task = asyncio.create_task(finish_draw_after(public_start["id"], delay))
    return {"draw": public_start, "admin": admin_payload}


# ---------------------------------------------------------------------------
# Scheduled draw
# ---------------------------------------------------------------------------


def cancel_schedule_task() -> None:
    global schedule_task
    if schedule_task and not schedule_task.done():
        schedule_task.cancel()
    schedule_task = None


async def run_scheduled_draw(schedule_id: str) -> None:
    try:
        while True:
            async with state_lock:
                scheduled = event.schedule
                if not scheduled or scheduled["id"] != schedule_id:
                    return
                due = parse_iso(scheduled["at"])
            remaining = (due - utc_now()).total_seconds()
            if remaining <= 0:
                break
            await asyncio.sleep(min(remaining, 30.0))

        # Time has arrived. Take the schedule out of state and attempt the draw.
        async with state_lock:
            scheduled = event.schedule
            if not scheduled or scheduled["id"] != schedule_id:
                return
            try:
                config = {
                    "title": scheduled["title"],
                    "mode": scheduled["mode"],
                    "prize_ids": scheduled["prize_ids"],
                    "prizes": [get_prize_or_404(pid) for pid in scheduled["prize_ids"]],
                }
                event.schedule = None
                public_start, admin_payload = prepare_draw(config, scheduled=True)
                delay = max(0.0, (parse_iso(public_start["reveal_at"]) - utc_now()).total_seconds())
            except HTTPException as exc:
                event.schedule = None
                error_message = exc.detail
                public_start = None
                admin_payload = None
                delay = 0
            except Exception as exc:  # defensive: never leave a broken schedule spinning
                event.schedule = None
                error_message = f"定时开奖执行失败：{exc}"
                public_start = None
                admin_payload = None
                delay = 0

        if public_start is None:
            await broadcast_event({"type": "schedule_failed", "message": error_message})
            await broadcast_refresh()
            return

        await broadcast_event(
            {
                "type": "draw_started",
                "draw": public_start,
                "scheduled": True,
            }
        )
        await broadcast_refresh()
        global finish_task
        finish_task = asyncio.create_task(finish_draw_after(public_start["id"], delay))
    except asyncio.CancelledError:
        raise
    except Exception:
        pass


# ---------------------------------------------------------------------------
# App routes
# ---------------------------------------------------------------------------


@app.get("/")
async def index_page() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/admin")
async def admin_page() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/favicon.ico")
async def favicon() -> Response:
    return Response(status_code=204)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok", "service": "lucky-draw"}


@app.get("/api/public")
async def api_public() -> dict[str, Any]:
    return public_state()


@app.post("/api/join")
async def api_join(payload: JoinRequest) -> dict[str, Any]:
    name = clean_text(payload.name, MAX_TEXT_SHORT)
    if not name:
        raise HTTPException(status_code=422, detail="请输入你的名字")
    normalized = name.casefold()
    async with state_lock:
        if not event.registration_open:
            raise HTTPException(status_code=403, detail="活动报名已关闭，无法再加入")
        if event.current_draw:
            raise HTTPException(status_code=409, detail="正在开奖，暂时无法加入")
        if len(event.participants) >= MAX_PARTICIPANTS:
            raise HTTPException(status_code=409, detail="参与人数已达上限")
        for participant in event.participants.values():
            if participant.normalized_name == normalized:
                raise HTTPException(status_code=409, detail="这个名字已经有人使用，请换一个名字")
        participant = Participant(
            id=new_id(),
            name=name,
            token=secrets.token_urlsafe(32),
            joined_at=iso(utc_now()),
            normalized_name=normalized,
        )
        event.participants[participant.id] = participant
    await broadcast_refresh()
    return {
        "participant": {"id": participant.id, "name": participant.name, "token": participant.token},
        "state": participant_state(participant),
    }


@app.get("/api/participant/state")
async def api_participant_state(participant: Participant = Depends(require_participant)) -> dict[str, Any]:
    return participant_state(participant)


@app.post("/api/admin/login")
async def api_admin_login(payload: AdminLoginRequest, request: Request) -> dict[str, Any]:
    ip = client_ip(request)
    now = time.time()
    attempts = login_attempts[ip]
    while attempts and now - attempts[0] > LOGIN_WINDOW_SECONDS:
        attempts.popleft()
    if len(attempts) >= LOGIN_MAX_ATTEMPTS:
        retry_after = int(LOGIN_WINDOW_SECONDS - (now - attempts[0]))
        raise HTTPException(status_code=429, detail=f"尝试过于频繁，请 {max(1, retry_after // 60)} 分钟后再试")

    if not secrets.compare_digest(payload.pin.strip(), ADMIN_PIN):
        attempts.append(now)
        raise HTTPException(status_code=401, detail="管理 PIN 不正确")
    attempts.clear()
    token, expires_at = issue_admin_session()
    return {"token": token, "expires_at": expires_at, "state": admin_state()}


@app.post("/api/admin/logout")
async def api_admin_logout(token: str = Depends(require_admin)) -> dict[str, bool]:
    admin_sessions.pop(token, None)
    return {"ok": True}


@app.get("/api/admin/state")
async def api_admin_state(_: str = Depends(require_admin)) -> dict[str, Any]:
    return admin_state()


@app.patch("/api/admin/event")
async def api_update_event(payload: EventUpdateRequest, _: str = Depends(require_admin)) -> dict[str, Any]:
    async with state_lock:
        if event.current_draw:
            raise HTTPException(status_code=409, detail="开奖过程中不能修改活动设置")
        if payload.name is not None:
            name = clean_text(payload.name, MAX_TEXT_MEDIUM)
            if not name:
                raise HTTPException(status_code=422, detail="活动名称不能为空")
            event.name = name
        if payload.registration_open is not None:
            if payload.registration_open and event.current_draw:
                raise HTTPException(status_code=409, detail="开奖过程中不能开放报名")
            event.registration_open = payload.registration_open
        if payload.cooldown_enabled is not None:
            event.cooldown_enabled = payload.cooldown_enabled
        if payload.cooldown_days is not None:
            event.cooldown_days = payload.cooldown_days
    await broadcast_refresh()
    return {"ok": True, "state": admin_state()}


@app.post("/api/admin/prizes")
async def api_create_prize(payload: PrizeRequest, _: str = Depends(require_admin)) -> dict[str, Any]:
    ensure_no_active_operation()
    title, level, reward_type, reward_name, quantity, cdks = validate_prize_payload(payload)
    async with state_lock:
        ensure_no_active_operation()
        prize = Prize(
            id=new_id(),
            title=title,
            level=level,
            reward_type=reward_type,
            reward_name=reward_name,
            quantity=quantity,
            cdks=cdks,
        )
        event.prizes.append(prize)
    await broadcast_refresh()
    return {"ok": True, "prize": prize.admin_view(), "state": admin_state()}


@app.patch("/api/admin/prizes/{prize_id}")
async def api_update_prize(prize_id: str, payload: PrizeRequest, _: str = Depends(require_admin)) -> dict[str, Any]:
    ensure_no_active_operation()
    title, level, reward_type, reward_name, quantity, cdks = validate_prize_payload(payload, prize_id=prize_id)
    async with state_lock:
        ensure_no_active_operation()
        prize = get_prize_or_404(prize_id)
        prize.title = title
        prize.level = level
        prize.reward_type = reward_type
        prize.reward_name = reward_name
        prize.quantity = quantity
        prize.cdks = cdks
        if reward_type != "cdk":
            prize.assigned_cdks = []
        else:
            prize.assigned_cdks = [code for code in prize.assigned_cdks if code in set(cdks)]
    await broadcast_refresh()
    return {"ok": True, "prize": prize.admin_view(), "state": admin_state()}


@app.delete("/api/admin/prizes/{prize_id}")
async def api_delete_prize(prize_id: str, _: str = Depends(require_admin)) -> dict[str, Any]:
    ensure_no_active_operation()
    async with state_lock:
        ensure_no_active_operation()
        get_prize_or_404(prize_id)
        event.prizes = [p for p in event.prizes if p.id != prize_id]
    await broadcast_refresh()
    return {"ok": True, "state": admin_state()}


@app.delete("/api/admin/participants/{participant_id}")
async def api_remove_participant(participant_id: str, _: str = Depends(require_admin)) -> dict[str, Any]:
    async with state_lock:
        if event.current_draw:
            raise HTTPException(status_code=409, detail="开奖过程中不能移除参与者")
        if participant_id not in event.participants:
            raise HTTPException(status_code=404, detail="参与者不存在")
        participant = event.participants.pop(participant_id)
    await close_participant_sockets(participant.id)
    await broadcast_refresh()
    return {"ok": True, "state": admin_state()}


@app.post("/api/admin/draws")
async def api_start_draw(payload: DrawRequest, _: str = Depends(require_admin)) -> dict[str, Any]:
    config = draw_config_from(payload)
    result = await launch_draw(config, scheduled=False)
    return {"ok": True, "draw": result["draw"], "admin": result["admin"], "state": admin_state()}


@app.post("/api/admin/schedule")
async def api_schedule_draw(payload: ScheduleRequest, _: str = Depends(require_admin)) -> dict[str, Any]:
    global schedule_task
    config = draw_config_from(payload)
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
    eligible, excluded = eligible_participants()
    if total_winners > len(eligible):
        raise HTTPException(
            status_code=409,
            detail=f"当前报名人数不足以抽出 {total_winners} 人，请增加参与者或减少奖项名额",
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
        if event.current_draw:
            raise HTTPException(status_code=409, detail="正在开奖中，不能设置定时开奖")
        event.schedule = schedule_item
    cancel_schedule_task()
    schedule_task = asyncio.create_task(run_scheduled_draw(schedule_item["id"]))
    await broadcast_refresh()
    return {"ok": True, "schedule": schedule_item, "state": admin_state()}


@app.delete("/api/admin/schedule")
async def api_cancel_schedule(_: str = Depends(require_admin)) -> dict[str, Any]:
    cancel_schedule_task()
    async with state_lock:
        event.schedule = None
    await broadcast_refresh()
    return {"ok": True, "state": admin_state()}


@app.post("/api/admin/history/clear")
async def api_clear_history(_: str = Depends(require_admin)) -> dict[str, Any]:
    async with state_lock:
        if event.current_draw:
            raise HTTPException(status_code=409, detail="开奖过程中不能清空中奖记录")
        event.history = []
    await broadcast_refresh()
    return {"ok": True, "state": admin_state()}


def export_rows() -> list[list[Any]]:
    rows: list[list[Any]] = []
    index = 1
    for record in reversed(event.history):
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


@app.get("/api/admin/export.xlsx")
async def api_export_xlsx(_: str = Depends(require_admin)) -> StreamingResponse:
    wb = Workbook()
    ws = wb.active
    ws.title = "中奖记录"
    headers = ["序号", "开奖时间（UTC）", "抽奖名称", "奖品等级", "奖项名称", "奖品类型", "奖品内容", "中奖者", "CDK"]
    ws.append(headers)
    header_fill = PatternFill("solid", fgColor="6D28D9")
    header_font = Font(color="FFFFFF", bold=True)
    for cell in ws[1]:
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center", vertical="center")
    for row in export_rows():
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
    summary.append(["序号", "开奖时间（UTC）", "抽奖名称", "方式", "奖项", "中奖人数", "参与人数", "可抽人数"])
    for cell in summary[1]:
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center", vertical="center")
    for idx, record in enumerate(reversed(event.history), start=1):
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

    stream = io.BytesIO()
    wb.save(stream)
    stream.seek(0)
    filename = f"lucky_draw_results_{utc_now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    headers_out = {
        "Content-Disposition": f"attachment; filename={filename}",
        "Cache-Control": "no-store",
    }
    return StreamingResponse(
        stream,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers_out,
    )


@app.get("/api/admin/export.txt")
async def api_export_txt(_: str = Depends(require_admin)) -> StreamingResponse:
    lines = ["序号\t开奖时间(UTC)\t抽奖名称\t奖品等级\t奖项名称\t奖品类型\t奖品内容\t中奖者\tCDK"]
    for row in export_rows():
        lines.append("\t".join(str(value).replace("\t", " ").replace("\n", " ") for value in row))
    content = "﻿" + "\n".join(lines) + "\n"
    filename = f"lucky_draw_results_{utc_now().strftime('%Y%m%d_%H%M%S')}.txt"
    return StreamingResponse(
        io.BytesIO(content.encode("utf-8")),
        media_type="text/plain; charset=utf-8",
        headers={
            "Content-Disposition": f"attachment; filename={filename}",
            "Cache-Control": "no-store",
        },
    )


# ---------------------------------------------------------------------------
# WebSocket
# ---------------------------------------------------------------------------


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, role: str = Query(default="participant"), token: str = Query(default="")):
    connected_participant: Optional[Participant] = None
    is_admin = False

    if role == "admin":
        expiry = admin_sessions.get(token)
        if not expiry or expiry <= time.time():
            await websocket.close(code=4401, reason="admin session expired")
            return
        is_admin = True
    else:
        try:
            connected_participant = participant_from_token(token)
        except HTTPException:
            await websocket.close(code=4401, reason="invalid participant token")
            return

    await websocket.accept()
    if is_admin:
        admin_sockets.add(websocket)
    elif connected_participant:
        participant_sockets[connected_participant.id].add(websocket)

    await send_json_safe(websocket, {"type": "hello", "role": "admin" if is_admin else "participant"})
    await broadcast_refresh()

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
        if is_admin:
            admin_sockets.discard(websocket)
        elif connected_participant:
            sockets = participant_sockets.get(connected_participant.id)
            if sockets:
                sockets.discard(websocket)
                if not sockets:
                    participant_sockets.pop(connected_participant.id, None)
        await broadcast_refresh()


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------


@app.on_event("startup")
async def on_startup() -> None:
    # Keep the process predictable and log only non-sensitive configuration.
    print(f"Lucky draw server starting; admin PIN configured: {'yes' if ADMIN_PIN else 'no'}", flush=True)
