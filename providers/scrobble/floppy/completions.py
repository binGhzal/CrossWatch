"""Journal each dated Floppy completion before its first network operation.

An active watcher retries admitted requests for one hour. The same logical source
session and original instant retain their destination binding for six hours.
An older explicitly dated event needs manual reconciliation. An undated source
supplies only the time first observed locally. An ambiguous legacy POST is held.
"""

from __future__ import annotations

import hashlib
import json
import time
from datetime import datetime
from sqlite3 import Row
from uuid import uuid4

from cw_platform.local_db.db import LocalDatabaseError, get_conn
from providers.auth._auth_FLOPPY import FloppyAuthError
from providers.sync.floppy._common import api_get, api_post

PATH = "scrobble/completions/v1"
RETRY_WINDOW = 3600
SESSION_TTL = 6 * 3600


def fingerprint(parts):
    return hashlib.sha256(json.dumps(parts, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def target(ev):
    if ev.media_type == "movie":
        tmdb = (ev.ids or {}).get("tmdb")
        coords = {}
    elif (
        ev.media_type == "episode"
        and type(ev.season) is int
        and type(ev.number) is int
        and 0 <= ev.season <= 2_147_483_647
        and 1 <= ev.number <= 2_147_483_647
    ):
        tmdb = (ev.ids or {}).get("tmdb_show")
        coords = {"media_type": "episode", "season_number": ev.season, "episode_number": ev.number}
    else:
        return None
    tmdb = str(tmdb or "")
    if not tmdb.isascii() or not tmdb.isdecimal() or len(tmdb) > 20 or tmdb.startswith("0"):
        return None
    return {"tmdb": tmdb, **coords}


def current_observation(played_at: str) -> bool:
    # After a session's tombstone expires we cannot identify a delayed replay.
    # Refuse to mint another key for an old, previously dated notification.
    try:
        stamp = datetime.fromisoformat(played_at.replace("Z", "+00:00"))
        age = time.time() - stamp.timestamp()
        return stamp.utcoffset() is not None and -300 <= age < SESSION_TTL
    except (AttributeError, TypeError, ValueError, OverflowError, OSError):
        return False


def journal():
    conn = get_conn()
    if conn is None:
        raise LocalDatabaseError("Floppy completion journal unavailable")
    return conn


def new_session(session: str) -> None:
    c = journal()
    with c:
        c.execute("UPDATE floppy_completions SET session=NULL WHERE session=?", (session,))


def remember(scope: str, session: str, ev, body: dict, coords: dict) -> Row:
    # Preserve only the normalized request and the fields for ordinary success recording.
    # Raw notifications and provider configuration can contain credentials.
    event = {
        k: getattr(ev, k)
        for k in (
            "media_type",
            "ids",
            "title",
            "year",
            "season",
            "number",
            "progress",
            "account",
            "server_uuid",
            "session_key",
        )
    }
    if isinstance((ev.raw or {}).get("_cw_episode_group_delivery"), str):
        event["group_delivery"] = ev.raw["_cw_episode_group_delivery"]
    observation = fingerprint([session, body["played_at"]])
    stamp = time.time()
    c = journal()
    with c:
        c.execute("DELETE FROM floppy_completions WHERE created < ?", (stamp - SESSION_TTL,))
        c.execute(
            """INSERT INTO floppy_completions
            (event_id,scope,session,created,phase,request,event,target,observation,next_try,attempts)
            VALUES (?,?,?,?,'probe',?,?,?,?,?,0) ON CONFLICT DO NOTHING""",
            (
                str(uuid4()),
                scope,
                session,
                stamp,
                json.dumps(body),
                json.dumps(event),
                json.dumps(coords),
                observation,
                stamp + 30,
            ),
        )
        return c.execute(
            """SELECT * FROM floppy_completions WHERE observation=? OR session=?
            ORDER BY observation=? DESC LIMIT 1""",
            (observation, session, observation),
        ).fetchone()


def phase_for(event_id: str) -> str | None:
    row = journal().execute("SELECT phase FROM floppy_completions WHERE event_id=?", (event_id,)).fetchone()
    return row["phase"] if row else None


def next_due(scope: str, accept=None) -> Row | None:
    now = time.time()
    c = journal()
    cursor = c.execute(
        """SELECT * FROM floppy_completions WHERE scope=? AND phase IN ('probe','guarded')
        AND next_try <= ? AND created >= ? ORDER BY next_try""",
        (scope, now, now - RETRY_WINDOW),
    )
    try:
        return next((row for row in cursor if accept is None or accept(row)), None)
    finally:
        cursor.close()


def transition(row: Row, old: str, new: str) -> bool:
    c = journal()
    with c:
        return (
            c.execute(
                "UPDATE floppy_completions SET phase=? WHERE event_id=? AND phase=?",
                (new, row["event_id"], old),
            ).rowcount
            == 1
        )


def postpone(row: Row) -> None:
    c = journal()
    attempts = min(5, row["attempts"] + 1)
    with c:
        c.execute(
            """UPDATE floppy_completions SET next_try=?, attempts=? WHERE event_id=?
            AND phase IN ('probe','guarded')""",
            (time.time() + min(300, 30 * 2 ** (attempts - 1)), attempts, row["event_id"]),
        )


def agrees(reply, coords):
    expected = (
        {"version": 1, "media_type": "movie"} if "media_type" not in coords else {"version": 1, **coords}
    )
    return isinstance(reply, dict) and all(
        type(reply.get(k)) is type(v) and reply[k] == v for k, v in expected.items()
    )


def confirmed(reply, event_id, coords, original):
    if (
        not isinstance(reply, dict)
        or reply.get("event_id") != event_id
        or type(reply.get("play_id")) is not int
        or reply["play_id"] <= 0
    ):
        return False
    if "media_type" in coords and any(
        type(reply.get(k)) is not type(v) or reply[k] != v for k, v in coords.items()
    ):
        return False
    try:
        ack = datetime.fromisoformat(reply["played_at"].replace("Z", "+00:00"))
        sent = datetime.fromisoformat(original.replace("Z", "+00:00"))
        return ack.utcoffset() is not None and sent.utcoffset() is not None and ack == sent
    except (KeyError, ValueError, AttributeError, TypeError):
        return False


def deliver(adapter, row: Row) -> bool:
    """Return True only for the caller that first confirms this viewing."""
    coords, body = json.loads(row["target"]), json.loads(row["request"])
    phase = row["phase"]
    if phase == "done":
        return False
    if phase in ("legacy", "held") or time.time() - row["created"] >= RETRY_WINDOW:
        raise FloppyAuthError("Completion requires reconciliation", reason="completion_unconfirmed")
    if phase == "probe":
        try:
            reply = api_get(adapter, PATH, params=coords, allow_redirects=False)
        except FloppyAuthError as exc:
            if exc.status_code != 404:
                raise
            reply = None
        if agrees(reply, coords):
            phase = "guarded"
        elif reply is None or (coords.get("media_type") == "episode" and agrees(reply, {})):
            # A known older receiver gets one legacy request. Persist the transition
            # before sending so an interrupted legacy write can never be replayed.
            phase = "legacy"
        else:
            raise FloppyAuthError("Unexpected capability response", reason="completion_unconfirmed")
        if not transition(row, "probe", phase):
            raise FloppyAuthError("Another delivery owns this event", reason="delivery_in_progress")
    if phase == "legacy":
        api_post(adapter, "scrobble", json=body, allow_redirects=False)
    else:
        payload = {
            **coords,
            **{k: body[k] for k in ("played_at", "position_seconds", "duration_seconds") if k in body},
        }
        ack = api_post(
            adapter, PATH, json=payload, headers={"Idempotency-Key": row["event_id"]}, allow_redirects=False
        )
        if not confirmed(ack, row["event_id"], coords, body["played_at"]):
            raise FloppyAuthError(
                "Floppy did not confirm the original viewing", reason="completion_unconfirmed"
            )
    return transition(row, phase, "done")
