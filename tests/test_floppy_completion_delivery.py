"""A legacy write may be ambiguous. Only a negotiated, identified play can be replayed."""

from dataclasses import replace
from datetime import datetime
from types import SimpleNamespace

import pytest

from providers.scrobble.floppy import completions
from providers.scrobble.floppy.sink import FloppySink
from providers.auth._auth_FLOPPY import FloppyAuthError
from providers.scrobble.scrobble import ScrobbleEvent


@pytest.fixture
def route(monkeypatch, tmp_path):
    now = datetime.fromisoformat("2026-01-02T14:02:00+04:00").timestamp()
    monkeypatch.setattr(completions, "time", SimpleNamespace(time=lambda: now))
    monkeypatch.setenv("CROSSWATCH_DB", str(tmp_path / "watcher.sqlite3"))
    cfg = {
        "floppy": {"server_url": "https://fixture.invalid", "api_token": "fixture-token", "verify_ssl": True}
    }
    sink = FloppySink(lambda: cfg)
    ev = ScrobbleEvent(
        action="stop",
        media_type="episode",
        ids={"tmdb_show": "700", "tmdb": "333"},
        title="Fixture series",
        year=None,
        season=0,
        number=1,
        progress=99,
        account="Fixture",
        server_uuid="fixture",
        session_key="session",
        raw={},
        duration_ms=6000000,
        position_ms=5940000,
        observed_at="2026-01-02T14:01:02.123456+04:00",
    )
    monkeypatch.setattr("providers.scrobble.floppy.sink.record_watch", lambda *a, **k: None)
    return sink, ev, cfg


@pytest.mark.parametrize(
    "error", ["malformed", "wrong_item", "wrong_time", "wrong_key", "naive_time", "wrong_play"]
)
def test_uncertain_ack_requires_reconciliation(monkeypatch, route, error):
    sink, ev, _ = route
    target = completions.target(ev)
    monkeypatch.setattr(completions, "api_get", lambda *a, **k: {"version": 1, **target})
    requests = []

    def post(adapter, path, *, headers, json, allow_redirects):
        requests.append((path, json))
        assert path == completions.PATH and not allow_redirects
        ack = {**target, "event_id": headers["Idempotency-Key"], "play_id": 1, "played_at": ev.observed_at}
        if error == "malformed":
            return None
        name, value = {
            "wrong_item": ("episode_number", 4),
            "wrong_time": ("played_at", "2026-01-02T14:05:00+04:00"),
            "wrong_key": ("event_id", "other"),
            "naive_time": ("played_at", "2026-01-02T10:01:02.123456"),
            "wrong_play": ("play_id", True),
        }[error]
        return {**ack, name: value}

    monkeypatch.setattr(completions, "api_post", post)
    assert sink.send(ev)["error"] == "completion_unconfirmed"
    assert sink.send(ev)["error"] == "completion_unconfirmed"
    sink.retry_completion()
    assert len(requests) == 1


@pytest.mark.parametrize("mode", ["route_missing", "movie_only"])
def test_old_peer_receives_a_single_legacy_attempt(monkeypatch, route, mode):
    sink, ev, _ = route
    paths = []

    def probe(*args, **kwargs):
        if mode == "route_missing":
            raise FloppyAuthError("fixture missing", reason="http_404", status_code=404)
        return {"version": 1, "media_type": "movie"}

    def interrupted(adapter, path, **kwargs):
        paths.append(path)
        raise ConnectionError("fixture disconnect")

    monkeypatch.setattr(completions, "api_get", probe)
    monkeypatch.setattr(completions, "api_post", interrupted)
    assert sink.send(ev)["ok"] is False
    sink.retry_completion()
    assert sink.send(ev)["error"] == "completion_unconfirmed"
    assert paths == ["scrobble"]


def test_probe_never_guesses_series_id(route):
    _, ev, _ = route
    assert completions.target(replace(ev, ids={"tmdb": "333"})) is None
    assert completions.target(replace(ev, season=-1)) is None
    assert completions.target(replace(ev, number=0)) is None
    assert completions.target(ev) == {
        "tmdb": "700",
        "media_type": "episode",
        "season_number": 0,
        "episode_number": 1,
    }


def test_sticky_protocol_and_scoped_restart(monkeypatch, route):
    sink, ev, cfg = route
    probes, posts = [], []

    def probe(adapter, path, *, params, allow_redirects):
        probes.append(params)
        assert not allow_redirects
        return {"version": 1, **params}

    def post(adapter, path, *, headers, json, allow_redirects):
        assert not allow_redirects and path == completions.PATH
        posts.append((headers["Idempotency-Key"], json))
        if len(posts) == 1:
            raise ConnectionError("fixture reply lost")
        return {
            **completions.target(ev),
            "event_id": posts[0][0],
            "play_id": 8,
            "played_at": "2026-01-02T10:01:02.123456Z",
        }

    monkeypatch.setattr(completions, "api_get", probe)
    monkeypatch.setattr(completions, "api_post", post)
    assert sink.send(ev)["ok"] is False
    c = completions.journal()
    with c:
        c.execute("UPDATE floppy_completions SET next_try=0")
    wrong = {"floppy": {**cfg["floppy"], "api_token": "different-fixture-account"}}
    FloppySink(lambda: wrong).retry_completion()
    assert len(posts) == 1
    from providers.scrobble.floppy import sink as module

    monkeypatch.setattr(module, "record_scrobble_event", lambda *a, **k: None)
    FloppySink(lambda: cfg).retry_completion()
    assert posts[0] == posts[1] and len(probes) == 1
    assert sink.send(ev)["reason"] == "session_completed"
    row = c.execute("SELECT * FROM floppy_completions").fetchone()
    assert "fixture-token" not in repr(tuple(row))


@pytest.mark.parametrize(
    "field,new", [("api_token", "replacement"), ("server_url", "https://other.invalid"), ("timeout", 9)]
)
def test_duplicate_stop_cannot_move_an_existing_viewing(monkeypatch, route, field, new):
    sink, ev, cfg = route
    target = completions.target(ev)
    posts = []
    monkeypatch.setattr(completions, "api_get", lambda *a, **k: {"version": 1, **target})

    def lost(adapter, path, **kwargs):
        posts.append(kwargs["headers"]["Idempotency-Key"])
        raise ConnectionError("fixture acknowledgement lost")

    monkeypatch.setattr(completions, "api_post", lost)
    assert not sink.send(ev)["ok"]
    cfg["floppy"][field] = new
    replacement = FloppySink(lambda: cfg)
    duplicate = replace(ev, observed_at="2026-01-02T14:01:03.000000+04:00")
    assert replacement.send(duplicate)["error"] == "completion_route_changed"
    assert len(posts) == 1
    # A real new viewing can use the newly configured destination.
    monkeypatch.setattr("providers.scrobble.floppy.sink.api_post", lambda *a, **k: {})
    replacement.send(replace(ev, action="start", progress=0, position_ms=0))
    assert not replacement.send(replace(ev, observed_at="2026-01-02T14:02:00+04:00"))["ok"]
    assert len(posts) == 2 and posts[0] != posts[1]
    # A dated redelivery of the original is still bound to its own destination.
    assert replacement.send(ev)["error"] == "completion_route_changed"
    assert len(posts) == 2


def test_existing_state_receives_completion_journal(tmp_path):
    import sqlite3
    from cw_platform.local_db.schema import apply_schema, SCHEMA_VERSION

    with sqlite3.connect(tmp_path / "existing.sqlite3") as old:
        apply_schema(old)
        old.execute("DROP TABLE floppy_completions")
        old.execute("DELETE FROM schema_migrations WHERE version=?", (SCHEMA_VERSION,))
        old.execute("INSERT OR IGNORE INTO schema_migrations VALUES (7, 1, 'local_state')")
        old.execute("INSERT INTO ttl_dedupe_entries VALUES ('fixture', 'older', 1, 2, 3)")
    with sqlite3.connect(tmp_path / "existing.sqlite3") as reopened:
        apply_schema(reopened)
        apply_schema(reopened)
        assert reopened.execute("SELECT count(*) FROM floppy_completions").fetchone() == (0,)
        assert reopened.execute("SELECT dedupe_key FROM ttl_dedupe_entries").fetchall() == [("older",)]
        assert reopened.execute(
            "SELECT count(*) FROM schema_migrations WHERE version=?", (SCHEMA_VERSION,)
        ).fetchone() == (1,)


def test_expired_dated_redelivery_cannot_create_a_new_uuid(monkeypatch, route):
    sink, ev, _ = route
    monkeypatch.setattr(
        completions, "api_get", lambda *a, **k: pytest.fail("stale viewing reached the network")
    )
    stale = replace(ev, observed_at="2026-01-02T07:00:00+04:00")
    assert sink.send(stale)["error"] == "completion_time_outside_window"
    assert completions.journal().execute("SELECT count(*) FROM floppy_completions").fetchone()[0] == 0
