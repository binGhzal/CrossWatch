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


def test_queued_acceptance_survives_abrupt_process_exit_and_native_watcher_retry(route, monkeypatch, tmp_path):
    import json
    import os
    import subprocess
    import sys
    from pathlib import Path

    sink, ev, cfg = route
    cfg['scrobble'] = {'watch': {'route_provider': 'plex', 'route_sink': 'floppy', 'route_id': 'fixture'}}
    proof = tmp_path / 'accepted.json'
    child = r'''
import json, os, sqlite3, sys
from dataclasses import replace
from datetime import datetime
from threading import Event, Thread
from types import SimpleNamespace
from pathlib import Path
original_mkdir=Path.mkdir
def fixture_mkdir(path,*a,**k):
    if path.parts[:2]==('/', 'config'):
        path=Path(os.environ['CONFIG_BASE']).joinpath(*path.parts[2:])
    return original_mkdir(path,*a,**k)
Path.mkdir=fixture_mkdir
from providers.scrobble.floppy import completions, sink as module
from providers.scrobble.floppy.sink import FloppySink
from providers.scrobble.scrobble import Dispatcher, ScrobbleEvent
from providers.scrobble.watch_manager import MultiDispatcher
from providers.scrobble.plex import watch as plex_watch
from providers.scrobble.plex.watch import WatchService
cfg=json.loads(sys.argv[1]);proof=sys.argv[2]
completions.time=SimpleNamespace(time=lambda:datetime.fromisoformat('2026-01-02T14:02:00+04:00').timestamp())
module.record_watch=lambda *a,**k:None
entered=Event();blocked=Event()
def post(*a,**k):
    entered.set()
    blocked.wait(60)
module.api_post=post
sink=FloppySink(lambda:cfg);dispatcher=Dispatcher([sink],cfg_provider=lambda:cfg)
caller=MultiDispatcher([dispatcher])
ev=ScrobbleEvent('stop','episode',{'tmdb_show':'700','tmdb':'333'},'Fixture series',None,0,1,99,
    'Fixture','fixture','session',{},duration_ms=6000000,position_ms=5940000,
    observed_at='2026-01-02T14:01:02.123456+04:00')
Thread(target=lambda:caller.dispatch(replace(ev,action='start',progress=10,session_key='other')),daemon=True).start()
assert entered.wait(3)
plex_watch._cw_update=lambda *a,**k:None
watcher=WatchService(dispatcher=caller,cfg_provider=lambda:cfg)
def close(viewing):
    watcher._last_event['session']=replace(viewing,action='start')
    watcher._last_emit['session']=('start',99)
    watcher._last_seen['session']=completions.time.time()
    return watcher._emit_closing_stop('session','_cw_fixture_stop','fixture',None)
accepted=[close(ev)]
assert 'session' in watcher._stop_sent
# The provider's next genuine viewing clears its closing state and rearms the same session.
accepted.append(caller.dispatch(replace(ev,action='start',progress=1,observed_at=None)))
watcher._stop_sent.discard('session')
accepted.append(close(replace(ev,observed_at='2026-01-02T14:01:42.654321+04:00')))
assert 'session' in watcher._stop_sent
# An independent connection proves the rows were committed before the positive acknowledgements.
c=sqlite3.connect(os.environ['CROSSWATCH_DB'])
rows=c.execute('SELECT event_id,request FROM floppy_completions ORDER BY created,rowid').fetchall()
with open(proof,'w')as f:json.dump({'accepted':accepted,'rows':rows,'caller_consumed_stop':sorted(watcher._stop_sent)},f)
assert all(accepted) and len(rows)==2, f"accepted={accepted}; committed_completions={len(rows)}"
# No sink contact for either completion, no clean shutdown, and no in-memory queue replay.
os._exit(0)
'''
    env = dict(os.environ)
    result = subprocess.run([sys.executable, '-c', child, json.dumps(cfg), str(proof)],
                            cwd=Path(__file__).resolve().parents[1], env=env, capture_output=True, timeout=20)
    assert result.returncode == 0, result.stderr.decode()
    accepted = json.loads(proof.read_text())
    expected = [(key, json.loads(body)) for key, body in accepted['rows']]
    assert [body['played_at'] for _, body in expected] == [ev.observed_at, '2026-01-02T14:01:42.654321+04:00']
    expected = [(key, {**completions.target(ev), **{k: body[k] for k in
                 ('played_at', 'position_seconds', 'duration_seconds') if k in body}}) for key, body in expected]
    posted = []
    monkeypatch.setattr(completions, 'time', SimpleNamespace(time=lambda: datetime.fromisoformat('2026-01-02T14:02:31+04:00').timestamp()))
    monkeypatch.setattr(completions, 'api_get', lambda *a, **k: {'version': 1, **completions.target(ev)})

    def recovered(adapter, path, *, headers, json, allow_redirects):
        posted.append((headers['Idempotency-Key'], json))
        return {**completions.target(ev), 'event_id': headers['Idempotency-Key'],
                'play_id': len(posted), 'played_at': json['played_at']}

    monkeypatch.setattr(completions, 'api_post', recovered)
    from providers.scrobble.watch_manager import WatchGroup, _retry_deliveries
    from providers.scrobble.scrobble import Dispatcher
    # Use the actual watch-manager retry caller with no original notification or RAM queue.
    restored = FloppySink(lambda: cfg)
    dispatcher = Dispatcher([restored], cfg_provider=lambda: cfg)

    class TwoPasses:
        def __init__(self): self.calls = 0
        def wait(self, timeout):
            restored._poll_after = 0
            self.calls += 1
            return self.calls > 2
        def is_set(self): return False

    group = WatchGroup('plex', 'default', None,
                       [SimpleNamespace(dispatcher=dispatcher, sink=restored)], 0, TwoPasses())
    _retry_deliveries(group)
    assert posted == expected
    assert not dispatcher._pending
    assert {r['phase'] for r in completions.journal().execute('SELECT * FROM floppy_completions')} == {'done'}


def test_prepared_start_does_not_clear_a_later_durable_completion_twice(monkeypatch, route):
    from copy import deepcopy
    sink, ev, cfg = route
    cfg['scrobble'] = {'watch': {'route_provider': 'plex', 'route_sink': 'floppy', 'route_id': 'fixture'}}
    assert sink.admit_for_queue(ev, cfg)[1]
    start, admitted = sink.admit_for_queue(replace(ev, action='start', progress=1), cfg)
    assert admitted
    next_stop = replace(ev, observed_at='2026-01-02T14:01:42.654321+04:00')
    assert sink.admit_for_queue(next_stop, cfg)[1]
    before = [dict(r) for r in completions.journal().execute('SELECT * FROM floppy_completions ORDER BY rowid')]
    assert len(before) == 2 and before[1]['session']
    monkeypatch.setattr('providers.scrobble.floppy.sink.api_post', lambda *a, **k: {})
    sink.send(deepcopy(start), cfg)
    # The queued start's later network call cannot rearm the source session again.
    assert sink.admit_for_queue(replace(next_stop, observed_at='2026-01-02T14:01:49+04:00'), cfg)[1]
    after = [dict(r) for r in completions.journal().execute('SELECT * FROM floppy_completions ORDER BY rowid')]
    assert after == before


@pytest.mark.parametrize('mode', ['journal_unavailable', 'unverified_peer', 'unexpanded_group'])
def test_volatile_stop_is_not_acknowledged_without_supported_durable_admission(monkeypatch, route, mode):
    from providers.scrobble.scrobble import Dispatcher
    sink, ev, cfg = route
    cfg['scrobble'] = {'watch': {'route_provider': 'plex', 'route_sink': 'floppy', 'route_id': 'fixture'}}
    connection = completions.journal()
    if mode == 'journal_unavailable':
        def unavailable(*args, **kwargs):
            from cw_platform.local_db.db import LocalDatabaseError
            raise LocalDatabaseError('synthetic unavailable journal')
        monkeypatch.setattr(completions, 'remember', unavailable)
    elif mode == 'unverified_peer':
        cfg['floppy']['verify_ssl'] = False
    else:
        ev = replace(ev, raw={'_cw_episode_group': {'unexpanded': True}})
    dispatcher = Dispatcher([sink], cfg_provider=lambda: cfg)
    dispatcher._inflight.add(id(sink))
    assert not dispatcher.dispatch(ev)
    assert dispatcher._pending
    assert connection.execute('SELECT COUNT(*) FROM floppy_completions').fetchone()[0] == 0


def test_durable_retry_skips_blocked_session_without_starving_another(monkeypatch, route):
    from providers.scrobble.scrobble import Dispatcher
    sink, ev, cfg = route
    cfg['scrobble'] = {'watch': {'route_provider': 'plex', 'route_sink': 'floppy', 'route_id': 'fixture'}}
    dispatcher = Dispatcher([sink], cfg_provider=lambda: cfg)
    dispatcher._inflight.add(id(sink))
    assert dispatcher.dispatch(ev)
    dispatcher._inflight.clear()
    # A is waiting behind its own RAM-ordered sequence; B has no such dependency.
    key = dispatcher._session_key(sink, ev)
    dispatcher._retry_after[key] = (10**20, 0)
    other = replace(ev, session_key='independent-session')
    assert sink.admit_for_queue(other, cfg)[1]
    connection = completions.journal()
    rows = [dict(r) for r in connection.execute('SELECT * FROM floppy_completions ORDER BY rowid')]
    assert len(rows) == 2
    with connection:
        connection.execute('UPDATE floppy_completions SET next_try=1 WHERE event_id=?', (rows[0]['event_id'],))
        connection.execute('UPDATE floppy_completions SET next_try=2 WHERE event_id=?', (rows[1]['event_id'],))
    sent = []
    monkeypatch.setattr(completions, 'api_get', lambda *a, **k: {'version': 1, **completions.target(ev)})
    def post(adapter, path, *, headers, json, allow_redirects):
        sent.append(headers['Idempotency-Key'])
        return {**completions.target(ev), 'event_id': headers['Idempotency-Key'],
                'play_id': 1, 'played_at': json['played_at']}
    monkeypatch.setattr(completions, 'api_post', post)
    dispatcher.retry_persisted(sink)
    assert sent == [rows[1]['event_id']]
    assert completions.phase_for(rows[0]['event_id']) == 'probe'
    assert completions.phase_for(rows[1]['event_id']) == 'done'
    assert dispatcher._pending


@pytest.mark.parametrize('media_type', ['movie', 'episode'])
@pytest.mark.parametrize('first_action', ['stop', 'start'])
@pytest.mark.parametrize('temporary_failure', [False, True])
def test_first_inflight_event_is_prepared_before_later_viewing(monkeypatch, route, media_type, first_action, temporary_failure):
    import json
    from threading import Event, Thread
    from providers.scrobble.scrobble import Dispatcher
    sink, ev, cfg = route
    cfg['scrobble'] = {'watch': {'route_provider': 'plex', 'route_sink': 'floppy', 'route_id': 'fixture'}}
    ev = replace(ev, media_type=media_type, ids={'tmdb': '333', 'tmdb_show': '700'})
    if first_action == 'start':
        # A prior admitted stop is rearmed by the first direct start, exactly once.
        assert sink.admit_for_queue(ev, cfg)[1]
    if temporary_failure:
        import sqlite3
        method = 'remember' if first_action == 'stop' else 'new_session'
        real_admission = getattr(completions, method)
        attempted = []
        def unavailable_once(*a, **k):
            if not attempted:
                attempted.append(True)
                raise sqlite3.OperationalError('synthetic recoverable local failure')
            return real_admission(*a, **k)
        monkeypatch.setattr(completions, method, unavailable_once)
    entered, release = Event(), Event()
    original = sink.send
    posted = []
    def delayed(event, cfg=None):
        if not entered.is_set():
            entered.set()
            assert release.wait(5)
        return original(event, cfg)
    monkeypatch.setattr(sink, 'send', delayed)
    monkeypatch.setattr('providers.scrobble.floppy.sink.api_post', lambda *a, **k: {})
    monkeypatch.setattr(completions, 'api_get', lambda *a, **k: {'version': 1, 'media_type': ev.media_type, **completions.target(ev)})
    def post(adapter, path, *, headers, json, allow_redirects):
        posted.append((headers['Idempotency-Key'], json['played_at']))
        return {**completions.target(ev), 'event_id': headers['Idempotency-Key'],
                'play_id': len(posted), 'played_at': json['played_at']}
    monkeypatch.setattr(completions, 'api_post', post)
    dispatcher = Dispatcher([sink], cfg_provider=lambda: cfg)
    first = []
    initial = ev if first_action == 'stop' else replace(ev, action='start', progress=1)
    thread = Thread(target=lambda: first.append(dispatcher.dispatch(initial)))
    thread.start()
    assert entered.wait(3)
    second_time = '2026-01-02T14:01:52.456789+04:00'
    try:
        accepted = []
        if first_action == 'stop':
            accepted.append(dispatcher.dispatch(replace(ev, action='start', progress=1)))
        accepted.append(dispatcher.dispatch(replace(ev, observed_at=second_time)))
        # Inspect the actual committed SQLite rows while the first send is still blocked.
        rows = [dict(row) for row in completions.journal().execute(
            'SELECT * FROM floppy_completions ORDER BY rowid')]
        if temporary_failure:
            assert not accepted[-1]  # The later stop has not reached local durable storage.
            assert len(rows) == (0 if first_action == 'stop' else 1)
        else:
            assert [json.loads(row['request'])['played_at'] for row in rows] == [ev.observed_at, second_time]
            assert all(accepted)
    finally:
        release.set()
        thread.join(5)
    assert first == [True] and not thread.is_alive()
    for _ in range(3):
        dispatcher.retry_pending()
    rows = [dict(row) for row in completions.journal().execute(
        'SELECT * FROM floppy_completions ORDER BY rowid')]
    assert [json.loads(row['request'])['played_at'] for row in rows] == [ev.observed_at, second_time]
    # A delayed first start cannot clear the second stop and create a third request.
    assert sink.admit_for_queue(replace(ev, observed_at='2026-01-02T14:01:59+04:00'), cfg)[1]
    assert completions.journal().execute('SELECT count(*) FROM floppy_completions').fetchone()[0] == 2
    expected = [(row['event_id'], json.loads(row['request'])['played_at']) for row in rows]
    assert posted == (expected if first_action == 'stop' else expected[1:])
