# tests/test_scrobble_dispatch_retries.py
# CrossWatch - Scrobble Retry Isolation and Concurrency Regression Tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from dataclasses import replace
from threading import Event, Thread
from typing import Any

import pytest

from providers.scrobble.scrobble import Dispatcher, ScrobbleEvent


def event(**changes):
    return replace(ScrobbleEvent("start", "episode", {"tmdb": "10"}, "Series", 2020, 1, 1, 10,
                                "user", "server", "session", {}), **changes)


@pytest.fixture
def setup(monkeypatch):
    now = [100.0]
    monkeypatch.setattr("providers.scrobble.scrobble.time.monotonic", lambda: now[0])
    cfg = {"jellyfin": {"server": "http://destination", "user_id": "user", "access_token": "token"},
           "scrobble": {"watch": {"route_provider": "plex", "route_provider_instance": "default",
                                   "route_sink": "jellyfin", "route_sink_instance": "default"}}}
    return cfg, now


@pytest.mark.parametrize("unrelated", [{"theme": "dark"}, {"trakt": {"access_token": "refreshed"}}])
def test_unrelated_config_edits_keep_pending_delivery(setup, unrelated):
    cfg, now = setup
    calls = []
    class Sink:
        def send(self, event, cfg=None) -> Any:
            calls.append(event.session_key)
            return {"ok": len(calls) > 1}
    dispatcher = Dispatcher([Sink()], cfg_provider=lambda: cfg)
    dispatcher.dispatch(event())
    cfg.update(unrelated)
    now[0] += 30
    dispatcher.retry_pending()
    assert calls == ["session", "session"] and not dispatcher._pending


def test_enriched_ids_do_not_reset_session_dedup(setup):
    cfg, _ = setup
    calls = []
    class Sink:
        def send(self, event, cfg=None) -> Any:
            calls.append(event)
    dispatcher = Dispatcher([Sink()], cfg_provider=lambda: cfg)
    dispatcher.dispatch(event())
    dispatcher.dispatch(event(ids={"tmdb": "10", "imdb": "tt1234567"}))
    assert len(calls) == 1


def test_new_ids_allow_retry_after_a_previous_resolution_miss(setup):
    cfg, _ = setup
    calls = []
    class Sink:
        def send(self, event, cfg=None) -> Any:
            calls.append(event)
            return {"ok": bool(event.ids.get("imdb")), "retryable": False}
    dispatcher = Dispatcher([Sink()], cfg_provider=lambda: cfg)
    dispatcher.dispatch(event())
    assert dispatcher.dispatch(event(ids={"tmdb": "10", "imdb": "tt1234567"}))
    assert len(calls) == 2


@pytest.mark.parametrize("changes", [{"account": "other"}, {"server_uuid": "other"}, {"number": 2},
                                      {"raw": {"NowPlayingItem": {"Id": "other-movie"}}}])
def test_session_dedup_keeps_accounts_servers_and_items_separate(setup, changes):
    cfg, _ = setup
    calls = []
    class Sink:
        def send(self, event, cfg=None) -> Any:
            calls.append(event)
    dispatcher = Dispatcher([Sink()], cfg_provider=lambda: cfg)
    dispatcher.dispatch(event())
    dispatcher.dispatch(event(**changes))
    assert len(calls) == 2


def test_retry_pass_is_bounded_and_loads_config_once(setup):
    cfg, now = setup
    loads, calls = [], []
    def config():
        loads.append(True)
        return cfg
    class Sink:
        def send(self, event, cfg=None) -> Any:
            calls.append(event.session_key)
            return {"ok": False, "retryable": True}
    dispatcher = Dispatcher([Sink()], cfg_provider=config)
    for index in range(9):
        dispatcher.dispatch(event(session_key=str(index)))
    now[0] += 30
    calls.clear()
    loads.clear()
    dispatcher.retry_pending()
    assert len(calls) == 4 and len(loads) == 1


def test_failed_retry_keeps_newest_ids_and_progress(setup):
    cfg, now = setup
    calls = []
    class Sink:
        def send(self, event, cfg=None) -> Any:
            calls.append(event)
            return {"ok": len(calls) > 1}
    dispatcher = Dispatcher([Sink()], cfg_provider=lambda: cfg)
    dispatcher.dispatch(event())
    dispatcher.dispatch(event(action="stop", progress=95, ids={"tmdb": "10", "imdb": "tt1234567"}))
    assert len(dispatcher._pending) == 1
    now[0] += 30
    dispatcher.retry_pending()
    assert len(calls) == 2 and calls[1].action == "stop" and "imdb" in calls[1].ids


def test_live_completion_replaces_an_older_retry_after_backoff(setup):
    cfg, now = setup
    calls = []
    class Sink:
        def send(self, event, cfg=None) -> Any:
            calls.append(event.action)
            return {"ok": len(calls) > 1}
    dispatcher = Dispatcher([Sink()], cfg_provider=lambda: cfg)
    dispatcher.dispatch(event())
    now[0] += 30
    assert dispatcher.dispatch(event(action="stop", progress=95))
    dispatcher.retry_pending()
    assert calls == ["start", "stop"] and not dispatcher._pending


def test_changing_another_destination_instance_keeps_pending_delivery(setup):
    cfg, now = setup
    cfg["jellyfin"]["instances"] = {"P01": {"server": "http://other"}}
    calls = []
    class Sink:
        def send(self, event, cfg=None) -> Any:
            calls.append(event)
            return {"ok": len(calls) > 1}
    dispatcher = Dispatcher([Sink()], cfg_provider=lambda: cfg)
    dispatcher.dispatch(event())
    cfg["jellyfin"]["instances"]["P01"]["server"] = "http://changed"
    now[0] += 30
    dispatcher.retry_pending()
    assert len(calls) == 2 and not dispatcher._pending


def test_live_stop_does_not_wait_for_retry_io_and_is_not_overwritten(setup):
    cfg, now = setup
    entered, release, finished = Event(), Event(), Event()
    calls = []
    class Sink:
        def send(self, event, cfg=None) -> Any:
            calls.append(event.action)
            if len(calls) == 1:
                return {"ok": False, "retryable": True}
            if len(calls) == 2:
                entered.set()
                assert release.wait(3)
            return {"ok": True}
    dispatcher = Dispatcher([Sink()], cfg_provider=lambda: cfg)
    dispatcher.dispatch(event())
    now[0] += 30
    retry = Thread(target=dispatcher.retry_pending)
    retry.start()
    assert entered.wait(1)
    def live_stop():
        try:
            dispatcher.dispatch(event(action="stop", progress=95))
        finally:
            finished.set()
    live = Thread(target=live_stop)
    live.start()
    try:
        assert finished.wait(0.5)
    finally:
        release.set()
        retry.join(2)
        live.join(2)
    dispatcher.retry_pending()
    assert calls == ["start", "start", "stop"]
    assert not dispatcher._pending


def test_reconfigured_destination_is_not_acknowledged_by_old_inflight_delivery(setup):
    cfg, _ = setup
    entered, release = Event(), Event()
    calls = []
    class Sink:
        def send(self, event, cfg=None) -> Any:
            assert cfg is not None
            calls.append(cfg["jellyfin"]["user_id"])
            if len(calls) == 1:
                entered.set()
                assert release.wait(3)
            return {"ok": True}
    dispatcher = Dispatcher([Sink()], cfg_provider=lambda: cfg)
    original = Thread(target=lambda: dispatcher.dispatch(event()))
    original.start()
    assert entered.wait(1)
    try:
        cfg["jellyfin"]["user_id"] = "replacement"
        assert dispatcher.dispatch(event())
    finally:
        release.set()
        original.join(2)
    dispatcher.retry_pending()
    assert calls == ["user", "replacement"] and not dispatcher._pending


def test_old_retry_snapshot_cannot_cancel_a_new_route_queue(setup):
    cfg, now = setup
    class Sink:
        def send(self, event, cfg=None) -> Any:
            return {"ok": False, "retryable": True}
    dispatcher = Dispatcher([Sink()], cfg_provider=lambda: cfg)
    dispatcher.dispatch(event())
    old_identity = dispatcher._config_identity
    old_key = next(iter(dispatcher._pending))
    cfg["jellyfin"]["user_id"] = "replacement"
    dispatcher.dispatch(event(session_key="new-session"))
    current = list(dispatcher._pending)
    now[0] += 30
    assert not dispatcher._dispatch(event(), retry_key=old_key, cfg=cfg, config_identity=old_identity)
    assert list(dispatcher._pending) == current


def test_slow_retry_yields_before_attempting_the_rest_of_the_batch(setup):
    cfg, now = setup
    calls = []
    slow = [False]
    class Sink:
        def send(self, event, cfg=None) -> Any:
            calls.append(event.session_key)
            if slow[0]:
                now[0] += 2
            return {"ok": False, "retryable": True}
    dispatcher = Dispatcher([Sink()], cfg_provider=lambda: cfg)
    for index in range(5):
        dispatcher.dispatch(event(session_key=str(index)))
    now[0] += 30
    slow[0] = True
    calls.clear()
    dispatcher.retry_pending()
    assert calls == ["0"]


def test_permanent_failure_identity_cache_is_bounded(setup, monkeypatch):
    cfg, _ = setup
    monkeypatch.setattr("providers.scrobble.scrobble._log", lambda *a, **kw: None)
    calls = []
    class Sink:
        def send(self, event, cfg=None) -> Any:
            calls.append(event.session_key)
            return {"ok": False, "retryable": False}
    sink = Sink()
    dispatcher = Dispatcher([sink], cfg_provider=lambda: cfg)
    for index in range(1025):
        dispatcher.dispatch(event(session_key=str(index)))
    assert not dispatcher._pending
    assert len(dispatcher._failed_ids) == 1024
    assert dispatcher._session_key(sink, event(session_key="0")) not in dispatcher._failed_ids
    dispatcher.dispatch(event(session_key="0", ids={"tmdb": "10", "imdb": "tt1234567"}))
    assert calls.count("0") == 2


@pytest.mark.parametrize("retry", [False, True])
def test_sink_interruption_always_releases_inflight_and_allows_redelivery(setup, retry):
    cfg, now = setup
    calls = []
    class Interrupted(BaseException):
        pass
    class Sink:
        def send(self, event, cfg=None) -> Any:
            calls.append(event)
            if retry and len(calls) == 1:
                return {"ok": False, "retryable": True}
            if len(calls) == (2 if retry else 1):
                raise Interrupted()
            return {"ok": True}
    dispatcher = Dispatcher([Sink()], cfg_provider=lambda: cfg)
    if retry:
        dispatcher.dispatch(event())
        now[0] += 30
    with pytest.raises(Interrupted):
        if retry:
            dispatcher.retry_pending()
        else:
            dispatcher.dispatch(event())
    assert not dispatcher._inflight
    assert dispatcher.dispatch(event())
    assert not dispatcher._pending


def _floppy_queue_config(cfg):
    cfg['scrobble']['watch']['route_sink'] = 'floppy'
    cfg['floppy'] = {'server_url': 'https://fixture.invalid', 'api_token': 'fixture-token', 'verify_ssl': True}


def test_floppy_queued_viewings_keep_first_stop_time_and_following_restart(setup):
    cfg, _ = setup
    _floppy_queue_config(cfg)
    entered, release = Event(), Event()
    received = []
    class Sink:
        def send(self, ev, cfg=None):
            received.append(ev)
            if len(received) == 1:
                entered.set()
                assert release.wait(3)
            return {'ok': True}
    dispatcher = Dispatcher([Sink()], cfg_provider=lambda: cfg)
    blocker = Thread(target=lambda: dispatcher.dispatch(event(session_key='other')))
    blocker.start()
    assert entered.wait(1)
    first_time = '2026-01-02T10:01:02.123456Z'
    second_time = '2026-01-02T12:03:04.654321Z'
    first = event(action='stop', progress=99, observed_at=first_time, raw={'original': True})
    try:
        assert dispatcher.dispatch(first)
        assert dispatcher.dispatch(replace(first, observed_at='2026-01-02T10:01:09Z'))
        # A new viewing reuses the provider session key; its start must not replace the old stop.
        assert dispatcher.dispatch(event(action='start', progress=1))
        assert dispatcher.dispatch(event(action='start', progress=20))
        assert dispatcher.dispatch(event(action='stop', progress=99, observed_at=second_time))
        first.ids['tmdb'] = 'caller-mutated'
        first.raw['original'] = False
    finally:
        release.set()
        blocker.join(2)
    for _ in range(5):
        dispatcher.retry_pending()
    plays = [e for e in received if e.session_key == 'session']
    assert [(e.action, e.progress) for e in plays] == [('stop', 99), ('start', 1), ('start', 20), ('stop', 99)]
    assert [e.observed_at for e in plays if e.action == 'stop'] == [first_time, second_time]
    assert plays[0].ids == {'tmdb': '10'} and plays[0].raw['original'] is True
    assert not dispatcher._pending and not dispatcher._pending_tail


def test_floppy_unadmitted_stop_retries_before_later_same_session_viewing(setup):
    cfg, now = setup
    _floppy_queue_config(cfg)
    calls = []
    class Sink:
        def send(self, ev, cfg=None):
            calls.append(ev)
            return {'ok': len(calls) > 1, 'retryable': True}
    dispatcher = Dispatcher([Sink()], cfg_provider=lambda: cfg)
    first = event(action='stop', progress=99, observed_at='2026-01-02T10:00:00Z')
    assert not dispatcher.dispatch(first)
    assert dispatcher.dispatch(event(action='start', progress=1))
    assert dispatcher.dispatch(event(action='stop', progress=99, observed_at='2026-01-02T12:00:00Z'))
    now[0] += 30
    for _ in range(4):
        dispatcher.retry_pending()
    assert [(e.action, e.observed_at) for e in calls] == [
        ('stop', first.observed_at), ('stop', first.observed_at), ('start', None), ('stop', '2026-01-02T12:00:00Z')]
    assert not dispatcher._pending and not dispatcher._pending_tail


def test_floppy_pending_count_is_bounded_without_evicting_the_first_completion(setup):
    cfg, _ = setup
    _floppy_queue_config(cfg)
    entered, release = Event(), Event()
    calls = []
    class Sink:
        def send(self, ev, cfg=None):
            calls.append(ev)
            if len(calls) == 1:
                entered.set()
                assert release.wait(3)
            return {'ok': True}
    dispatcher = Dispatcher([Sink()], cfg_provider=lambda: cfg)
    blocker = Thread(target=lambda: dispatcher.dispatch(event(session_key='other')))
    blocker.start()
    assert entered.wait(1)
    try:
        results = [dispatcher.dispatch(event(action='stop', progress=99, raw={'notification': n},
                                           observed_at=f'2026-01-02T10:00:{n:02d}Z')) for n in range(40)]
        assert sum(results) == 31 and results[0] and not results[-1]
        head = next(iter(dispatcher._pending.values()))[1]
        assert head.observed_at == '2026-01-02T10:00:00Z'
        assert len(dispatcher._pending) + sum(map(len, dispatcher._pending_tail.values())) == 31
    finally:
        release.set()
        blocker.join(2)


def test_floppy_pending_stop_does_not_expire_before_sink_admission(setup):
    cfg, now = setup
    _floppy_queue_config(cfg)
    calls = []
    class Sink:
        def send(self, ev, cfg=None):
            calls.append(ev)
            return {'ok': len(calls) > 1, 'retryable': True}
    dispatcher = Dispatcher([Sink()], cfg_provider=lambda: cfg)
    original = event(action='stop', progress=99, observed_at='2026-01-02T10:00:00Z')
    dispatcher.dispatch(original)
    now[0] += 3601
    dispatcher.retry_pending()
    assert len(calls) == 2 and calls[-1].observed_at == original.observed_at
    assert not dispatcher._pending


def test_non_floppy_busy_sink_retains_existing_latest_event_coalescing(setup):
    cfg, _ = setup
    entered, release = Event(), Event()
    calls = []
    class Sink:
        def send(self, ev, cfg=None):
            calls.append((ev.session_key, ev.action))
            if len(calls) == 1:
                entered.set()
                assert release.wait(3)
            return {'ok': True}
    dispatcher = Dispatcher([Sink()], cfg_provider=lambda: cfg)
    blocker = Thread(target=lambda: dispatcher.dispatch(event(session_key='other')))
    blocker.start()
    assert entered.wait(1)
    try:
        dispatcher.dispatch(event(action='stop', progress=99))
        dispatcher.dispatch(event(action='start', progress=1))
    finally:
        release.set()
        blocker.join(2)
    dispatcher.retry_pending()
    assert calls == [('other', 'start'), ('session', 'start')]
    assert not dispatcher._pending_ordered and not dispatcher._pending_tail


def test_floppy_repeated_failure_keeps_head_ahead_of_queued_new_view(setup):
    cfg, now = setup
    _floppy_queue_config(cfg)
    calls = []
    class Sink:
        def send(self, ev, cfg=None):
            calls.append((ev.action, ev.observed_at))
            return {'ok': len(calls) > 2, 'retryable': True}
    dispatcher = Dispatcher([Sink()], cfg_provider=lambda: cfg)
    old = event(action='stop', progress=99, observed_at='2026-01-02T10:00:00Z')
    dispatcher.dispatch(old)
    dispatcher.dispatch(event(action='start', progress=1))
    dispatcher.dispatch(event(action='stop', progress=99, observed_at='2026-01-02T12:00:00Z'))
    now[0] += 30
    dispatcher.retry_pending()
    now[0] += 60
    for _ in range(4):
        dispatcher.retry_pending()
    assert calls == [('stop', old.observed_at)] * 3 + [('start', None), ('stop', '2026-01-02T12:00:00Z')]
    assert not dispatcher._pending and not dispatcher._pending_tail


def test_floppy_failed_inflight_start_precedes_its_queued_completion(setup):
    cfg, now = setup
    _floppy_queue_config(cfg)
    entered, release = Event(), Event()
    calls = []
    class Sink:
        def send(self, ev, cfg=None):
            calls.append((ev.action, ev.progress))
            if len(calls) == 1:
                entered.set()
                assert release.wait(3)
                return {'ok': False, 'retryable': True}
            return {'ok': True}
    dispatcher = Dispatcher([Sink()], cfg_provider=lambda: cfg)
    first = Thread(target=lambda: dispatcher.dispatch(event(action='start', progress=1)))
    first.start()
    assert entered.wait(1)
    try:
        dispatcher.dispatch(event(action='stop', progress=99, observed_at='2026-01-02T10:00:00Z'))
        dispatcher.dispatch(event(action='start', progress=1))
        dispatcher.dispatch(event(action='stop', progress=99, observed_at='2026-01-02T12:00:00Z'))
    finally:
        release.set()
        first.join(2)
    now[0] += 30
    for _ in range(5):
        dispatcher.retry_pending()
    assert calls == [('start', 1), ('start', 1), ('stop', 99), ('start', 1), ('stop', 99)]
    assert not dispatcher._pending and not dispatcher._pending_tail


def test_floppy_global_bound_reserves_failed_inflight_completion_without_eviction(setup, monkeypatch):
    cfg, _ = setup
    _floppy_queue_config(cfg)
    monkeypatch.setattr('cw_platform.event_archive.playback_recorder.record_playback', lambda *a, **k: None)
    entered, release = Event(), Event()
    calls = []
    class Sink:
        def send(self, ev, cfg=None):
            calls.append(ev)
            entered.set()
            assert release.wait(3)
            return {'ok': False, 'retryable': True}
    sink = Sink()
    dispatcher = Dispatcher([sink], cfg_provider=lambda: cfg)
    original = event(action='stop', progress=99, session_key='original', observed_at='2026-01-02T10:00:00Z')
    first = Thread(target=lambda: dispatcher.dispatch(original))
    first.start()
    assert entered.wait(1)
    try:
        for n in range(1023):
            assert dispatcher.dispatch(event(action='stop', progress=99, session_key=str(n)))
        assert not dispatcher.dispatch(event(action='stop', progress=99, session_key='overflow'))
    finally:
        release.set()
        first.join(2)
    assert len(dispatcher._pending) == 1024
    assert dispatcher._session_key(sink, original) in dispatcher._pending
    assert dispatcher._pending[dispatcher._session_key(sink, original)][1].observed_at == original.observed_at
    assert not dispatcher.dispatch(event(action='stop', progress=99, session_key='still-full'))
    assert len(calls) == 1 and len(dispatcher._pending) == 1024


def test_floppy_destination_change_cancels_old_fifo_without_retargeting_tail(setup):
    cfg, _ = setup
    _floppy_queue_config(cfg)
    entered, release = Event(), Event()
    calls = []
    class Sink:
        def send(self, ev, cfg=None):
            calls.append((cfg['floppy']['api_token'], ev.session_key, ev.action))
            if len(calls) == 1:
                entered.set()
                assert release.wait(3)
            return {'ok': True}
    dispatcher = Dispatcher([Sink()], cfg_provider=lambda: cfg)
    first = Thread(target=lambda: dispatcher.dispatch(event(session_key='old-inflight')))
    first.start()
    assert entered.wait(1)
    try:
        dispatcher.dispatch(event(action='stop', progress=99, observed_at='2026-01-02T10:00:00Z'))
        dispatcher.dispatch(event(action='start', progress=1))
        assert dispatcher._pending_tail
        cfg['floppy']['api_token'] = 'replacement-fixture-token'
        assert dispatcher.dispatch(event(session_key='new-view', action='start', progress=1))
    finally:
        release.set()
        first.join(2)
    dispatcher.retry_pending()
    assert calls == [('fixture-token', 'old-inflight', 'start'), ('replacement-fixture-token', 'new-view', 'start')]
    assert not dispatcher._pending and not dispatcher._pending_tail and not dispatcher._pending_ordered
