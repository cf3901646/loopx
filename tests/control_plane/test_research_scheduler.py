"""A durable feedback hint wakes the existing worker, never grants dispatch."""
from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from benchmark.runtime.research_scheduler import FeedbackEventSleep, main
from benchmark.runtime.research_feedback_wait import synchronize_feedback_waits
from scripts import external_scheduler_worker as worker
import test_research_feedback_wait as wait_fixture

runtime = wait_fixture.runtime


def publish(path, revision):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps({"revision": revision}))
    temporary.replace(path)


def test_event_between_quota_and_sleep_is_not_lost_or_replayed_as_endless_wake(tmp_path):
    path = tmp_path / "ingestion/wake.json"
    publish(path, "a" * 64)
    now, pauses = [0.0], []

    def pause(seconds):
        pauses.append(seconds)
        now[0] += seconds

    sleeper = FeedbackEventSleep(path, clock=lambda: now[0], pause=pause)
    publish(path, "b" * 64)  # Publication races the last quota read, before sleep.
    sleeper(3600)
    assert now[0] == 0 and pauses == []
    sleeper(3)
    assert now[0] == 3 and sum(pauses) == 3
    path.write_text("{partial")
    sleeper(2)
    assert now[0] == 5  # Missing/partial signals preserve the requested cadence.
    publish(path, "c" * 64)
    sleeper(3600)
    assert now[0] == 5


def test_research_scheduler_entry_is_explicit_and_reuses_ordinary_arguments(tmp_path, monkeypatch):
    monkeypatch.delenv("LOOPX_RESEARCH_LOOP", raising=False)
    with pytest.raises(ValueError, match="explicit research-loop"):
        main([])
    monkeypatch.setenv("LOOPX_RESEARCH_LOOP", "1")
    monkeypatch.setenv("LOOPX_RESEARCH_FEEDBACK_ROOT", str(tmp_path))
    called = []
    monkeypatch.setattr(worker, "main", lambda argv, **kwargs: called.append((argv, kwargs)) or 0)
    argv = ["--goal-id", "synthetic-goal", "--agent-id", "synthetic-agent", "--once"]
    assert main(argv) == 0
    assert called[0][0] is argv
    assert called[0][1]["sleep"].path == tmp_path / "ingestion/wake.json"


def test_event_rechecks_real_quota_and_only_released_dependency_dispatches(runtime, monkeypatch):
    original = wait_fixture.waiting(runtime)
    event = runtime.temp / "native-feedback/ingestion/wake.json"
    publish(event, "a" * 64)
    seen, pauses, now, dispatches = [], [], [0.0], []
    original_turn = runtime.env["LOOPX_TURN"]
    original_deadline = runtime.env["LOOPX_PHASE_DEADLINE_EPOCH"]

    def quota(_command, **_kwargs):
        value = runtime.quota()
        seen.append(value)
        return value

    def observe(seconds):
        pauses.append(seconds)
        now[0] += seconds
        if len(pauses) == 1:
            publish(event, "b" * 64)  # A signal alone cannot satisfy the dependency.
        elif len(pauses) == 2:
            value = wait_fixture.packet(status="completed", attribution="verified")
            receipt = synchronize_feedback_waits(runtime.env, value)
            assert receipt["released"][0]["dependency_todo_id"] == original["dependency_todo_id"]
            publish(event, "c" * 64)  # Consumer publication follows durable evidence.
        else:
            raise AssertionError("released evidence did not interrupt the long scheduler sleep")

    def wake(_command, **_kwargs):
        assert len(seen) == 3 and [packet["should_run"] for packet in seen] == [False, False, True]
        assert seen[-1]["selected_todo"]["todo_id"] == wait_fixture.fixture.TODO_ID
        assert runtime.env["LOOPX_TURN"] == original_turn
        assert runtime.env["LOOPX_PHASE_DEADLINE_EPOCH"] == original_deadline
        dispatches.append(original_turn)
        # Transport-only bounded synthetic Host; no model or scoring request.
        return SimpleNamespace(returncode=75, failure_kind=None)

    monkeypatch.setattr(worker, "run_quota_should_run", quota)
    monkeypatch.setattr(worker, "_run_wake", wake)
    args = SimpleNamespace(cli_bin="unused", registry=str(runtime.registry), runtime_root=str(runtime.root),
        runtime_profile="generic_cli", goal_id=runtime.env["LOOPX_GOAL_ID"], agent_id=runtime.env["LOOPX_AGENT_ID"],
        state_file=str(runtime.temp / "scheduler.json"), once=False, wake_cmd="synthetic-host",
        wake_stop_exit_code=75, error_backoff_seconds=5, quota_timeout_seconds=60, wake_timeout_seconds=30)
    sleeper = FeedbackEventSleep(event, clock=lambda: now[0], pause=observe)
    assert worker.run_worker(args, sleep=sleeper) == 0
    assert len(dispatches) == 1 and now[0] == 2
    assert json.loads((runtime.temp / "scheduler.json").read_text())["unchanged_count"] == 0
