"""Unit tests: orchestrator lifecycle over FakeTransport (no SSH)."""
import json
import os
import sys
import tempfile
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))), "src"))

from rao import protocol  # noqa: E402
from rao.devices import DeviceRegistry  # noqa: E402
from rao.orchestrator import Orchestrator  # noqa: E402
from rao.transport import FakeTransport  # noqa: E402


def _ev(kind, data, **extra):
    return protocol.make_event(kind, "will-be-rewritten", data, **extra)


def _device(maxc=4):
    return {"id": "d1", "host": "fake", "port": 22, "user": "u",
            "maxConcurrentGoals": maxc}


def _goal(**over):
    g = {"protocol": protocol.PROTOCOL_VERSION, "goalId": None,
         "deviceId": "d1", "objective": "unit test goal"}
    g.update(over)
    return g


def _make_orch(script_factory, tmpdir, maxc=4):
    reg = DeviceRegistry(config_path=os.devnull)
    reg.devices = {"d1": _device(maxc)}
    orch = Orchestrator(registry=reg,
                        transport=FakeTransport(script_factory),
                        runs_dir=str(tmpdir))
    return orch


def _success_script():
    return [
        {"event": _ev(protocol.KIND_STARTED, {"rdaVersion": "fake"})},
        {"event": _ev(protocol.KIND_STEP_RESULT,
                      {"stepId": "s1", "status": "ok", "attempt": 1})},
        {"event": _ev(protocol.KIND_RESULT,
                      {"status": "success", "summary": "done", "steps": [],
                       "artifacts": [], "assumptions": [],
                       "metrics": {"stepAttempts": 1}})},
    ]


def test_dispatch_success_lifecycle(tmp_path):
    orch = _make_orch(lambda gid: _success_script(), tmp_path)
    ds = orch.dispatch(["d1"], _goal())
    assert len(ds) == 1
    gid = ds[0]["goalId"]
    assert gid
    run = orch.get_run(gid)
    assert run.wait_terminal(10)
    assert run.state == protocol.RUN_DONE
    assert run.result["status"] == "success"
    # run record persisted
    rec_file = os.path.join(str(tmp_path), "%s.json" % gid)
    assert os.path.isfile(rec_file)
    with open(rec_file, encoding="utf-8") as fh:
        rec = json.load(fh)
    assert rec["result"]["status"] == "success"
    assert any(e["kind"] == "started" for e in rec["events"])


def test_dispatch_invalid_goal_rejected(tmp_path):
    orch = _make_orch(lambda gid: _success_script(), tmp_path)
    with pytest.raises(ValueError):
        orch.dispatch(["d1"], {"objective": ""})  # missing protocol etc.


def test_unknown_device_rejected(tmp_path):
    orch = _make_orch(lambda gid: _success_script(), tmp_path)
    with pytest.raises(KeyError):
        orch.dispatch(["nope"], _goal())


def test_failed_goal_maps_state(tmp_path):
    script = [
        {"event": _ev(protocol.KIND_STARTED, {})},
        {"event": _ev(protocol.KIND_ERROR,
                      {"message": "step s1 failed", "fatal": True})},
        {"event": _ev(protocol.KIND_RESULT,
                      {"status": "failed", "summary": "boom", "steps": [],
                       "artifacts": [], "assumptions": [], "metrics": {}})},
    ]
    orch = _make_orch(lambda gid: script, tmp_path)
    ds = orch.dispatch(["d1"], _goal())
    run = orch.get_run(ds[0]["goalId"])
    assert run.wait_terminal(10)
    assert run.state == protocol.RUN_FAILED
    assert run.result["status"] == "failed"


def test_ask_then_result(tmp_path):
    script = [
        {"event": _ev(protocol.KIND_ASK, {"question": "which plan?"},
                      askId="a1")},
        {"event": _ev(protocol.KIND_RESULT,
                      {"status": "success", "summary": "after ask", "steps": [],
                       "artifacts": [], "assumptions": [], "metrics": {}}),
         "waitForSteer": True},
    ]
    orch = _make_orch(lambda gid: script, tmp_path)
    ds = orch.dispatch(["d1"], _goal())
    gid = ds[0]["goalId"]
    run = orch.get_run(gid)
    # wait until ask_waiting state appears
    for _ in range(50):
        if run.state == protocol.RUN_ASK_WAITING:
            break
        time.sleep(0.05)
    assert run.state == protocol.RUN_ASK_WAITING
    assert run.steer(ask_id="a1", answer={"plan": "do x"})
    assert run.wait_terminal(10)
    assert run.state == protocol.RUN_DONE


def test_cancel_mid_run(tmp_path):
    script = [
        {"event": _ev(protocol.KIND_STARTED, {})},
        {"event": _ev(protocol.KIND_PROGRESS,
                      {"stepId": "s1", "attempt": 1, "phase": "running"}),
         "waitForSteer": True},
        {"event": _ev(protocol.KIND_RESULT,
                      {"status": "cancelled", "summary": "via steer",
                       "steps": [], "artifacts": [], "assumptions": [],
                       "metrics": {}})},
    ]
    orch = _make_orch(lambda gid: script, tmp_path)
    ds = orch.dispatch(["d1"], _goal())
    run = orch.get_run(ds[0]["goalId"])
    for _ in range(50):
        if run.state == protocol.RUN_RUNNING:
            break
        time.sleep(0.05)
    assert run.cancel("test cancel")
    assert run.wait_terminal(10)
    assert run.state == protocol.RUN_CANCELLED


def test_collect_aggregates(tmp_path):
    def factory(gid):
        return _success_script()
    orch = _make_orch(factory, tmp_path)
    ds = orch.dispatch(["d1", "d1"], _goal())
    gids = [d["goalId"] for d in ds]
    report = orch.collect(gids, timeout_ms=15000)
    assert len(report["matrix"]) == 2
    assert report["successCount"] == 2
    assert report["failCount"] == 0
    assert os.path.isfile(report["reportFile"])


def test_exit_without_result_marks_failed(tmp_path):
    script = [{"event": _ev(protocol.KIND_STARTED, {})}]  # no result event
    orch = _make_orch(lambda gid: script, tmp_path)
    ds = orch.dispatch(["d1"], _goal())
    run = orch.get_run(ds[0]["goalId"])
    assert run.wait_terminal(10)
    assert run.state == protocol.RUN_FAILED
    assert "without result" in run.result["summary"]


def test_per_device_limit_serializes(tmp_path):
    import threading
    started = []
    lock = threading.Lock()

    def factory(gid):
        return [
            {"event": _ev(protocol.KIND_STARTED, {}), "sleepMs": 400},
            {"event": _ev(protocol.KIND_RESULT,
                          {"status": "success", "summary": "ok", "steps": [],
                           "artifacts": [], "assumptions": [],
                           "metrics": {}})},
        ]

    orch = _make_orch(factory, tmp_path, maxc=1)  # device allows 1 concurrent
    ds = orch.dispatch(["d1", "d1"], _goal())
    # first runs; second stays queued until first releases its slot
    states = sorted(r.state for _, r in orch.runs.items())
    assert states.count(protocol.RUN_QUEUED) == 1
    for d in ds:
        orch.get_run(d["goalId"]).wait_terminal(15)
    for d in ds:
        assert orch.get_run(d["goalId"]).state == protocol.RUN_DONE
