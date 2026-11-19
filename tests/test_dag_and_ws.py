"""Iteration v0.2 tests: ensureWorkspace (local RDA) + DAG scheduling (fake)."""
import json
import os
import subprocess
import sys
import time

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from rao import protocol  # noqa: E402
from rao.devices import DeviceRegistry  # noqa: E402
from rao.orchestrator import Orchestrator  # noqa: E402
from rao.transport import FakeTransport  # noqa: E402

from test_rda_local import RdaProc, _goal  # noqa: E402  (helpers)


def _mk_origin(tmp_path, fname="hello.txt", branch="master"):
    """Create a local git repo to serve as clone origin."""
    origin = os.path.join(str(tmp_path), "origin")
    env = dict(os.environ, GIT_AUTHOR_NAME="t", GIT_AUTHOR_EMAIL="t@t",
               GIT_COMMITTER_NAME="t", GIT_COMMITTER_EMAIL="t@t")
    def git(*args, cwd=None):
        subprocess.run(("git",) + args, cwd=cwd or origin, env=env,
                       capture_output=True, check=True)
    os.makedirs(origin)
    git("init", "-b", branch)
    with open(os.path.join(origin, fname), "w") as fh:
        fh.write("rao-ensure-ws\n")
    git("add", ".")
    git("commit", "-m", "init")
    return origin


# --- ensureWorkspace: clone path -------------------------------------------
def test_ensure_ws_clone_creates_repo(tmp_path):
    origin = _mk_origin(tmp_path)
    target = os.path.join(str(tmp_path), "ws")
    url = "file:///" + origin.replace("\\", "/")
    g = _goal(
        workspace=target.replace("\\", "/"),
        ensureWorkspace={"repoUrl": url, "path": target.replace("\\", "/")},
        plan={"steps": [{"id": "s1", "type": "shell",
                         "command": 'type hello.txt 2>nul || cat hello.txt',
                         "success": {"exitCode": 0,
                                     "stdoutContains": "rao-ensure-ws"}}]})
    r = RdaProc(g)
    res = r.wait_result(30)
    assert res and res["data"]["status"] == "success", \
        json.dumps(r.events[-3:], ensure_ascii=False)
    assert os.path.isfile(os.path.join(target, "hello.txt"))
    r.close()


# --- DAG scheduling (FakeTransport) ----------------------------------------
def _device(maxc=4):
    return {"id": "d1", "host": "fake", "port": 22, "user": "u",
            "maxConcurrentGoals": maxc}


def _make_orch(tmpdir, maxc=4):
    reg = DeviceRegistry(config_path=os.devnull)
    reg.devices = {"d1": _device(maxc)}
    script = [
        {"event": protocol.make_event(protocol.KIND_STARTED, "x", {})},
        {"event": protocol.make_event(protocol.KIND_RESULT, "x", {
            "status": "success", "summary": "ok", "steps": [],
            "artifacts": [], "assumptions": [], "metrics": {}})},
    ]
    return Orchestrator(registry=reg, transport=FakeTransport(lambda gid: script),
                        runs_dir=str(tmpdir))


def _node(key, obj):
    return {"key": key, "deviceId": "d1",
            "goal": {"protocol": "rao/1", "objective": "dag %s" % key}}


def test_dag_linear_chain(tmp_path):
    orch = _make_orch(tmp_path)
    dag = orch.dispatch_dag(
        [_node("a", 1), _node("b", 2), _node("c", 3)],
        [["a", "b"], ["b", "c"]])
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        snap = dag.snapshot()
        if all(n["state"] in ("done", "failed", "skipped", "timeout",
                              "cancelled") for n in snap["nodes"]):
            break
        time.sleep(0.05)
    states = {n["key"]: n["state"] for n in dag.snapshot()["nodes"]}
    assert states == {"a": "done", "b": "done", "c": "done"}
    # order respected: b's goal dispatched after a's run registered
    assert os.path.isfile(os.path.join(str(tmp_path),
                                       "dag-%s.json" % dag.dag_id))


def test_dag_failure_skips_downstream(tmp_path):
    reg = DeviceRegistry(config_path=os.devnull)
    reg.devices = {"d1": _device()}

    def factory(gid):
        return [{"event": protocol.make_event(protocol.KIND_RESULT, gid, {
            "status": "failed", "summary": "boom", "steps": [],
            "artifacts": [], "assumptions": [], "metrics": {}})}]

    orch = Orchestrator(registry=reg, transport=FakeTransport(factory),
                        runs_dir=str(tmp_path))
    dag = orch.dispatch_dag([_node("a", 1), _node("b", 2)], [["a", "b"]])
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        snap = dag.snapshot()
        if all(n["state"] in ("done", "failed", "skipped") for n in snap["nodes"]):
            break
        time.sleep(0.05)
    states = {n["key"]: n["state"] for n in dag.snapshot()["nodes"]}
    assert states["a"] == "failed"
    assert states["b"] == "skipped"
    assert "dependency" in dag.snapshot()["nodes"][1]["reason"]


def test_dag_cycle_rejected(tmp_path):
    orch = _make_orch(tmp_path)
    with pytest.raises(ValueError):
        orch.dispatch_dag([_node("a", 1), _node("b", 2)],
                          [["a", "b"], ["b", "a"]])


def test_dag_lenient_accepts_partial(tmp_path):
    reg = DeviceRegistry(config_path=os.devnull)
    reg.devices = {"d1": _device()}

    def factory(gid):
        return [{"event": protocol.make_event(protocol.KIND_RESULT, gid, {
            "status": "partial", "summary": "skipped optional", "steps": [],
            "artifacts": [], "assumptions": [], "metrics": {}})}]

    orch = Orchestrator(registry=reg, transport=FakeTransport(factory),
                        runs_dir=str(tmp_path))
    dag = orch.dispatch_dag([_node("a", 1), _node("b", 2)], [["a", "b"]],
                            policy="lenient")
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        snap = dag.snapshot()
        if all(n["state"] in ("done", "failed", "skipped") for n in snap["nodes"]):
            break
        time.sleep(0.05)
    states = {n["key"]: n["state"] for n in dag.snapshot()["nodes"]}
    assert states == {"a": "done", "b": "done"}


def test_dag_strict_rejects_partial(tmp_path):
    reg = DeviceRegistry(config_path=os.devnull)
    reg.devices = {"d1": _device()}

    def factory(gid):
        return [{"event": protocol.make_event(protocol.KIND_RESULT, gid, {
            "status": "partial", "summary": "skipped optional", "steps": [],
            "artifacts": [], "assumptions": [], "metrics": {}})}]

    orch = Orchestrator(registry=reg, transport=FakeTransport(factory),
                        runs_dir=str(tmp_path))
    dag = orch.dispatch_dag([_node("a", 1), _node("b", 2)], [["a", "b"]])
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        snap = dag.snapshot()
        if all(n["state"] in ("done", "failed", "skipped") for n in snap["nodes"]):
            break
        time.sleep(0.05)
    states = {n["key"]: n["state"] for n in dag.snapshot()["nodes"]}
    assert states["a"] == "done"
    assert states["b"] == "skipped"


# --- report history & diff --------------------------------------------------
def test_report_history_and_diff(tmp_path):
    orch = _make_orch(tmp_path)
    ds = orch.dispatch(["d1"], {"protocol": "rao/1", "objective": "r1"})
    orch.collect([d["goalId"] for d in ds], timeout_ms=15000)
    reports = orch.run_store.list_reports()
    assert len(reports) == 1
    rep = orch.run_store.load_report(reports[0]["file"])
    # fabricate an older "baseline" with a failed status for the same goal
    rep["matrix"][0]["status"] = "failed"
    rep["successCount"], rep["failCount"] = 0, 1
    base_file = "report-baseline.json"
    with open(os.path.join(str(tmp_path), base_file), "w",
              encoding="utf-8") as fh:
        json.dump(rep, fh)
    diff = orch.run_store.diff_reports(
        orch.run_store.load_report(base_file),
        orch.run_store.load_report(reports[0]["file"]))
    assert diff["changed"][0]["from"] == "failed"
    assert diff["changed"][0]["to"] == "success"
    # failed->success is a fix, not a regression
    assert diff["regressions"] == []


def test_report_diff_flags_regression(tmp_path):
    orch = _make_orch(tmp_path)
    ds = orch.dispatch(["d1"], {"protocol": "rao/1", "objective": "r1"})
    orch.collect([d["goalId"] for d in ds], timeout_ms=15000)
    reports = orch.run_store.list_reports()
    rep = orch.run_store.load_report(reports[0]["file"])
    # newer report regressed to failed
    rep["matrix"][0]["status"] = "failed"
    rep["successCount"], rep["failCount"] = 0, 1
    with open(os.path.join(str(tmp_path), "report-newer.json"), "w",
              encoding="utf-8") as fh:
        json.dump(rep, fh)
    diff = orch.run_store.diff_reports(
        orch.run_store.load_report(reports[0]["file"]),
        orch.run_store.load_report("report-newer.json"))
    assert len(diff["regressions"]) == 1
    assert diff["regressions"][0]["to"] == "failed"
