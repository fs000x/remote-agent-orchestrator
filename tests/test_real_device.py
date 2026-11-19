"""Real-device integration tests (DESIGN.md §10.1 acceptance table T1-T10).

Run against the configured device (diablo-100 = diablo@192.168.0.100).
Skipped automatically when the device is unreachable or credentials missing.

These exercise Orchestrator + SshTransport + real RDA over SSH — the full
local side of the system end to end.
"""
import json
import os
import sys
import time

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from rao import protocol  # noqa: E402
from rao.devices import DeviceRegistry  # noqa: E402
from rao.orchestrator import Orchestrator  # noqa: E402
from rao.transport import SshTransport, probe_tcp, password_of  # noqa: E402

RDA_DIR = os.environ.get("RAO_RDA_DIR", "/home/diablo/git500/zcode_prj/rao")
DEVICE_ID = os.environ.get("RAO_TEST_DEVICE", "diablo-100")
OUT_DIR = "/home/diablo/git500/zcode_prj/out"

reg = DeviceRegistry()
_have_device = DEVICE_ID in reg.list_ids()
_pw_ok = _have_device and bool(password_of(reg.require(DEVICE_ID)))
_net_ok = _have_device and probe_tcp(reg.require(DEVICE_ID)["host"],
                                     int(reg.require(DEVICE_ID).get("port", 22)))

pytestmark = pytest.mark.skipif(
    not (_have_device and _pw_ok and _net_ok),
    reason="device %s unreachable or credentials missing" % DEVICE_ID)


@pytest.fixture(scope="module")
def orch(tmp_path_factory):
    runs = tmp_path_factory.mktemp("runs")
    o = Orchestrator(registry=reg, transport=SshTransport(rda_dir=RDA_DIR),
                     runs_dir=str(runs))
    return o


def _goal(**over):
    g = {"protocol": protocol.PROTOCOL_VERSION, "goalId": None,
         "deviceId": DEVICE_ID, "objective": "real-device integration"}
    g.update(over)
    return g


def _wait(orch, gid, timeout_s=90):
    run = orch.get_run(gid)
    assert run, "run missing: %s" % gid
    done = run.wait_terminal(timeout_s)
    assert done, "run %s did not finish in %ss (state=%s, events_tail=%s)" % (
        gid, timeout_s, run.state,
        [e["kind"] for e in (run.chan.events if run.chan else [])[-5:]])
    return run


def _remote_exec(cmd):
    """Direct SSH exec for assertions (bypasses RDA)."""
    import paramiko
    d = reg.require(DEVICE_ID)
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    c.connect(d["host"], port=int(d.get("port", 22)), username=d["user"],
              password=password_of(d), timeout=10, allow_agent=False,
              look_for_keys=False)
    try:
        _, out, err = c.exec_command(cmd, timeout=15)
        rc = out.channel.recv_exit_status()
        return rc, out.read().decode("utf-8", "replace"), \
            err.read().decode("utf-8", "replace")
    finally:
        c.close()


# --- T1 ------------------------------------------------------------------
def test_t1_devices_list():
    devs = reg.describe_all(probe=True)
    mine = [d for d in devs if d["id"] == DEVICE_ID]
    assert mine and mine[0]["online"] is True
    assert mine[0]["authConfigured"]


# --- T2 + T10 ------------------------------------------------------------
def test_t2_t10_goal_closed_loop_with_artifact(orch):
    goal = _goal(
        workspace="/home/diablo/git500/zcode_prj",
        objective="collect device facts into artifact",
        plan={"steps": [
            {"id": "mk", "type": "shell",
             "command": "mkdir -p %s && (uname -a; date -u) > %s/facts.txt"
                        % (OUT_DIR, OUT_DIR),
             "success": {"exitCode": 0}},
            {"id": "chk", "type": "artifact", "paths": ["out/facts.txt"]},
        ]})
    ds = orch.dispatch([DEVICE_ID], goal)
    gid = ds[0]["goalId"]
    run = _wait(orch, gid)
    assert run.result["status"] == "success", run.result["summary"]
    assert run.result["artifacts"], "artifact not reported"
    arts = [a for a in run.result["artifacts"] if a["path"].endswith("facts.txt")]
    assert arts and arts[0]["size"] > 0
    # independent verification via direct SSH
    rc, out, _ = _remote_exec("cat %s/facts.txt" % OUT_DIR)
    assert rc == 0 and "Linux" in out
    # T10: collect aggregation
    report = orch.collect([gid], timeout_ms=10000)
    assert report["matrix"][0]["status"] == "success"
    assert report["matrix"][0]["deviceId"] == DEVICE_ID
    assert os.path.isfile(report["reportFile"])
    rec = json.load(open(os.path.join(os.path.dirname(report["reportFile"]),
                                      "%s.json" % gid), encoding="utf-8"))
    assert rec["result"]["status"] == "success"


# --- T3 ------------------------------------------------------------------
def test_t3_retry_then_fallback(orch):
    goal = _goal(plan={"steps": [{
        "id": "s1", "type": "shell", "command": "false",
        "retries": 1, "retryBackoffMs": 200,
        "fallbacks": [{"id": "s1f", "type": "shell", "command": "true"}],
        "success": {"exitCode": 0}}]})
    ds = orch.dispatch([DEVICE_ID], goal)
    run = _wait(orch, ds[0]["goalId"])
    assert run.result["status"] == "success"
    statuses = [s["data"].get("status") for s in run.chan.events
                if s["kind"] == "step_result"]
    assert statuses == ["failed", "failed", "ok"]  # 2 tries + fallback ok
    assert run.result["steps"][0]["status"] == "fallback_ok"


# --- T4 ------------------------------------------------------------------
def test_t4_ask_steer_plan_roundtrip(orch):
    goal = _goal(objective="no-plan goal expecting recon+ask then steered plan")
    ds = orch.dispatch([DEVICE_ID], goal)
    gid = ds[0]["goalId"]
    run = orch.get_run(gid)
    ask_ev = None
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        for e in (run.chan.events if run.chan else []):
            if e["kind"] == "ask":
                ask_ev = e
                break
        if ask_ev:
            break
        time.sleep(0.1)
    assert ask_ev, "no ask within 30s"
    assert ask_ev["data"]["context"]["recon"]["hostname"]
    plan = {"steps": [
        {"id": "p1", "type": "shell", "command": "echo steered-plan-ran",
         "success": {"exitCode": 0, "stdoutContains": "steered-plan-ran"}}]}
    assert run.steer(ask_id=ask_ev["askId"], answer={"plan": plan})
    _wait(orch, gid)
    assert run.result["status"] == "success"
    assert any("steer" in a for a in run.result["assumptions"])


# --- T5 ------------------------------------------------------------------
def test_t5_parallel_same_device(orch):
    def mk(i):
        return _goal(plan={"steps": [{"id": "w", "type": "shell",
                                      "command": "sleep 2; echo g%d" % i,
                                      "success": {"exitCode": 0}}]})
    ds = orch.dispatch([DEVICE_ID, DEVICE_ID], mk(1))
    gids = [d["goalId"] for d in ds]
    runs = [orch.get_run(g) for g in gids]
    for r in runs:
        assert r.wait_terminal(60)
        assert r.result["status"] == "success"
    # overlap evidence: the second run started before the first finished
    starts = [r.dispatched_ms for r in runs]
    ends = [r.chan.events[-1]["ts"] for r in runs]
    assert min(starts) < max(ends), "runs did not overlap"


# --- T6 ------------------------------------------------------------------
def test_t6_cancel_mid_run(orch):
    goal = _goal(plan={"steps": [{"id": "w", "type": "wait_file",
                                  "path": "/tmp/rao-never-appears",
                                  "timeoutMs": 60000}]})
    ds = orch.dispatch([DEVICE_ID], goal)
    gid = ds[0]["goalId"]
    run = orch.get_run(gid)
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if run.chan and run.chan.events:
            break
        time.sleep(0.1)
    assert run.cancel("integration test cancel")
    assert run.wait_terminal(30)
    assert run.state == protocol.RUN_CANCELLED
    assert run.result["status"] == "cancelled"


# --- T7 ------------------------------------------------------------------
def test_t7_orchestrator_deadline_timeout(orch):
    goal = _goal(constraints={"timeoutMs": 4000},
                 plan={"steps": [{"id": "w", "type": "wait_file",
                                  "path": "/tmp/rao-never-appears-2",
                                  "timeoutMs": 60000}]})
    ds = orch.dispatch([DEVICE_ID], goal)
    run = orch.get_run(ds[0]["goalId"])
    t0 = time.monotonic()
    assert run.wait_terminal(30)
    elapsed = time.monotonic() - t0
    assert run.state == protocol.RUN_TIMEOUT
    assert elapsed < 15, "deadline enforcement took too long: %.1fs" % elapsed


# --- T8 ------------------------------------------------------------------
def test_t8_steer_directive_skip(orch):
    goal = _goal(plan={"steps": [
        {"id": "s1", "type": "shell", "command": "sleep 3",
         "success": {"exitCode": 0}},
        {"id": "s2", "type": "shell", "command": "false",
         "success": {"exitCode": 0}}]})
    ds = orch.dispatch([DEVICE_ID], goal)
    gid = ds[0]["goalId"]
    run = orch.get_run(gid)
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if run.chan and any(e["kind"] == "progress" for e in run.chan.events):
            break
        time.sleep(0.1)
    assert run.steer(message="skip:s2")
    assert run.wait_terminal(60)
    assert run.result["status"] in ("partial", "success")
    notes = json.dumps(run.result.get("steerNotes", []))
    assert "skip:s2" in notes
    assert any(s["id"] == "s2" and s["status"] == "skipped"
               for s in run.result["steps"])


# --- T9 ------------------------------------------------------------------
def test_t9_forbidden_command_blocked(orch):
    goal = _goal(constraints={"forbidden": ["rm -rf /", "mkfs"]},
                 plan={"steps": [{"id": "bad", "type": "shell",
                                  "command": "echo x; rm -rf /"}]})
    ds = orch.dispatch([DEVICE_ID], goal)
    run = _wait(orch, ds[0]["goalId"])
    assert run.result["status"] == "failed"
    warns = [e for e in run.chan.events if e["kind"] == "warning"]
    assert any("forbidden" in json.dumps(w) for w in warns)
    # the dangerous command never ran: system still responsive
    rc, out, _ = _remote_exec("echo alive")
    assert rc == 0 and "alive" in out


# --- T11 (v0.2): ensureWorkspace on real device ---------------------------
def test_t11_ensure_workspace_clone(orch):
    # create a fresh origin repo on the device, then let ensureWorkspace clone it
    ws_root = "/home/diablo/git500/zcode_prj/out"
    origin = "%s/ws-origin-%d" % (ws_root, protocol.now_ms())
    target = "%s/ws-clone-%d" % (ws_root, protocol.now_ms())
    rc, _, err = _remote_exec(
        "mkdir -p %s && git init -q %s && cd %s && "
        "git checkout -q -b master && echo rao-real-ws > hello.txt && "
        "git add . && GIT_AUTHOR_NAME=t GIT_AUTHOR_EMAIL=t@t "
        "GIT_COMMITTER_NAME=t GIT_COMMITTER_EMAIL=t@t "
        "git commit -qm init" % (ws_root, origin, origin))
    assert rc == 0, err
    goal = _goal(
        workspace=target,
        ensureWorkspace={"repoUrl": "file://%s" % origin, "path": target,
                         "branch": "master"},
        plan={"steps": [{"id": "s1", "type": "shell", "command": "cat hello.txt",
                         "success": {"exitCode": 0,
                                     "stdoutContains": "rao-real-ws"}}]})
    ds = orch.dispatch([DEVICE_ID], goal)
    run = _wait(orch, ds[0]["goalId"])
    assert run.result["status"] == "success", run.result["summary"]
    step_ids = [s["id"] for s in run.result["steps"]]
    assert "ensure-ws-clone" in step_ids
    assert any("master" in a for a in run.result["assumptions"])
    rc, out, _ = _remote_exec("cat %s/hello.txt" % target)
    assert rc == 0 and "rao-real-ws" in out


# --- T12 (v0.2): DAG chain on real device ---------------------------------
def test_t12_dag_two_node_chain(orch):
    stamp = protocol.now_ms()
    marker = "%s/dag-%s.txt" % (OUT_DIR, stamp)
    nodes = [
        {"key": "writer", "deviceId": DEVICE_ID, "goal": {
            "protocol": protocol.PROTOCOL_VERSION, "objective": "write marker",
            "plan": {"steps": [{"id": "w", "type": "shell",
                                "command": "echo dag-ok > %s" % marker,
                                "success": {"exitCode": 0}}]}}},
        {"key": "reader", "deviceId": DEVICE_ID, "goal": {
            "protocol": protocol.PROTOCOL_VERSION, "objective": "read marker",
            "plan": {"steps": [{"id": "r", "type": "shell",
                                "command": "cat %s" % marker,
                                "success": {"exitCode": 0,
                                            "stdoutContains": "dag-ok"}}]}}},
    ]
    dag = orch.dispatch_dag(nodes, [["writer", "reader"]])
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        snap = dag.snapshot()
        if all(n["state"] in ("done", "failed", "skipped", "timeout",
                              "cancelled") for n in snap["nodes"]):
            break
        time.sleep(0.2)
    snap = dag.snapshot()
    states = {n["key"]: n["state"] for n in snap["nodes"]}
    assert states == {"writer": "done", "reader": "done"}, snap
    rc, out, _ = _remote_exec("cat %s" % marker)
    assert rc == 0 and "dag-ok" in out


# --- T13 (v0.2): report history & diff ------------------------------------
def test_t13_report_history_and_diff(orch):
    goal = _goal(plan={"steps": [{"id": "s", "type": "shell",
                                  "command": "echo report-subject",
                                  "success": {"exitCode": 0}}]})
    ds = orch.dispatch([DEVICE_ID], goal)
    report = orch.collect([ds[0]["goalId"]], timeout_ms=60000)
    assert report["matrix"][0]["status"] == "success"
    names = [r["file"] for r in orch.run_store.list_reports()]
    assert os.path.basename(report["reportFile"]) in names
    # diff the report against itself -> no changes, no regressions
    diff = orch.run_store.diff_reports(report, report)
    assert diff["changed"] == [] and diff["regressions"] == []
