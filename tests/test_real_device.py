"""Real-device integration tests (DESIGN.md §10.1 acceptance table T1-T10).

Run against the first configured device (override with RAO_TEST_DEVICE).
Skipped automatically when no device is configured, reachable, or credentialed.

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
from rao.transport import (SshTransport, SshDaemonTransport,  # noqa: E402
                           probe_tcp, password_of)  # noqa: E402

reg = DeviceRegistry()
DEVICE_ID = (os.environ.get("RAO_TEST_DEVICE")
             or (reg.list_ids()[0] if reg.list_ids() else "unconfigured"))
_DEV = reg.require(DEVICE_ID) if DEVICE_ID in reg.list_ids() else {}
WS = _DEV.get("workspace") or "/home/user/project"
OUT_DIR = WS + "/out"
RDA_DIR = (os.environ.get("RAO_RDA_DIR") or _DEV.get("rdaDir")
           or "$HOME/.rao/runtime")
RDA_HOST = _DEV.get("host") or ""
_pw_ok = bool(_DEV) and bool(password_of(_DEV))
_net_ok = bool(_DEV) and bool(RDA_HOST) and probe_tcp(
    RDA_HOST, int(_DEV.get("port") or 22))
_have_device = DEVICE_ID in reg.list_ids()

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
        workspace=WS,
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
    ws_root = OUT_DIR
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


# --- v0.3: daemon mode on real device (T14-T16) ----------------------------
@pytest.fixture(scope="module")
def daemon_orch(tmp_path_factory):
    runs = tmp_path_factory.mktemp("runs-daemon")
    dt = SshDaemonTransport(rda_dir=RDA_DIR)
    dev = reg.require(DEVICE_ID)
    assert dt.ensure_daemon(dev), "daemon failed to start on device"
    o = Orchestrator(registry=reg, transport=dt, runs_dir=str(runs))
    yield o
    try:
        dt._exec(dev, "cd %s && python3 rda_daemon.py stop" % RDA_DIR)
    except Exception:
        pass


def test_t14_daemon_goal_closed_loop(daemon_orch):
    goal = _goal(plan={"steps": [{"id": "s1", "type": "shell",
                                  "command": "echo daemon-real-ok",
                                  "success": {"exitCode": 0,
                                              "stdoutContains": "daemon-real-ok"}}]})
    ds = daemon_orch.dispatch([DEVICE_ID], goal)
    run = _wait(daemon_orch, ds[0]["goalId"])
    assert run.result["status"] == "success", run.result["summary"]
    assert getattr(run, "_last_seq", 0) >= 3  # started+step_result+result seen


def test_t15_daemon_resume_after_link_drop(daemon_orch):
    goal = _goal(plan={"steps": [
        {"id": "slow", "type": "shell", "command": "sleep 8",
         "success": {"exitCode": 0}},
        {"id": "after", "type": "shell", "command": "echo resumed-ok",
         "success": {"exitCode": 0, "stdoutContains": "resumed-ok"}}]})
    ds = daemon_orch.dispatch([DEVICE_ID], goal)
    run = daemon_orch.get_run(ds[0]["goalId"])
    # wait until the slow step is running, then drop the SSH link
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if run.chan and any(e["kind"] == "progress" for e in run.chan.events):
            break
        time.sleep(0.1)
    assert run.chan is not None, "no progress before drop"
    time.sleep(1)
    run.chan.close()   # simulate SSH loss mid-goal
    assert run.wait_terminal(90)
    assert run.state == protocol.RUN_DONE, (run.state, run.error)
    assert run.result["status"] == "success", run.result
    assert run._last_seq >= 4  # replay processed on attach


def test_t16_daemon_stop(daemon_orch):
    dt = daemon_orch.transport
    dev = reg.require(DEVICE_ID)
    rc, out = dt._exec(dev, "cd %s && python3 rda_daemon.py ping" % RDA_DIR)
    assert rc == 0, "daemon should be alive before stop"
    rc, out = dt._exec(dev, "cd %s && python3 rda_daemon.py stop" % RDA_DIR)
    assert rc == 0
    time.sleep(1)
    rc, _ = dt._exec(dev, "cd %s && python3 rda_daemon.py ping" % RDA_DIR)
    assert rc != 0, "daemon should be stopped"


# --- T17 (v0.4): remote-llm planner slot on real device ---------------------
def test_t17_remote_planner_slot(orch):
    stub = OUT_DIR + "/planner-stub.py"
    planner_cmd = json.dumps(
        {"command": "python3 %s {objective} {recon_file}" % stub})
    rc, _, err = _remote_exec(
        "mkdir -p $HOME/.rao %s && cat > %s <<'PYEOF'\n"
        "import json\n"
        "print(json.dumps({'steps': [{'id': 'p1', 'type': 'shell',\n"
        "    'command': 'echo planned-on-remote',\n"
        "    'success': {'exitCode': 0, 'stdoutContains': 'planned-on-remote'}}]}))\n"
        "PYEOF\n"
        "cat > $HOME/.rao/planner.json <<'JEOF'\n"
        "%s\n"
        "JEOF" % (OUT_DIR, stub, planner_cmd))
    assert rc == 0, err
    goal = _goal(planner="remote-llm", workspace=WS)
    ds = orch.dispatch([DEVICE_ID], goal)
    run = _wait(orch, ds[0]["goalId"])
    assert run.result["status"] == "success", run.result["summary"]
    assert any("remote planner" in a for a in run.result["assumptions"])
    kinds = [e["kind"] for e in run.chan.events]
    assert "ask" not in kinds
    # cleanup: remove the planner config so other tests are unaffected
    _remote_exec("rm -f $HOME/.rao/planner.json")


# --- T20/T21 (v0.6): capability matrix + claude backend plumbing ------------
def test_t20_capability_matrix_fields():
    d = [x for x in reg.describe_all(probe=False)
         if x["id"] == DEVICE_ID][0]
    ex = d["execution"]
    assert ex["executor"] is True and ex["is_agent"] is False
    assert ex["agent_claude"] == "unknown"   # 远端探测由 doctor/recon 完成


def test_t21_claude_backend_fast_fail_without_claude(orch):
    """远端无 claude CLI → agent backend 快速失败并给出清晰原因。
    v0.10 回退探测会命中 npm 包内二进制，因此需同时移走 PATH 链接与包内
    二进制，才能真正模拟"无 claude"场景。"""
    pw = reg.require(DEVICE_ID).get("auth", {}).get("password") or ""
    pkg_bin = ("/usr/lib/node_modules/@anthropic-ai/"
               "claude-code/bin/claude.exe")
    rc, out, _ = _remote_exec("which claude >/dev/null 2>&1 && echo yes || echo no")
    if out.strip() == "yes":
        rc2, _, _ = _remote_exec(
            "echo %s | sudo -S mv $(which claude) $(which claude).bak"
            % pw)
        assert rc2 == 0, "sudo mv claude failed"
    # 包内二进制也移走（回退探测的第一候选）
    rc3, _, _ = _remote_exec(
        "echo %s | sudo -S bash -c "
        "'test -f %s && mv %s %s.bak' 2>/dev/null; true"
        % (pw, pkg_bin, pkg_bin, pkg_bin))
    try:
        goal = _goal(agent="claude",
                     objective="whatever the remote agent decides to do",
                     constraints={"maxSteps": 3, "timeoutMs": 60000})
        ds = orch.dispatch([DEVICE_ID], goal)
        run = _wait(orch, ds[0]["goalId"], timeout_s=60)
        assert run.result["status"] == "failed"
        assert "claude CLI not found" in run.result["summary"]
    finally:
        # 恢复包内二进制与链接（顺序：先包内，再链接）
        _remote_exec(
            "echo %s | sudo -S bash -c "
            "'test -f %s.bak && mv %s.bak %s' 2>/dev/null; true"
            % (pw, pkg_bin, pkg_bin, pkg_bin))
        if out.strip() == "yes":
            _remote_exec(
                "echo %s | sudo -S bash -c "
                "'test -f $(which claude).bak && mv $(which claude).bak $(which claude)' "
                "2>/dev/null; true"
                % (reg.require(DEVICE_ID).get("auth", {}).get("password") or ""))
