"""v0.3 daemon tests: real rda_daemon.py serve (local TCP) + orchestrator
resume semantics (scripted channel loss + seq dedup)."""
import json
import os
import subprocess
import sys
import tempfile
import threading
import time

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from rao import protocol  # noqa: E402
from rao.devices import DeviceRegistry  # noqa: E402
from rao.orchestrator import Orchestrator  # noqa: E402
from rao.transport import (FakeTransport, TcpDaemonTransport,  # noqa: E402
                           FakeGoalChannel, _DaemonLink)

RDA_DIR = os.path.join(ROOT, "src", "rao", "runtime")
PY = sys.executable


class LocalDaemon:
    """Runs the REAL rda_daemon.py serve in-process-adjacent subprocess."""

    def __init__(self, tmpdir, port=0):
        self.home = os.path.join(str(tmpdir), "rao-home")
        os.makedirs(self.home, exist_ok=True)
        self.env = dict(os.environ, RAO_HOME=self.home)
        self._logf = open(os.path.join(self.home, "serve-stderr.log"), "wb")
        self.proc = subprocess.Popen(
            [PY, os.path.join(RDA_DIR, "rda_daemon.py"), "serve",
             "--port", str(port)],
            env=self.env, stdout=self._logf, stderr=self._logf,
            stdin=subprocess.DEVNULL)
        self._wait_ready(10)

    def _wait_ready(self, timeout_s):
        dj_path = os.path.join(self.home, "daemon.json")
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if os.path.isfile(dj_path):
                with open(dj_path, encoding="utf-8") as fh:
                    return json.load(fh)
            time.sleep(0.05)
        raise RuntimeError("daemon.json not written in %ss" % timeout_s)

    def info(self):
        with open(os.path.join(self.home, "daemon.json"),
                  encoding="utf-8") as fh:
            return json.load(fh)

    def stop(self):
        try:
            info = self.info()
            import socket as _s
            s = _s.create_connection(("127.0.0.1", info["port"]), timeout=3)
            f = s.makefile("rwb")
            f.write((json.dumps({"cmd": "auth", "token": info["token"]}) +
                     "\n" + json.dumps({"cmd": "shutdown"}) + "\n")
                    .encode("utf-8"))
            f.flush()
            f.readline()
            s.close()
        except Exception:
            pass
        try:
            self.proc.wait(timeout=5)
        except Exception:
            self.proc.kill()
        return self.proc.returncode


def _device(port):
    return {"id": "d1", "host": "127.0.0.1", "port": 22, "user": "u",
            "daemonPort": port, "maxConcurrentGoals": 4}


def _goal(**over):
    g = {"protocol": "rao/1", "goalId": None, "deviceId": "d1",
         "objective": "daemon test"}
    g.update(over)
    return g


@pytest.fixture()
def daemon(tmp_path):
    d = LocalDaemon(tmp_path)
    yield d
    d.stop()


def test_daemon_full_local_loop(daemon, tmp_path):
    info = daemon.info()
    reg = DeviceRegistry(config_path=os.devnull)
    reg.devices = {"d1": _device(info["port"])}
    orch = Orchestrator(registry=reg,
                        transport=TcpDaemonTransport(info["token"]),
                        runs_dir=str(tmp_path))
    goal = _goal(plan={"steps": [{"id": "s1", "type": "shell",
                                  "command": "echo daemon-ok",
                                  "success": {"exitCode": 0,
                                              "stdoutContains": "daemon-ok"}}]})
    ds = orch.dispatch(["d1"], goal)
    gid = ds[0]["goalId"]
    run = orch.get_run(gid)
    assert run.wait_terminal(30)
    assert run.state == protocol.RUN_DONE, run.result
    assert run.result["status"] == "success"
    # goal record persisted by the daemon under RAO_HOME/goals (async from
    # the client's view — poll briefly)
    goal_file = os.path.join(daemon.home, "goals", "%s.json" % gid)
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and not os.path.isfile(goal_file):
        time.sleep(0.05)
    assert os.path.isfile(goal_file)
    # daemon keeps history: attach after completion replays everything.
    # The pump may read ahead of our loop (result and goal_status can both
    # already be buffered), so exit only when result seen AND exit set.
    t = TcpDaemonTransport(info["token"])
    chan = t.open_resume_session(gid, _device(info["port"]))
    kinds = []
    idx = 0
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        ev, idx = chan.wait_event(idx, 1)
        if ev is not None:
            kinds.append(ev["kind"])
        if "result" in kinds and chan.exit_code is not None:
            break
    assert "result" in kinds, (
        kinds, "exit=%s" % chan.exit_code,
        "stderr=%s" % chan.stderr_text[-5:])
    assert chan.exit_code is not None
    chan.close()


def test_daemon_inject_cancel(daemon, tmp_path):
    info = daemon.info()
    reg = DeviceRegistry(config_path=os.devnull)
    reg.devices = {"d1": _device(info["port"])}
    orch = Orchestrator(registry=reg,
                        transport=TcpDaemonTransport(info["token"]),
                        runs_dir=str(tmp_path))
    goal = _goal(plan={"steps": [{"id": "w", "type": "wait_file",
                                  "path": "/tmp/rao-daemon-never",
                                  "timeoutMs": 60000}]})
    ds = orch.dispatch(["d1"], goal)
    run = orch.get_run(ds[0]["goalId"])
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if run.chan and run.chan.events:
            break
        time.sleep(0.05)
    assert run.cancel("daemon cancel test")
    assert run.wait_terminal(30)
    assert run.state == protocol.RUN_CANCELLED


# --- orchestrator-level resume with scripted channel loss -------------------
class ResumableChannel(_DaemonLink):
    """Daemon-style channel: emits scripted events with seqs; can simulate
    link death after N events (die_after) — replay semantics on resume."""

    def __init__(self, goal_id, script, die_after=None):
        super(ResumableChannel, self).__init__(goal_id, {"id": "fake"},
                                               None, handshake="bridge")
        self.script = script
        self.die_after = die_after
        self.emitted = 0
        self._injected = []
        self._inj_cond = threading.Condition()

    def _open_link(self):
        pass

    def _send_line(self, obj):
        # inject command: unwrap payload into the queue (engine-facing)
        payload = obj.get("payload") if obj.get("cmd") == "inject" else obj
        with self._inj_cond:
            self._injected.append(payload)
            self._inj_cond.notify_all()

    def _recv_line(self, timeout_s):
        return None

    def connect_and_start(self, goal_obj, resume=False):
        # fake bridge handshake: no auth/ready round-trip
        self.start_pump(self)
        return self

    def _close_link(self):
        pass

    def alive(self):
        return self.exit_code is None and not self.closed

    def _wait_injection(self, timeout_s):
        deadline = time.monotonic() + timeout_s
        with self._inj_cond:
            while time.monotonic() < deadline:
                if self._injected:
                    return self._injected.pop(0)
                self._inj_cond.wait(0.05)
        return None

    def __iter__(self):
        for item in self.script:
            if self.killed or self.closed:
                break
            if item.get("waitForSteer"):
                got = self._wait_injection(30)
                if got is None or got.get("type") == "cancel":
                    self.exit_code = 4
                    return
                if got is None or got.get("type") != "steer":
                    yield {"type": "error", "message": "no steer"}
                    self.exit_code = 1
                    return
            if self.die_after is not None and self.emitted >= self.die_after:
                self.exit_code = 255   # link death, no result event
                return
            ev = dict(item["event"])
            ev.setdefault("protocol", protocol.PROTOCOL_VERSION)
            ev.setdefault("goalId", self.goal_id)
            ev.setdefault("ts", protocol.now_ms())
            ev.setdefault("data", {})
            self.emitted += 1
            yield {"type": "event", "event": ev, "seq": self.emitted}
        if self.exit_code is None:
            self.exit_code = 4 if self.killed else 0


class ResumableFakeTransport:
    """First connection emits N events then dies; resume connections replay
    ALL events with original seqs (daemon attach semantics) then continue."""

    def __init__(self, events, die_after):
        self.events = events
        self.die_after = die_after

    def open_session(self, goal_id, device, goal_obj):
        return self._mk(goal_id, die_after=self.die_after)

    def open_resume_session(self, goal_id, device):
        return self._mk(goal_id, die_after=None)

    def _mk(self, goal_id, die_after):
        script = [{"event": ev} for ev in self.events]
        ch = ResumableChannel(goal_id, script, die_after=die_after)
        ch.connect_and_start(None)
        return ch


def test_orchestrator_resume_dedup(tmp_path):
    events = [
        protocol.make_event(protocol.KIND_STARTED, "x", {}),
        protocol.make_event(protocol.KIND_PROGRESS, "x",
                            {"stepId": "s1", "attempt": 1, "phase": "running"}),
        protocol.make_event(protocol.KIND_STEP_RESULT, "x",
                            {"stepId": "s1", "status": "ok", "attempt": 1}),
        protocol.make_event(protocol.KIND_RESULT, "x",
                            {"status": "success", "summary": "after resume",
                             "steps": [], "artifacts": [], "assumptions": [],
                             "metrics": {}}),
    ]
    reg = DeviceRegistry(config_path=os.devnull)
    reg.devices = {"d1": {"id": "d1", "host": "fake", "port": 22, "user": "u",
                          "maxConcurrentGoals": 4}}
    orch = Orchestrator(
        registry=reg,
        transport=ResumableFakeTransport(events, die_after=2),
        runs_dir=str(tmp_path))
    ds = orch.dispatch(["d1"], {"protocol": "rao/1", "objective": "resume t"})
    run = orch.get_run(ds[0]["goalId"])
    assert run.wait_terminal(20)
    assert run.state == protocol.RUN_DONE, (run.state, run.error)
    assert run.result["status"] == "success"
    # dedup proof: attempt counted once despite full replay on resume
    assert run._attempt_count == 1


def test_orchestrator_resume_budget_exhaustion(tmp_path):
    events = [
        protocol.make_event(protocol.KIND_STARTED, "x", {}),
        protocol.make_event(protocol.KIND_PROGRESS, "x", {}),
    ]
    reg = DeviceRegistry(config_path=os.devnull)
    reg.devices = {"d1": {"id": "d1", "host": "fake", "port": 22, "user": "u",
                          "maxConcurrentGoals": 4}}

    class AlwaysDying(ResumableFakeTransport):
        def open_resume_session(self, goal_id, device):
            self.session += 1
            if self.session > 2:
                raise Exception("device unreachable")
            return self._mk(goal_id, die_after=None)

    # channel that dies immediately, repeatedly
    t = AlwaysDying(events, die_after=0)
    orch = Orchestrator(registry=reg, transport=t, runs_dir=str(tmp_path))
    os.environ["RAO_RESUME_MAX"] = "1"
    try:
        ds = orch.dispatch(["d1"], {"protocol": "rao/1", "objective": "x"})
        run = orch.get_run(ds[0]["goalId"])
        assert run.wait_terminal(30)
        assert run.state == protocol.RUN_FAILED
        assert "resume budget" in (run.error or "")
    finally:
        os.environ.pop("RAO_RESUME_MAX", None)
