"""Protocol integration tests: drive rda.py as a local subprocess (no SSH).

Covers the engine behaviors from DESIGN.md §10 rows "协议集成" — same wire
format the SSH transport carries. Commands are Windows/POSIX neutral where
possible.
"""
import json
import os
import subprocess
import sys
import tempfile
import threading
import time

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RDA = os.path.join(ROOT, "src", "rao", "runtime", "rda.py")
PY = sys.executable


def _goal(**over):
    g = {
        "protocol": "rao/1",
        "goalId": "g-local-1",
        "deviceId": "local",
        "objective": "local protocol test",
    }
    g.update(over)
    return g


class RdaProc:
    def __init__(self, goal):
        self.proc = subprocess.Popen(
            [PY, RDA, "run-goal"], stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.events = []
        self._reader = threading.Thread(target=self._read, daemon=True)
        self._reader.start()
        self.proc.stdin.write((json.dumps(goal) + "\n").encode("utf-8"))
        self.proc.stdin.flush()

    def _read(self):
        for raw in self.proc.stdout:
            line = raw.decode("utf-8", "replace").strip()
            if not line:
                continue
            try:
                self.events.append(json.loads(line))
            except ValueError:
                pass

    def inject(self, obj):
        self.proc.stdin.write((json.dumps(obj) + "\n").encode("utf-8"))
        self.proc.stdin.flush()

    def kinds(self):
        return [e.get("kind") for e in self.events]

    def wait_result(self, timeout_s=20):
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if any(e.get("kind") == "result" for e in self.events):
                return next(e for e in self.events if e.get("kind") == "result")
            time.sleep(0.05)
        return None

    def close(self):
        try:
            self.proc.stdin.close()
        except Exception:
            pass
        try:
            self.proc.wait(timeout=5)
        except Exception:
            self.proc.kill()
        return self.proc.returncode


def test_shell_step_success():
    r = RdaProc(_goal(plan={"steps": [
        {"id": "s1", "type": "shell", "command": "echo rao-ok",
         "success": {"exitCode": 0, "stdoutContains": "rao-ok"}}]}))
    res = r.wait_result()
    assert res and res["data"]["status"] == "success"
    assert "started" in r.kinds() and "step_result" in r.kinds()
    code = r.close()
    assert code == 0


def test_no_plan_triggers_recon_and_ask():
    r = RdaProc(_goal())
    deadline = time.monotonic() + 15
    ask = None
    while time.monotonic() < deadline:
        asks = [e for e in r.events if e.get("kind") == "ask"]
        if asks:
            ask = asks[0]
            break
        time.sleep(0.05)
    assert ask, "no ask event"
    assert ask["data"]["context"]["recon"]["hostname"]
    plan = {"steps": [{"id": "p1", "type": "shell", "command": "echo steered"}]}
    r.inject({"type": "steer", "askId": ask["askId"], "answer": {"plan": plan}})
    res = r.wait_result()
    assert res and res["data"]["status"] == "success"
    assert any("steer" in a for a in res["data"]["assumptions"])
    assert r.close() == 0


def test_retry_then_fallback(tmp_path):
    # marker file appears only on the fallback command
    marker = os.path.join(str(tmp_path), "fb-done")
    fb_cmd = 'echo fallback-ran > "%s"' % marker
    r = RdaProc(_goal(plan={"steps": [{
        "id": "s1", "type": "shell", "command": "exit 1",
        "retries": 1, "retryBackoffMs": 100,
        "fallbacks": [{"id": "s1f", "type": "shell", "command": fb_cmd}],
        "success": {"exitCode": 0}}]}))
    res = r.wait_result()
    assert res and res["data"]["status"] == "success"
    steps = [e for e in r.events if e.get("kind") == "step_result"]
    attempts = [s for s in steps if s["data"].get("stepId") == "s1"]
    assert len(attempts) >= 3  # original x2 + fallback
    assert r.close() == 0


def test_forbidden_command_blocked():
    r = RdaProc(_goal(constraints={"forbidden": ["git push"]},
                      plan={"steps": [{"id": "s1", "type": "shell",
                                       "command": "git push origin main"}]}))
    res = r.wait_result()
    assert res and res["data"]["status"] == "failed"
    warns = [e for e in r.events if e.get("kind") == "warning"]
    assert any("forbidden" in json.dumps(w) for w in warns)
    r.close()


def test_cancel_injection():
    r = RdaProc(_goal(plan={"steps": [
        {"id": "s1", "type": "wait_file", "path": "/definitely/not/there",
         "timeoutMs": 60000}]}))
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if "progress" in r.kinds() or "started" in r.kinds():
            break
        time.sleep(0.05)
    r.inject({"type": "cancel", "reason": "test"})
    res = r.wait_result()
    assert res and res["data"]["status"] == "cancelled"
    code = r.close()
    assert code == 4


def test_step_budget_exhaustion():
    # maxSteps=2 with 3 steps -> engine stops with timeout-style budget error
    r = RdaProc(_goal(constraints={"maxSteps": 2}, plan={"steps": [
        {"id": "s1", "type": "shell", "command": "echo a"},
        {"id": "s2", "type": "shell", "command": "echo b"},
        {"id": "s3", "type": "shell", "command": "echo c"}]}))
    res = r.wait_result()
    assert res and res["data"]["status"] == "timeout"
    assert "budget" in res["data"]["summary"].lower()
    r.close()


def test_invalid_goal_rejected():
    proc = subprocess.Popen([PY, RDA, "run-goal"], stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    bad = {"protocol": "rao/9"}
    out, err = proc.communicate((json.dumps(bad) + "\n").encode(), timeout=10)
    assert proc.returncode == 2
    assert b"RAO_GOAL_INVALID" in out
