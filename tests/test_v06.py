"""v0.6 tests: claude agent backend (ZCode device/credential reuse was
removed by product decision — rao manages its own devices only)."""
import json
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from rao import protocol  # noqa: E402

from test_rda_local import RdaProc, _goal  # noqa: E402

FAKE_CLAUDE = r'''import json, sys
lines = [
    {"type": "system", "subtype": "init"},
    {"type": "assistant", "message": {"content": [
        {"type": "tool_use", "name": "Bash", "input": {"command": "ls"}}]}},
    {"type": "assistant", "message": {"content": [
        {"type": "text", "text": "done"}]}},
    {"type": "result", "subtype": "success",
     "result": "fake-claude-finished-ok"},
]
for ln in lines:
    sys.stdout.write(json.dumps(ln) + "\n")
sys.stdout.flush()
'''


def test_claude_backend_success(tmp_path, monkeypatch):
    # fake claude shim: cmd 体用 %~dp0 引用同目录脚本（避免路径转义问题）
    (tmp_path / "fake_claude.py").write_text(FAKE_CLAUDE, encoding="utf-8")
    if os.name == "nt":
        shim = tmp_path / "claude.cmd"
        shim.write_text('@echo off\n@python "%~dp0fake_claude.py" %*\n',
                        encoding="utf-8")
    else:
        shim = tmp_path / "claude"
        shim.write_text('#!/bin/sh\ndir=$(dirname "$0")\n'
                        'python3 "$dir/fake_claude.py" "$@"\n',
                        encoding="utf-8")
        os.chmod(shim, 0o755)
    monkeypatch.setenv("PATH", str(tmp_path) + os.pathsep +
                       os.environ["PATH"])
    g = _goal(agent="claude",
              objective="run the fake claude",
              constraints={"maxSteps": 5})
    r = RdaProc(g)
    res = r.wait_result(30)
    assert res and res["data"]["status"] == "success", \
        json.dumps(r.events[-3:], ensure_ascii=False)
    assert "fake-claude-finished-ok" in res["data"]["summary"]
    tools = [e["data"].get("tool") for e in r.events
             if e["kind"] == "progress" and e["data"].get("tool")]
    assert "Bash" in tools
    r.close()

def test_claude_backend_missing_cli_fast_fails(tmp_path, monkeypatch):
    monkeypatch.setattr("shutil.which", lambda name: None)
    g = _goal(agent="claude", objective="x")
    r = RdaProc(g)
    res = r.wait_result(15)
    assert res and res["data"]["status"] == "failed"
    assert "claude CLI not found" in res["data"]["summary"]
    r.close()

def test_goal_agent_validation():
    ok, _ = protocol.validate_goal(_goal(agent="claude"))
    assert ok
    ok, err = protocol.validate_goal(_goal(agent="codex"))
    assert not ok and "goal.agent" in err
    ok, err = protocol.validate_goal(_goal(
        agent="claude", plan={"steps": [{"id": "s", "type": "shell",
                                         "command": "x"}]}))
    assert not ok and "drop plan" in err


PLANNER_OK = r'''import json, sys
print(json.dumps({"steps": [{"id": "p1", "type": "shell",
                             "command": "echo planned-ok",
                             "success": {"exitCode": 0,
                                         "stdoutContains": "planned-ok"}}]}))
'''

PLANNER_FAIL = r'''import sys
sys.stderr.write("no model available")
sys.exit(1)
'''


def _write_planner(tmp_path, body, ok=True):
    p = tmp_path / "planner.py"
    p.write_text(body, encoding="utf-8")
    cmd = '"%s" "%s" {objective} {recon_file}' % (sys.executable, p)
    if not ok:
        cmd = '"%s" "%s"' % (sys.executable, p)  # planner script exits 1
    (tmp_path / "planner.json").write_text(json.dumps({"command": cmd}),
                                           encoding="utf-8")


def test_remote_llm_planner_produces_plan(tmp_path):
    _write_planner(tmp_path, PLANNER_OK, ok=True)
    g = _goal(planner="remote-llm")   # no plan → planner should be used
    r = RdaProc(g, extra_args=["--state-dir", str(tmp_path)])
    res = r.wait_result(30)
    assert res and res["data"]["status"] == "success"
    assert [s["id"] for s in res["data"]["steps"]] == ["p1"]
    tails = [json.dumps(e["data"]) for e in r.events
             if e["kind"] == "step_result"]
    assert any("planned-ok" in t for t in tails)
    assert any("remote planner" in a for a in res["data"]["assumptions"])
    assert "ask" not in r.kinds()
    r.close()


def test_remote_llm_planner_failure_falls_back_to_ask(tmp_path):
    _write_planner(tmp_path, PLANNER_FAIL, ok=False)
    g = _goal(planner="remote-llm")
    r = RdaProc(g, extra_args=["--state-dir", str(tmp_path)])
    import time
    deadline = time.monotonic() + 15
    asks = []
    while time.monotonic() < deadline:
        asks = [e for e in r.events if e.get("kind") == "ask"]
        if asks:
            break
        time.sleep(0.05)
    assert asks, "expected fallback ask"
    plan = {"steps": [{"id": "p1", "type": "shell", "command": "echo fallback",
                       "success": {"exitCode": 0}}]}
    r.inject({"type": "steer", "askId": asks[0]["askId"],
              "answer": {"plan": plan}})
    res = r.wait_result(20)
    assert res and res["data"]["status"] == "success"
    assert any("planner.json missing" in a or "planner" in a
               for a in res["data"]["assumptions"])
    r.close()
