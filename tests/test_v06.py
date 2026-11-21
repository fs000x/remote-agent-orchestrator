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
