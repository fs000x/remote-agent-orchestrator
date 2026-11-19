"""Unit tests: protocol schema validation (DESIGN.md §10 layer 1)."""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))), "src"))

from rao import protocol  # noqa: E402


def _base_goal(**over):
    g = {
        "protocol": protocol.PROTOCOL_VERSION,
        "goalId": "g-123",
        "deviceId": "d1",
        "objective": "do the thing",
    }
    g.update(over)
    return g


def test_valid_minimal_goal():
    ok, err = protocol.validate_goal(_base_goal())
    assert ok, err


def test_valid_goal_with_plan():
    g = _base_goal(plan={"steps": [
        {"id": "s1", "type": "shell", "command": "ls",
         "success": {"exitCode": 0}, "retries": 1,
         "fallbacks": [{"id": "s1f", "type": "shell", "command": "ls -la"}]},
        {"id": "s2", "type": "ask", "question": "continue?"},
        {"id": "s3", "type": "artifact", "paths": ["a.log"]},
        {"id": "s4", "type": "wait_file", "path": "/tmp/x"},
    ]})
    ok, err = protocol.validate_goal(g)
    assert ok, err


def test_rejects_wrong_protocol():
    ok, _ = protocol.validate_goal(_base_goal(protocol="rao/2"))
    assert not ok


def test_rejects_missing_objective():
    g = _base_goal()
    del g["objective"]
    ok, _ = protocol.validate_goal(g)
    assert not ok


def test_rejects_duplicate_step_ids():
    g = _base_goal(plan={"steps": [
        {"id": "s1", "type": "shell", "command": "a"},
        {"id": "s1", "type": "shell", "command": "b"},
    ]})
    ok, err = protocol.validate_goal(g)
    assert not ok and "duplicated" in err


def test_rejects_shell_without_command():
    g = _base_goal(plan={"steps": [{"id": "s1", "type": "shell"}]})
    ok, _ = protocol.validate_goal(g)
    assert not ok


def test_rejects_unknown_planner():
    g = _base_goal(plan={"planner": "remote-llm", "steps": [
        {"id": "s1", "type": "shell", "command": "a"}]})
    ok, _ = protocol.validate_goal(g)
    assert not ok


def test_rejects_workspace_relative():
    ok, _ = protocol.validate_goal(_base_goal(workspace="relative/path"))
    assert not ok


def test_rejects_bad_constraints():
    ok, _ = protocol.validate_goal(_base_goal(constraints={"maxSteps": "ten"}))
    assert not ok


def test_event_validation_roundtrip():
    ev = protocol.make_event(protocol.KIND_STARTED, "g1", {"a": 1})
    ok, err = protocol.validate_event(ev)
    assert ok, err
    ev["kind"] = "bogus"
    ok, _ = protocol.validate_event(ev)
    assert not ok
    ev["kind"] = protocol.KIND_STARTED
    ev["protocol"] = "rao/9"
    ok, _ = protocol.validate_event(ev)
    assert not ok


def test_forbidden_command_check():
    assert protocol.command_violates_forbidden(
        "git push origin main", ["git push"]) == "git push"
    assert protocol.command_violates_forbidden("ls", ["git push"]) is None
    assert protocol.command_violates_forbidden(None, ["x"]) is None


def test_step_timeout_fallback_chain():
    step = {"id": "s", "type": "shell", "command": "x", "timeoutMs": 5000}
    assert protocol.step_effective_timeout(step, {}) == 5000
    assert protocol.step_effective_timeout(
        {"id": "s", "type": "shell", "command": "x"}, {}) == \
        protocol.DEFAULTS["stepTimeoutMs"]


def test_ensure_workspace_valid():
    ok, err = protocol.validate_goal(_base_goal(ensureWorkspace={
        "repoUrl": "https://example.com/r.git", "path": "/srv/r",
        "branch": "main"}))
    assert ok, err
    ok, _ = protocol.validate_goal(_base_goal(ensureWorkspace={
        "repoUrl": "file:///tmp/r", "path": "/srv/r"}))
    assert ok


def test_ensure_workspace_invalid():
    ok, _ = protocol.validate_goal(_base_goal(ensureWorkspace={"path": "/x"}))
    assert not ok, "repoUrl required"
    ok, _ = protocol.validate_goal(_base_goal(ensureWorkspace={
        "repoUrl": "x", "path": "relative"}))
    assert not ok, "path must be absolute"
    ok, _ = protocol.validate_goal(_base_goal(ensureWorkspace="string"))
    assert not ok
