"""RAO Wire Protocol v1 — shared schema/validation (stdlib only, py3.8 compatible).

This module is deployed verbatim to remote devices alongside rda.py, so it must
stay dependency-free and syntax-compatible with Python 3.8.
"""
import re
import time
import uuid

PROTOCOL_VERSION = "rao/1"

# --- event kinds (RDA -> orchestrator) ---
KIND_STARTED = "started"
KIND_RECON = "recon"
KIND_ASK = "ask"
KIND_PROGRESS = "progress"
KIND_MILESTONE = "milestone"
KIND_STEP_RESULT = "step_result"
KIND_WARNING = "warning"
KIND_ERROR = "error"
KIND_RESULT = "result"
KNOWN_KINDS = (
    KIND_STARTED, KIND_RECON, KIND_ASK, KIND_PROGRESS, KIND_MILESTONE,
    KIND_STEP_RESULT, KIND_WARNING, KIND_ERROR, KIND_RESULT,
)

# --- result status ---
ST_SUCCESS = "success"
ST_PARTIAL = "partial"
ST_FAILED = "failed"
ST_TIMEOUT = "timeout"
ST_CANCELLED = "cancelled"
KNOWN_STATUS = (ST_SUCCESS, ST_PARTIAL, ST_FAILED, ST_TIMEOUT, ST_CANCELLED)

# --- goal run states (orchestrator side) ---
RUN_QUEUED = "queued"
RUN_RUNNING = "running"
RUN_ASK_WAITING = "ask_waiting"
RUN_DONE = "done"
RUN_FAILED = "failed"
RUN_TIMEOUT = "timeout"
RUN_CANCELLED = "cancelled"

STEP_TYPES = ("shell", "ask", "artifact", "wait_file")

DEFAULTS = {
    "maxSteps": 40,
    "timeoutMs": 600000,
    "stepTimeoutMs": 120000,
    "retryBackoffMs": 2000,
    "eventSilenceMs": 90000,
}

_ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


def new_goal_id():
    return str(uuid.uuid4())


def now_ms():
    return int(time.time() * 1000)


def make_event(kind, goal_id, data, **extra):
    ev = {"protocol": PROTOCOL_VERSION, "kind": kind, "goalId": goal_id,
          "ts": now_ms(), "data": data}
    ev.update(extra)
    return ev


def _check_step(step, idx, seen_ids):
    """Validate one step dict; return error string or None."""
    if not isinstance(step, dict):
        return "plan.steps[%d] must be an object" % idx
    sid = step.get("id")
    if not isinstance(sid, str) or not _ID_RE.match(sid):
        return "plan.steps[%d].id invalid" % idx
    if sid in seen_ids:
        return "plan.steps[%d].id duplicated: %s" % (idx, sid)
    seen_ids.add(sid)
    if step.get("type") not in STEP_TYPES:
        return "plan.steps[%d].type must be one of %s" % (idx, STEP_TYPES)
    if step["type"] == "shell" and not isinstance(step.get("command"), str):
        return "plan.steps[%d] shell requires command" % idx
    if step["type"] == "ask" and not isinstance(step.get("question"), str):
        return "plan.steps[%d] ask requires question" % idx
    if step["type"] == "wait_file":
        p = step.get("path")
        if not isinstance(p, str) or not p:
            return "plan.steps[%d] wait_file requires path" % idx
    if step["type"] == "artifact" and not isinstance(step.get("paths"), list):
        return "plan.steps[%d] artifact requires paths list" % idx
    success = step.get("success", {"exitCode": 0})
    if not isinstance(success, dict):
        return "plan.steps[%d].success must be object" % idx
    for k in success:
        if k not in ("exitCode", "stdoutContains", "stderrContains", "fileExists"):
            return "plan.steps[%d].success unknown key: %s" % (idx, k)
    for int_k in ("retries", "retryBackoffMs", "timeoutMs"):
        if int_k in step and not isinstance(step[int_k], int):
            return "plan.steps[%d].%s must be int" % (idx, int_k)
    fbs = step.get("fallbacks", [])
    if not isinstance(fbs, list):
        return "plan.steps[%d].fallbacks must be list" % idx
    for j, fb in enumerate(fbs):
        if not isinstance(fb, dict) or fb.get("type") not in STEP_TYPES:
            return "plan.steps[%d].fallbacks[%d] invalid" % (idx, j)
        if fb["type"] == "shell" and not isinstance(fb.get("command"), str):
            return "plan.steps[%d].fallbacks[%d] shell requires command" % (idx, j)
    return None


def validate_goal(goal):
    """Validate a goal dict. Returns (ok, error_string_or_None)."""
    if not isinstance(goal, dict):
        return False, "goal must be an object"
    if goal.get("protocol") != PROTOCOL_VERSION:
        return False, "protocol must be %s" % PROTOCOL_VERSION
    if not isinstance(goal.get("goalId"), str) or not goal["goalId"]:
        return False, "goalId required"
    if not isinstance(goal.get("deviceId"), str) or not goal["deviceId"]:
        return False, "deviceId required"
    if not isinstance(goal.get("objective"), str) or not goal["objective"].strip():
        return False, "objective required"
    ws = goal.get("workspace")
    if ws is not None and (not isinstance(ws, str) or not ws.startswith("/")):
        return False, "workspace must be absolute path when present"
    c = goal.get("constraints", {})
    if not isinstance(c, dict):
        return False, "constraints must be object"
    for k, dflt in DEFAULTS.items():
        if k in c and not isinstance(c[k], int):
            return False, "constraints.%s must be int" % k
    for f in c.get("forbidden", []):
        if not isinstance(f, str):
            return False, "constraints.forbidden entries must be strings"
    planner = (goal.get("plan") or {}).get("planner", "main-agent")
    if planner != "main-agent":
        return False, "plan.planner must be 'main-agent' in v1"
    if "plan" in goal and goal["plan"] is not None:
        plan = goal["plan"]
        steps = plan.get("steps")
        if not isinstance(steps, list) or not steps:
            return False, "plan.steps must be a non-empty list"
        seen = set()
        for i, s in enumerate(steps):
            err = _check_step(s, i, seen)
            if err:
                return False, err
    return True, None


def validate_event(ev):
    """Validate an event dict coming from RDA. Returns (ok, error)."""
    if not isinstance(ev, dict):
        return False, "event must be object"
    if ev.get("protocol") != PROTOCOL_VERSION:
        return False, "event protocol mismatch"
    if ev.get("kind") not in KNOWN_KINDS:
        return False, "unknown event kind: %r" % ev.get("kind")
    if not isinstance(ev.get("data"), dict):
        return False, "event.data must be object"
    return True, None


def command_violates_forbidden(command, forbidden):
    """Soft constraint check (documented as non-sandbox). Returns violated pattern or None."""
    if not command:
        return None
    for pat in forbidden or []:
        if pat and pat in command:
            return pat
    return None


def step_effective_timeout(step, goal_constraints):
    return int(step.get("timeoutMs") or goal_constraints.get("stepTimeoutMs")
               or DEFAULTS["stepTimeoutMs"])
