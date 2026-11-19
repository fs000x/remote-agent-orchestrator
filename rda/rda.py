#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""RDA — Remote Device Agent (goal execution engine, v1).

Usage: python3 rda.py run-goal
Protocol: one JSON line (goal) on stdin, then JSON-line events on stdout;
further stdin lines are injections (steer/cancel/ping). See DESIGN.md §4/§9.

Python >= 3.8, stdlib only. stdout carries ONLY protocol lines;
RDA's own logs go to stderr.
"""
import json
import os
import platform
import socket
import subprocess
import sys
import threading
import time
import queue

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import protocol  # noqa: E402  (deployed alongside this file)

RDA_VERSION = "0.1.0"
EXIT_OK, EXIT_TIMEOUT, EXIT_CANCELLED, EXIT_ERROR = 0, 3, 4, 1
POLL_INTERVAL_S = 0.2


class InjectionReader(threading.Thread):
    """Reads post-goal stdin lines (steer/cancel/ping) in the background."""

    def __init__(self, stream):
        super(InjectionReader, self).__init__(name="rda-inject", daemon=True)
        self.stream = stream
        self.q = queue.Queue()
        self.stopped = False

    def run(self):
        try:
            for raw in self.stream:
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    self.q.put(json.loads(raw))
                except ValueError:
                    self.log("non-json injection dropped: %.120s" % raw)
        except Exception as exc:
            self.log("injection reader ended: %s" % exc)

    def log(self, msg):
        sys.stderr.write("[rda] %s\n" % msg)
        sys.stderr.flush()


class GoalEngine(object):
    def __init__(self, goal):
        self.goal = goal
        self.goal_id = goal["goalId"]
        self.constraints = goal.get("constraints", {})
        self.deadline_ms = protocol.now_ms() + int(
            self.constraints.get("timeoutMs", protocol.DEFAULTS["timeoutMs"]))
        self.step_attempts = 0
        self.max_steps = int(self.constraints.get("maxSteps",
                                                  protocol.DEFAULTS["maxSteps"]))
        self.injections = InjectionReader(sys.stdin)
        self.injections.start()
        self.cancelled = False
        self.timed_out = False
        self.ask_responses = {}       # askId -> answer payload
        self.steer_notes = []         # free-text guidance, echoed in result
        self.skip_ids = set()         # steer directive skip:<id>
        self.extra_runs = []          # steer directive run:<command>
        self.assumptions = []
        self.steps_report = []
        self.artifacts = []
        self.total_retries = 0

    # --- plumbing ----------------------------------------------------------
    def emit(self, kind, data, **extra):
        ev = protocol.make_event(kind, self.goal_id, data, **extra)
        sys.stdout.write(json.dumps(ev, ensure_ascii=False) + "\n")
        sys.stdout.flush()
        return ev

    def log(self, msg):
        sys.stderr.write("[rda] %s\n" % msg)
        sys.stderr.flush()

    def deadline_passed(self):
        return protocol.now_ms() >= self.deadline_ms

    def budget_left(self):
        return self.step_attempts < self.max_steps

    def poll_injections(self):
        """Drain injections; returns False if engine must stop."""
        while True:
            try:
                inj = self.injections.q.get_nowait()
            except queue.Empty:
                return True
            t = inj.get("type")
            if t == "cancel":
                self.cancelled = True
                return False
            if t == "ping":
                self.emit(protocol.KIND_PROGRESS,
                          {"ping": "pong", "stepAttempts": self.step_attempts})
            elif t == "steer":
                self._apply_steer(inj)
        # unreachable

    def _apply_steer(self, inj):
        ask_id = inj.get("askId")
        if ask_id:
            self.ask_responses[ask_id] = inj.get("answer")
            return
        msg = inj.get("message")
        if not isinstance(msg, str) or not msg.strip():
            return
        msg = msg.strip()
        if msg.lower().startswith("skip:"):
            self.skip_ids.add(msg[5:].strip())
            self.steer_notes.append({"directive": msg})
            return
        if msg.lower().startswith("run:"):
            self.extra_runs.append({"id": "steer-%d" % len(self.extra_runs),
                                    "type": "shell", "command": msg[4:].strip(),
                                    "optional": True})
            self.steer_notes.append({"directive": msg})
            return
        self.steer_notes.append({"message": msg})

    # --- event wait --------------------------------------------------------
    def wait_ask(self, ask_id, timeout_s):
        """Block for the steer answer of ask_id; None on timeout/stop."""
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if ask_id in self.ask_responses:
                return self.ask_responses.pop(ask_id)
            if not self.poll_injections():
                return None
            if self.deadline_passed():
                self.timed_out = True
                return None
            time.sleep(POLL_INTERVAL_S)
        return None

    # --- criteria ----------------------------------------------------------
    def _criteria_met(self, step, res):
        crit = step.get("success", {"exitCode": 0})
        if "exitCode" in crit and res.returncode != crit["exitCode"]:
            return False
        if "stdoutContains" in crit and crit["stdoutContains"] not in (res.stdout or ""):
            return False
        if "stderrContains" in crit and crit["stderrContains"] not in (res.stderr or ""):
            return False
        if "fileExists" in crit:
            base = step.get("cwd") or os.getcwd()
            if not os.path.exists(os.path.join(base, crit["fileExists"])):
                return False
        return True

    def _collect_artifacts(self, step, res, attempt_data):
        base = step.get("cwd") or os.getcwd()
        for rel in step.get("artifacts", []) or []:
            p = os.path.join(base, rel)
            if os.path.exists(p):
                self.artifacts.append({"path": p, "size": os.path.getsize(p),
                                       "stepId": step.get("id")})
        if res is not None:
            tail = (res.stdout or "")[-400:]
            if tail.strip():
                attempt_data["stdoutTail"] = tail
            errtail = (res.stderr or "")[-400:]
            if errtail.strip():
                attempt_data["stderrTail"] = errtail

    # --- step execution ----------------------------------------------------
    def run_step(self, step):
        """Run one step (with retries/fallbacks). Returns report dict."""
        sid = step.get("id", "?")
        stype = step.get("type")
        if sid in self.skip_ids:
            self.emit(protocol.KIND_STEP_RESULT,
                      {"stepId": sid, "status": "skipped",
                       "reason": "steer skip directive"})
            rep = {"id": sid, "status": "skipped"}
            self.steps_report.append(rep)
            return rep

        retries = int(step.get("retries", 0))
        backoff = int(step.get("retryBackoffMs",
                               protocol.DEFAULTS["retryBackoffMs"])) / 1000.0
        attempt_data = None
        variants = [(step, "ok")] + [(fb, "fallback:%d" % i)
                                     for i, fb in enumerate(step.get("fallbacks", []) or [])]
        rep = {"id": sid, "status": "failed"}

        for v_idx, (variant, v_label) in enumerate(variants):
            tries = 1 if v_label.startswith("fallback") else (retries + 1)
            for attempt in range(1, tries + 1):
                if self.cancelled or self.timed_out or not self.budget_left():
                    break
                self.step_attempts += 1
                self.total_retries += 1 if attempt > 1 else 0
                attempt_data = {"stepId": sid, "attempt": attempt,
                                "variant": v_label, "phase": "running"}
                self.emit(protocol.KIND_PROGRESS, attempt_data)
                attempt_data = dict(attempt_data)
                ok = False
                if v_label == "ok" or v_label.startswith("fallback"):
                    ok = self._run_variant(variant, sid, attempt_data)
                attempt_data["phase"] = "done"
                attempt_data["status"] = "ok" if ok else "failed"
                self.emit(protocol.KIND_STEP_RESULT, attempt_data)
                if ok:
                    rep = {"id": sid,
                           "status": "ok" if v_label == "ok" else "fallback_ok",
                           "variant": v_label, "attempts": attempt}
                    self.steps_report.append(rep)
                    return rep
                if attempt < tries and not self.cancelled and not self.deadline_passed():
                    time.sleep(backoff)
            if self.cancelled or self.timed_out:
                break

        optional = bool(step.get("optional"))
        rep = {"id": sid, "status": "skipped" if optional else "failed"}
        self.steps_report.append(rep)
        self.emit(protocol.KIND_ERROR,
                  {"message": "step %s %s" % (
                      sid, "skipped (optional)" if optional else "failed"),
                   "fatal": not optional})
        return rep

    def _run_variant(self, step, sid, attempt_data):
        stype = step.get("type")
        timeout_s = protocol.step_effective_timeout(step, self.constraints) / 1000.0
        if stype == "shell":
            cmd = step.get("command", "")
            violated = protocol.command_violates_forbidden(
                cmd, self.constraints.get("forbidden", []))
            if violated:
                attempt_data["status"] = "failed"
                attempt_data["reason"] = "forbidden command pattern: %s" % violated
                self.emit(protocol.KIND_WARNING,
                          {"message": "step %s blocked: matches forbidden %r"
                                      % (sid, violated)})
                return False
            cwd = step.get("cwd") or self.goal.get("workspace") or None
            try:
                res = subprocess.run(cmd, shell=True, cwd=cwd,
                                     capture_output=True, timeout=timeout_s)
            except subprocess.TimeoutExpired:
                attempt_data["reason"] = "step timeout"
                return False
            except Exception as exc:
                attempt_data["reason"] = "spawn error: %s" % exc
                return False
            self._collect_artifacts(step, res, attempt_data)
            return self._criteria_met(step, res)
        if stype == "wait_file":
            path = step["path"]
            deadline = time.monotonic() + timeout_s
            while time.monotonic() < deadline:
                if os.path.exists(path):
                    return True
                time.sleep(0.5)
                self.poll_injections()
            attempt_data["reason"] = "wait_file timeout: %s" % path
            return False
        if stype == "artifact":
            base = step.get("cwd") or self.goal.get("workspace") or "."
            missing = [p for p in step.get("paths", [])
                       if not os.path.exists(os.path.join(base, p))]
            if missing:
                attempt_data["reason"] = "missing artifacts: %s" % ", ".join(missing)
                return False
            for p in step.get("paths", []):
                full = os.path.join(base, p)
                self.artifacts.append({"path": full, "size": os.path.getsize(full),
                                       "stepId": sid})
            return True
        self.log("unknown step type %r" % stype)
        return False

    # --- ask step ----------------------------------------------------------
    def run_ask_step(self, step):
        ask_id = "ask-%s" % step.get("id", "x")
        ev = self.emit(protocol.KIND_ASK,
                       {"question": step.get("question", ""),
                        "context": step.get("context", {})}, askId=ask_id)
        answer = self.wait_ask(ask_id, timeout_s=3600)
        if answer is None:
            rep = {"id": step.get("id"), "status": "failed",
                   "reason": "ask unanswered"}
            self.steps_report.append(rep)
            self.emit(protocol.KIND_ERROR,
                      {"message": "ask %s unanswered" % ask_id, "fatal": True})
            return rep
        rep = {"id": step.get("id"), "status": "ok", "answer": answer}
        self.steps_report.append(rep)
        return rep

    # --- recon -------------------------------------------------------------
    def run_recon(self):
        facts = {}
        try:
            facts["os"] = "%s %s" % (platform.system(), platform.release())
            facts["arch"] = platform.machine()
            facts["python"] = sys.version.split()[0]
            facts["hostname"] = socket.gethostname()
            facts["cpus"] = os.cpu_count()
            if os.path.isdir("/proc"):
                try:
                    with open("/proc/meminfo") as fh:
                        for line in fh:
                            if line.startswith("MemAvailable"):
                                facts["memAvailableKB"] = int(line.split()[1])
                                break
                except Exception:
                    pass
            tools = {}
            for t in ("git", "make", "gcc", "python3", "node", "docker", "adb"):
                path = None
                for d in os.environ.get("PATH", "").split(os.pathsep):
                    cand = os.path.join(d, t)
                    if os.path.isfile(cand) and os.access(cand, os.X_OK):
                        path = cand
                        break
                tools[t] = bool(path)
            facts["tools"] = tools
            ws = self.goal.get("workspace")
            if ws and os.path.isdir(ws):
                entries = sorted(os.listdir(ws))[:30]
                git_state = {}
                try:
                    r = subprocess.run("git status --porcelain -b | head -5",
                                       shell=True, cwd=ws, capture_output=True,
                                       timeout=10)
                    git_state["statusTail"] = (r.stdout or b"").decode(
                        "utf-8", "replace").strip()
                    r2 = subprocess.run("git log --oneline -1",
                                        shell=True, cwd=ws, capture_output=True,
                                        timeout=10)
                    git_state["head"] = (r2.stdout or b"").decode(
                        "utf-8", "replace").strip()
                except Exception:
                    pass
                facts["workspace"] = {"path": ws, "entries": entries,
                                      "git": git_state}
        except Exception as exc:
            facts["error"] = str(exc)
        return facts

    # --- main loop ---------------------------------------------------------
    def run(self):
        try:
            ws = self.goal.get("workspace")
            if ws and os.path.isdir(ws):
                os.chdir(ws)
            host = socket.gethostname()
            self.emit(protocol.KIND_STARTED,
                      {"rdaVersion": RDA_VERSION, "python": sys.version.split()[0],
                       "host": host, "pid": os.getpid()})

            plan = self.goal.get("plan")
            if not plan or not (plan.get("steps") or []):
                facts = self.run_recon()
                self.emit(protocol.KIND_RECON, {"facts": facts})
                ask_id = "plan-req-1"
                self.emit(protocol.KIND_ASK,
                          {"question": "goal has no plan; send steer with plan "
                                       "steps after reviewing recon",
                           "context": {"recon": facts}}, askId=ask_id)
                answer = self.wait_ask(ask_id, timeout_s=3600)
                if answer is None:
                    return self.finish(protocol.ST_TIMEOUT if self.timed_out
                                       else protocol.ST_FAILED,
                                       "no plan received for plan-less goal")
                new_plan = (answer or {}).get("plan")
                synthetic = {"protocol": protocol.PROTOCOL_VERSION,
                             "goalId": self.goal_id, "deviceId": "x",
                             "objective": "validate steered plan",
                             "plan": new_plan}
                ok, err = protocol.validate_goal(synthetic)
                if not ok:
                    return self.finish(protocol.ST_FAILED,
                                       "steered plan invalid: %s" % err)
                plan = new_plan
                self.assumptions.append("plan received via steer at runtime")

            steps = list(plan.get("steps", []))
            queue_steps = steps + self.extra_runs  # steer run: directives append
            failed_required = False
            executed = 0
            while executed < len(queue_steps):
                if not self.poll_injections():
                    break
                if self.timed_out or self.deadline_passed():
                    self.timed_out = True
                    break
                if not self.budget_left():
                    self.emit(protocol.KIND_ERROR,
                              {"message": "RAO_BUDGET_EXCEEDED: maxSteps=%d"
                                          % self.max_steps, "fatal": True})
                    return self.finish(protocol.ST_TIMEOUT, "step budget exhausted")
                step = queue_steps[executed]
                executed += 1
                if step.get("type") == "ask":
                    rep = self.run_ask_step(step)
                else:
                    rep = self.run_step(step)
                if rep.get("status") == "failed":
                    failed_required = True
                    break
                # new steer run: directives may have appeared
                if len(queue_steps) < len(steps) + len(self.extra_runs):
                    queue_steps = steps + self.extra_runs

            if self.cancelled:
                return self.finish(protocol.ST_CANCELLED, "cancelled via steer")
            if self.timed_out:
                return self.finish(protocol.ST_TIMEOUT, "goal deadline exceeded")
            if failed_required:
                return self.finish(protocol.ST_FAILED, "required step failed")
            done_ok = [r for r in self.steps_report if r.get("status") == "ok"]
            skipped = [r for r in self.steps_report if r.get("status") == "skipped"]
            if skipped and done_ok:
                return self.finish(protocol.ST_PARTIAL,
                                   "completed with skipped optional steps")
            return self.finish(protocol.ST_SUCCESS, "all steps ok")

        except Exception as exc:  # engine bug / unexpected
            self.log("engine error: %r" % exc)
            try:
                self.emit(protocol.KIND_ERROR,
                          {"message": "rda engine error: %s" % exc, "fatal": True})
            except Exception:
                pass
            return self.finish(protocol.ST_FAILED, "engine error: %s" % exc)

    def finish(self, status, summary):
        data = {"status": status, "summary": summary,
                "steps": self.steps_report, "artifacts": self.artifacts,
                "assumptions": self.assumptions,
                "steerNotes": self.steer_notes,
                "metrics": {"stepAttempts": self.step_attempts,
                            "retries": self.total_retries,
                            "durationMs": max(
                                0, protocol.DEFAULTS["timeoutMs"]
                                - (self.deadline_ms - protocol.now_ms()))}}
        self.emit(protocol.KIND_RESULT, data)
        self.injections.stopped = True
        return EXIT_TIMEOUT if status == protocol.ST_TIMEOUT else \
            EXIT_CANCELLED if status == protocol.ST_CANCELLED else EXIT_OK


def main():
    if len(sys.argv) < 2 or sys.argv[1] != "run-goal":
        sys.stderr.write("usage: rda.py run-goal\n")
        return 2
    line = sys.stdin.readline()
    line = line.strip()
    if not line:
        sys.stderr.write("no goal received on stdin\n")
        return 2
    try:
        goal = json.loads(line)
    except ValueError as exc:
        sys.stderr.write("goal json invalid: %s\n" % exc)
        return 2
    ok, err = protocol.validate_goal(goal)
    if not ok:
        sys.stderr.write("goal invalid: %s\n" % err)
        sys.stdout.write(json.dumps(protocol.make_event(
            protocol.KIND_ERROR, goal.get("goalId", "?"),
            {"message": "RAO_GOAL_INVALID: %s" % err, "fatal": True})) + "\n")
        sys.stdout.flush()
        return 2
    return GoalEngine(goal).run()


if __name__ == "__main__":
    sys.exit(main())
