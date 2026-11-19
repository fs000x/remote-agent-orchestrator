"""Orchestrator: dispatch pool, per-goal run threads, budgets, run store, reports."""
import copy
import json
import os
import threading
import time

from . import protocol
from .transport import SshTransport, TransportError

KILL_GRACE_S = 5.0
PUMP_SLICE_S = 1.0
ASK_TIMEOUT_MS = 3600000  # ask may wait long; overall deadline still applies


class GoalRun:
    _SEQ = [0]
    _SEQ_LOCK = threading.Lock()

    def __init__(self, goal, device, orch):
        self.goal = goal
        self.device = device
        self.orch = orch
        self.goal_id = goal["goalId"]
        self.state = protocol.RUN_QUEUED
        self.error = None
        self.result = None
        self.chan = None
        self.dispatched_ms = protocol.now_ms()
        self.deadline_ms = self.dispatched_ms + int(
            goal.get("constraints", {}).get(
                "timeoutMs", protocol.DEFAULTS["timeoutMs"]))
        self.silence_ms = int(goal.get("constraints", {}).get(
            "eventSilenceMs", protocol.DEFAULTS["eventSilenceMs"]))
        self.max_steps = int(goal.get("constraints", {}).get(
            "maxSteps", protocol.DEFAULTS["maxSteps"]))
        self._attempt_count = 0
        self._finalized = False
        self.cancel_requested = False
        self._wake = threading.Condition()
        self._thread = threading.Thread(target=self._run, daemon=True,
                                        name="rao-goal-%s" % self.goal_id[:8])

    # --- state helpers -----------------------------------------------------
    def _set_state(self, state):
        # terminal states are sticky: whoever decides the outcome first wins
        if self.is_terminal():
            return
        self.state = state
        with self._wake:
            self._wake.notify_all()

    def is_terminal(self):
        return self.state in (protocol.RUN_DONE, protocol.RUN_FAILED,
                              protocol.RUN_TIMEOUT, protocol.RUN_CANCELLED)

    def wait_terminal(self, timeout_s):
        deadline = time.monotonic() + timeout_s
        with self._wake:
            while not (self.is_terminal() and self._finalized):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._wake.wait(remaining)
        return True

    def start(self):
        self._thread.start()

    # --- main pump ---------------------------------------------------------
    def _run(self):
        orch = self.orch
        try:
            with orch._slot_for(self.goal):
                if self.cancel_requested:
                    # cancelled while queued: never open a channel
                    self.result = {"status": protocol.ST_CANCELLED,
                                   "summary": "cancelled before start",
                                   "steps": [], "artifacts": [],
                                   "assumptions": [], "metrics": {}}
                    self._set_state(protocol.RUN_CANCELLED)
                    return
                self._set_state(protocol.RUN_RUNNING)
                try:
                    # pass the goal OBJECT; transport encodes it exactly once
                    self.chan = orch.transport.open_session(
                        self.goal_id, self.device, self.goal)
                except TransportError as exc:
                    self.error = "%s: %s" % (exc.code, exc)
                    self._set_state(protocol.RUN_FAILED)
                    return
                if self.cancel_requested:
                    self.chan.close()
                    self.result = {"status": protocol.ST_CANCELLED,
                                   "summary": "cancelled at start",
                                   "steps": [], "artifacts": [],
                                   "assumptions": [], "metrics": {}}
                    self._set_state(protocol.RUN_CANCELLED)
                    return
                self._pump()
        except Exception as exc:  # never leak thread exceptions
            self.error = "orchestrator error: %s" % exc
            self._set_state(protocol.RUN_FAILED)
        finally:
            self._finalize()

    def _pump(self):
        idx = 0
        last_event_ms = protocol.now_ms()
        asked = False
        while True:
            ev, idx = self.chan.wait_event(idx, PUMP_SLICE_S)
            now = protocol.now_ms()
            if ev is not None:
                last_event_ms = now
                kind = ev.get("kind")
                if kind == protocol.KIND_ASK:
                    asked = True
                    self._set_state(protocol.RUN_ASK_WAITING)
                elif asked and kind in (protocol.KIND_PROGRESS,
                                        protocol.KIND_MILESTONE,
                                        protocol.KIND_STEP_RESULT,
                                        protocol.KIND_RECON,
                                        protocol.KIND_WARNING):
                    asked = False
                    self._set_state(protocol.RUN_RUNNING)
                if kind == protocol.KIND_PROGRESS and \
                        ev.get("data", {}).get("phase") == "running":
                    self._attempt_count += 1
                if kind == protocol.KIND_RESULT:
                    if self.is_terminal():
                        # deadline/cancel already decided the outcome
                        return
                    self.result = ev.get("data", {})
                    self._set_state(self._result_state(self.result.get("status")))
                    return
            # orchestrator-side enforcement (DESIGN.md A6): decide the terminal
            # state FIRST (sticky), then stop the process — a racing result
            # event from RDA must not override (T7).
            if self._attempt_count > self.max_steps:
                self._set_state(protocol.RUN_TIMEOUT)
                self._force_cancel("RAO_BUDGET_EXCEEDED (orchestrator)")
                return
            if now >= self.deadline_ms:
                self._set_state(protocol.RUN_TIMEOUT)
                self._force_cancel("goal deadline exceeded (orchestrator)")
                return
            if not self.chan.closed and \
                    now - last_event_ms > self.silence_ms and \
                    not self.chan.alive():
                self._set_state(protocol.RUN_FAILED)
                self.error = "RAO_TRANSPORT_LOST: silence watchdog"
                self._force_cancel("event silence + dead channel")
                return
            if now - last_event_ms > self.silence_ms * 3:
                self._set_state(protocol.RUN_FAILED)
                self.error = "RAO_TRANSPORT_LOST: silence watchdog 3x"
                self._force_cancel("event silence watchdog (3x)")
                return
            if self.chan.exit_code is not None and idx >= len(self.chan.events):
                break  # process exited; drain complete
        # exited without result event
        if self.chan.killed or self.cancel_requested:
            status = protocol.ST_CANCELLED
        else:
            status = protocol.ST_FAILED
        self.result = {"status": status,
                       "summary": "RDA exited (code=%s) without result event"
                                  % self.chan.exit_code,
                       "steps": [], "artifacts": [], "assumptions": [],
                       "metrics": {}}
        self._set_state(self._result_state(status))

    def _result_state(self, status):
        return {
            protocol.ST_SUCCESS: protocol.RUN_DONE,
            protocol.ST_PARTIAL: protocol.RUN_DONE,
            protocol.ST_FAILED: protocol.RUN_FAILED,
            protocol.ST_TIMEOUT: protocol.RUN_TIMEOUT,
            protocol.ST_CANCELLED: protocol.RUN_CANCELLED,
        }.get(status, protocol.RUN_FAILED)

    def _force_cancel(self, reason):
        if self.chan is None or self.chan.closed:
            return
        self.chan.write_line({"type": "cancel", "reason": reason})
        exited = self.chan.wait_exit(KILL_GRACE_S)
        if exited is None:
            self.chan.kill()
        self.error = reason

    # --- control API (called from MCP tools) --------------------------------
    def steer(self, message=None, ask_id=None, answer=None):
        if self.chan is None:
            return False
        payload = {"type": "steer"}
        if ask_id is not None:
            payload["askId"] = ask_id
            payload["answer"] = answer
        else:
            payload["message"] = message
        return self.chan.write_line(payload)

    def cancel(self, reason="user requested"):
        if self.is_terminal():
            return False
        self.cancel_requested = True
        self._set_state(protocol.RUN_CANCELLED)  # sticky decision first
        self._force_cancel(reason)
        return True

    def snapshot(self, max_events=50):
        events = self.chan.events[-max_events:] if self.chan else []
        return {
            "goalId": self.goal_id,
            "deviceId": self.goal["deviceId"],
            "state": self.state,
            "error": self.error,
            "attemptCount": self._attempt_count,
            "result": self.result,
            "events": events,
            "stderrTail": (self.chan.stderr_text[-20:] if self.chan else []),
        }

    def _finalize(self):
        if self.chan is not None:
            if self.result is None:
                # no result event: synthesize from the decided state
                st = {protocol.RUN_TIMEOUT: protocol.ST_TIMEOUT,
                      protocol.RUN_CANCELLED: protocol.ST_CANCELLED,
                      protocol.RUN_DONE: protocol.ST_SUCCESS}.get(
                          self.state, protocol.ST_FAILED)
                self.result = {"status": st,
                               "summary": self.error or
                                          "run ended without result event",
                               "steps": [], "artifacts": [], "assumptions": [],
                               "metrics": {}}
            self.chan.close()
        self.orch.run_store.save(self)
        self._finalized = True
        with self._wake:
            self._wake.notify_all()
        if not self.is_terminal():
            self._set_state(self._result_state(
                (self.result or {}).get("status", protocol.ST_FAILED)))


class RunStore:
    def __init__(self, runs_dir):
        self.runs_dir = runs_dir
        os.makedirs(runs_dir, exist_ok=True)

    def save(self, run):
        rec = {
            "goal": run.goal,
            "device": {"id": run.device["id"], "host": run.device["host"],
                       "user": run.device["user"]},
            "state": run.state,
            "error": run.error,
            "result": run.result,
            "events": (run.chan.events if run.chan else []),
            "stderrTail": (run.chan.stderr_text[-100:] if run.chan else []),
            "dispatchedMs": run.dispatched_ms,
            "finishedMs": protocol.now_ms(),
        }
        path = os.path.join(self.runs_dir, "%s.json" % run.goal_id)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(rec, fh, ensure_ascii=False, indent=1)
        os.replace(tmp, path)
        return path

    def save_report(self, report):
        path = os.path.join(self.runs_dir,
                            "report-%d.json" % protocol.now_ms())
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(report, fh, ensure_ascii=False, indent=1)
        return path


class Orchestrator:
    def __init__(self, registry, transport=None, runs_dir=None,
                 max_concurrent_goals=5):
        self.registry = registry
        self.transport = transport or SshTransport(
            rda_dir=os.environ.get("RAO_RDA_DIR",
                                   "/home/diablo/git500/zcode_prj/rao"))
        self.max_concurrent_goals = max_concurrent_goals
        self.run_store = RunStore(runs_dir or os.environ.get(
            "RAO_RUNS_DIR",
            os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(
                os.path.abspath(__file__)))), "runs")))
        self._lock = threading.RLock()
        self._global_slots = threading.Semaphore(max_concurrent_goals)
        self._device_slots = {}   # device_id -> Semaphore
        self._device_slot_lock = threading.Lock()
        self.runs = {}            # goalId -> GoalRun

    # --- slot management ---------------------------------------------------
    def _slot_for(self, goal):
        did = goal["deviceId"]
        with self._device_slot_lock:
            if did not in self._device_slots:
                per_device = int(self.registry.require(did).get(
                    "maxConcurrentGoals", 2))
                self._device_slots[did] = threading.Semaphore(per_device)
        class _SlotCtx:
            def __init__(s, orch, sem_dev):
                s.orch, s.sem_dev = orch, sem_dev
            def __enter__(s):
                s.orch._global_slots.acquire()
                s.sem_dev.acquire()
            def __exit__(s, *a):
                s.sem_dev.release()
                s.orch._global_slots.release()
        return _SlotCtx(self, self._device_slots[did])

    # --- dispatch ----------------------------------------------------------
    def dispatch(self, device_ids, goal):
        g = dict(goal)
        g.setdefault("protocol", protocol.PROTOCOL_VERSION)
        # validate a probe clone: goalId/deviceId are per-device values filled below
        probe = dict(g)
        if not probe.get("goalId"):
            probe["goalId"] = "validation-probe"
        if not probe.get("deviceId"):
            probe["deviceId"] = device_ids[0] if device_ids else "x"
        ok, err = protocol.validate_goal(probe)
        if not ok:
            raise ValueError("RAO_GOAL_INVALID: %s" % err)
        dispatches = []
        for did in device_ids:
            device = self.registry.require(did)  # KeyError -> tool surfaces it
            g = copy.deepcopy(goal)
            if not g.get("goalId"):
                g["goalId"] = protocol.new_goal_id()
            g["deviceId"] = did
            run = GoalRun(g, device, self)
            with self._lock:
                self.runs[g["goalId"]] = run
            run.start()
            dispatches.append({"goalId": g["goalId"], "deviceId": did,
                               "state": run.state})
        return dispatches

    def get_run(self, goal_id):
        with self._lock:
            return self.runs.get(goal_id)

    def runs_for_device(self, device_id):
        with self._lock:
            return [r for r in self.runs.values()
                    if r.goal["deviceId"] == device_id]

    # --- collect -----------------------------------------------------------
    def collect(self, goal_ids, timeout_ms=120000):
        deadline = time.monotonic() + timeout_ms / 1000.0
        matrix = []
        for gid in goal_ids:
            run = self.get_run(gid)
            if run is None:
                matrix.append({"goalId": gid, "status": "unknown",
                               "summary": "no such goalId"})
                continue
            remaining = max(0.5, deadline - time.monotonic())
            run.wait_terminal(remaining)
            snap = run.snapshot(max_events=0)
            res = run.result or {}
            matrix.append({
                "goalId": gid,
                "deviceId": snap["deviceId"],
                "status": res.get("status", run.state),
                "summary": res.get("summary", run.error),
                "steps": res.get("steps", []),
                "artifacts": res.get("artifacts", []),
                "assumptions": res.get("assumptions", []),
                "steerNotes": res.get("steerNotes", []),
                "metrics": res.get("metrics", {}),
            })
        report = {
            "generatedMs": protocol.now_ms(),
            "matrix": matrix,
            "successCount": sum(1 for m in matrix
                                if m["status"] in (protocol.ST_SUCCESS,
                                                   protocol.ST_PARTIAL)),
            "failCount": sum(1 for m in matrix
                             if m["status"] not in (protocol.ST_SUCCESS,
                                                    protocol.ST_PARTIAL,
                                                    "unknown")),
        }
        report["reportFile"] = self.run_store.save_report(report)
        return report
