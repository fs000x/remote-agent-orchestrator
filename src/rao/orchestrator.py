"""Orchestrator: dispatch pool, per-goal run threads, budgets, run store, reports."""
import copy
import json
import os
import threading
import time

from . import protocol
from .transport import (SshTransport, TransportError, ChannelLost)  # noqa: F401

KILL_GRACE_S = 5.0
PUMP_SLICE_S = 1.0
ASK_TIMEOUT_MS = 3600000  # ask may wait long; overall deadline still applies
RESUME_MAX_DEFAULT = 5


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
        self.resume_max = int(getattr(orch, "resume_max",
                                      RESUME_MAX_DEFAULT))
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
                if hasattr(orch.transport, "provision"):
                    # v0.5: auto-provision remote runtime BEFORE first use
                    orch.transport.provision(self.device)
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
                if getattr(self.chan, "supports_resume", False):
                    self._pump_daemon()   # v0.3: reconnect + attach on loss
                else:
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

    def _pump_daemon(self):
        """Daemon-mode pump: on link loss, reconnect + attach and continue
        (events deduped by daemon seq). Budget/deadline still enforced."""
        self._last_seq = 0
        attempt = 0
        while True:
            try:
                idx = 0
                last_event_ms = protocol.now_ms()
                asked = False
                while True:
                    ev, idx = self.chan.wait_event(idx, PUMP_SLICE_S)
                    now = protocol.now_ms()
                    if ev is not None:
                        seq = (self.chan.event_seqs[idx - 1]
                               if idx - 1 < len(self.chan.event_seqs) else None)
                        if seq is not None and seq <= self._last_seq:
                            continue  # replayed on attach: already processed
                        self._last_seq = seq or self._last_seq
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
                                return
                            self.result = ev.get("data", {})
                            self._set_state(
                                self._result_state(self.result.get("status")))
                            return
                    if self._attempt_count > self.max_steps:
                        self._set_state(protocol.RUN_TIMEOUT)
                        self._force_cancel("RAO_BUDGET_EXCEEDED (orchestrator)")
                        return
                    if now >= self.deadline_ms:
                        self._set_state(protocol.RUN_TIMEOUT)
                        self._force_cancel("goal deadline exceeded")
                        return
                    if not self.chan.alive():
                        if now - last_event_ms > 2 * PUMP_SLICE_S:
                            print("[DBG-LOST] silence+dead at seq-last=%s" % self._last_seq)
                            raise ChannelLost()
                        # give a dead link a final drain window
                        ev2, idx2 = self.chan.wait_event(idx, PUMP_SLICE_S)
                        if ev2 is None:
                            print("[DBG-LOST] dead-drain-empty at seq-last=%s" % self._last_seq)
                            raise ChannelLost()
                        idx = idx2 - 1  # reprocess next loop
            except ChannelLost:
                attempt += 1
                if self.is_terminal() or self.cancel_requested:
                    return
                if attempt > self.resume_max:
                    self.error = "resume budget exhausted after %d attempts" \
                                 % attempt
                    self._set_state(protocol.RUN_FAILED)
                    return
                time.sleep(min(2.0 * attempt, 5.0))
                try:
                    self.chan.close()
                except Exception:
                    pass
                try:
                    self.chan = self.orch.transport.open_resume_session(
                        self.goal_id, self.device)
                except Exception:   # reconnect failure counts against budget
                    continue

    def _result_state(self, status):
        return {
            protocol.ST_SUCCESS: protocol.RUN_DONE,
            protocol.ST_PARTIAL: protocol.RUN_DONE,
            protocol.ST_FAILED: protocol.RUN_FAILED,
            protocol.ST_TIMEOUT: protocol.RUN_TIMEOUT,
            protocol.ST_CANCELLED: protocol.RUN_CANCELLED,
        }.get(status, protocol.RUN_FAILED)

    def _force_cancel(self, reason):
        # v0.9 control-plane cancel via a fresh on-device connection when the
        # transport supports it; spawn mode keeps the in-band stdin write.
        if hasattr(self.orch.transport, "steer"):
            try:
                self.orch.transport.steer(
                    self.device, self.goal_id,
                    {"type": "cancel", "reason": reason})
            except Exception:
                pass
        elif self.chan is not None and not self.chan.closed:
            self.chan.write_line({"type": "cancel", "reason": reason})
        if self.chan is not None:
            exited = self.chan.wait_exit(KILL_GRACE_S)
            if exited is None:
                self.chan.kill()
        self.error = reason

    # --- control API (called from MCP tools) --------------------------------
    def steer(self, message=None, ask_id=None, answer=None):
        payload = {"type": "steer"}
        if ask_id is not None:
            payload["askId"] = ask_id
            payload["answer"] = answer
        else:
            payload["message"] = message
        # v0.9: control-plane inject via a fresh connection when available —
        # immune to the goal bridge's state (the event bridge has known races)
        if hasattr(self.orch.transport, "steer"):
            import time as _time
            for _attempt in range(3):        # SSH round-trip may race a fast goal
                t0 = _time.monotonic()
                try:
                    ok = self.orch.transport.steer(self.device, self.goal_id,
                                                   payload)
                    print("[steer-dbg] attempt=%d ok=%s t+%.1fs" % (
                        _attempt, ok, _time.monotonic() - t0), flush=True)
                    if ok:
                        return True
                except Exception as exc:
                    print("[steer-dbg] attempt=%d exc=%r" % (_attempt, exc),
                          flush=True)
                if self.is_terminal():
                    return False
                _time.sleep(1.0)
        if self.chan is None:
            return False
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
        if self.chan is not None:
            # v0.9.1: reconcile the event mirror with the daemon's
            # authoritative buffer (SSH-stream event loss after reconnects)
            if hasattr(self.orch.transport, "goal_events"):
                try:
                    evs = self.orch.transport.goal_events(
                        self.device, self.goal_id)
                    if evs and len(evs) > len(self.chan.events or []):
                        self.chan.events = evs
                        self.chan.event_seqs = [None] * len(evs)
                except Exception:
                    pass
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

    # -- reports (iteration v0.2: history & diff) ---------------------------
    def list_reports(self):
        out = []
        for name in sorted(os.listdir(self.runs_dir)):
            if not name.startswith("report-") or not name.endswith(".json"):
                continue
            try:
                with open(os.path.join(self.runs_dir, name),
                          encoding="utf-8") as fh:
                    data = json.load(fh)
                out.append({"file": name,
                            "generatedMs": data.get("generatedMs"),
                            "successCount": data.get("successCount"),
                            "failCount": data.get("failCount"),
                            "goals": len(data.get("matrix", []))})
            except (ValueError, OSError):
                continue
        return out

    def load_report(self, name):
        if os.sep in name or "/" in name or ".." in name:
            raise ValueError("report name must be a bare filename")
        path = os.path.join(self.runs_dir, name)
        if not os.path.isfile(path):
            raise KeyError("no such report: %s" % name)
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)

    @staticmethod
    def diff_reports(base, newer):
        """Diff two report dicts by goalId. Returns a change summary."""
        def index(rep):
            return {m.get("goalId"): m for m in rep.get("matrix", [])}
        ia, ib = index(base), index(newer)
        changes = []
        for gid in sorted(set(ia) | set(ib)):
            sa = (ia.get(gid) or {}).get("status")
            sb = (ib.get(gid) or {}).get("status")
            if sa != sb:
                changes.append({"goalId": gid, "from": sa or "(absent)",
                                "to": sb or "(absent)",
                                "summary": (ib.get(gid) or ia.get(gid) or {})
                                           .get("summary")})
        return {
            "baseFile": base.get("reportFile"),
            "newerFile": newer.get("reportFile"),
            "counts": {"base": {"success": base.get("successCount"),
                                "fail": base.get("failCount")},
                       "newer": {"success": newer.get("successCount"),
                                 "fail": newer.get("failCount")}},
            "changed": changes,
            "regressions": [c for c in changes
                            if c["from"] in (protocol.ST_SUCCESS,
                                             protocol.ST_PARTIAL)
                            and c["to"] not in (protocol.ST_SUCCESS,
                                                protocol.ST_PARTIAL,
                                                "(absent)")],
        }

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

    def load_goal_record(self, goal_id):
        """Load a persisted run record (goals from earlier processes)."""
        path = os.path.join(self.runs_dir, "%s.json" % goal_id)
        if not os.path.isfile(path):
            return None
        try:
            with open(path, encoding="utf-8") as fh:
                return json.load(fh)
        except (OSError, ValueError):
            return None

    def save_dag(self, dag):
        path = os.path.join(self.runs_dir, "dag-%s.json" % dag.dag_id)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(dag.snapshot(), fh, ensure_ascii=False, indent=1)
        os.replace(tmp, path)
        return path


DAG_POLICIES = ("success", "lenient")
_DAG_POLL_S = 0.3


class DagRun:
    """Cross-device goal DAG: nodes dispatch in topological order; a node
    starts only when its dependencies satisfy the policy (DESIGN.md §11.3)."""

    def __init__(self, dag_id, nodes, edges, policy, orch):
        self.dag_id = dag_id
        self.policy = policy
        self.orch = orch
        # node: {key, deviceId, goal, goalId, state, status, reason, run}
        self.nodes = {}
        for n in nodes:
            self.nodes[n["key"]] = {"key": n["key"], "deviceId": n["deviceId"],
                                    "goal": n["goal"], "goalId": None,
                                    "state": "pending", "status": None,
                                    "reason": None, "run": None}
        self.edges = [list(e) for e in edges]
        self.deps = {}
        for frm, to in self.edges:
            self.deps.setdefault(to, []).append(frm)
        self._thread = threading.Thread(target=self._schedule, daemon=True,
                                        name="rao-dag-%s" % dag_id[:8])

    # -- graph helpers ------------------------------------------------------
    def _dep_status_ok(self, dep_node):
        st = dep_node["status"]
        if self.policy == "lenient":
            return st in (protocol.ST_SUCCESS, protocol.ST_PARTIAL)
        return st == protocol.ST_SUCCESS

    def _downstream(self, keys):
        """All nodes transitively depending on any key in `keys`."""
        out, frontier = set(), list(keys)
        while frontier:
            k = frontier.pop()
            for frm, to in self.edges:
                if frm == k and to not in out:
                    out.add(to)
                    frontier.append(to)
        return out

    # -- scheduler ----------------------------------------------------------
    def start(self):
        self._thread.start()

    def _schedule(self):
        try:
            while True:
                progressed = False
                # 1. propagate failures to downstream (transitive skip)
                failed_keys = [k for k, n in self.nodes.items()
                               if n["state"] in ("failed", "timeout",
                                                 "cancelled", "skipped")]
                for k in self._downstream(failed_keys):
                    n = self.nodes[k]
                    if n["state"] == "pending":
                        n["state"] = "skipped"
                        n["reason"] = "dependency %s did not succeed" % k
                        progressed = True
                # 2. policy-violating deps: skip pending nodes whose deps are
                #    all terminal but at least one status violates the policy
                for k, n in self.nodes.items():
                    if n["state"] != "pending":
                        continue
                    deps = self.deps.get(k, [])
                    if deps and all(self.nodes[d]["state"] in
                                    ("done", "failed", "timeout", "cancelled",
                                     "skipped") for d in deps):
                        bad = [d for d in deps
                               if not self._dep_status_ok(self.nodes[d])]
                        if bad:
                            n["state"] = "skipped"
                            n["reason"] = ("dependency %s status=%s violates "
                                           "policy=%s" % (bad[0],
                                                          self.nodes[bad[0]]["status"],
                                                          self.policy))
                            progressed = True
                # 3. launch ready nodes
                for k, n in self.nodes.items():
                    if n["state"] != "pending":
                        continue
                    deps = self.deps.get(k, [])
                    if not deps or all(
                            self.nodes[d]["state"] == "done"
                            and self._dep_status_ok(self.nodes[d])
                            for d in deps):
                        n["goalId"] = protocol.new_goal_id()
                        n["goal"]["goalId"] = n["goalId"]
                        n["goal"]["deviceId"] = n["deviceId"]
                        n["state"] = "queued"
                        self.orch.dispatch([n["deviceId"]], n["goal"])
                        n["run"] = self.orch.get_run(n["goalId"])
                        progressed = True
                # 3. refresh terminal nodes
                for k, n in self.nodes.items():
                    if n["state"] in ("queued", "running") and n["run"]:
                        if n["run"].is_terminal():
                            n["state"] = "done" if n["run"].state == \
                                protocol.RUN_DONE else n["run"].state
                            n["status"] = (n["run"].result or {}).get("status")
                            progressed = True
                if all(n["state"] in ("done", "failed", "timeout",
                                      "cancelled", "skipped")
                       for n in self.nodes.values()):
                    break
                if not progressed:
                    time.sleep(_DAG_POLL_S)
        except Exception as exc:  # scheduler bug: fail remaining pending nodes
            for n in self.nodes.values():
                if n["state"] in ("pending", "queued", "running"):
                    n["state"] = "failed"
                    n["reason"] = "dag scheduler error: %s" % exc
        finally:
            self.orch.run_store.save_dag(self)

    def snapshot(self):
        return {
            "dagId": self.dag_id,
            "policy": self.policy,
            "edges": self.edges,
            "nodes": [{"key": k, "deviceId": n["deviceId"],
                       "goalId": n["goalId"], "state": n["state"],
                       "status": n["status"], "reason": n["reason"]}
                      for k, n in self.nodes.items()],
        }


class Orchestrator:
    def __init__(self, registry, transport=None, runs_dir=None,
                 max_concurrent_goals=5, resume_max=RESUME_MAX_DEFAULT,
                 rda_dir="/home/diablo/git500/zcode_prj/rao"):
        self.registry = registry
        self.transport = transport or SshTransport(rda_dir=rda_dir)
        self.max_concurrent_goals = max_concurrent_goals
        self.run_store = RunStore(runs_dir or os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(
                os.path.abspath(__file__)))), "runs"))
        self._lock = threading.RLock()
        self._global_slots = threading.Semaphore(max_concurrent_goals)
        self._device_slots = {}   # device_id -> Semaphore
        self._device_slot_lock = threading.Lock()
        self.runs = {}            # goalId -> GoalRun
        self.dags = {}            # dagId -> DagRun

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
        # prepare ONCE (protocol backfilled); every device deepcopies this base
        prepared = dict(goal)
        prepared.setdefault("protocol", protocol.PROTOCOL_VERSION)
        probe = dict(prepared)
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
            g = copy.deepcopy(prepared)
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

    # --- DAG dispatch (v0.2) -----------------------------------------------
    def dispatch_dag(self, nodes, edges, policy="success"):
        """nodes: [{key, deviceId, goal}]; edges: [[fromKey, toKey]] (to waits
        for from). Validates graph, then schedules topologically."""
        if policy not in DAG_POLICIES:
            raise ValueError("policy must be one of %s" % (DAG_POLICIES,))
        keys = [n.get("key") for n in nodes]
        if not keys or len(set(keys)) != len(keys):
            raise ValueError("RAO_DAG_INVALID: node keys must be non-empty "
                             "and unique")
        keyset = set(keys)
        for n in nodes:
            if n.get("deviceId") not in self.registry.list_ids():
                raise KeyError("unknown deviceId: %r" % n.get("deviceId"))
            g = dict(n.get("goal") or {})
            g.setdefault("protocol", protocol.PROTOCOL_VERSION)
            ok, err = protocol.validate_goal(
                dict(g, goalId="probe", deviceId=n["deviceId"]))
            if not ok:
                raise ValueError("RAO_DAG_INVALID node %r: %s"
                                 % (n.get("key"), err))
        for e in edges:
            if not isinstance(e, (list, tuple)) or len(e) != 2 or \
                    e[0] not in keyset or e[1] not in keyset:
                raise ValueError("RAO_DAG_INVALID edge: %r" % (e,))
            if e[0] == e[1]:
                raise ValueError("RAO_DAG_INVALID self-edge: %r" % (e,))
        # cycle detection (Kahn)
        indeg = {k: 0 for k in keys}
        for frm, to in edges:
            indeg[to] += 1
        queue = [k for k, d in indeg.items() if d == 0]
        seen = 0
        adj = {}
        for frm, to in edges:
            adj.setdefault(frm, []).append(to)
        while queue:
            k = queue.pop()
            seen += 1
            for nxt in adj.get(k, []):
                indeg[nxt] -= 1
                if indeg[nxt] == 0:
                    queue.append(nxt)
        if seen != len(keys):
            raise ValueError("RAO_DAG_INVALID: cycle detected")

        dag_id = protocol.new_goal_id()
        dag = DagRun(dag_id, nodes, edges, policy, self)
        with self._lock:
            self.dags[dag_id] = dag
        dag.start()
        return dag

    def runs_for_device(self, device_id):
        with self._lock:
            return [r for r in self.runs.values()
                    if r.goal["deviceId"] == device_id]

    def get_dag(self, dag_id):
        with self._lock:
            return self.dags.get(dag_id)

    # --- collect -----------------------------------------------------------
    def collect(self, goal_ids, timeout_ms=120000):
        deadline = time.monotonic() + timeout_ms / 1000.0
        matrix = []
        for gid in goal_ids:
            run = self.get_run(gid)
            if run is None:
                # goal from an earlier process: fall back to persisted record
                rec = self.run_store.load_goal_record(gid)
                if rec:
                    res = rec.get("result") or {}
                    matrix.append({
                        "goalId": gid,
                        "deviceId": (rec.get("goal") or {}).get("deviceId"),
                        "status": res.get("status", rec.get("state")),
                        "summary": res.get("summary"),
                        "steps": res.get("steps", []),
                        "artifacts": res.get("artifacts", []),
                        "assumptions": res.get("assumptions", []),
                        "steerNotes": res.get("steerNotes", []),
                        "metrics": res.get("metrics", {}),
                    })
                else:
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
