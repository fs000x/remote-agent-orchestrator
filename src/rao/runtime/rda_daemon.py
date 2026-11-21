#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""RDA daemon (v0.3): persistent goal engine host on a device.

Subcommands:
  serve [--port N]            run the daemon (TCP 127.0.0.1, token auth)
  start [--port N]            spawn serve detached, wait for daemon.json
  ping                        check daemon liveness (exit 0 = alive)
  stop                        ask the daemon to shut down
  client-bridge               stdio <-> TCP bridge (used over SSH by orchestrator)

State dir ~/.rao/: daemon.json {pid, port, token, startedMs, heartbeatMs},
daemon.log, goals/<goalId>.json (per-goal buffered events for resume).

Wire protocol (JSON lines over TCP; FIRST line must be auth):
  -> {"cmd":"auth","token":"..."}
  <- {"type":"ready"}
  -> {"cmd":"submit","goal":{...}}          <- {"type":"ack","goalId":...}
     then live events: {"type":"event","goalId":...,"event":{...}}
     terminal:              {"type":"goal_status","goalId":...,"status":...,"exit":N}
  -> {"cmd":"attach","goalId":"..."}        <- {"type":"attached",...} + buffered
                                               events + live tail + goal_status
  -> {"cmd":"status","goalId":"..."}        <- {"type":"goal_snapshot",...}
  -> {"cmd":"list"}                         <- {"type":"goal_list",...}
  -> {"cmd":"inject","goalId","payload":{...}}   <- {"type":"injected"}
  -> {"cmd":"ping"}                         <- {"type":"pong","ts":...}
  -> {"cmd":"shutdown"}                     <- {"type":"bye"} (daemon exits)

Python >= 3.8, stdlib only.
"""
import argparse
import json
import os
import secrets
import socket
import socketserver
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import protocol  # noqa: E402
from rda import GoalEngine  # noqa: E402

DAEMON_VERSION = "0.3.0"
RAO_DIR = os.environ.get("RAO_HOME") or os.path.join(
    os.path.expanduser("~"), ".rao")
DAEMON_JSON = os.path.join(RAO_DIR, "daemon.json")
DAEMON_LOG = os.path.join(RAO_DIR, "daemon.log")
GOALS_DIR = os.path.join(RAO_DIR, "goals")
HEARTBEAT_S = 5
EVENT_BUFFER_MAX = 5000


def log(msg):
    sys.stderr.write("[rda-daemon] %s\n" % msg)
    sys.stderr.flush()


def load_daemon_json():
    try:
        with open(DAEMON_JSON, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return None


class GoalRecord(object):
    def __init__(self, goal):
        self.goal = goal
        self.goal_id = goal["goalId"]
        self.events = []
        self.total_ever = 0     # seq counter (1-based, survives buffer trim)
        self.trimmed = 0        # events dropped from the front of the buffer
        self.status = "running"
        self.exit_code = None
        self.started_ms = protocol.now_ms()
        self.finished_ms = None
        self.lock = threading.Lock()
        self.subscribers = []   # list of _send(callable) sinks
        self.engine = None

    def sink(self, ev):
        """GoalEngine event sink: buffer + persist + fan out."""
        with self.lock:
            self.total_ever += 1
            seq = self.total_ever
            self.events.append(ev)
            if len(self.events) > EVENT_BUFFER_MAX:
                removed = len(self.events) - EVENT_BUFFER_MAX
                del self.events[:removed]
                self.trimmed += removed
            subs = list(self.subscribers)
        for send in subs:
            try:
                send({"type": "event", "goalId": self.goal_id, "event": ev,
                      "seq": seq})
            except Exception:
                pass

    def finish(self, exit_code):
        res = None
        with self.lock:
            for ev in reversed(self.events):
                if ev.get("kind") == "result":
                    res = ev
                    break
            self.status = (res or {}).get("data", {}).get(
                "status", "failed") if res else "failed"
            self.exit_code = exit_code
            self.finished_ms = protocol.now_ms()
            subs = list(self.subscribers)
        self.persist()   # goal_status implies the record is on disk
        payload = {"type": "goal_status", "goalId": self.goal_id,
                   "status": self.status, "exit": exit_code}
        for send in subs:
            try:
                send(payload)
            except Exception:
                pass

    def persist(self):
        try:
            if not os.path.isdir(GOALS_DIR):
                os.makedirs(GOALS_DIR)
            path = os.path.join(GOALS_DIR, "%s.json" % self.goal_id)
            tmp = path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump({"goal": self.goal, "status": self.status,
                           "exit": self.exit_code,
                           "startedMs": self.started_ms,
                           "finishedMs": self.finished_ms,
                           "events": self.events}, fh,
                          ensure_ascii=False)
            os.replace(tmp, path)
        except Exception as exc:
            log("persist goal %s failed: %s" % (self.goal_id, exc))


class DaemonState(object):
    def __init__(self, token):
        self.token = token
        self.goals = {}
        self.goals_lock = threading.Lock()
        self.shutting_down = threading.Event()

    def submit(self, goal):
        ok, err = protocol.validate_goal(goal)
        if not ok:
            raise ValueError("RAO_GOAL_INVALID: %s" % err)
        gid = goal["goalId"]
        with self.goals_lock:
            existing = self.goals.get(gid)
        if existing is not None and existing.status == "running":
            raise ValueError("goal %s already running" % gid)
        rec = GoalRecord(goal)
        with self.goals_lock:
            self.goals[gid] = rec

        def engine_thread():
            try:
                engine = GoalEngine(goal, sink=rec.sink, read_stdin=False)
                rec.engine = engine
                code = engine.run()
            except Exception as exc:
                log("engine crash %s: %r" % (gid, exc))
                try:
                    rec.sink(protocol.make_event(
                        protocol.KIND_ERROR, gid,
                        {"message": "daemon engine crash: %s" % exc,
                         "fatal": True}))
                except Exception:
                    pass
                code = 1
            rec.finish(code)

        threading.Thread(target=engine_thread, daemon=True,
                         name="rda-goal-%s" % gid[:8]).start()
        return rec

    def heartbeat_loop(self):
        while not self.shutting_down.is_set():
            try:
                data = load_daemon_json() or {}
                data.update({"pid": os.getpid(), "heartbeatMs":
                             protocol.now_ms(),
                             "goals": len(self.goals)})
                tmp = DAEMON_JSON + ".tmp"
                with open(tmp, "w", encoding="utf-8") as fh:
                    json.dump(data, fh)
                os.replace(tmp, DAEMON_JSON)
            except Exception as exc:
                log("heartbeat failed: %s" % exc)
            self.shutting_down.wait(HEARTBEAT_S)


class Handler(socketserver.StreamRequestHandler):
    daemon = None  # set on the server

    def _send(self, obj):
        with self._wlock:
            self.wfile.write((json.dumps(obj, ensure_ascii=False) + "\n")
                             .encode("utf-8"))
            self.wfile.flush()

    def handle(self):
        self._wlock = threading.Lock()
        self._live_goal_ids = set()
        # auth
        line = self.rfile.readline()
        try:
            first = json.loads(line.decode("utf-8", "replace"))
        except ValueError:
            return
        if first.get("cmd") != "auth" or \
                first.get("token") != self.server.daemon.token:
            self._send({"type": "error", "message": "auth required"})
            return
        self._send({"type": "ready", "version": DAEMON_VERSION})
        for raw in self.rfile:
            line = raw.strip()
            if not line:
                continue
            try:
                msg = json.loads(line.decode("utf-8", "replace"))
            except ValueError:
                self._send({"type": "error", "message": "bad json"})
                continue
            cmd = msg.get("cmd")
            try:
                if cmd == "ping":
                    self._send({"type": "pong", "ts": protocol.now_ms()})
                elif cmd == "submit":
                    rec = self.server.daemon.submit(msg["goal"])
                    self._send({"type": "ack", "goalId": rec.goal_id})
                    # live stream this goal to THIS connection
                    rec.subscribers.append(self._send)
                    self._live_goal_ids.add(rec.goal_id)
                elif cmd == "attach":
                    gid = msg["goalId"]
                    rec = self.server.daemon.goals.get(gid)
                    if rec is None:
                        self._send({"type": "error",
                                    "message": "unknown goalId %s" % gid})
                        continue
                    self._send({"type": "attached", "goalId": gid,
                                "status": rec.status})
                    log("attach %s: status=%s buffered=%d" % (gid, rec.status, len(rec.events)))
                    with rec.lock:
                        base = rec.trimmed
                        evs = list(rec.events)
                    for i, ev in enumerate(evs):
                        self._send({"type": "event", "goalId": gid,
                                    "event": ev, "seq": base + i + 1})
                    log("attach %s: replayed %d events" % (gid, len(evs)))
                    if rec.status != "running":
                        self._send({"type": "goal_status", "goalId": gid,
                                    "status": rec.status,
                                    "exit": rec.exit_code})
                    else:
                        rec.subscribers.append(self._send)
                elif cmd == "status":
                    gid = msg.get("goalId")
                    rec = self.server.daemon.goals.get(gid)
                    if rec is None:
                        self._send({"type": "error",
                                    "message": "unknown goalId %s" % gid})
                        continue
                    with rec.lock:
                        self._send({"type": "goal_snapshot",
                                    "goalId": gid, "status": rec.status,
                                    "exit": rec.exit_code,
                                    "events": list(rec.events[-50:])})
                elif cmd == "list":
                    with self.server.daemon.goals_lock:
                        rows = [{"goalId": g.goal_id, "status": g.status,
                                 "exit": g.exit_code}
                                for g in self.server.daemon.goals.values()]
                    self._send({"type": "goal_list", "goals": rows})
                elif cmd == "inject":
                    gid = msg["goalId"]
                    rec = self.server.daemon.goals.get(gid)
                    if rec is None or rec.engine is None:
                        self._send({"type": "error",
                                    "message": "unknown or finished goal"})
                        continue
                    rec.engine.injections.q.put(msg.get("payload", {}))
                    self._send({"type": "injected", "goalId": gid})
                elif cmd == "shutdown":
                    self._send({"type": "bye"})
                    self.server.daemon.shutting_down.set()
                    threading.Thread(target=self.server.shutdown,
                                     daemon=True).start()
                    return
                else:
                    self._send({"type": "error",
                                "message": "unknown cmd %r" % cmd})
            except Exception as exc:
                try:
                    self._send({"type": "error", "message": str(exc)})
                except Exception:
                    return

    def finish(self):
        # unsubscribe this connection's sends from any live goals
        for gid in getattr(self, "_live_goal_ids", set()):
            rec = self.server.daemon.goals.get(gid)
            if rec is not None:
                with rec.lock:
                    if self._send in rec.subscribers:
                        rec.subscribers.remove(self._send)
        super(Handler, self).finish()


class DaemonServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


def cmd_serve(port):
    os.makedirs(RAO_DIR, exist_ok=True)
    token = secrets.token_hex(16)
    if port == 0:
        # pick a free port
        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
        s.close()
    state = DaemonState(token)
    server = DaemonServer(("127.0.0.1", port), Handler)
    server.daemon = state
    with open(DAEMON_JSON, "w", encoding="utf-8") as fh:
        json.dump({"pid": os.getpid(), "port": port, "token": token,
                   "version": DAEMON_VERSION,
                   "startedMs": protocol.now_ms()}, fh)
    if os.name != "nt":
        os.chmod(DAEMON_JSON, 0o600)
    threading.Thread(target=state.heartbeat_loop, daemon=True).start()
    log("serving on 127.0.0.1:%d" % port)
    try:
        server.serve_forever()
    finally:
        server.server_close()
        try:
            os.remove(DAEMON_JSON)
        except OSError:
            pass
    return 0


EXPECTED_REPLY = {"ping": "pong", "stop": "bye", "start": None}


def _local_client(port, token, messages, read_timeout_s=5):
    """One-shot local TCP client: auth + messages, collect reply lines.

    Early-exits when the expected reply type for the last command arrives;
    otherwise reads until quiet timeout. Never raises on timeouts."""
    sock = socket.create_connection(("127.0.0.1", port), timeout=5)
    try:
        payload = "\n".join(
            [json.dumps({"cmd": "auth", "token": token})] +
            [json.dumps(m) for m in messages]) + "\n"
        sock.sendall(payload.encode("utf-8"))
        sock.settimeout(read_timeout_s)
        buf = b""
        lines = []
        streaming = any(m.get("cmd") == "submit" for m in messages)
        expected = EXPECTED_REPLY.get(messages[-1].get("cmd")) \
            if messages else None
        while True:
            try:
                chunk = sock.recv(4096)
            except socket.timeout:
                break
            if not chunk:
                break
            buf += chunk
            while b"\n" in buf:
                ln, buf = buf.split(b"\n", 1)
                try:
                    lines.append(json.loads(ln.decode("utf-8", "replace")))
                except ValueError:
                    continue
                if expected and lines[-1].get("type") == expected:
                    return lines
            if streaming and any(m.get("type") == "goal_status"
                                 for m in lines):
                break
        return lines
    finally:
        sock.close()


def cmd_start(port):
    os.makedirs(RAO_DIR, exist_ok=True)
    dj = load_daemon_json()
    if dj and _alive(dj):
        print("already running on port %d" % dj["port"])
        return 0
    kwargs = {}
    if os.name == "nt":
        kwargs["creationflags"] = 0x00000008 | 0x00000200  # DETACHED|NEWGROUP
    else:
        kwargs["start_new_session"] = True
    logf = open(DAEMON_LOG, "ab")
    subprocess.Popen(
        [sys.executable, os.path.abspath(__file__), "serve",
         "--port", str(port or 0)],
        stdout=logf, stderr=logf, stdin=subprocess.DEVNULL, **kwargs)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        dj = load_daemon_json()
        if dj and _alive(dj):
            print("started on port %d" % dj["port"])
            return 0
        time.sleep(0.3)
    print("start timed out; see %s" % DAEMON_LOG)
    return 1


def _alive(dj):
    try:
        lines = _local_client(dj["port"], dj["token"],
                              [{"cmd": "ping"}], read_timeout_s=3)
        return any(m.get("type") == "pong" for m in lines)
    except Exception:
        return False


def cmd_ping():
    dj = load_daemon_json()
    if dj and _alive(dj):
        print("alive port=%d" % dj["port"])
        return 0
    print("not running")
    return 1


def cmd_stop():
    dj = load_daemon_json()
    if not dj:
        print("not running")
        return 0
    try:
        _local_client(dj["port"], dj["token"], [{"cmd": "shutdown"}])
    except Exception:
        pass
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if not _alive(dj):
            print("stopped")
            return 0
        time.sleep(0.3)
    print("stop timed out")
    return 1


def cmd_bridge():
    """stdio <-> daemon TCP bridge (remote side of the SSH channel)."""
    dj = load_daemon_json()
    if not (dj and _alive(dj)):
        print(json.dumps({"type": "error",
                          "message": "daemon not running"}))
        return 3
    sock = socket.create_connection(("127.0.0.1", dj["port"]), timeout=5)
    f = sock.makefile("rwb")
    f.write((json.dumps({"cmd": "auth", "token": dj["token"]}) + "\n")
            .encode("utf-8"))
    f.flush()
    ready = f.readline()
    sys.stdout.write(ready.decode("utf-8", "replace"))
    sys.stdout.flush()

    def pump_socket_to_stdout():
        try:
            while True:
                chunk = f.read1(4096)   # read1: return as soon as ANY data
                if not chunk:
                    break
                sys.stdout.write(chunk.decode("utf-8", "replace"))
                sys.stdout.flush()
        except Exception:
            pass
        finally:
            try:
                sys.stdin.close()
            except Exception:
                pass

    threading.Thread(target=pump_socket_to_stdout, daemon=True).start()
    # stdin: os.read avoids Python's read-ahead buffering, which would hold
    # small SSH-channel lines until the buffer fills (submit never forwarded)
    try:
        buf = b""
        while True:
            chunk = os.read(0, 4096)
            if not chunk:
                if buf:
                    f.write(buf)
                    f.flush()
                break
            buf += chunk
            while b"\n" in buf:
                ln, buf = buf.split(b"\n", 1)
                f.write(ln + b"\n")
                f.flush()
    except Exception:
        pass
    try:
        sock.close()
    except Exception:
        pass
    return 0


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd")
    p_serve = sub.add_parser("serve")
    p_serve.add_argument("--port", type=int, default=0)
    p_start = sub.add_parser("start")
    p_start.add_argument("--port", type=int, default=0)
    sub.add_parser("ping")
    sub.add_parser("stop")
    sub.add_parser("client-bridge")
    args = ap.parse_args()
    if args.cmd == "serve":
        return cmd_serve(args.port)
    if args.cmd == "start":
        return cmd_start(args.port)
    if args.cmd == "ping":
        return cmd_ping()
    if args.cmd == "stop":
        return cmd_stop()
    if args.cmd == "client-bridge":
        return cmd_bridge()
    ap.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
