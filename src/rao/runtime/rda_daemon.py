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
import hashlib
import json
import os
import platform
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
RAO_DIR = os.path.join(os.path.expanduser("~"), ".rao")


def _set_home(home):
    global RAO_DIR, DAEMON_JSON, DAEMON_LOG, GOALS_DIR
    RAO_DIR = home
    DAEMON_JSON = os.path.join(RAO_DIR, "daemon.json")
    DAEMON_LOG = os.path.join(RAO_DIR, "daemon.log")
    GOALS_DIR = os.path.join(RAO_DIR, "goals")
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
        self.pending_injections = []   # injections arriving before engine ready

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
                for payload in rec.pending_injections:
                    engine.injections.q.put(payload)
                rec.pending_injections = []
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
                log("handler: cmd=%s goalId=%s" % (cmd, msg.get("goalId", msg.get("goal", {}).get("goalId", "-"))))
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
                    if rec is None:
                        self._send({"type": "injected", "goalId": gid,
                                    "queued": "dropped",
                                    "reason": "unknown goalId",
                                    "ack": msg.get("ack")})
                        continue
                    payload = msg.get("payload", {})
                    if rec.status != "running":
                        self._send({"type": "injected", "goalId": gid,
                                    "queued": "dropped",
                                    "reason": "goal already finished",
                                    "ack": msg.get("ack")})
                        continue
                    if rec.engine is None:
                        # engine thread not started yet: hold and flush on start
                        with rec.lock:
                            rec.pending_injections.append(payload)
                        self._send({"type": "injected", "goalId": gid,
                                    "queued": "pending", "ack": msg.get("ack")})
                        continue
                    rec.engine.injections.q.put(payload)
                    self._send({"type": "injected", "goalId": gid,
                                "queued": "delivered", "ack": msg.get("ack")})
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
        log("handler finish: connection closing")
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


def master_link_loop(state, host, mport, enroll_token, node_id):
    """v0.10 node mode: dial the master, register, heartbeat, forward goal
    events, and handle inject commands over the outbound connection."""
    backoff = 0.5
    while True:
        sock = None
        f = None
        try:
            sock = socket.create_connection((host, mport), timeout=10)
            f = sock.makefile("rwb")
            caps = {"os": platform.system(),
                    "python": sys.version.split()[0],
                    "user": os.environ.get("USER") or os.environ.get(
                        "USERNAME") or "?"}
            f.write((json.dumps({"cmd": "register", "nodeId": node_id,
                                 "token": enroll_token,
                                 "capabilities": caps}) + "\n").encode("utf-8"))
            f.flush()
            reply = json.loads(f.readline().decode("utf-8", "replace"))
            if reply.get("type") != "registered":
                log("register rejected: %s" % reply)
                time.sleep(5.0)
                continue
            log("registered with master %s:%s" % (host, mport))
            backoff = 0.5

            def fwd(msg):
                f.write((json.dumps(msg, ensure_ascii=False) + "\n")
                        .encode("utf-8"))
                f.flush()

            # resubscribe running goals to this connection
            with state.goals_lock:
                for rec in state.goals.values():
                    if rec.status == "running":
                        rec.subscribers.append(fwd)

            # master -> node commands
            while True:
                raw = f.readline()
                if not raw:
                    raise ConnectionError("master link EOF")
                msg = json.loads(raw.decode("utf-8", "replace"))
                cmd = msg.get("cmd")
                if cmd == "goal":
                    gid = (msg.get("goal") or {}).get("goalId") or \
                        protocol.new_goal_id()
                    g = dict(msg["goal"])
                    g["goalId"] = gid
                    rec = state.submit(g)
                    rec.subscribers.append(fwd)   # events stream to master
                    fwd({"type": "goal_accepted", "goalId": gid})
                elif cmd == "attach":
                    gid = msg.get("goalId")
                    rec = state.goals.get(gid)
                    if rec is None:
                        fwd({"type": "error", "goalId": gid,
                             "message": "unknown goalId"})
                        continue
                    with rec.lock:
                        base = rec.trimmed
                        evs = list(rec.events)
                    for i, ev in enumerate(evs):
                        fwd({"type": "event", "goalId": gid, "event": ev,
                             "seq": base + i + 1})
                    if rec.status != "running":
                        fwd({"type": "goal_status", "goalId": gid,
                             "status": rec.status, "exit": rec.exit_code})
                    else:
                        rec.subscribers.append(fwd)
                elif cmd == "inject":
                    rec = state.goals.get(msg.get("goalId"))
                    payload = msg.get("payload", {})
                    if rec is None or rec.engine is None:
                        fwd({"type": "receipt", "goalId": msg.get("goalId"),
                             "ack": msg.get("ack"), "queued": "dropped",
                             "reason": "engine not ready"})
                    else:
                        rec.engine.injections.q.put(payload)
                        fwd({"type": "receipt", "goalId": msg.get("goalId"),
                             "ack": msg.get("ack"), "queued": "delivered"})
                elif cmd == "ping":
                    fwd({"type": "pong", "ts": protocol.now_ms()})
        except Exception as exc:
            log("master link lost: %r" % exc)
        time.sleep(backoff)
        backoff = min(backoff * 2, 30.0)


def cmd_serve(port, master=None, node_token=None):
    os.makedirs(RAO_DIR, exist_ok=True)
    token = secrets.token_hex(16)
    enroll_token = node_token   # v0.10: master's enrollment token (via --token)
    if port == 0:
        # pick a free port
        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
        s.close()
    state = DaemonState(token)
    server = DaemonServer(("127.0.0.1", port), Handler)
    server.daemon = state
    # v0.10: hash the WHOLE runtime (rda.py changes alone must trigger
    # daemon restart, else the in-memory GoalEngine stays stale)
    h = hashlib.sha256()
    base = os.path.dirname(os.path.abspath(__file__))
    for name in sorted(("rda_daemon.py", "rda.py", "protocol.py")):
        fp = os.path.join(base, name)
        if os.path.isfile(fp):
            h.update(name.encode())
            h.update(open(fp, "rb").read())
    code_hash = h.hexdigest()
    with open(DAEMON_JSON, "w", encoding="utf-8") as fh:
        json.dump({"pid": os.getpid(), "port": port, "token": token,
                   "version": DAEMON_VERSION, "codeHash": code_hash,
                   "startedMs": protocol.now_ms()}, fh)
    if os.name != "nt":
        os.chmod(DAEMON_JSON, 0o600)
    threading.Thread(target=state.heartbeat_loop, daemon=True).start()
    if master:
        host, _, mport = master.rpartition(":")
        threading.Thread(target=master_link_loop, daemon=True,
                         args=(state, host or "127.0.0.1", int(mport),
                               enroll_token, socket.gethostname()),
                         name="rao-master-link").start()
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


def cmd_start(port, master=None, node_token=None):
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
    serve_argv = [sys.executable, os.path.abspath(__file__), "serve",
                  "--port", str(port or 0)]
    if master:
        serve_argv += ["--master", master]
    if node_token:
        serve_argv += ["--token", node_token]
    subprocess.Popen(
        serve_argv,
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


def cmd_inject_one(goal_id, payload_json):
    """v0.9 control-plane: one-shot inject over a fresh local connection.
    Prints the receipt JSON line; exit 0 only when queued == delivered."""
    dj = load_daemon_json()
    if not dj:
        print("not running")
        return 1
    try:
        payload = json.loads(payload_json)
        lines = _local_client(dj["port"], dj["token"],
                              [{"cmd": "inject", "goalId": goal_id,
                                "payload": payload}], read_timeout_s=5)
    except Exception as exc:
        print(json.dumps({"type": "error", "message": str(exc)}))
        return 1
    receipt = next((m for m in lines if m.get("type") == "injected"), None)
    if receipt is None:
        err = next((m for m in lines if m.get("type") == "error"), None)
        print(json.dumps({"type": "injected", "goalId": goal_id,
                          "queued": "dropped",
                          "reason": (err or {}).get("message",
                                                    "no receipt")}))
        return 1
    print(json.dumps(receipt, ensure_ascii=False))
    return 0 if receipt.get("queued") == "delivered" else 1


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
    """stdio <-> daemon TCP bridge (remote side of the SSH channel).

    v0.9 fix: the socket reader and writer use SEPARATE file objects
    (a shared makefile("rwb") is not thread-safe — the first client->daemon
    write after submit could corrupt the reader and silently freeze the
    event stream). If the read pump dies, the bridge EXITS so the SSH
    channel EOFs and the orchestrator reconnects (attach) instead of
    hanging on a dead-but-alive channel."""
    dj = load_daemon_json()
    if not (dj and _alive(dj)):
        os.write(2, ("[bridge] startup check failed: dj=%s alive=%s\n"
                     % (bool(dj), bool(dj and _alive(dj)))).encode())
        print(json.dumps({"type": "error",
                          "message": "daemon not running"}))
        return 3
    sock = socket.create_connection(("127.0.0.1", dj["port"]), timeout=5)
    sock.settimeout(None)   # pump reads block indefinitely (event stream)
    f_read = sock.makefile("rb")
    f_write = sock.makefile("wb")
    f_write.write((json.dumps({"cmd": "auth", "token": dj["token"]}) + "\n")
                  .encode("utf-8"))
    f_write.flush()
    ready = f_read.readline()
    sys.stdout.write(ready.decode("utf-8", "replace"))
    sys.stdout.flush()

    def pump_socket_to_stdout():
        try:
            while True:
                chunk = f_read.read1(4096)   # read1: return as soon as ANY data
                if not chunk:
                    break
                sys.stdout.write(chunk.decode("utf-8", "replace"))
                sys.stdout.flush()
        except Exception as exc:
            os.write(2, ("[bridge] read pump died: %r\n" % exc).encode())
        finally:
            os.write(2, b"[bridge] pump exit (EOF from daemon or error)\n")
            # reader lost the daemon stream: kill the bridge so the SSH
            # channel EOFs and the orchestrator reconnects with attach
            try:
                sock.close()
            except Exception:
                pass
            os._exit(3)

    threading.Thread(target=pump_socket_to_stdout, daemon=True).start()
    # stdin: os.read avoids Python's read-ahead buffering, which would hold
    # small SSH-channel lines until the buffer fills (submit never forwarded)
    try:
        buf = b""
        while True:
            chunk = os.read(0, 4096)
            if not chunk:
                if buf:
                    f_write.write(buf)
                    f_write.flush()
                break
            buf += chunk
            while b"\n" in buf:
                ln, buf = buf.split(b"\n", 1)
                f_write.write(ln + b"\n")
                f_write.flush()
    except Exception:
        pass
    try:
        sock.close()
    except Exception:
        pass
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--home", help="state dir (default ~/.rao)")
    sub = ap.add_subparsers(dest="cmd")
    p_serve = sub.add_parser("serve")
    p_serve.add_argument("--port", type=int, default=0)
    p_serve.add_argument("--master", help="master endpoint host:port (node mode)")
    p_serve.add_argument("--token", help="master enrollment token (node mode)")
    p_start = sub.add_parser("start")
    p_start.add_argument("--port", type=int, default=0)
    p_start.add_argument("--master", help="master endpoint host:port")
    p_start.add_argument("--token", help="master enrollment token")
    sub.add_parser("ping")
    sub.add_parser("stop")
    p_stat = sub.add_parser("status-one")
    p_stat.add_argument("--goal-id", required=True)
    sub.add_parser("client-bridge")
    p_inj = sub.add_parser("inject-one")
    p_inj.add_argument("--goal-id", required=True)
    p_inj.add_argument("--payload", help="payload JSON (steer/cancel/ping)")
    p_inj.add_argument("--payload-b64", help="payload JSON, base64")
    args = ap.parse_args()
    if args.home:
        _set_home(args.home)
    if args.cmd == "serve":
        return cmd_serve(args.port, master=args.master, node_token=args.token)
    if args.cmd == "start":
        return cmd_start(args.port, master=args.master, node_token=args.token)
    if args.cmd == "ping":
        return cmd_ping()
    if args.cmd == "stop":
        return cmd_stop()
    if args.cmd == "inject-one":
        import base64 as _b64
        payload_json = args.payload
        if args.payload_b64:
            payload_json = _b64.b64decode(args.payload_b64).decode("utf-8")
        return cmd_inject_one(args.goal_id, payload_json)
    if args.cmd == "status-one":
        dj = load_daemon_json()
        if not dj:
            print(json.dumps({"type": "error", "message": "daemon not running"}))
            return 1
        try:
            lines = _local_client(dj["port"], dj["token"],
                                  [{"cmd": "status",
                                    "goalId": args.goal_id}],
                                  read_timeout_s=5)
        except Exception as exc:
            print(json.dumps({"type": "error", "message": str(exc)}))
            return 1
        snap = next((m for m in lines if m.get("type") == "goal_snapshot"),
                    None)
        print(json.dumps(snap or {"type": "error", "message": "no snapshot"},
                         ensure_ascii=False))
        return 0 if snap else 1
    if args.cmd == "client-bridge":
        return cmd_bridge()
    ap.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
