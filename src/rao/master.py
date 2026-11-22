"""rao master — node-mode control endpoint (v0.10).

A TCP listener that registered rao nodes dial INTO (outbound from the node).
Control/data plane for node devices runs over this connection; SSH devices
keep using SshTransport unchanged (coexistence per PROTOCOL-NODE.md).

Messages (JSON Lines), node -> master:
  {"cmd":"register","nodeId":...,"token":...,"capabilities":{...}}
  {"cmd":"heartbeat","ts":N}
  {"type":"event","goalId":G,"event":{...},"seq":N}   (forwarded by node)
  {"type":"receipt","goalId":G,"ack":A,"queued":Q}    (inject receipt)
master -> node:
  {"cmd":"goal","goal":{...}}
  {"cmd":"inject","goalId":G,"payload":{...},"ack":A}
"""
import json
import os
import queue
import socketserver
import threading
import time

import hashlib
import secrets


def master_token_path():
    return os.path.join(
        os.environ.get("RAO_STATE_DIR") or os.path.join(
            os.path.expanduser("~"), ".rao"), "master-token")


def master_nodes_path():
    return os.path.join(
        os.environ.get("RAO_STATE_DIR") or os.path.join(
            os.path.expanduser("~"), ".rao"), "nodes.json")


def ensure_master_token():
    path = master_token_path()
    if os.path.isfile(path):
        with open(path, encoding="utf-8") as fh:
            token = fh.read().strip()
        if token:
            return token
    token = secrets.token_hex(20)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(token + "\n")
    if os.name != "nt":
        os.chmod(path, 0o600)
    return token


class GoalChannel:
    """Per-goal view over a node connection: orchestrator pump <-> master router."""

    def __init__(self, goal_id, node_id, master):
        self.goal_id = goal_id
        self.node_id = node_id
        self.master = master
        self.inbox = []            # wrapped messages (events + receipts + status)
        self.closed = False
        self.exit_code = None
        self.receipts = {}         # ackId -> receipt msg
        self.status = None         # set on goal_status
        self.result = None         # set when the result event arrives
        self._cond = threading.Condition()
        # _DaemonLink-compatible attributes used by GoalRun._pump_daemon
        self.stderr_text = []
        self.killed = False
        self.supports_resume = True

    def send_goal(self, goal_obj):
        """Dispatch the goal to the node (over its master connection)."""
        self.master.send_to_node(self.node_id, {"cmd": "goal",
                                                "goal": goal_obj})

    def send_attach(self, goal_id):
        self.master.send_to_node(self.node_id, {"cmd": "attach",
                                                "goalId": goal_id})

    # -- pump-facing API (mirrors _DaemonLink) ------------------------------
    def push(self, msg):
        """Master router feeds a wrapped message here."""
        t = msg.get("type")
        with self._cond:
            if t == "event":
                ev = msg.get("event") or {}
                if ev.get("kind") == "result":
                    self.result = ev.get("data")
                self.inbox.append(msg)
            elif t == "receipt":
                self.receipts[msg.get("ack")] = msg
            elif t == "goal_status":
                self.status = msg.get("status")
            self._cond.notify_all()

    def wait_event(self, since_index, timeout_s):
        """Return the next PROTOCOL event at index >= since_index.

        since_index counts EVENTS (aligns with events/event_seqs)."""
        deadline = time.monotonic() + timeout_s
        with self._cond:
            while True:
                if len(self.events) > since_index:
                    return self.events[since_index], since_index + 1
                remaining = deadline - time.monotonic()
                if remaining <= 0 or self.closed:
                    return None, since_index
                self._cond.wait(remaining)

    @property
    def events(self):
        return [m.get("event") or {} for m in self.inbox
                if m.get("type") == "event"]

    @property
    def event_seqs(self):
        return [m.get("seq") for m in self.inbox if m.get("type") == "event"]

    def write_line(self, obj, wait_ack_s=5.0):
        """Inject via the node connection; waits for the receipt."""
        return self.master.inject(self.node_id, self.goal_id, obj,
                                  wait_ack_s=wait_ack_s)

    def alive(self):
        return not self.closed and self.master.node_alive(self.node_id)

    def wait_exit(self, timeout_s):
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if self.master.goal_finished(self.node_id, self.goal_id):
                return 0
            time.sleep(0.1)
        return None

    def kill(self):
        self.closed = True

    def close(self):
        with self._cond:
            self.closed = True
            self._cond.notify_all()
        self.master.detach(self.goal_id)


class MasterServer:
    """TCP listener + node registry + per-goal routing."""

    def __init__(self, port=0, state_dir=None, registry=None):
        self.port = port
        self.registry = registry
        self.enrollment_token = ensure_master_token()
        self.nodes = {}          # nodeId -> {info, lastSeen, writer, wlock}
        self.goal_channels = {}  # goalId -> GoalChannel
        self.goal_nodes = {}     # goalId -> nodeId
        self.lock = threading.RLock()
        self._server = None
        self._thread = None

    # -- node connection plumbing -------------------------------------------
    def send_to_node(self, node_id, obj):
        with self.lock:
            node = self.nodes.get(node_id)
        if node is None:
            raise ConnectionError("node %s not connected" % node_id)
        with node["wlock"]:
            node["writer"].write(
                (json.dumps(obj, ensure_ascii=False) + "\n").encode("utf-8"))
            node["writer"].flush()

    def node_alive(self, node_id):
        with self.lock:
            node = self.nodes.get(node_id)
        return bool(node and node["writer"] is not None)

    def node_alive_by_conn(self, conn_id):
        with self.lock:
            for node in self.nodes.values():
                if node.get("conn_id") == conn_id:
                    return node["writer"] is not None
        return False

    def persist_nodes(self):
        path = os.path.join(
            os.environ.get("RAO_STATE_DIR") or os.path.join(
                os.path.expanduser("~"), ".rao"), "nodes.json")
        data = {}
        with self.lock:
            for nid, node in self.nodes.items():
                data[nid] = {"capabilities": node.get("info", {}),
                             "peer": node.get("peer"),
                             "lastSeen": node.get("lastSeen")}
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, indent=2)

    # -- registry ------------------------------------------------------------
    def registered_devices(self):
        out = []
        with self.lock:
            for nid, node in self.nodes.items():
                info = node.get("info", {})
                out.append({
                    "id": nid,
                    "host": info.get("host") or node.get("peer"),
                    "port": None,
                    "user": info.get("user"),
                    "workspace": info.get("workspace"),
                    "transport": "node",
                    "source": "node",
                    "maxConcurrentGoals": info.get("maxConcurrentGoals", 2),
                    "online": True,
                    "capabilities": info.get("capabilities", {}),
                })
        return out

    # -- goal channel routing -------------------------------------------------
    def get_node_channel(self, node_id, goal_id):
        with self.lock:
            ch = self.goal_channels.get(goal_id)
            if ch is None:
                ch = GoalChannel(goal_id, node_id, self)
                self.goal_channels[goal_id] = ch
                self.goal_nodes[goal_id] = node_id
            node = self.nodes.get(node_id)
            if node is None:
                raise ConnectionError("node %s not connected" % node_id)
        return ch

    def route_inbound(self, msg):
        """Route an inbound node message to its goal channel (if any)."""
        gid = msg.get("goalId")
        with self.lock:
            ch = self.goal_channels.get(gid)
        if ch is None:
            return
        ch.push(msg)

    def inject(self, node_id, goal_id, payload, wait_ack_s=5.0):
        """Control-plane inject over the node connection; waits for receipt."""
        ack_id = "ack-%d" % int(time.monotonic() * 1000)
        with self.lock:
            ch = self.goal_channels.get(goal_id)
        self.send_to_node(node_id, {"cmd": "inject", "goalId": goal_id,
                                    "payload": payload, "ack": ack_id})
        deadline = time.monotonic() + wait_ack_s
        if ch is None:
            return False
        with ch._cond:
            while True:
                if ack_id in ch.receipts:
                    return ch.receipts[ack_id].get("queued") == "delivered"
                remaining = deadline - time.monotonic()
                if remaining <= 0 or ch.closed:
                    return False
                ch._cond.wait(remaining)

    def detach(self, goal_id):
        with self.lock:
            self.goal_channels.pop(goal_id, None)
            self.goal_nodes.pop(goal_id, None)

    def goal_finished(self, node_id, goal_id):
        with self.lock:
            ch = self.goal_channels.get(goal_id)
        if ch is None:
            return False
        return ch.status is not None or any(
            (m.get("event") or {}).get("kind") == "result" for m in ch.inbox)

    # -- lifecycle ------------------------------------------------------------
    def start(self, port=None):
        import socketserver
        if port:
            self.port = port

        class Handler(socketserver.StreamRequestHandler):
            master = None

            def _send(self, obj):
                with self._wlock:
                    self.wfile.write(
                        (json.dumps(obj, ensure_ascii=False) + "\n")
                        .encode("utf-8"))
                    self.wfile.flush()

            def handle(self):
                self._wlock = threading.Lock()
                self._node_id = None
                line = self.rfile.readline()
                print("[master] register line: %r" % line[:120], flush=True)
                try:
                    reg = json.loads(line.decode("utf-8", "replace"))
                except ValueError:
                    return
                if reg.get("cmd") != "register" or \
                        reg.get("token") != self.server.daemon.enrollment_token:
                    print("[master] register rejected", flush=True)
                    self._send({"type": "error",
                                "message": "register with valid token first"})
                    return
                node_id = reg.get("nodeId")
                if not node_id:
                    self._send({"type": "error", "message": "nodeId required"})
                    return
                with self.server.daemon.lock:
                    prev = self.server.daemon.nodes.get(node_id)
                    if prev and prev.get("writer") is not None:
                        # replace stale connection
                        try:
                            prev["writer"].close()
                        except Exception:
                            pass
                    self.server.daemon.nodes[node_id] = {
                        "info": reg.get("capabilities", {}),
                        "lastSeen": time.monotonic(),
                        "writer": self.wfile,
                        "wlock": self._wlock,
                        "conn_id": id(self),
                        "peer": self.client_address[0],
                    }
                self._node_id = node_id
                self._send({"type": "registered", "nodeId": node_id,
                            "masterVersion": "0.10.0"})
                self.server.daemon.persist_nodes()
                try:
                    if self.server.daemon.registry is not None:
                        self.server.daemon.registry.reload()
                except Exception:
                    pass
                for raw in self.rfile:
                    raw = raw.strip()
                    if not raw:
                        continue
                    try:
                        msg = json.loads(raw.decode("utf-8", "replace"))
                    except ValueError:
                        continue
                    msg.setdefault("goalId", None)
                    self.server.daemon.route_inbound(msg)
                    with self.server.daemon.lock:
                        node = self.server.daemon.nodes.get(self._node_id)
                        if node is not None:
                            node["lastSeen"] = time.monotonic()

            def finish(self):
                nid = getattr(self, "_node_id", None)
                if nid:
                    with self.server.daemon.lock:
                        node = self.server.daemon.nodes.get(nid)
                        if node is not None and node.get("conn_id") == id(self):
                            node["writer"] = None   # node offline (keep record)
                super().finish()

        self._server = socketserver.ThreadingTCPServer(("0.0.0.0", self.port),
                                                       Handler)
        self._server.daemon = self
        self._thread = threading.Thread(target=self._serve, daemon=True,
                                        name="rao-master")
        self._thread.start()

    def _serve(self):
        try:
            self._server.serve_forever()
        finally:
            self._server.server_close()

    def stop(self):
        if self._server:
            self._server.shutdown()


def start_master_thread(port=9700, registry=None):
    """Start the master listener as a daemon thread; returns MasterServer."""
    m = MasterServer(port=port, registry=registry)
    m.start(port)
    return m
