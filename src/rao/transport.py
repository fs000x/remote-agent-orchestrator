"""Transport layer: SSH (paramiko) + scripted fake for tests.

Transport contract (per goal):
  open_session(device) -> GoalChannel
  GoalChannel: write_line / events iterator / stderr capture / alive / kill / close

The orchestrator never touches paramiko directly — this module is the seam that
lets a future R3SessionControlTransport drop in (DESIGN.md §11.1).
"""
import json
import logging
import socket
import threading
import time

import paramiko

from . import protocol

log = logging.getLogger("rao.transport")


class TransportError(Exception):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code  # RAO_DEVICE_UNREACHABLE | RAO_TRANSPORT_LOST


def probe_tcp(host, port, timeout_s=1.5):
    """Returns True if TCP connect succeeds."""
    try:
        with socket.create_connection((host, port), timeout=timeout_s):
            return True
    except OSError:
        return False


class GoalChannel:
    """Bidirectional line channel to one RDA process (remote or fake)."""

    def __init__(self, goal_id):
        self.goal_id = goal_id
        self.events = []          # parsed, validated event dicts (protocol order)
        self.stderr_text = []     # RDA's own diagnostics, not protocol
        self.exit_code = None
        self.killed = False
        self.closed = False
        self._close_lock = threading.Lock()
        self._event_cond = threading.Condition()
        self._pump = None

    # -- lifecycle ----------------------------------------------------------
    def start_pump(self, reader_fn):
        self._pump = threading.Thread(
            target=self._pump_main, args=(reader_fn,),
            name="rao-pump-%s" % self.goal_id[:8], daemon=True)
        self._pump.start()

    def _pump_main(self, reader_fn):
        try:
            for raw_kind, raw_line in reader_fn:
                if raw_kind == "stdout":
                    self._handle_stdout_line(raw_line)
                else:
                    if len(self.stderr_text) < 4000:
                        self.stderr_text.append(raw_line)
        except Exception as exc:  # transport died mid-run
            log.warning("pump error goal=%s: %s", self.goal_id, exc)
            self._push_protocol(protocol.make_event(
                protocol.KIND_ERROR, self.goal_id,
                {"message": "RAO_TRANSPORT_LOST: %s" % exc, "fatal": True}))
        finally:
            # exit_code may be an int attribute (generator sets it) or a method
            ec = getattr(reader_fn, "exit_code", None)
            if callable(ec):
                try:
                    ec = ec()
                except Exception:
                    ec = None
            if ec is not None:
                self.exit_code = ec
            with self._event_cond:
                self._event_cond.notify_all()

    def _handle_stdout_line(self, line):
        line = line.strip()
        if not line:
            return
        import json
        try:
            ev = json.loads(line)
        except ValueError:
            self.stderr_text.append("[non-json stdout] %s" % line[:200])
            return
        ok, err = protocol.validate_event(ev)
        if not ok:
            self.stderr_text.append("[bad event] %s" % err)
            return
        self._push_protocol(ev)

    def _push_protocol(self, ev):
        with self._event_cond:
            self.events.append(ev)
            self._event_cond.notify_all()

    # -- reads --------------------------------------------------------------
    def wait_event(self, since_index, timeout_s):
        """Block until an event exists at index >= since_index.

        Returns (event_or_None, new_index)."""
        deadline = time.monotonic() + timeout_s
        with self._event_cond:
            while True:
                if len(self.events) > since_index:
                    return self.events[since_index], since_index + 1
                remaining = deadline - time.monotonic()
                if remaining <= 0 or self.closed:
                    return None, since_index
                self._event_cond.wait(remaining)

    def wait_exit(self, timeout_s):
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if self.exit_code is not None:
                return self.exit_code
            time.sleep(0.1)
        return None

    # -- writes / teardown --------------------------------------------------
    def write_line(self, obj):
        raise NotImplementedError

    def alive(self):
        raise NotImplementedError

    def kill(self):
        self.killed = True

    def close(self):
        with self._close_lock:
            self.closed = True
            with self._event_cond:
                self._event_cond.notify_all()


class SshGoalChannel(GoalChannel):
    """One paramiko connection + one exec channel per goal (DESIGN.md A3)."""

    RDA_COMMAND = "cd {rda_dir} && python3 rda.py run-goal"

    def __init__(self, goal_id, device, password, rda_dir):
        super().__init__(goal_id)
        self.device = device
        self.rda_dir = rda_dir
        self._client = None
        self._chan = None
        self._stdin = None
        self._buf_lock = threading.Lock()
        self._out_buf = b""
        self._err_buf = b""

    # -- connect & spawn ----------------------------------------------------
    def connect_and_start(self, goal_line):
        try:
            self._client = paramiko.SSHClient()
            self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self._client.connect(
                self.device["host"], port=int(self.device.get("port", 22)),
                username=self.device["user"], password=password_of(self.device),
                timeout=10, banner_timeout=10, auth_timeout=10,
                allow_agent=False, look_for_keys=False)
        except Exception as exc:
            raise TransportError("RAO_DEVICE_UNREACHABLE",
                                 "ssh %s@%s: %s" % (self.device["user"],
                                                    self.device["host"], exc))
        try:
            self._chan = self._client.get_transport().open_session(timeout=10)
            self._chan.exec_command(self.RDA_COMMAND.format(rda_dir=self.rda_dir))
        except Exception as exc:
            self.close()
            raise TransportError("RAO_TRANSPORT_LOST", "exec: %s" % exc)
        self._stdin = self._chan.makefile_stdin("wb")
        self.write_line(goal_line)  # goal must arrive before RDA waits for plan
        self.start_pump(self)
        return self

    # -- GoalChannel API ----------------------------------------------------
    def write_line(self, obj):
        import json
        if self.closed or self._chan is None or self._chan.closed:
            return False
        try:
            self._stdin.write((json.dumps(obj, ensure_ascii=False) + "\n")
                              .encode("utf-8"))
            self._stdin.flush()
            return True
        except Exception as exc:
            log.warning("stdin write failed goal=%s: %s", self.goal_id, exc)
            return False

    def alive(self):
        if self._chan is None:
            return False
        if self.exit_code is not None:
            return False
        return not (self._chan.exit_status_ready()
                    and not self._chan.recv_ready()
                    and not self._chan.recv_stderr_ready())

    def kill(self):
        self.killed = True
        self.close()

    def close(self):
        super().close()
        try:
            if self._chan is not None:
                self._chan.close()
        except Exception:
            pass
        try:
            if self._client is not None:
                self._client.close()
        except Exception:
            pass

    # -- reader_fn protocol for the pump (generator + exit_code attr) -------
    _STDOUT = "stdout"
    _STDERR = "stderr"

    def __iter__(self):
        return self._reader()

    def _reader(self):
        while True:
            progressed = False
            if self._chan.recv_ready():
                with self._buf_lock:
                    self._out_buf += self._chan.recv(8192)
                progressed = True
            if self._chan.recv_stderr_ready():
                with self._buf_lock:
                    self._err_buf += self._chan.recv_stderr(8192)
                progressed = True
            with self._buf_lock:
                out_lines, self._out_buf = _drain_lines(self._out_buf)
                err_lines, self._err_buf = _drain_lines(self._err_buf)
            for ln in out_lines:
                yield self._STDOUT, ln.decode("utf-8", "replace")
            for ln in err_lines:
                yield self._STDERR, ln.decode("utf-8", "replace")
            if self._chan.exit_status_ready() and not progressed:
                # final drain then stop
                with self._buf_lock:
                    out_lines, self._out_buf = _drain_lines(self._out_buf, final=True)
                    err_lines, self._err_buf = _drain_lines(self._err_buf, final=True)
                for ln in out_lines:
                    yield self._STDOUT, ln.decode("utf-8", "replace")
                for ln in err_lines:
                    yield self._STDERR, ln.decode("utf-8", "replace")
                try:
                    self.exit_code = self._chan.recv_exit_status()
                except Exception:
                    self.exit_code = -1
                return
            if self.closed and not progressed:
                time.sleep(0.05)
                if not (self._chan.recv_ready() or self._chan.recv_stderr_ready()):
                    return
            if not progressed:
                time.sleep(0.03)

    def exit_code(self):
        return self.exit_code


def _drain_lines(buf, final=False):
    lines = []
    while True:
        idx = buf.find(b"\n")
        if idx >= 0:
            lines.append(buf[:idx])
            buf = buf[idx + 1:]
        elif final and buf:
            lines.append(buf)
            buf = b""
        else:
            break
    return lines, buf


def password_of(device):
    """Credential chain: env > OS keyring > config file > ZCode store (v0.6,
    only for devices imported from ZCode; opt-out RAO_REUSE_ZCODE_CREDS=0)."""
    import os
    env_key = "RAO_DEVICE_%s_PASSWORD" % device["id"].upper().replace("-", "_")
    pw = os.environ.get(env_key)
    if pw:
        return pw
    try:
        import keyring  # optional dependency
        pw = keyring.get_password("rao", device["id"])
        if pw:
            return pw
    except Exception:
        pass  # keyring not installed / backend unavailable
    pw = device.get("auth", {}).get("password")
    if pw:
        return pw
    if device.get("zcodeProfileId") and \
            os.environ.get("RAO_REUSE_ZCODE_CREDS", "1") != "0":
        try:
            from .zcode_store import get_profile_password
            return get_profile_password(device["zcodeProfileId"])
        except Exception:
            return None
    return None


def _open_paramiko_client(device):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(device["host"], port=int(device.get("port", 22)),
                   username=device["user"], password=password_of(device),
                   timeout=10, allow_agent=False, look_for_keys=False)
    return client


class SshTransport:
    """Real transport: one SSH connection per goal."""

    def __init__(self, rda_dir):
        self.rda_dir = rda_dir

    def _exec(self, device, cmd, timeout_s=15):
        client = _open_paramiko_client(device)
        try:
            _, out, err = client.exec_command(cmd, timeout=timeout_s)
            rc = out.channel.recv_exit_status()
            return rc, out.read().decode("utf-8", "replace")
        finally:
            client.close()

    def _put_files(self, device, rda_dir, mapping):
        client = _open_paramiko_client(device)
        try:
            client.exec_command("mkdir -p %s" % rda_dir)[1].channel.recv_exit_status()
            sftp = client.open_sftp()
            uploaded = []
            for name, local_path in mapping.items():
                sftp.put(local_path, "%s/%s" % (rda_dir, name))
                uploaded.append(name)
            sftp.close()
            return uploaded
        finally:
            client.close()

    def provision(self, device):
        from .provision import ensure_rda
        r = ensure_rda(self, device, self.rda_dir)
        if r.get("error"):
            raise TransportError("RAO_DEVICE_UNREACHABLE",
                                 "provision failed: %s" % r["error"])
        return r

    def open_session(self, goal_id, device, goal_obj):
        chan = SshGoalChannel(goal_id, device, password_of(device), self.rda_dir)
        chan.connect_and_start(goal_obj)
        return chan


class FakeGoalChannel(GoalChannel):
    """Scripted RDA for unit tests: emits queued events, honors steer/cancel.

    script: list of dicts {"event": {...}, "waitForSteer": bool, "sleepMs": n}
    """

    def __init__(self, goal_id, script=None, on_event=None):
        super().__init__(goal_id)
        self.script = list(script or [])
        self._injected = []
        self._inj_cond = threading.Condition()
        self.on_event = on_event
        self._exit_at_end = 0

    def connect_and_start(self, goal_line):
        self.start_pump(self)
        return self

    def __iter__(self):
        return self._scripted()

    def _scripted(self):
        import json
        for item in self.script:
            if self.killed or self.closed:
                break
            # honor cancel injection like the real RDA does
            with self._inj_cond:
                cancel_seen = any(i.get("type") == "cancel"
                                  for i in self._injected)
                self._injected = [i for i in self._injected
                                  if i.get("type") != "cancel"]
            if cancel_seen or self.killed:
                ev = protocol.make_event(protocol.KIND_RESULT, self.goal_id,
                                         {"status": protocol.ST_CANCELLED,
                                          "summary": "cancelled",
                                          "steps": [], "artifacts": [],
                                          "assumptions": [],
                                          "metrics": {}})
                yield "stdout", json.dumps(ev)
                self.exit_code = 4
                return
            sleep_ms = item.get("sleepMs", 0)
            if sleep_ms:
                self._sleep_interruptible(sleep_ms / 1000.0)
            if self.killed or self.closed:
                break
            if item.get("waitForSteer"):
                got = self._wait_injection(30)
                if got is not None and got.get("type") == "cancel":
                    ev = protocol.make_event(protocol.KIND_RESULT, self.goal_id,
                                             {"status": protocol.ST_CANCELLED,
                                              "summary": "cancelled while waiting",
                                              "steps": [], "artifacts": [],
                                              "assumptions": [],
                                              "metrics": {}})
                    yield "stdout", json.dumps(ev)
                    self.exit_code = 4
                    return
                if got is None or got.get("type") != "steer":
                    yield "stdout", json.dumps(protocol.make_event(
                        protocol.KIND_ERROR, self.goal_id,
                        {"message": "no steer received", "fatal": True}))
                    self._exit_at_end = 1
                    return
            ev = dict(item["event"])
            ev.setdefault("protocol", protocol.PROTOCOL_VERSION)
            ev.setdefault("goalId", self.goal_id)
            ev.setdefault("ts", protocol.now_ms())
            ev.setdefault("data", {})
            yield "stdout", json.dumps(ev)
            if self.on_event:
                self.on_event(ev)
        self._exit_at_end = 4 if self.killed else self._exit_at_end
        self.exit_code = self._exit_at_end

    def exit_code(self):
        return self.exit_code

    def _sleep_interruptible(self, seconds):
        end = time.monotonic() + seconds
        while time.monotonic() < end and not (self.killed or self.closed):
            time.sleep(0.02)

    def _wait_injection(self, timeout_s):
        deadline = time.monotonic() + timeout_s
        with self._inj_cond:
            while time.monotonic() < deadline:
                if self._injected:
                    return self._injected.pop(0)
                self._inj_cond.wait(0.05)
        return None

    def write_line(self, obj):
        with self._inj_cond:
            self._injected.append(obj)
            self._inj_cond.notify_all()
        return True

    def alive(self):
        return self.exit_code is None and not self.closed

    def close(self):
        super().close()


class FakeTransport:
    def __init__(self, script_factory):
        self.script_factory = script_factory  # fn(goal_id) -> script list
        self.opened = []

    def open_session(self, goal_id, device, goal_obj):
        chan = FakeGoalChannel(goal_id, self.script_factory(goal_id))
        self.opened.append((goal_id, goal_obj))
        chan.connect_and_start(goal_obj)
        return chan

    def open_resume_session(self, goal_id, device):
        raise TransportError("RAO_TRANSPORT_LOST", "fake transport cannot resume")


class ChannelLost(Exception):
    """Raised by pumps when the link died mid-run (resume may be possible)."""


# ---------------------------------------------------------------------------
# Daemon-mode transport (v0.3): persistent rda_daemon on the device.
# ---------------------------------------------------------------------------

class _DaemonLink(object):
    """Line reader/writer over a link. Subclasses provide _recv_line() /
    _send_line() / alive() / close() / exit_code.

    handshake: "bridge" (SSH client-bridge auths itself; ready arrives
    immediately) or "direct" (TCP: client must auth with the daemon token)."""

    def __init__(self, goal_id, device, rda_dir, handshake="bridge",
                 token=None):
        self.goal_id = goal_id
        self.device = device
        self.rda_dir = rda_dir
        self.handshake = handshake
        self.token = token
        self.events = []
        self.event_seqs = []     # daemon seq per event (for resume dedup)
        self.stderr_text = []
        self.exit_code = None
        self.killed = False
        self.closed = False
        self._line_buf = b""     # shared line buffer: handshake AND reader
        self._line_lock = threading.Lock()
        self._close_lock = threading.Lock()
        self._event_cond = threading.Condition()
        self.supports_resume = True

    # -- pump plumbing -------------------------------------------------------
    def start_pump(self, reader_fn):
        self._pump_thread = threading.Thread(
            target=self._pump_main, args=(reader_fn,),
            name="rao-dpump-%s" % self.goal_id[:8], daemon=True)
        self._pump_thread.start()

    # -- daemon wire --------------------------------------------------------
    def connect_and_start(self, goal_obj, resume=False):
        self._open_link()
        if self.handshake == "direct":
            self._send_line({"cmd": "auth", "token": self.token})
        ready = self._recv_line(10)
        if not ready or ready.get("type") != "ready":
            raise TransportError("RAO_TRANSPORT_LOST",
                                 "daemon bridge not ready: %r" % (ready,))
        if resume:
            self._send_line({"cmd": "attach", "goalId": self.goal_id})
            ack = self._recv_line(10)
            if not ack or ack.get("type") != "attached":
                raise TransportError("RAO_TRANSPORT_LOST",
                                     "attach failed: %r" % (ack,))
        else:
            self._send_line({"cmd": "submit", "goal": goal_obj})
            ack = self._recv_line(10)
            if not ack or ack.get("type") != "ack":
                raise TransportError("RAO_TRANSPORT_LOST",
                                     "submit failed: %r" % (ack,))
        self._after_handshake()
        self.start_pump(self)
        return self

    def _after_handshake(self):
        """Hook: reset timeouts for the streaming reader (TCP variant)."""

    # -- shared line framing (handshake and reader use the SAME buffer) ------
    def _recv_line(self, timeout_s=None):
        import os as _os
        _dbg = _os.environ.get("RAO_WIRE_DEBUG")
        if _dbg:
            print("[wire %s] recv_line t=%s buf=%r" % (
                self.goal_id[:8], timeout_s, self._line_buf[:120]),
                flush=True)
        line = self.__recv_line_inner(timeout_s)
        if _dbg:
            print("[wire %s] -> %r" % (self.goal_id[:8], line), flush=True)
        return line

    def __recv_line_inner(self, timeout_s=None):
        """Return the next parsed line, or None on EOF. timeout_s=None blocks
        until EOF; otherwise raises nothing, returns None on quiet timeout."""
        deadline = None if timeout_s is None else time.monotonic() + timeout_s
        while True:
            with self._line_lock:
                if b"\n" in self._line_buf:
                    ln, self._line_buf = self._line_buf.split(b"\n", 1)
                    try:
                        return json.loads(ln.decode("utf-8", "replace"))
                    except ValueError:
                        return {"type": "bridge_noise",
                                "raw": ln.decode("utf-8", "replace")[:120]}
            remaining = None
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
            chunk = self._recv_chunk(remaining)
            if chunk is None:
                # EOF: final drain
                with self._line_lock:
                    if b"\n" in self._line_buf:
                        continue
                return None
            with self._line_lock:
                self._line_buf += chunk

    def _pump_main(self, reader_fn):
        try:
            for msg in reader_fn:
                mtype = msg.get("type")
                if mtype == "event":
                    ev = msg.get("event") or {}
                    ok, err = protocol.validate_event(ev)
                    if not ok:
                        self.stderr_text.append("[bad event] %s" % err)
                        continue
                    self._push_protocol(ev, msg.get("seq"))
                elif mtype == "goal_status":
                    # terminal signal from the daemon (post-result safety net)
                    self.exit_code = msg.get("exit", 0)
                    with self._event_cond:
                        self._event_cond.notify_all()
                    if msg.get("status") in ("timeout", "cancelled", "failed"):
                        if not any(e.get("kind") == "result"
                                   for e in self.events):
                            status = msg.get("status")
                            self._push_protocol(protocol.make_event(
                                protocol.KIND_RESULT, self.goal_id,
                                {"status": status,
                                 "summary": "daemon goal_status (%s)" % status,
                                 "steps": [], "artifacts": [],
                                 "assumptions": [], "metrics": {}}))
                    return
                elif mtype == "error":
                    self.stderr_text.append("[daemon error] %s"
                                            % msg.get("message"))
                else:
                    self.stderr_text.append("[daemon] %s" % json.dumps(msg)[:120])
        except Exception as exc:
            log.warning("daemon pump error goal=%s: %s", self.goal_id, exc)
        finally:
            with self._event_cond:
                self._event_cond.notify_all()

    def _push_protocol(self, ev, seq=None):
        with self._event_cond:
            self.events.append(ev)
            self.event_seqs.append(seq)
            self._event_cond.notify_all()

    def wait_event(self, since_index, timeout_s):
        deadline = time.monotonic() + timeout_s
        with self._event_cond:
            while True:
                if len(self.events) > since_index:
                    return self.events[since_index], since_index + 1
                remaining = deadline - time.monotonic()
                if remaining <= 0 or self.closed:
                    return None, since_index
                self._event_cond.wait(remaining)

    def wait_exit(self, timeout_s):
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if self.exit_code is not None:
                return self.exit_code
            time.sleep(0.1)
        return None

    def write_line(self, obj):
        if self.closed:
            return False
        try:
            self._send_line({"cmd": "inject", "goalId": self.goal_id,
                             "payload": obj})
            return True
        except Exception as exc:
            log.warning("daemon inject failed goal=%s: %s", self.goal_id, exc)
            return False

    def alive(self):
        raise NotImplementedError

    def kill(self):
        self.killed = True
        self.close()

    def close(self):
        with self._close_lock:
            self.closed = True
            with self._event_cond:
                self._event_cond.notify_all()
        self._close_link()


class TcpDaemonChannel(_DaemonLink):
    """Direct TCP to a daemon (used in local tests)."""

    def __init__(self, goal_id, device, token, rda_dir=None):
        super(TcpDaemonChannel, self).__init__(goal_id, device, rda_dir,
                                               handshake="direct",
                                               token=token)
        self.sock = None

    def _open_link(self):
        self.sock = socket.create_connection(
            (self.device["host"], int(self.device.get("daemonPort", 0))),
            timeout=5)
        self.sock.settimeout(None)

    def _send_line(self, obj):
        self.sock.sendall((json.dumps(obj, ensure_ascii=False) + "\n")
                          .encode("utf-8"))

    def _recv_chunk(self, remaining):
        if remaining is None:
            self.sock.settimeout(None)
        else:
            self.sock.settimeout(max(0.05, remaining))
        try:
            return self.sock.recv(4096)
        except socket.timeout:
            return b""   # quiet timeout: caller re-checks deadline/buffer
        except OSError:
            return None

    def __iter__(self):
        while True:
            msg = self._recv_line(None)
            if msg is None:
                return
            yield msg

    def alive(self):
        return self.sock is not None and not self.closed \
            and self.exit_code is None

    def _after_handshake(self):
        self.sock.settimeout(None)   # streaming reader must not time out

    def _close_link(self):
        try:
            self.sock.close()
        except Exception:
            pass


class TcpDaemonTransport:
    """Local-test transport straight to a running daemon (no SSH)."""

    def __init__(self, token):
        self.token = token

    def open_session(self, goal_id, device, goal_obj):
        chan = TcpDaemonChannel(goal_id, device, self.token,
                                rda_dir=None)
        chan.connect_and_start(goal_obj, resume=False)
        return chan

    def open_resume_session(self, goal_id, device):
        chan = TcpDaemonChannel(goal_id, device, self.token, rda_dir=None)
        chan.connect_and_start(None, resume=True)
        return chan


class SshDaemonChannel(_DaemonLink):
    """Daemon access over SSH: client-bridge pipes the daemon TCP socket
    through the SSH channel (loopback-only daemon, token inside daemon.json)."""

    BRIDGE_CMD = "cd {rda_dir} && python3 rda_daemon.py client-bridge"

    def __init__(self, goal_id, device, password, rda_dir):
        super(SshDaemonChannel, self).__init__(goal_id, device, rda_dir)
        self._client = None
        self._chan = None
        self._stdin = None
        self._buf_lock = threading.Lock()
        self._out_buf = b""
        self._err_buf = b""

    def _open_link(self):
        try:
            self._client = paramiko.SSHClient()
            self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self._client.connect(
                self.device["host"], port=int(self.device.get("port", 22)),
                username=self.device["user"], password=password_of(self.device),
                timeout=10, banner_timeout=10, auth_timeout=10,
                allow_agent=False, look_for_keys=False)
            self._chan = self._client.get_transport().open_session(timeout=10)
            self._chan.exec_command(self.BRIDGE_CMD.format(rda_dir=self.rda_dir))
        except Exception as exc:
            raise TransportError("RAO_DEVICE_UNREACHABLE",
                                 "ssh bridge: %s" % exc)
        self._stdin = self._chan.makefile_stdin("wb")

    def _send_line(self, obj):
        self._stdin.write((json.dumps(obj, ensure_ascii=False) + "\n")
                          .encode("utf-8"))
        self._stdin.flush()

    def _recv_chunk(self, remaining):
        """Poll the SSH channel. Returns bytes when stdout data is available,
        b"" on quiet deadline expiry, None on EOF (bridge exited, drained)."""
        deadline = None if remaining is None else time.monotonic() + remaining
        while True:
            if self.closed:
                return None
            if self._chan.recv_stderr_ready():
                try:
                    err = self._chan.recv_stderr(8192)
                    self.stderr_text.append(
                        err.decode("utf-8", "replace")[:200])
                except Exception:
                    pass
            if self._chan.recv_ready():
                return self._chan.recv(8192)
            if self._chan.exit_status_ready() and not self._chan.recv_ready():
                return None
            if deadline is not None and time.monotonic() >= deadline:
                return b""
            time.sleep(0.03)

    def __iter__(self):
        while True:
            msg = self._recv_line(None)
            if msg is None:
                return
            yield msg

    def alive(self):
        if self.closed or self._chan is None:
            return False
        if self.exit_code is not None:
            return False
        return not (self._chan.exit_status_ready()
                    and not self._chan.recv_ready()
                    and not self._chan.recv_stderr_ready())

    def _close_link(self):
        try:
            if self._chan is not None:
                self._chan.close()
        except Exception:
            pass
        try:
            if self._client is not None:
                self._client.close()
        except Exception:
            pass


class SshDaemonTransport:
    """Persistent-daemon transport: one SSH bridge per goal connection."""

    def __init__(self, rda_dir, password_of_fn=password_of):
        self.rda_dir = rda_dir
        self._password_of = password_of_fn

    def _exec(self, device, cmd, timeout_s=15):
        """One-shot SSH exec; returns (rc, stdout)."""
        client = _open_paramiko_client(device)
        try:
            _, out, err = client.exec_command(cmd, timeout=timeout_s)
            rc = out.channel.recv_exit_status()
            return rc, out.read().decode("utf-8", "replace")
        finally:
            client.close()

    def _put_files(self, device, rda_dir, mapping):
        client = _open_paramiko_client(device)
        try:
            client.exec_command("mkdir -p %s" % rda_dir)[1].channel.recv_exit_status()
            sftp = client.open_sftp()
            uploaded = []
            for name, local_path in mapping.items():
                sftp.put(local_path, "%s/%s" % (rda_dir, name))
                uploaded.append(name)
            sftp.close()
            return uploaded
        finally:
            client.close()

    def provision(self, device):
        from .provision import ensure_rda
        r = ensure_rda(self, device, self.rda_dir)
        if r.get("error"):
            raise TransportError("RAO_DEVICE_UNREACHABLE",
                                 "provision failed: %s" % r["error"])
        return r

    def ensure_daemon(self, device):
        """Ping the daemon; start it if not alive. Returns True if alive."""
        ping = "cd %s && python3 rda_daemon.py ping" % self.rda_dir
        rc, _ = self._exec(device, ping)
        if rc == 0:
            return True
        rc, out = self._exec(device, "cd %s && python3 rda_daemon.py start"
                             % self.rda_dir, timeout_s=20)
        rc, _ = self._exec(device, ping)
        return rc == 0

    def open_session(self, goal_id, device, goal_obj):
        chan = SshDaemonChannel(goal_id, device, self._password_of(device),
                                self.rda_dir)
        chan.connect_and_start(goal_obj, resume=False)
        return chan

    def open_resume_session(self, goal_id, device):
        chan = SshDaemonChannel(goal_id, device, self._password_of(device),
                                self.rda_dir)
        chan.connect_and_start(None, resume=True)
        return chan
