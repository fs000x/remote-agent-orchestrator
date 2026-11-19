"""Transport layer: SSH (paramiko) + scripted fake for tests.

Transport contract (per goal):
  open_session(device) -> GoalChannel
  GoalChannel: write_line / events iterator / stderr capture / alive / kill / close

The orchestrator never touches paramiko directly — this module is the seam that
lets a future R3SessionControlTransport drop in (DESIGN.md §11.1).
"""
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
            self.exit_code = reader_fn.exit_code() \
                if hasattr(reader_fn, "exit_code") else self.exit_code
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
    import os
    env_key = "RAO_DEVICE_%s_PASSWORD" % device["id"].upper().replace("-", "_")
    return os.environ.get(env_key) or device.get("auth", {}).get("password")


class SshTransport:
    """Real transport: one SSH connection per goal."""

    def __init__(self, rda_dir):
        self.rda_dir = rda_dir

    def open_session(self, goal_id, device, goal_line):
        chan = SshGoalChannel(goal_id, device, password_of(device), self.rda_dir)
        chan.connect_and_start(goal_line)
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

    def open_session(self, goal_id, device, goal_line):
        chan = FakeGoalChannel(goal_id, self.script_factory(goal_id))
        self.opened.append((goal_id, goal_line))
        chan.connect_and_start(goal_line)
        return chan
