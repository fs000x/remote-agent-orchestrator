"""Debug: bridge stdin forwarding + daemon receipt, 5 iterations."""
import json
import os
import subprocess
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))), "src"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))), "tests"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

PY = sys.executable
RUNTIME = os.path.join(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))), "src", "rao", "runtime")

from test_rda_local import _goal  # noqa: E402
from rao.devices import DeviceRegistry  # noqa: E402
from rao.orchestrator import Orchestrator  # noqa: E402
from rao.transport import _DaemonLink  # noqa: E402


class LocalBridgeChannel(_DaemonLink):
    """本地复刻 SSH 桥（client-bridge 子进程），带转发日志。"""

    ch_counter = 0

    def __init__(self, goal_id, home):
        LocalBridgeChannel.ch_counter += 1
        self.ch_no = LocalBridgeChannel.ch_counter
        errlog = open(os.path.join(home, "bridge-%d-err.log" % self.ch_no),
                      "wb")
        super().__init__(goal_id, {"id": "local"}, home, handshake="bridge")
        self._errlog = errlog
        self.proc = None

    def _open_link(self):
        self.proc = subprocess.Popen(
            [PY, os.path.join(RUNTIME, "rda_daemon.py"), "--home",
             self.rda_dir, "client-bridge"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=self._errlog)

    def _send_line(self, obj):
        line = json.dumps(obj, ensure_ascii=False)
        os.write(2, ("[ch%d C->D] %s\n" % (self.ch_no, line[:110])).encode())
        try:
            self.proc.stdin.write((line + "\n").encode("utf-8"))
            self.proc.stdin.flush()
        except Exception as exc:
            import traceback
            self._errlog.write(("SEND-FAIL %r: %s\n" % (obj, exc)).encode())
            self._errlog.write(traceback.format_exc().encode())
            self._errlog.flush()
            raise

    def _recv_chunk(self, remaining):
        try:
            chunk = self.proc.stdout.read1(65536)
        except Exception:
            return None
        return chunk if chunk else None

    def alive(self):
        return (not self.closed and self.proc is not None
                and self.proc.poll() is None and self.exit_code is None)

    def _close_link(self):
        import traceback
        self._errlog.write(b"=== CLOSE-LINK called ===\n")
        self._errlog.write(traceback.format_stack().encode())
        self._errlog.flush()
        try:
            self.proc.stdin.close()
        except Exception:
            pass
        try:
            self.proc.terminate()
        except Exception:
            pass

    def __iter__(self):
        while True:
            msg = self._recv_line(None)
            if msg is None:
                return
            yield msg


class LocalBridgeTransport:
    def __init__(self, home):
        self.home = home

    def open_session(self, goal_id, device, goal_obj):
        ch = LocalBridgeChannel(goal_id, self.home)
        ch.connect_and_start(goal_obj, resume=False)
        return ch

    def open_resume_session(self, goal_id, device):
        ch = LocalBridgeChannel(goal_id, self.home)
        ch.connect_and_start(None, resume=True)
        return ch


def main():
    home = tempfile.mkdtemp()
    dlog = open(os.path.join(home, "d.log"), "wb")
    dproc = subprocess.Popen(
        [PY, os.path.join(RUNTIME, "rda_daemon.py"), "--home", home,
         "serve", "--port", "0"],
        stdout=dlog, stderr=dlog, stdin=subprocess.DEVNULL)
    dj_path = os.path.join(home, "daemon.json")
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and not os.path.isfile(dj_path):
        time.sleep(0.05)

    try:
        reg = DeviceRegistry(config_path=os.devnull)
        reg.devices = {"d1": {"id": "d1", "host": "local", "port": 0,
                              "user": "u", "maxConcurrentGoals": 2}}
        orch = Orchestrator(registry=reg, transport=LocalBridgeTransport(home),
                            runs_dir=tempfile.mkdtemp())
        for it in range(5):
            goal = _goal(objective="local steer %d" % it,
                         constraints={"timeoutMs": 60000},
                         plan={"steps": [
                             {"id": "s1-hold", "type": "shell",
                              "command": "ping -n 6 127.0.0.1 >NUL"
                              if os.name == "nt" else "sleep 6",
                              "success": {"exitCode": 0}},
                             {"id": "s2", "type": "shell",
                              "command": "echo plain-done",
                              "success": {"exitCode": 0}}]})
            ds = orch.dispatch(["d1"], goal)
            run = orch.get_run(ds[0]["goalId"])
            end = time.monotonic() + 20
            while time.monotonic() < end:
                if run.chan and run.chan.events and any(
                        e["kind"] == "progress" for e in run.chan.events):
                    break
                time.sleep(0.05)
            ok = run.steer(message="run:echo STEERED-MARKER-%d" % it)
            if not ok:
                ch = run.chan
                print("  FAIL-DIAG acks=%s stderr=%s" % (
                    dict(ch._acks), ch.stderr_text[-6:]))
            done = run.wait_terminal(60)
            steps = [s.get("id") for s in
                     (run.result or {}).get("steps", [])]
            print("iter %d: steer=%s state=%s steps=%s" % (
                it, ok, run.state, steps))
    finally:
        dproc.terminate()


if __name__ == "__main__":
    main()
