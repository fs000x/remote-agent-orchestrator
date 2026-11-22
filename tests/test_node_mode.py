"""v0.10 节点模式测试：master + node（同机回环）全链路。"""
import json
import os
import subprocess
import sys
import tempfile
import time

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
sys.path.insert(0, os.path.join(ROOT, "tests"))

PY = sys.executable
RUNTIME = os.path.join(ROOT, "src", "rao", "runtime")

from test_rda_local import _goal  # noqa: E402
from rao.devices import DeviceRegistry  # noqa: E402
from rao.orchestrator import Orchestrator  # noqa: E402
from rao.master import start_master_thread  # noqa: E402
from rao.transport import NodeTransport  # noqa: E402


@pytest.fixture()
def node_env(tmp_path):
    home = tmp_path / "node-home"
    home.mkdir()
    return home


def _spawn_node(home, master_port, token):
    logf = open(os.path.join(str(home), "nodelog.txt"), "wb")
    proc = subprocess.Popen(
        [PY, os.path.join(RUNTIME, "rda_daemon.py"), "serve",
         "--port", "0", "--master", "127.0.0.1:%d" % master_port,
         "--token", token],
        stdout=logf, stderr=logf, stdin=subprocess.DEVNULL)
    return proc


def test_node_mode_registration_and_goal(tmp_path, node_env):
    """节点注册 → devices 自动出现 → goal 经节点连接执行 → steer → collect。"""
    master = start_master_thread(port=0, registry=None)
    token = master.enrollment_token
    try:
        # 等主控监听就绪
        time.sleep(0.3)
        proc = _spawn_node(node_env, master._server.server_address[1], token)
        # 等节点注册
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if master.registered_devices():
                break
            time.sleep(0.05)
        devs = master.registered_devices()
        assert len(devs) == 1 and devs[0]["transport"] == "node"

        # 注册表自动并入
        reg = DeviceRegistry(config_path=os.devnull)
        reg.reload()
        assert devs[0]["id"] in reg.list_ids()
        d = reg.require(devs[0]["id"])
        assert d["transport"] == "node" and d["source"] == "node"

        # goal 经节点连接执行
        node_t = NodeTransport(master)
        orch = Orchestrator(registry=reg, transport=node_t,
                            runs_dir=str(tmp_path / "runs"))
        goal = _goal(plan={"steps": [
            {"id": "s1", "type": "shell", "command": "echo node-mode-ok",
             "success": {"exitCode": 0, "stdoutContains": "node-mode-ok"}}]})
        ds = orch.dispatch([devs[0]["id"]], goal)
        run = orch.get_run(ds[0]["goalId"])
        assert run.wait_terminal(30)
        assert run.result["status"] == "success", run.result["summary"]

        # steer（门闩模式）
        goal2 = _goal(plan={"steps": [
            {"id": "hold", "type": "wait_file", "path": "/tmp/rao-node-rel",
             "timeoutMs": 30000},
            {"id": "s2", "type": "shell", "command": "echo plain-done",
             "success": {"exitCode": 0}}]})
        ds2 = orch.dispatch([devs[0]["id"]], goal2)
        run2 = orch.get_run(ds2[0]["goalId"])
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if run2.chan and run2.chan.events and any(
                    e["kind"] == "progress" for e in run2.chan.events):
                break
            time.sleep(0.05)
        assert run2.steer(message="run:echo node-steered")
        assert run2.wait_terminal(30)
        assert run2.result["status"] == "success"

        proc.terminate()
    finally:
        pass


def test_node_registration_bad_token(tmp_path):
    master = start_master_thread(port=0, registry=None)
    bad_token = "wrong-token"
    proc = _spawn_node(tmp_path, master._server.server_address[1], bad_token)
    time.sleep(1.5)
    assert master.registered_devices() == []   # 令牌错误 → 拒绝
    proc.terminate()
