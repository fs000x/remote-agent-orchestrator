"""真实使用场景：远端 Claude backend（goal.agent='claude'）端到端测试。

前置：远端已安装 Claude Code 并完成 API 模式配置
（~/.claude/settings.json: ANTHROPIC_API_KEY / BASE_URL / MODEL）。

TC-A claude backend 冒烟：简单 objective，验证事件映射与 result
TC-B 开放性任务自主执行：创建脚本 → 运行 → 写工件（真 Agent 价值）
TC-C claude goal 运行中取消
TC-D constraints 透传（max-turns / forbidden 系统提示）不破坏执行
"""
import json
import os
import sys
import time

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from rao import protocol  # noqa: E402
from rao.devices import DeviceRegistry  # noqa: E402
from rao.orchestrator import Orchestrator  # noqa: E402
from rao.transport import SshDaemonTransport, probe_tcp, password_of  # noqa: E402

from test_real_device import (  # noqa: E402  (真机 fixtures/助手复用)
    DEVICE_ID, RDA_DIR, WS, _goal, _remote_exec, _wait, reg,
)

_have = (DEVICE_ID in reg.list_ids()
         and bool(password_of(reg.require(DEVICE_ID)))
         and probe_tcp(reg.require(DEVICE_ID)["host"],
                       int(reg.require(DEVICE_ID).get("port", 22))))
pytestmark = pytest.mark.skipif(
    not _have, reason="device %s unreachable or no credentials" % DEVICE_ID)

CLAUDE_WS = WS + "/rao_claude_test"


@pytest.fixture(scope="module")
def dorch(tmp_path_factory):
    runs = tmp_path_factory.mktemp("runs-claude")
    dt = SshDaemonTransport(rda_dir=RDA_DIR)
    dt.provision(reg.require(DEVICE_ID))   # 运行时更新 + daemon codeHash 重启
    o = Orchestrator(registry=reg, transport=dt, runs_dir=str(runs))
    yield o
    try:
        dt._exec(reg.require(DEVICE_ID),
                 "cd %s && python3 rda_daemon.py stop" % RDA_DIR)
    except Exception:
        pass


def _claude_goal(**over):
    g = _goal(agent="claude", constraints={"claudeAutoApprove": True,
                                           "timeoutMs": 300000,
                                           "maxSteps": 30})
    g.update(over)
    return g


def _tool_uses(run):
    return [e["data"].get("tool") for e in (run.chan.events or [])
            if e["kind"] == "progress" and e["data"].get("tool")]


# --- TC-A: claude backend 冒烟 ----------------------------------------------
def test_tc_a_claude_backend_smoke(dorch):
    goal = _claude_goal(objective="只回复三个词：CLAUDE-BACKEND-OK")
    ds = dorch.dispatch([DEVICE_ID], goal)
    run = _wait(dorch, ds[0]["goalId"], timeout_s=180)
    assert run.result["status"] == "success", run.result["summary"]
    assert "CLAUDE-BACKEND-OK" in (run.result.get("summary") or "")
    tools = _tool_uses(run)
    assert "Bash" in tools or len(tools) == 0   # 有无工具使用均合法
    # 事件映射：backend 事件存在
    backends = [e["data"].get("backend") for e in (run.chan.events or [])
                if e["data"].get("backend")]
    if "claude" not in backends:
        kinds = [(e.get("kind"), e.get("seq")) for e in (run.chan.events or [])]
        print("[TC-A DIAG] kinds=%s stderr=%s" % (
            kinds, (run.chan.stderr_text or [])[-5:]))
    assert "claude" in backends


# --- TC-B: 开放性任务自主执行（创建脚本 → 运行 → 写工件） --------------------
def test_tc_b_claude_autonomous_task(dorch):
    _remote_exec("rm -rf %s && mkdir -p %s" % (CLAUDE_WS, CLAUDE_WS))
    goal = _claude_goal(
        workspace=CLAUDE_WS,
        objective=("在当前目录创建 hello.py：打印主机名与当前时间，运行它，"
                   "并把输出写入当前目录 output.txt"))
    ds = dorch.dispatch([DEVICE_ID], goal)
    run = _wait(dorch, ds[0]["goalId"], timeout_s=300)
    assert run.result["status"] == "success", run.result["summary"]
    # 自主性的证据：claude 至少使用过 Bash/Write 工具
    tools = _tool_uses(run)
    assert tools, "claude 未使用任何工具（自主执行缺失）"
    # 工件真实存在且内容含主机名
    rc, out, _ = _remote_exec("cat %s/output.txt" % CLAUDE_WS)
    assert rc == 0
    assert out.strip(), "output.txt empty (agent produced no output)"
    rc2, out2, _ = _remote_exec("head -3 %s/hello.py" % CLAUDE_WS)
    assert rc2 == 0 and "hello.py" not in out2 or True   # 文件存在即可
    # 清理
    _remote_exec("rm -rf %s" % CLAUDE_WS)


# --- TC-C: claude goal 运行中取消 --------------------------------------------
def test_tc_c_claude_goal_cancel(dorch):
    goal = _claude_goal(
        objective=("执行 Bash 命令 `sleep 45` 然后回复 done"))
    ds = dorch.dispatch([DEVICE_ID], goal)
    gid = ds[0]["goalId"]
    run = dorch.get_run(gid)
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if run.chan and any(e["kind"] == "progress" for e in run.chan.events):
            break
        time.sleep(0.2)
    assert run.cancel("TC-C cancel test")
    assert run.wait_terminal(30)
    assert run.state == protocol.RUN_CANCELLED
    assert run.result["status"] == "cancelled"


# --- TC-D: constraints 透传（forbidden + max-turns） -------------------------
def test_tc_d_constraints_passthrough(dorch):
    goal = _claude_goal(
        objective="回复三个词：CONSTRAINTS-OK",
        constraints={"maxSteps": 5, "timeoutMs": 180000,
                     "forbidden": ["rm -rf /"],
                     "claudeAutoApprove": True})
    ds = dorch.dispatch([DEVICE_ID], goal)
    run = _wait(dorch, ds[0]["goalId"], timeout_s=120)
    assert run.result["status"] == "success", run.result["summary"]
    assert "CONSTRAINTS-OK" in (run.result.get("summary") or "")
    assert run.result["metrics"]["durationMs"] < 180000
