"""真实使用场景测试（源自 sess_e05cbf8c 实测发现的问题）。

RU1 用户原始场景：远端 HTTP 服务 + 本地下载 + 清理 + collect
RU2 daemon 模式 steer 回归（桥接竞态修复验证，多次迭代）
RU3 durationMs 指标真实性（B3 回归）
RU4 collect 跨进程回载持久化记录（B5）
RU5 无 protocol 字段的 goal 直接 dispatch（B4 回归）
RU6 rao run CLI 一条命令派发
"""
import json
import os
import sys
import tempfile
import time
import urllib.request

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from rao import protocol  # noqa: E402
from rao.devices import DeviceRegistry  # noqa: E402
from rao.orchestrator import Orchestrator  # noqa: E402
from rao.transport import SshTransport, SshDaemonTransport, probe_tcp  # noqa: E402

from test_real_device import (  # noqa: E402
    DEVICE_ID, RDA_DIR, OUT_DIR, RDA_HOST, _goal, _remote_exec, _wait, reg,
    password_of,
)

_have_device = DEVICE_ID in reg.list_ids()
_dev_rec = reg.require(DEVICE_ID) if _have_device else {}
# node-registered devices carry port=None (they dial out) and no SSH
# credentials — those ride NodeTransport, not SSH, so skip them here.
_net_ok = bool(_dev_rec.get("host")) and _dev_rec.get("port") is not None \
    and probe_tcp(_dev_rec["host"], int(_dev_rec["port"]))
pytestmark = pytest.mark.skipif(
    not (_have_device and _net_ok and bool(password_of(_dev_rec))),
    reason="device unreachable or credentials missing")


@pytest.fixture(scope="module")
def orch(tmp_path_factory):
    runs = tmp_path_factory.mktemp("runs-usage")
    return Orchestrator(registry=reg, transport=SshTransport(rda_dir=RDA_DIR),
                        runs_dir=str(runs))


@pytest.fixture(scope="module")
def daemon_orch(tmp_path_factory):
    runs = tmp_path_factory.mktemp("runs-daemon")
    dt = SshDaemonTransport(rda_dir=RDA_DIR)
    dt.provision(reg.require(DEVICE_ID))   # 含 daemon 旧代码重启（B6）
    o = Orchestrator(registry=reg, transport=dt, runs_dir=str(runs))
    yield o
    try:
        dt._exec(reg.require(DEVICE_ID),
                 "cd %s && python3 rda_daemon.py stop" % RDA_DIR)
    except Exception:
        pass


# --- RU1: 用户原始场景（远端 HTTP 服务 + 本地下载 + 清理 + collect） --------
def test_ru1_remote_http_server_full_flow(orch):
    stamp = protocol.now_ms()
    webroot = "%s/rao_http_test_%d" % (OUT_DIR, stamp)
    fname = "hello-%d.txt" % stamp
    content = "rao-real-usage-%d" % stamp

    main_goal = _goal(
        objective="远端起 HTTP 服务并提供下载文件",
        constraints={"timeoutMs": 120000},
        plan={"steps": [
            {"id": "prep", "type": "shell",
             "command": "mkdir -p %s && echo '%s' > %s/%s"
                        % (webroot, content, webroot, fname),
             "success": {"exitCode": 0}},
            {"id": "serve", "type": "shell",
             "command": "cd %s && nohup python3 -m http.server 8899 "
                        "--bind 0.0.0.0 >/dev/null 2>&1 & sleep 1" % webroot,
             "success": {"exitCode": 0}},
            {"id": "self-check", "type": "shell",
             "command": "curl -s http://127.0.0.1:8899/%s" % fname,
             "success": {"exitCode": 0, "stdoutContains": content}},
        ]})
    ds = orch.dispatch([DEVICE_ID], main_goal)
    run = _wait(orch, ds[0]["goalId"], timeout_s=60)
    assert run.result["status"] == "success", run.result["summary"]

    # 本地通过 HTTP 真实下载（复刻用户场景）
    url = "http://%s:8899/%s" % (RDA_HOST, fname)
    with urllib.request.urlopen(url, timeout=10) as resp:
        body = resp.read().decode("utf-8", "replace")
    assert resp.status == 200
    assert content in body

    # 清理 goal（pkill 自匹配用 [h]ttp 正则技巧规避）
    cleanup = _goal(constraints={"timeoutMs": 90000},
                    plan={"steps": [
                        {"id": "s1-stop", "type": "shell",
                         "command": "pkill -f '[h]ttp.server 8899'",
                         "success": {"exitCode": 0}, "retries": 2},
                        {"id": "s2-rm", "type": "shell",
                         "command": "rm -rf %s" % webroot,
                         "success": {"exitCode": 0}},
                    ]})
    ds2 = orch.dispatch([DEVICE_ID], cleanup)
    run2 = _wait(orch, ds2[0]["goalId"], timeout_s=60)
    assert run2.result["status"] == "success", run2.result["summary"]
    rc, out, _ = _remote_exec("curl -s -m 3 http://127.0.0.1:8899/ 2>&1 || echo PORT-CLOSED")
    assert "PORT-CLOSED" in out or rc != 0

    # collect 聚合两个 goal
    report = orch.collect([ds[0]["goalId"], ds2[0]["goalId"]],
                          timeout_ms=30000)
    assert all(m["status"] == "success" for m in report["matrix"])


# --- RU2: daemon 模式 steer 回归（桥竞态修复，跑 3 轮） ----------------------
def test_ru2_daemon_steer_delivery_and_stream(daemon_orch):
    """门闩模式消除时序竞态：goal 在 wait_file 安全点等待，steer 确认送达后
    写释放文件，goal 继续。steer 需在 goal 等待期内确认送达。"""
    for iteration in range(3):
        release = "/tmp/rao-release-%d" % protocol.now_ms()
        goal = _goal(plan={"steps": [
            {"id": "s1-hold", "type": "wait_file", "path": release,
             "timeoutMs": 60000},
            {"id": "s2", "type": "shell", "command": "echo plain-done",
             "success": {"exitCode": 0}}]})
        ds = daemon_orch.dispatch([DEVICE_ID], goal)
        run = daemon_orch.get_run(ds[0]["goalId"])
        # goal 在 wait_file 安全点等待（无时序竞态）
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if run.chan and any(e["kind"] == "progress"
                                for e in run.chan.events):
                break
            time.sleep(0.1)
        # steer：追加带标记步骤并释放门闩
        assert run.steer(message="run:echo STEERED-MARKER-%d" % iteration), \
            "steer 应被 daemon 确认送达（B2 回归）"
        _remote_exec("touch %s" % release)
        assert run.wait_terminal(90), \
            "steer 后事件流冻结（B1 桥竞态回归）: state=%s error=%s" % (
                run.state, run.error)
        assert run.result["status"] == "success", run.result["summary"]
        notes = json.dumps(run.result.get("steerNotes", []))
        assert "run:" in notes
        seqs = [s for s in (run.chan.event_seqs or []) if s]
        assert seqs == sorted(seqs) and len(seqs) >= 3  # 事件流未冻结


# --- RU3: durationMs 真实性（B3 回归） ---------------------------------------
def test_ru3_duration_ms_is_real(orch):
    goal = _goal(constraints={"timeoutMs": 120000},
                 plan={"steps": [{"id": "s1", "type": "shell",
                                  "command": "echo quick",
                                  "success": {"exitCode": 0}}]})
    ds = orch.dispatch([DEVICE_ID], goal)
    run = _wait(orch, ds[0]["goalId"])
    assert run.result["status"] == "success"
    dur = run.result["metrics"]["durationMs"]
    assert dur < 30000, "durationMs 虚高（B3 回归）: %s" % dur


# --- RU4: collect 跨进程回载持久化记录（B5） --------------------------------
def test_ru4_collect_falls_back_to_persisted_record(tmp_path):
    orch_a = Orchestrator(registry=reg, transport=SshTransport(rda_dir=RDA_DIR),
                          runs_dir=str(tmp_path))
    goal = _goal(plan={"steps": [{"id": "s", "type": "shell",
                                  "command": "echo persisted",
                                  "success": {"exitCode": 0}}]})
    ds = orch_a.dispatch([DEVICE_ID], goal)
    run = orch_a.get_run(ds[0]["goalId"])
    assert run.wait_terminal(60)
    gid = ds[0]["goalId"]
    # 新的 orchestrator 实例（模拟进程重启）也能 collect 该 goal
    orch_b = Orchestrator(registry=reg, transport=SshTransport(rda_dir=RDA_DIR),
                          runs_dir=str(tmp_path))
    report = orch_b.collect([gid], timeout_ms=10000)
    m = report["matrix"][0]
    assert m["status"] == "success", m
    assert m["deviceId"] == DEVICE_ID


# --- RU5: 无 protocol 字段的 goal 直接 dispatch（B4 回归） ------------------
def test_ru5_dispatch_without_protocol_field(orch):
    raw_goal = {"deviceId": DEVICE_ID, "objective": "no protocol field",
                "plan": {"steps": [{"id": "s1", "type": "shell",
                                    "command": "echo no-protocol-ok",
                                    "success": {"exitCode": 0,
                                                "stdoutContains":
                                                    "no-protocol-ok"}}]}}
    ds = orch.dispatch([DEVICE_ID], raw_goal)   # 不带 protocol/header 字段
    run = _wait(orch, ds[0]["goalId"])
    assert run.result["status"] == "success", run.result["summary"]


# --- RU6: rao run CLI 一条命令派发 -------------------------------------------
def test_ru6_rao_run_cli(orch):
    import shutil
    import subprocess
    # Console scripts land in Scripts/ on Windows, bin/ on POSIX. Prefer the
    # installed entry point; fall back to `python -m rao.cli`.
    exe = shutil.which("rao")
    if exe:
        argv = [exe]
    else:
        argv = [sys.executable, "-m", "rao.cli"]
    r = subprocess.run(argv + ["run", "--device", DEVICE_ID,
                               "echo rao-run-cli-ok"],
                       capture_output=True, timeout=120)
    out = r.stdout.decode("utf-8", "replace")
    assert r.returncode == 0, out + r.stderr.decode("utf-8", "replace")
    assert "status: success" in out
    assert "rao-run-cli-ok" in out
