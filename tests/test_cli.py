"""v0.5 CLI tests: agent detection/injection/rollback with fake homes,
device add/remove, RDA auto-provision (fake transport + real device path)."""
import json
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from rao import agents  # noqa: E402
from rao.devices import DeviceRegistry  # noqa: E402
from rao.orchestrator import Orchestrator  # noqa: E402
from rao import protocol  # noqa: E402


@pytest.fixture()
def fake_home(tmp_path, monkeypatch):
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setattr(agents, "get_agent_def", _fake_get_agent_def(home))
    monkeypatch.setattr(agents, "state_dir",
                        lambda *a: str(home / ".rao-state"))
    return home


def _fake_get_agent_def(home):
    original = agents.get_agent_def   # capture BEFORE monkeypatch replaces it
    def _get(name, config_path=None, skills_dir=None):
        return original(
            name,
            config_path or str(home / (".zcode/cli/config.json"
                                       if name == "zcode" else ".claude.json")),
            skills_dir or str(home / (".zcode/skills" if name == "zcode"
                                      else ".claude/skills")))
    return _get


def test_detect_agents_with_fake_configs(fake_home):
    (fake_home / ".zcode/cli").mkdir(parents=True)
    (fake_home / ".zcode/cli/config.json").write_text("{}", encoding="utf-8")
    found = {d["name"]: d for d in agents.detect_agents()}
    # 本机若装有 ZCode 桌面版，installed 会是 True（探测的是全局安装）——
    # 跨机器稳定断言只针对：注入状态、以及 fake_home 里只有 zcode 时 claude 未装
    assert found["zcode"]["injected"] is False
    assert found["claude"]["config_path"] ==         str(fake_home / ".claude.json")   # 参数化路径生效


def test_inject_zcode_idempotent_and_skill(fake_home):
    r1 = agents.inject_agent("zcode")
    assert r1["ok"]
    cfg = json.load(open(str(fake_home / ".zcode/cli/config.json"),
                         encoding="utf-8"))
    entry = cfg["mcp"]["servers"]["remote-agent-orchestrator"]
    assert entry["args"] == ["-m", "rao.server"]
    skill = fake_home / ".zcode/skills/cross-device-orchestrator/SKILL.md"
    assert skill.is_file()
    # idempotent second run: no new writes
    r2 = agents.inject_agent("zcode")
    assert any("already up-to-date" in a for a in r2["actions"])
    # existing foreign servers preserved
    (fake_home / ".zcode/cli/config.json").write_text(json.dumps(
        {"mcp": {"servers": {"other": {"command": "x"}}}}), encoding="utf-8")
    agents.inject_agent("zcode")
    cfg = json.load(open(str(fake_home / ".zcode/cli/config.json"),
                         encoding="utf-8"))
    assert "other" in cfg["mcp"]["servers"]          # merge keeps others
    assert "remote-agent-orchestrator" in cfg["mcp"]["servers"]


def test_uninstall_restores_backup(fake_home):
    pre = str(fake_home / ".zcode/cli/config.json")
    os.makedirs(os.path.dirname(pre), exist_ok=True)
    with open(pre, "w", encoding="utf-8") as fh:
        json.dump({"user_key": "keep-me"}, fh)
    agents.inject_agent("zcode")
    agents.uninstall_agent("zcode")
    cfg = json.load(open(pre, encoding="utf-8"))
    assert cfg == {"user_key": "keep-me"}              # exact pre-state restored
    assert not (fake_home / ".zcode/skills/cross-device-orchestrator").exists()
    assert agents.is_injected("zcode") is False


def test_inject_claude_uses_mcpServers_key(fake_home):
    agents.inject_agent("claude")
    cfg = json.load(open(str(fake_home / ".claude.json"), encoding="utf-8"))
    assert "remote-agent-orchestrator" in cfg["mcpServers"]
    assert (fake_home / ".claude/skills/cross-device-orchestrator/SKILL.md") \
        .is_file()


# --- device add/remove via registry file -----------------------------------
def test_devices_add_remove(tmp_path, monkeypatch):
    cfg = tmp_path / "devices.json"
    monkeypatch.setattr(DeviceRegistry, "_find_config", lambda self: str(cfg))
    reg = DeviceRegistry(config_path=str(cfg))
    # simulate `rao devices add` non-interactive payload
    dev = {"id": "new-1", "host": "10.0.0.5", "port": 22, "user": "u",
           "workspace": "/srv", "maxConcurrentGoals": 2,
           "auth": {"type": "password", "password": "p"}}
    data = {"devices": []}
    data["devices"].append(dev)
    cfg.write_text(json.dumps(data), encoding="utf-8")
    reg.reload()
    assert reg.list_ids() == ["new-1"]
    assert DeviceRegistry.password_of(reg.require("new-1")) == "p"
    # remove
    data["devices"] = []
    cfg.write_text(json.dumps(data), encoding="utf-8")
    reg.reload()
    assert reg.list_ids() == []


# --- RDA auto-provision (fake transport with hash mismatch) ----------------
class ProvTransport:
    """Simulates a remote whose runtime is stale until provisioned."""

    def __init__(self):
        self.remote = {}          # name -> sha
        self.uploaded = []

    def _exec(self, device, cmd, timeout_s=15):
        import hashlib
        lines = [self.remote.get(n) or "" for n in
                 ("rda.py", "protocol.py", "rda_daemon.py")]
        return 0, "\n".join(lines)

    def _put_files(self, device, rda_dir, mapping):
        import hashlib
        for name, local_path in mapping.items():
            with open(local_path, "rb") as fh:
                self.remote[name] = hashlib.sha256(fh.read()).hexdigest()
            self.uploaded.append(name)
        return list(mapping)

    def provision(self, device):
        from rao.provision import ensure_rda
        r = ensure_rda(self, device, "/remote/rao")
        if r.get("error"):
            raise Exception(r["error"])
        return r


def test_provision_detects_stale_and_uploads(tmp_path):
    t = ProvTransport()
    dev = {"id": "d1"}
    r1 = t.provision(dev)
    assert r1["deployed"] and set(r1["uploaded"]) == \
        {"rda.py", "protocol.py", "rda_daemon.py"}
    r2 = t.provision(dev)
    assert r2["deployed"] is False          # now in sync
    assert t.provision(dev)["checked"]


def test_orchestrator_dispatch_triggers_provision(tmp_path):
    reg = DeviceRegistry(config_path=os.devnull)
    reg.devices = {"d1": {"id": "d1", "host": "fake", "port": 22,
                          "user": "u", "maxConcurrentGoals": 2}}
    t = ProvTransport()
    orch = Orchestrator(registry=reg, transport=t, runs_dir=str(tmp_path))
    script = [
        {"event": protocol.make_event(protocol.KIND_STARTED, "x", {})},
        {"event": protocol.make_event(protocol.KIND_RESULT, "x", {
            "status": "success", "summary": "ok", "steps": [],
            "artifacts": [], "assumptions": [], "metrics": {}})},
    ]
    # replace inner transport use: GoalRun calls transport.open_session —
    # emulate by combining provision transport with a simple session factory
    class Both(ProvTransport):
        def open_session(self, goal_id, device, goal_obj):
            self.provision(device)
            from rao.transport import FakeGoalChannel
            ch = FakeGoalChannel(goal_id, script)
            ch.connect_and_start(None)
            return ch
    both = Both()
    orch.transport = both
    ds = orch.dispatch(["d1"], {"protocol": "rao/1", "objective": "prov t"})
    run = orch.get_run(ds[0]["goalId"])
    assert run.wait_terminal(10)
    assert run.state == protocol.RUN_DONE
    assert both.uploaded, "provision should have uploaded on first dispatch"
