"""v0.6 tests: ZCode device import + credential decrypt + claude backend."""
import json
import os
import sys
import time

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from rao import protocol  # noqa: E402
from rao.devices import DeviceRegistry  # noqa: E402
from rao.transport import password_of  # noqa: E402

from test_rda_local import RdaProc, _goal  # noqa: E402

TEST_SECRET = "unit-test-os-secret"


def _seal(value, secret=TEST_SECRET):
    """Python mirror of ZCode's seal() (AES-256-GCM, base64url iv.tag.ct)."""
    import base64
    import hashlib
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    key = hashlib.sha256(secret.encode("utf-8")).digest()
    iv = os.urandom(12)
    ct_with_tag = AESGCM(key).encrypt(iv, value.encode("utf-8"), None)
    ct, tag = ct_with_tag[:-16], ct_with_tag[-16:]   # GCM: tag is the tail
    b64 = lambda b: base64.urlsafe_b64encode(b).decode().rstrip("=")
    return "enc:v2:os:%s.%s.%s" % (b64(iv), b64(tag), b64(ct))


def _mk_fake_zcode_home(tmp_path, host="10.1.2.3", user="remote-u",
                        password="zcode-stored-pw"):
    """Build a fake ZCode desktop store with one SSH device."""
    home = tmp_path / "zc"
    home.mkdir()
    sid = "11111111-2222-3333-4444-555566667777"
    pid = "aaaaaaaa-bbbb-cccc-dddd-eeeeffff0000"
    setting = {
        "sourceServers": [{
            "sourceServerId": sid, "displayName": "fake-zc-host",
            "hostname": "fake-zc", "osUser": user,
            "lastSeenVersion": "3.19.0", "installState": "installed",
        }],
        "connectionProfiles": [{
            "profileId": pid, "sourceServerId": sid,
            "displayName": host, "transport": "ssh",
            "target": {"kind": "ssh", "host": host, "port": 22,
                       "username": user},
        }],
        "sourceServerWorkspaceHistory": [{
            "sourceServerId": sid, "workspacePath": "/remote/ws"}],
    }
    creds = {"connection-profile:%s:password" % pid: _seal(password)}
    (home / "setting.json").write_text(json.dumps(setting), encoding="utf-8")
    (home / "credentials.json").write_text(json.dumps(creds), encoding="utf-8")
    os.environ["RAO_ZCODE_HOME"] = str(home)
    return {"id": sid, "profileId": pid, "host": host, "user": user,
            "password": password, "workspace": "/remote/ws"}


@pytest.fixture()
def fake_zcode(tmp_path, monkeypatch):
    info = _mk_fake_zcode_home(tmp_path)
    # fake OS secret fetch (avoids touching real Credential Manager)
    import rao.zcode_store as zs
    monkeypatch.setattr(zs, "_system_secret",
                        lambda path: (TEST_SECRET, "utf-8"))
    yield info
    os.environ.pop("RAO_ZCODE_HOME", None)


def test_list_zcode_devices(fake_zcode):
    from rao.zcode_store import list_zcode_ssh_devices
    devs = list_zcode_ssh_devices()
    assert len(devs) == 1
    d = devs[0]
    assert d["id"] == fake_zcode["id"]
    assert d["host"] == fake_zcode["host"]
    assert d["user"] == fake_zcode["user"]
    assert d["workspace"] == fake_zcode["workspace"]
    assert d["source"] == "zcode"


def test_get_profile_password_decrypts(fake_zcode):
    from rao.zcode_store import get_profile_password
    assert get_profile_password(fake_zcode["profileId"]) == \
        fake_zcode["password"]


def test_registry_merges_zcode_devices(fake_zcode):
    reg = DeviceRegistry(config_path=os.devnull)
    assert fake_zcode["id"] in reg.list_ids()
    d = reg.describe_all(probe=False)
    mine = [x for x in d if x["id"] == fake_zcode["id"]][0]
    assert mine["source"] == "zcode"
    assert mine["execution"]["is_agent"] is False


def test_registry_merge_opt_out(fake_zcode, monkeypatch):
    monkeypatch.setenv("RAO_MERGE_ZCODE_DEVICES", "0")
    reg = DeviceRegistry(config_path=os.devnull)
    assert fake_zcode["id"] not in reg.list_ids()


def test_password_chain_uses_zcode_store(fake_zcode, monkeypatch):
    reg = DeviceRegistry(config_path=os.devnull)
    dev = reg.require(fake_zcode["id"])
    monkeypatch.delenv("RAO_DEVICE_%s_PASSWORD"
                       % fake_zcode["id"].upper().replace("-", "_"),
                       raising=False)
    assert password_of(dev) == fake_zcode["password"]
    # opt-out: falls back to None (no other credential configured)
    monkeypatch.setenv("RAO_REUSE_ZCODE_CREDS", "0")
    assert password_of(dev) is None


def test_local_device_wins_on_collision(fake_zcode, tmp_path):
    cfg = tmp_path / "devices.json"
    local = {"devices": [{
        "id": fake_zcode["id"], "host": "override-host", "port": 2222,
        "user": "local-u", "maxConcurrentGoals": 1,
        "auth": {"type": "password", "password": "local-pw"},
        "source": "local"}]}
    cfg.write_text(json.dumps(local), encoding="utf-8")
    reg = DeviceRegistry(config_path=str(cfg))
    dev = reg.require(fake_zcode["id"])
    assert dev["host"] == "override-host"          # local entry wins
    assert password_of(dev) == "local-pw"


# --- claude backend (local RDA with fake claude shim) -----------------------
FAKE_CLAUDE = r'''import json, sys
# consume args; emit minimal stream-json
sys.argv = sys.argv  # keep
lines = [
    {"type": "system", "subtype": "init"},
    {"type": "assistant", "message": {"content": [
        {"type": "tool_use", "name": "Bash", "input": {"command": "ls"}}]}},
    {"type": "assistant", "message": {"content": [
        {"type": "text", "text": "done"}]}},
    {"type": "result", "subtype": "success",
     "result": "fake-claude-finished-ok"},
]
for ln in lines:
    sys.stdout.write(json.dumps(ln) + "\n")
sys.stdout.flush()
'''


def test_claude_backend_success(tmp_path, monkeypatch):
    # fake claude shim: cmd 体用 %~dp0 引用同目录脚本（避免路径转义问题）
    (tmp_path / "fake_claude.py").write_text(FAKE_CLAUDE, encoding="utf-8")
    if os.name == "nt":
        shim = tmp_path / "claude.cmd"
        shim.write_text('@echo off\n@python "%~dp0fake_claude.py" %*\n',
                        encoding="utf-8")
    else:
        shim = tmp_path / "claude"
        shim.write_text('#!/bin/sh\ndir=$(dirname "$0")\n'
                        'python3 "$dir/fake_claude.py" "$@"\n',
                        encoding="utf-8")
        os.chmod(shim, 0o755)
    monkeypatch.setenv("PATH", str(tmp_path) + os.pathsep +
                       os.environ["PATH"])
    g = _goal(agent="claude",
              objective="run the fake claude",
              constraints={"maxSteps": 5})
    r = RdaProc(g)
    res = r.wait_result(30)
    assert res and res["data"]["status"] == "success", \
        json.dumps(r.events[-3:], ensure_ascii=False)
    assert "fake-claude-finished-ok" in res["data"]["summary"]
    tools = [e["data"].get("tool") for e in r.events
             if e["kind"] == "progress" and e["data"].get("tool")]
    assert "Bash" in tools
    r.close()


def test_claude_backend_missing_cli_fast_fails(tmp_path, monkeypatch):
    monkeypatch.setattr("shutil.which", lambda name: None)
    g = _goal(agent="claude", objective="x")
    r = RdaProc(g)
    res = r.wait_result(15)
    assert res and res["data"]["status"] == "failed"
    assert "claude CLI not found" in res["data"]["summary"]
    r.close()


def test_goal_agent_validation():
    ok, _ = protocol.validate_goal(_goal(agent="claude"))
    assert ok
    ok, err = protocol.validate_goal(_goal(agent="codex"))
    assert not ok and "goal.agent" in err
    ok, err = protocol.validate_goal(_goal(
        agent="claude", plan={"steps": [{"id": "s", "type": "shell",
                                         "command": "x"}]}))
    assert not ok and "drop plan" in err
