"""v0.4 tests: keyring credential chain + remote-llm planner slot."""
import json
import os
import sys
import types

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))), "src"))

from rao import protocol  # noqa: E402
from rao.devices import DeviceRegistry  # noqa: E402
from rao.transport import password_of  # noqa: E402

from test_rda_local import RdaProc, _goal  # noqa: E402


# --- keyring chain ----------------------------------------------------------
class FakeKeyring:
    store = {}

    @staticmethod
    def get_password(service, username):
        return FakeKeyring.store.get((service, username))


def _mkdevice():
    reg = DeviceRegistry(config_path=os.devnull)
    reg.devices = {"d1": {"id": "d1", "host": "h", "port": 22, "user": "u",
                          "auth": {"type": "password",
                                   "password": "from-config"}}}
    return reg


def test_env_var_wins_over_everything(monkeypatch):
    reg = _mkdevice()
    FakeKeyring.store[("rao", "d1")] = "from-keyring"
    monkeypatch.setitem(sys.modules, "keyring", FakeKeyring)
    monkeypatch.setenv("RAO_DEVICE_D1_PASSWORD", "from-env")
    assert password_of(reg.require("d1")) == "from-env"


def test_keyring_used_when_no_env(monkeypatch):
    reg = _mkdevice()
    FakeKeyring.store[("rao", "d1")] = "from-keyring"
    fake = types.ModuleType("keyring")
    fake.get_password = FakeKeyring.get_password
    monkeypatch.setitem(sys.modules, "keyring", fake)
    monkeypatch.delenv("RAO_DEVICE_D1_PASSWORD", raising=False)
    assert password_of(reg.require("d1")) == "from-keyring"


def test_config_used_when_keyring_absent(monkeypatch):
    reg = _mkdevice()
    monkeypatch.delitem(sys.modules, "keyring", raising=False)
    monkeypatch.delenv("RAO_DEVICE_D1_PASSWORD", raising=False)
    assert password_of(reg.require("d1")) == "from-config"


def test_devices_registry_delegates(monkeypatch):
    reg = _mkdevice()
    FakeKeyring.store[("rao", "d1")] = "from-keyring"
    fake = types.ModuleType("keyring")
    fake.get_password = FakeKeyring.get_password
    monkeypatch.setitem(sys.modules, "keyring", fake)
    monkeypatch.delenv("RAO_DEVICE_D1_PASSWORD", raising=False)
    assert DeviceRegistry.password_of(reg.require("d1")) == "from-keyring"


# --- remote-llm planner slot ------------------------------------------------
PLANNER_OK = r'''import json, sys
print(json.dumps({"steps": [{"id": "p1", "type": "shell",
                             "command": "echo planned-ok",
                             "success": {"exitCode": 0,
                                         "stdoutContains": "planned-ok"}}]}))
'''

PLANNER_FAIL = r'''import sys
sys.stderr.write("no model available")
sys.exit(1)
'''


def _write_planner(tmp_path, body):
    p = tmp_path / "planner.py"
    p.write_text(body, encoding="utf-8")
    return '"%s" "%s"' % (sys.executable, p)


def test_remote_llm_planner_produces_plan(tmp_path, monkeypatch):
    monkeypatch.setenv("RDA_PLANNER_CMD", _write_planner(tmp_path, PLANNER_OK))
    g = _goal(planner="remote-llm")   # no plan → planner should be used
    r = RdaProc(g)
    res = r.wait_result(30)
    assert res and res["data"]["status"] == "success"
    assert any("remote planner" in a for a in res["data"]["assumptions"])
    assert "ask" not in r.kinds()
    r.close()


def test_remote_llm_planner_failure_falls_back_to_ask(tmp_path):
    # no RDA_PLANNER_CMD at all → straight to ask flow
    os.environ.pop("RDA_PLANNER_CMD", None)
    g = _goal(planner="remote-llm")
    r = RdaProc(g)
    deadline = __import__("time").monotonic() + 15
    asks = []
    while __import__("time").monotonic() < deadline:
        asks = [e for e in r.events if e.get("kind") == "ask"]
        if asks:
            break
        __import__("time").sleep(0.05)
    assert asks, "expected fallback ask"
    plan = {"steps": [{"id": "p1", "type": "shell", "command": "echo fallback",
                       "success": {"exitCode": 0}}]}
    r.inject({"type": "steer", "askId": asks[0]["askId"],
              "answer": {"plan": plan}})
    res = r.wait_result(20)
    assert res and res["data"]["status"] == "success"
    assert any("no planner command configured" in a
               for a in res["data"]["assumptions"])
    r.close()


def test_remote_llm_planner_crash_falls_back(tmp_path):
    os.environ["RDA_PLANNER_CMD"] = _write_planner(tmp_path, PLANNER_FAIL)
    try:
        g = _goal(planner="remote-llm")
        r = RdaProc(g)
        import time
        deadline = time.monotonic() + 15
        asks = []
        while time.monotonic() < deadline:
            asks = [e for e in r.events if e.get("kind") == "ask"]
            if asks:
                break
            time.sleep(0.05)
        assert asks, "expected fallback ask after planner crash"
        plan = {"steps": [{"id": "p1", "type": "shell",
                           "command": "echo after-crash",
                           "success": {"exitCode": 0}}]}
        r.inject({"type": "steer", "askId": asks[0]["askId"],
                  "answer": {"plan": plan}})
        res = r.wait_result(20)
        assert res and res["data"]["status"] == "success"
        assert any("planner failed" in a or "rc=1" in a
                   for a in res["data"]["assumptions"])
        r.close()
    finally:
        os.environ.pop("RDA_PLANNER_CMD", None)


def test_invalid_goal_planner_rejected():
    ok, err = protocol.validate_goal(_goal(planner="gpt-9000"))
    assert not ok and "goal.planner" in err
