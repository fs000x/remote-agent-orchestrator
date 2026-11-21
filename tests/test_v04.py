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

def test_keyring_used_when_configured(monkeypatch):
    reg = _mkdevice()
    FakeKeyring.store[("rao", "d1")] = "from-keyring"
    fake = types.ModuleType("keyring")
    fake.get_password = FakeKeyring.get_password
    monkeypatch.setitem(sys.modules, "keyring", fake)
    assert password_of(reg.require("d1")) == "from-keyring"

def test_config_used_when_keyring_absent(monkeypatch):
    reg = _mkdevice()
    monkeypatch.delitem(sys.modules, "keyring", raising=False)
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

def test_invalid_goal_planner_rejected():
    ok, err = protocol.validate_goal(_goal(planner="gpt-9000"))
    assert not ok and "goal.planner" in err
