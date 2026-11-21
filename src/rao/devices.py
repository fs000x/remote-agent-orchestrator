"""Device registry: load config, resolve credentials, probe reachability."""
import json
import os
import threading

from .transport import probe_tcp

DEFAULT_CONFIG_PATHS = [
    os.environ.get("RAO_CONFIG"),
    os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__)))), "config", "devices.json"),
]


class DeviceRegistry:
    def __init__(self, config_path=None):
        self.path = config_path or self._find_config()
        self._lock = threading.Lock()
        self.devices = {}
        self.reload()

    def _find_config(self):
        for p in DEFAULT_CONFIG_PATHS:
            if p and os.path.isfile(p):
                return p
        return DEFAULT_CONFIG_PATHS[1]

    def reload(self):
        with self._lock:
            self.devices = {}
            if self.path and os.path.isfile(self.path):
                with open(self.path, "r", encoding="utf-8") as fh:
                    data = json.load(fh)
                for d in data.get("devices", []):
                    if isinstance(d, dict) and d.get("id"):
                        d.setdefault("source", "local")
                        self.devices[d["id"]] = d
            self._merge_zcode_devices()
        return dict(self.devices)

    def _merge_zcode_devices(self):
        """v0.6: import devices configured in ZCode desktop (read-only).
        Local devices.json entries win on id collision.
        Opt-out: RAO_MERGE_ZCODE_DEVICES=0."""
        if os.environ.get("RAO_MERGE_ZCODE_DEVICES", "1") == "0":
            return
        try:
            from .zcode_store import list_zcode_ssh_devices
            for d in list_zcode_ssh_devices():
                if d.get("id") and d["id"] not in self.devices:
                    d.setdefault("maxConcurrentGoals", 2)
                    self.devices[d["id"]] = d
        except Exception:
            pass  # ZCode not installed / malformed store: local-only mode

    def list_ids(self):
        with self._lock:
            return sorted(self.devices.keys())

    def get(self, device_id):
        with self._lock:
            return self.devices.get(device_id)

    def require(self, device_id):
        d = self.get(device_id)
        if d is None:
            raise KeyError("unknown deviceId: %s (config: %s)" % (device_id, self.path))
        return d

    def describe_all(self, probe=True):
        out = []
        for did in self.list_ids():
            d = self.get(did)
            entry = {
                "id": did,
                "host": d.get("host"),
                "port": d.get("port", 22),
                "user": d.get("user"),
                "workspace": d.get("workspace"),
                "maxConcurrentGoals": d.get("maxConcurrentGoals", 2),
                "source": d.get("source", "local"),
                "online": None,
                "execution": {
                    # v0.6 capability matrix (honest naming, DESIGN §产品扩展)
                    "executor": True,          # builtin 引擎：按步骤程序执行命令（非 Agent）
                    "is_agent": False,         # executor 模式不是自主 Agent
                    "agent_claude": "unknown", # 需远端探测（rao doctor / recon）
                },
            }
            if probe:
                entry["online"] = probe_tcp(d.get("host"), int(d.get("port", 22)))
            pw = self.password_of(d)
            entry["authConfigured"] = bool(pw)
            out.append(entry)
        return out

    @staticmethod
    def password_of(device):
        # delegates to transport.password_of (env > keyring > config chain)
        from .transport import password_of as _pw
        return _pw(device)
