#!/usr/bin/env python3
"""Deploy rda.py + protocol.py to a device via SFTP (uses config/devices.json).

Usage: python scripts/deploy_rda.py [device_id]
Env:   RAO_CONFIG / RAO_DEVICE_<ID>_PASSWORD override config.
"""
import os
import sys

import paramiko

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from rao.devices import DeviceRegistry  # noqa: E402
from rao.transport import password_of  # noqa: E402

RDA_FILES = ["rda.py", "protocol.py", "rda_daemon.py"]
RUNTIME_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                           "src", "rao", "runtime")


def main():
    reg = DeviceRegistry()
    device_id = sys.argv[1] if len(sys.argv) > 1 else reg.list_ids()[0]
    device = reg.require(device_id)
    rda_dir = os.environ.get("RAO_RDA_DIR", "/home/diablo/git500/zcode_prj/rao")
    pw = password_of(device)
    if not pw:
        print("ERROR: no password for %s (config or RAO_DEVICE_%s_PASSWORD)"
              % (device_id, device_id.upper().replace("-", "_")))
        return 2

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(device["host"], port=int(device.get("port", 22)),
                   username=device["user"], password=pw,
                   timeout=10, allow_agent=False, look_for_keys=False)
    try:
        _, out, err = client.exec_command("mkdir -p %s" % rda_dir)
        out.channel.recv_exit_status()
        sftp = client.open_sftp()
        local_dir = RUNTIME_DIR
        for name in RDA_FILES:
            local = os.path.join(local_dir, name)
            remote = "%s/%s" % (rda_dir, name)
            sftp.put(local, remote)
            print("uploaded %s -> %s:%s" % (name, device["host"], remote))
        sftp.close()
        _, out, err = client.exec_command(
            "cd %s && python3 -c 'import protocol, ast; "
            "ast.parse(open(\"rda.py\").read()); "
            "print(\"remote check OK\", sys.version.split()[0]) if False else None' "
            "&& python3 rda.py 2>&1 | head -1" % rda_dir)
        rc = out.channel.recv_exit_status()
        print("remote sanity:", out.read().decode().strip(),
              "| rc:", rc, "|", err.read().decode().strip()[:200])
    finally:
        client.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
