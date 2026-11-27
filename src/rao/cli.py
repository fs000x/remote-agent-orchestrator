"""rao CLI: setup / devices / doctor / uninstall / mcp (v0.5)."""
import argparse
import json
import os
import sys

from rao import agents
from rao.devices import DeviceRegistry


# ------------------------------------------------------------------ devices
def cmd_devices(args):
    reg = DeviceRegistry()
    if args.device_cmd == "list":
        for d in reg.describe_all(probe=True):
            print("%-16s %-15s:%-5s user=%-10s ws=%s online=%s creds=%s" % (
                d["id"], d["host"], d["port"], d["user"],
                d.get("workspace") or "-", d["online"], d["authConfigured"]))
        return 0
    if args.device_cmd == "add":
        fields = _ask_device_fields(args)
        path = reg.path
        data = {"devices": []}
        if os.path.isfile(path):
            with open(path, encoding="utf-8") as fh:
                data = json.load(fh)
        data["devices"] = [d for d in data.get("devices", [])
                           if d.get("id") != fields["id"]]
        dev = {k: fields[k] for k in ("id", "host", "port", "user",
                                      "workspace", "maxConcurrentGoals")}
        dev["auth"] = {"type": "password", "password": fields["password"]}
        data["devices"].append(dev)
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, indent=2)
        print("device %s written to %s" % (fields["id"], path))
        return 0
    if args.device_cmd == "remove":
        path = reg.path
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        before = len(data.get("devices", []))
        data["devices"] = [d for d in data.get("devices", [])
                           if d.get("id") != args.id]
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, indent=2)
        print("removed %d device(s)" % (before - len(data["devices"])))
        return 0
    print("unknown devices subcommand")
    return 2


def _ask_device_fields(args):
    def ask(prompt, default=None):
        if args.yes and default is not None:
            return default
        v = input("%s%s: " % (prompt, " [%s]" % default if default else ""))
        return v.strip() or default
    fid = args.id or ask("设备 id (如 my-linux-box)")
    fhost = args.host or ask("主机地址")
    fport = int(args.port or ask("端口", "22"))
    fuser = args.user or ask("SSH 用户")
    fpass = args.password or __import__("getpass").getpass("密码: ")
    fws = args.workspace or ask("workspace 路径(远端绝对路径,可空)")
    return {"id": fid, "host": fhost, "port": fport, "user": fuser,
            "password": fpass, "workspace": fws or None,
            "maxConcurrentGoals": 2}


# ------------------------------------------------------------------ setup
def cmd_setup(args):
    wanted = [a.strip() for a in (args.agents or "").split(",") if a.strip()]
    detected = agents.detect_agents()
    print("=== Agent 检测 ===")
    for d in detected:
        state = "已注入" if d["injected"] else "未注入"
        print("  %-8s installed=%s cli=%s [%s]" % (
            d["name"], d["installed"], d["cli_path"] or "-", state))
    targets = wanted or [d["name"] for d in detected if d["installed"]]
    if not args.yes:
        ans = input("将注入 MCP+Skill 到: %s，继续? [Y/n] " % targets)
        if ans.strip().lower() == "n":
            print("aborted")
            return 1
    rc = 0
    for name in targets:
        try:
            r = agents.inject_agent(name)
            print("=== 注入 %s ===" % name)
            for a in r["actions"]:
                print("  -", a)
        except Exception as exc:
            print("=== 注入 %s 失败: %s ===" % (name, exc))
            rc = 1
    return rc


def cmd_uninstall(args):
    names = [a.strip() for a in (args.agents or "").split(",") if a.strip()] \
        or list(agents.SUPPORTED_AGENTS)
    for name in names:
        r = agents.uninstall_agent(name)
        print("=== 回滚 %s ===" % name)
        for a in r["actions"]:
            print("  -", a)
    return 0


# ------------------------------------------------------------------ doctor
def cmd_doctor(args):
    print("=== 本地 Agent ===")
    for d in agents.detect_agents():
        print("  %-8s installed=%s injected=%s" % (d["name"], d["installed"],
                                                   d["injected"]))
    print("=== 设备 ===")
    reg = DeviceRegistry()
    for d in reg.describe_all(probe=True):
        print("  %-16s online=%s auth=%s" % (d["id"], d["online"],
                                             d["authConfigured"]))
        if not d["online"]:
            continue
        try:
            from rao.transport import SshTransport
            t = SshTransport(rda_dir=_rda_dir(reg))
            # device-level rdaDir override applies inside transport calls
            from rao.provision import remote_hashes
            rh = remote_hashes(t, reg.require(d["id"]), _rda_dir(reg))
            ok = all(v for v in rh.values())
            print("    RDA 运行时: %s" % ("就绪" if ok else "缺失/过期（派发时自动补部署）"))
            rc, out = t._exec(reg.require(d["id"]),
                              "which claude 2>/dev/null; "
                              "test -f ~/.zcode/server/run/status.json && echo zcode-server")
            marks = [l.strip() for l in out.splitlines() if l.strip()]
            print("    远端 Agent: %s" % (marks or ["claude=✗", "zcode-server=✗"]))
        except Exception as exc:
            print("    远端检查失败: %s" % exc)
    return 0


def _rda_dir(reg):
    return (reg.settings or {}).get("rdaDir", "$HOME/.rao/runtime")


# ------------------------------------------------------------------ run
def cmd_run(args):
    """One-shot remote execution without MCP (works from any Agent's Bash)."""
    from rao.orchestrator import Orchestrator
    from rao.transport import SshTransport
    reg = DeviceRegistry()
    dev = reg.require(args.device)
    orch = Orchestrator(registry=reg,
                        transport=SshTransport(
                            rda_dir=dev.get("rdaDir") or _rda_dir(reg)),
                        runs_dir=_runs_dir())
    goal = {"objective": "rao run: %s" % args.command,
            "workspace": dev.get("workspace"),
            "constraints": {"timeoutMs": args.timeout_ms,
                            "stepTimeoutMs": args.timeout_ms},
            "plan": {"steps": [
                {"id": "s1", "type": "shell", "command": args.command,
                 "success": {"exitCode": 0}}]}}
    ds = orch.dispatch([dev["id"]], goal)
    run = orch.get_run(ds[0]["goalId"])
    if not run.wait_terminal(args.timeout_ms / 1000.0 + 30):
        print("[rao] goal did not finish in time; state=%s" % run.state)
        return 1
    res = run.result or {}
    print("[rao] status: %s" % res.get("status"))
    if res.get("summary"):
        print("[rao] summary: %s" % res["summary"])
    for ev in (run.chan.events if run.chan else []):
        if ev["kind"] == "step_result" and ev["data"].get("stdoutTail"):
            print("[out] %s" % ev["data"]["stdoutTail"].strip())
    return 0 if res.get("status") == "success" else 1


def _runs_dir():
    reg = DeviceRegistry()
    return (reg.settings or {}).get("runsDir") or os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(
            os.path.abspath(__file__)))), "runs")


# ------------------------------------------------------------------ mcp
def cmd_mcp(args):
    from rao.server import mcp as server
    server.run()
    return 0


def main(argv=None):
    ap = argparse.ArgumentParser(prog="rao",
                                 description="Remote Agent Orchestrator")
    from . import __version__
    ap.add_argument("--version", action="version",
                    version="rao %s" % __version__)
    sub = ap.add_subparsers(dest="cmd")

    p_setup = sub.add_parser("setup", help="检测 Agent 并注入 MCP+Skill")
    p_setup.add_argument("--agents", help="逗号分隔: zcode,claude（默认自动）")
    p_setup.add_argument("--yes", action="store_true", help="免确认")

    p_dev = sub.add_parser("devices", help="远程设备配置管理")
    p_dev.add_argument("device_cmd", choices=["list", "add", "remove"])
    p_dev.add_argument("--id"); p_dev.add_argument("--host")
    p_dev.add_argument("--port"); p_dev.add_argument("--user")
    p_dev.add_argument("--password"); p_dev.add_argument("--workspace")
    p_dev.add_argument("--yes", action="store_true")

    sub.add_parser("doctor", help="诊断：Agent/设备/远端能力")
    p_un = sub.add_parser("uninstall", help="回滚注入")
    p_un.add_argument("--agents", help="逗号分隔（默认全部）")
    p_run = sub.add_parser("run", help="一条命令在远程设备上执行并等待结果")
    p_run.add_argument("--device", required=True)
    p_run.add_argument("--timeout-ms", type=int, default=300000)
    p_run.add_argument("command", help="要在远端执行的 shell 命令")
    sub.add_parser("mcp", help="启动 MCP server（stdio）")

    args = ap.parse_args(argv)
    if args.cmd == "setup":
        return cmd_setup(args)
    if args.cmd == "devices":
        return cmd_devices(args)
    if args.cmd == "doctor":
        return cmd_doctor(args)
    if args.cmd == "uninstall":
        return cmd_uninstall(args)
    if args.cmd == "run":
        return cmd_run(args)
    if args.cmd == "mcp":
        return cmd_mcp(args)
    ap.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
