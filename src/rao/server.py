"""rao-mcp: ZCode MCP server exposing the 6 orchestration tools.

Launch: python server.py  (stdio transport; ZCode spawns it per MCP config)
Settings come from devices.json top-level "settings" object:
  {"settings": {"transportMode": "auto", "rdaDir": "/path/on/remote", "runsDir": "runs"}}
"""
import json
import logging
import os
import sys

from mcp.server.fastmcp import FastMCP

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from rao import protocol  # noqa: E402
from rao.devices import DeviceRegistry  # noqa: E402
from rao.orchestrator import Orchestrator  # noqa: E402
from rao.transport import (SshTransport, SshDaemonTransport,  # noqa: E402
                           NodeTransport, MultiTransport)
from rao.master import start_master_thread  # noqa: E402

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(name)s %(levelname)s %(message)s")
log = logging.getLogger("rao.server")

mcp = FastMCP("remote-agent-orchestrator")

registry = DeviceRegistry()


def _build_transport():
    """transportMode: spawn | daemon | auto (default, from devices.json
    settings). auto probes the persistent daemon once; falls back to spawn.
    Registered nodes (transport=node) ride MultiTransport/NodeTransport."""
    settings = registry.settings or {}
    rda_dir = settings.get("rdaDir", "$HOME/.rao/runtime")
    mode = settings.get("transportMode", "auto")
    if mode == "spawn":
        ssh_t = SshTransport(rda_dir=rda_dir)
        return MultiTransport(ssh_t, None) if _nodes_registered() else ssh_t
    daemon_t = SshDaemonTransport(rda_dir=rda_dir)
    if mode == "daemon":
        return _wrap(daemon_t)
    try:
        ids = [i for i in registry.list_ids()
               if (registry.get(i) or {}).get("transport") != "node"]
        if ids and daemon_t.ensure_daemon(registry.require(ids[0])):
            log.info("transport mode: daemon (probe ok)")
            return _wrap(daemon_t)
    except Exception as exc:
        log.warning("daemon probe failed: %s", exc)
    log.info("transport mode: spawn (daemon unavailable)")
    return _wrap(SshTransport(rda_dir=rda_dir))


def _nodes_registered():
    return bool(master_server and master_server.registered_devices())


def _wrap(ssh_t):
    if master_server is not None and master_server.registered_devices():
        return MultiTransport(ssh_t, NodeTransport(master_server))
    return ssh_t


master_server = start_master_thread(
    port=(registry.settings or {}).get("masterPort", 9700),
    registry=registry)
orch = Orchestrator(registry, transport=_build_transport(),
                    runs_dir=(registry.settings or {}).get("runsDir"))


@mcp.tool()
def devices_list() -> str:
    """List configured remote devices with online status (TCP probe)."""
    return json.dumps({"devices": registry.describe_all(probe=True)},
                      ensure_ascii=False, indent=1)


@mcp.tool()
def remote_dispatch(device_ids: list, goal: dict) -> str:
    """Dispatch one goal to N devices in parallel.

    device_ids: list of configured deviceId strings.
    goal: {objective, workspace?, constraints?, plan?} — goalId optional
    (generated per device); protocol/protocol fields injected by server.
    Returns immediately with per-device goalIds (state may still be queued).
    """
    g = dict(goal)
    g.setdefault("protocol", protocol.PROTOCOL_VERSION)
    if not g.get("deviceId"):
        g.pop("deviceId", None)  # per-clone value
    try:
        dispatches = orch.dispatch(list(device_ids), g)
    except KeyError as exc:
        return json.dumps({"error": str(exc)}, ensure_ascii=False)
    except ValueError as exc:
        return json.dumps({"error": str(exc)}, ensure_ascii=False)
    return json.dumps({"dispatches": dispatches}, ensure_ascii=False, indent=1)


@mcp.tool()
def remote_status(goal_id: str = None, device_id: str = None) -> str:
    """Status of one goal (goal_id) or all goals of a device (device_id)."""
    if goal_id:
        run = orch.get_run(goal_id)
        if run is None:
            # goal from an earlier process: fall back to persisted record
            rec = orch.run_store.load_goal_record(goal_id)
            if rec is None:
                return json.dumps({"error": "no such goalId: %s" % goal_id},
                                  ensure_ascii=False)
            return json.dumps({
                "goalId": goal_id,
                "deviceId": (rec.get("goal") or {}).get("deviceId"),
                "state": rec.get("state"),
                "result": rec.get("result"),
                "events": rec.get("events", [])[-50:],
                "persisted": True,
            }, ensure_ascii=False, indent=1)
        return json.dumps(run.snapshot(), ensure_ascii=False, indent=1)
    if device_id:
        runs = orch.runs_for_device(device_id)
        return json.dumps({"deviceId": device_id,
                           "goals": [r.snapshot(max_events=20) for r in runs]},
                          ensure_ascii=False, indent=1)
    return json.dumps({"error": "provide goal_id or device_id"},
                      ensure_ascii=False)


@mcp.tool()
def remote_steer(goal_id: str, message: str = None, ask_id: str = None,
                 answer: dict = None) -> str:
    """Steer a running goal: answer an ask (ask_id+answer) or send guidance
    (message; directives: 'skip:<stepId>' / 'run:<command>')."""
    run = orch.get_run(goal_id)
    if run is None:
        return json.dumps({"error": "no such goalId: %s" % goal_id},
                          ensure_ascii=False)
    ok = run.steer(message=message, ask_id=ask_id, answer=answer)
    return json.dumps({"accepted": bool(ok), "goalId": goal_id,
                       "state": run.state}, ensure_ascii=False)


@mcp.tool()
def remote_cancel(goal_id: str, reason: str = "user requested") -> str:
    """Cancel a running/queued goal."""
    run = orch.get_run(goal_id)
    if run is None:
        return json.dumps({"error": "no such goalId: %s" % goal_id},
                          ensure_ascii=False)
    ok = run.cancel(reason)
    return json.dumps({"cancelled": bool(ok), "goalId": goal_id,
                       "state": run.state}, ensure_ascii=False)


@mcp.tool()
def remote_collect(goal_ids: list, timeout_ms: int = 120000) -> str:
    """Wait for goals to finish (each within timeout_ms total budget) and
    return the aggregated cross-device report (also persisted under runs/)."""
    report = orch.collect(list(goal_ids), timeout_ms=timeout_ms)
    return json.dumps(report, ensure_ascii=False, indent=1)


@mcp.tool()
def remote_dispatch_dag(nodes: list, edges: list, policy: str = "success") -> str:
    """Dispatch a goal DAG across devices. nodes: [{key, deviceId, goal}];
    edges: [[fromKey, toKey]] — toKey starts only after fromKey satisfies the
    policy ("success" default; "lenient" also accepts partial). Failed deps
    transitively skip downstream nodes. Returns dagId + node plan."""
    try:
        dag = orch.dispatch_dag(nodes, edges, policy=policy)
    except (ValueError, KeyError) as exc:
        return json.dumps({"error": str(exc)}, ensure_ascii=False)
    return json.dumps({"dagId": dag.dag_id, "snapshot": dag.snapshot()},
                      ensure_ascii=False, indent=1)


@mcp.tool()
def remote_dag_status(dag_id: str) -> str:
    """Status snapshot of a dispatched DAG (node states + goalIds)."""
    dag = orch.get_dag(dag_id)
    if dag is None:
        return json.dumps({"error": "no such dagId: %s" % dag_id},
                          ensure_ascii=False)
    return json.dumps(dag.snapshot(), ensure_ascii=False, indent=1)


@mcp.tool()
def remote_reports(action: str = "list", file: str = None,
                   file_b: str = None) -> str:
    """Report history: action="list" (all saved reports), "get" (one report
    by filename), or "diff" (compare file vs file_b by goalId; includes
    regressions list)."""
    try:
        if action == "list":
            return json.dumps({"reports": orch.run_store.list_reports()},
                              ensure_ascii=False, indent=1)
        if action == "get":
            return json.dumps(orch.run_store.load_report(file),
                              ensure_ascii=False, indent=1)
        if action == "diff":
            return json.dumps(
                orch.run_store.diff_reports(orch.run_store.load_report(file),
                                            orch.run_store.load_report(file_b)),
                ensure_ascii=False, indent=1)
        return json.dumps({"error": "action must be list|get|diff"},
                          ensure_ascii=False)
    except (KeyError, ValueError) as exc:
        return json.dumps({"error": str(exc)}, ensure_ascii=False)


if __name__ == "__main__":
    mcp.run()
