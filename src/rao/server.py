"""rao-mcp: ZCode MCP server exposing the 6 orchestration tools.

Launch: python server.py  (stdio transport; ZCode spawns it per MCP config)
Env:    RAO_CONFIG (devices.json path), RAO_RUNS_DIR, RAO_RDA_DIR
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

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(name)s %(levelname)s %(message)s")
log = logging.getLogger("rao.server")

mcp = FastMCP("remote-agent-orchestrator")

registry = DeviceRegistry()
orch = Orchestrator(registry)


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
            return json.dumps({"error": "no such goalId: %s" % goal_id},
                              ensure_ascii=False)
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


if __name__ == "__main__":
    mcp.run()
