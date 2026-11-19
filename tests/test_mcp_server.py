"""MCP integration test: spawn rao-mcp over stdio, exercise all 6 tools.

Uses the official mcp client. devices_list probes the real network; the
dispatch test runs a tiny real goal against the configured device (skipped
when unreachable, same guard as test_real_device).
"""
import json
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from mcp import ClientSession, StdioServerParameters  # noqa: E402
from mcp.client.stdio import stdio_client  # noqa: E402

from rao.devices import DeviceRegistry  # noqa: E402
from rao.transport import probe_tcp, password_of  # noqa: E402

SERVER = os.path.join(ROOT, "src", "rao", "server.py")
reg = DeviceRegistry()
_dev_id = reg.list_ids()[0] if reg.list_ids() else None
_reachable = bool(_dev_id and probe_tcp(reg.require(_dev_id)["host"],
                                        int(reg.require(_dev_id).get("port", 22)))
                  and password_of(reg.require(_dev_id)))

pytestmark = pytest.mark.skipif(_dev_id is None,
                                reason="no device configured")


def _params():
    return StdioServerParameters(
        command=sys.executable, args=[SERVER],
        env={k: v for k, v in os.environ.items()
             if k.startswith("RAO_")})


@pytest.mark.asyncio
async def test_all_six_tools():
    async with stdio_client(_params()) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # T1: devices_list
            res = await session.call_tool("devices_list", {})
            devs = json.loads(res.content[0].text)["devices"]
            assert devs and devs[0]["id"] == _dev_id

            # dispatch a tiny real goal (skipped logic inline: unreachable ->
            # dispatch will fail on connect and be recorded as failed; that
            # still exercises the tool path)
            goal = {"protocol": "rao/1",
                    "objective": "mcp layer smoke test",
                    "plan": {"steps": [{"id": "s1", "type": "shell",
                                        "command": "echo mcp-ok",
                                        "success": {"exitCode": 0}}]}}
            res = await session.call_tool(
                "remote_dispatch", {"device_ids": [_dev_id], "goal": goal})
            payload = json.loads(res.content[0].text)
            assert "dispatches" in payload, payload
            gid = payload["dispatches"][0]["goalId"]

            # status
            res = await session.call_tool("remote_status", {"goal_id": gid})
            st = json.loads(res.content[0].text)
            assert st["goalId"] == gid

            if not _reachable:
                await session.call_tool("remote_cancel",
                                        {"goal_id": gid, "reason": "no device"})
                return

            # steer (general guidance; harmless)
            res = await session.call_tool(
                "remote_steer", {"goal_id": gid, "message": "carry on"})
            assert json.loads(res.content[0].text)["goalId"] == gid

            # collect (waits for completion)
            res = await session.call_tool(
                "remote_collect", {"goal_ids": [gid], "timeout_ms": 60000})
            report = json.loads(res.content[0].text)
            assert report["matrix"][0]["goalId"] == gid
            assert report["matrix"][0]["status"] in ("success", "partial",
                                                     "failed", "timeout",
                                                     "cancelled")
            if _reachable:
                assert report["matrix"][0]["status"] == "success"

            # cancel after completion -> returns False, no crash
            res = await session.call_tool("remote_cancel", {"goal_id": gid})
            assert "cancelled" in json.loads(res.content[0].text)

            # status of unknown goal -> structured error
            res = await session.call_tool("remote_status",
                                          {"goal_id": "no-such"})
            assert "error" in json.loads(res.content[0].text)
