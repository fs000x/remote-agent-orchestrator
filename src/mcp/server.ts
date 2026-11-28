/**
 * rao-mcp: MCP server exposing the 9 orchestration tools.
 *
 * Launch: `rao mcp` (stdio transport; the host agent spawns it per MCP
 * config). Settings come from devices.json top-level "settings":
 *   {"settings": {"transportMode": "auto|daemon|spawn",
 *                 "rdaDir": "/path/on/remote", "runsDir": "runs",
 *                 "masterPort": 9700}}
 *
 * P1 borrow (ds-harness-remote): every MUTATING tool requires an
 * `operationId`. Same key + same params -> same receipt; same key + changed
 * params -> fingerprint-mismatch rejection; receipts are lost when this
 * process resets (`unknown` receipt is NOT proof of absence — verify via
 * remote_status/remote_collect instead of retrying with a fresh key).
 */
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import { DeviceRegistry } from "../devices.js";
import { Orchestrator, RunStore } from "../orchestrator.js";
import { ErrorCode, PROTOCOL_VERSION, RaoError } from "../protocol.js";
import { SshTransport } from "../transports/ssh.js";
import { SshDaemonTransport } from "../transports/sshDaemon.js";
import { MultiTransport, NodeTransport, startMaster, MasterServer } from "../master.js";
import { globalLedger } from "../ledger.js";
import { RAO_VERSION } from "../version.js";
import type { Transport } from "../orchestrator.js";

function jsonOk(payload: unknown): { content: Array<{ type: "text"; text: string }> } {
  return { content: [{ type: "text", text: JSON.stringify(payload, null, 1) }] };
}

function jsonErr(err: unknown): { content: Array<{ type: "text"; text: string }> } {
  const payload =
    err instanceof RaoError
      ? { error: err.message, code: err.code, retryable: err.retryable }
      : { error: String((err as Error)?.message ?? err) };
  return { content: [{ type: "text", text: JSON.stringify(payload, null, 1) }] };
}

export interface RaoServerOptions {
  configPath?: string;
  /** test seam: skip the master listener */
  noMaster?: boolean;
  /** test seam: inject a transport (skips SSH probing entirely) */
  transport?: Transport;
}

export async function buildRaoServer(opts: RaoServerOptions = {}): Promise<{
  server: McpServer;
  registry: DeviceRegistry;
  orch: Orchestrator;
  master: MasterServer | null;
  transport: Transport;
}> {
  const registry = new DeviceRegistry(opts.configPath);
  await registry.reload();

  /* -- master listener (node mode; nodes dial INTO it) -------------------- */
  let master: MasterServer | null = null;
  if (!opts.noMaster && process.env.RAO_MASTER !== "0") {
    try {
      master = await startMaster(
        registry.settings.masterPort ?? 9700,
        registry,
      );
    } catch (err) {
      // port busy → node mode unavailable; SSH transports keep working
      console.error(`[rao-mcp] master listener unavailable: ${String(err)}`);
      master = null;
    }
  }

  /* -- transport selection (spawn | daemon | auto) ------------------------- */
  const buildTransport = async (): Promise<Transport> => {
    const settings = registry.settings ?? {};
    const rdaDir = settings.rdaDir ?? "$HOME/.rao/runtime";
    const mode = settings.transportMode ?? "auto";
    const nodeT = master ? new NodeTransport(master) : null;
    const wrap = (sshT: Transport): Transport =>
      master ? new MultiTransport(sshT, nodeT) : sshT;
    if (mode === "spawn") {
      return wrap(new SshTransport(rdaDir));
    }
    const daemonT = new SshDaemonTransport(rdaDir);
    if (mode === "daemon") return wrap(daemonT);
    // auto: probe the persistent daemon once; fall back to spawn
    try {
      const ids = registry.listIds().filter((id) => {
        const d = registry.get(id);
        return d && d.transport !== "node" && d.host;
      });
      if (ids.length && ids[0] &&
          (await daemonT.ensureDaemon(registry.require(ids[0])))) {
        return wrap(daemonT);
      }
    } catch (err) {
      console.error(`[rao-mcp] daemon probe failed: ${String(err)}`);
    }
    return wrap(new SshTransport(rdaDir));
  };
  const transport = opts.transport ?? (await buildTransport());

  const orch = new Orchestrator({
    registry,
    transport,
    runsDir: registry.settings.runsDir,
  });

  /* -- MCP tools ----------------------------------------------------------- */
  const server = new McpServer({
    name: "remote-agent-orchestrator",
    version: RAO_VERSION,
  });

  server.registerTool(
    "devices_list",
    {
      description: "List configured remote devices with online status (TCP probe) and an honest execution matrix.",
      inputSchema: {},
    },
    async () => {
      try {
        return jsonOk({ devices: await registry.describeAll(true) });
      } catch (err) {
        return jsonErr(err);
      }
    },
  );

  server.registerTool(
    "remote_dispatch",
    {
      description:
        "Dispatch one goal to N devices in parallel. " +
        "goal: {objective, workspace?, agent? (builtin|claude), constraints?, plan?, ensureWorkspace?}. " +
        "plan is REQUIRED for agent=builtin (recon→ask flow otherwise); agent='claude' must NOT carry a plan. " +
        "Returns immediately with per-device goalIds (state may still be queued). " +
        "IDEMPOTENCY: reuse the SAME operationId when retrying this exact dispatch; " +
        "a different operationId re-executes; the same key with changed parameters is rejected.",
      inputSchema: {
        operationId: z.string().min(1).describe(
          "idempotency key; generate a fresh UUID per logical dispatch, reuse on retry"),
        deviceIds: z.array(z.string().min(1)).min(1).max(16),
        goal: z.object({
          objective: z.string().min(1),
          workspace: z.string().optional(),
          agent: z.enum(["builtin", "claude"]).default("builtin"),
          ensureWorkspace: z
            .object({
              repoUrl: z.string().min(1),
              path: z.string(),
              branch: z.string().optional(),
            })
            .optional(),
          constraints: z
            .object({
              maxSteps: z.number().int().positive().optional(),
              timeoutMs: z.number().int().positive().optional(),
              stepTimeoutMs: z.number().int().positive().optional(),
              eventSilenceMs: z.number().int().positive().optional(),
              forbidden: z.array(z.string()).optional(),
              claudeAutoApprove: z.boolean().optional(),
            })
            .optional(),
          plan: z
            .object({
              planner: z.literal("main-agent").default("main-agent"),
              steps: z.array(z.record(z.unknown())).min(1),
            })
            .optional(),
          reporting: z.object({ finalArtifacts: z.boolean().optional() }).optional(),
        }),
      },
    },
    async ({ operationId, deviceIds, goal }) => {
      try {
        const result = await globalLedger.run(
          operationId,
          { deviceIds, goal },
          async () => {
            const dispatches = orch.dispatch(deviceIds, {
              protocol: PROTOCOL_VERSION,
              ...goal,
            });
            return {
              status: "accepted" as const,
              dispatches,
              note: "accepted = queued, NOT completed",
            };
          },
        );
        return jsonOk(result);
      } catch (err) {
        return jsonErr(err);
      }
    },
  );

  server.registerTool(
    "remote_status",
    {
      description:
        "Status of one goal (goalId) or all goals of a device (deviceId). " +
        "Goals from an earlier server process fall back to their persisted record.",
      inputSchema: {
        goalId: z.string().optional(),
        deviceId: z.string().optional(),
      },
    },
    async ({ goalId, deviceId }) => {
      try {
        if (goalId) {
          const run = orch.getRun(goalId);
          if (run) return jsonOk(run.snapshot());
          const rec = await orch.runStore.load(goalId);
          if (!rec) {
            return jsonOk({ error: `no such goalId: ${goalId}` });
          }
          return jsonOk({
            goalId,
            deviceId: (rec.goal as Record<string, unknown> | undefined)?.deviceId,
            state: rec.state,
            result: rec.result,
            events: (Array.isArray(rec.events) ? rec.events : []).slice(-50),
            persisted: true,
          });
        }
        if (deviceId) {
          return jsonOk({
            deviceId,
            goals: orch.runsForDevice(deviceId).map((r) => r.snapshot(20)),
          });
        }
        return jsonOk({ error: "provide goalId or deviceId" });
      } catch (err) {
        return jsonErr(err);
      }
    },
  );

  server.registerTool(
    "remote_steer",
    {
      description:
        "Steer a running goal: answer an ask (askId+answer) or send guidance " +
        "(message; directives: 'skip:<stepId>' / 'run:<command>').",
      inputSchema: {
        operationId: z.string().min(1).describe("idempotency key for this steer"),
        goalId: z.string().min(1),
        message: z.string().optional(),
        askId: z.string().optional(),
        answer: z.union([z.record(z.unknown()), z.string(), z.number(), z.boolean()]).optional(),
      },
    },
    async ({ operationId, goalId, message, askId, answer }) => {
      try {
        const result = await globalLedger.run(
          operationId,
          { goalId, message, askId, answer },
          async () => {
            const run = orch.getRun(goalId);
            if (!run) {
              throw new RaoError(ErrorCode.GoalNotFound, `no such goalId: ${goalId}`);
            }
            const ok = await run.steer(message, askId, answer);
            return { status: "accepted" as const, accepted: ok, state: run.state };
          },
        );
        return jsonOk({ goalId, ...result });
      } catch (err) {
        return jsonErr(err);
      }
    },
  );

  server.registerTool(
    "remote_cancel",
    {
      description: "Cancel a running/queued goal. Terminal states are sticky — cancel after completion is a no-op.",
      inputSchema: {
        operationId: z.string().min(1).describe("idempotency key for this cancel"),
        goalId: z.string().min(1),
        reason: z.string().optional(),
      },
    },
    async ({ operationId, goalId, reason }) => {
      try {
        const result = await globalLedger.run(
          operationId,
          { goalId, reason },
          async () => {
            const run = orch.getRun(goalId);
            if (!run) {
              throw new RaoError(ErrorCode.GoalNotFound, `no such goalId: ${goalId}`);
            }
            const ok = await run.cancel(reason ?? "user requested");
            return { status: "accepted" as const, cancelled: ok, state: run.state };
          },
        );
        return jsonOk({ goalId, ...result });
      } catch (err) {
        return jsonErr(err);
      }
    },
  );

  server.registerTool(
    "remote_collect",
    {
      description:
        "Wait for goals to finish (each within timeoutMs total budget) and return the aggregated cross-device report (persisted under runs/).",
      inputSchema: {
        goalIds: z.array(z.string().min(1)).min(1),
        timeoutMs: z.number().int().positive().optional(),
      },
    },
    async ({ goalIds, timeoutMs }) => {
      try {
        return jsonOk(await orch.collect(goalIds, timeoutMs ?? 120_000));
      } catch (err) {
        return jsonErr(err);
      }
    },
  );

  server.registerTool(
    "remote_dispatch_dag",
    {
      description:
        "Dispatch a goal DAG across devices. nodes: [{key, deviceId, goal}]; " +
        "edges: [[fromKey, toKey]] — toKey starts only after fromKey satisfies " +
        "the policy ('success' default; 'lenient' also accepts partial). " +
        "Failed deps transitively skip downstream nodes. " +
        "IDEMPOTENCY: same operationId + same graph → same dagId.",
      inputSchema: {
        operationId: z.string().min(1).describe("idempotency key"),
        nodes: z
          .array(
            z.object({
              key: z.string().min(1),
              deviceId: z.string().min(1),
              goal: z.record(z.unknown()),
            }),
          )
          .min(1),
        edges: z.array(z.tuple([z.string(), z.string()])),
        policy: z.enum(["success", "lenient"]).optional(),
      },
    },
    async ({ operationId, nodes, edges, policy }) => {
      try {
        const result = await globalLedger.run(
          operationId,
          { nodes, edges, policy },
          async () => {
            const dag = orch.dispatchDag(nodes, edges, policy ?? "success");
            return {
              status: "accepted" as const,
              dagId: dag.dagId,
              snapshot: dag.snapshot(),
            };
          },
        );
        return jsonOk(result);
      } catch (err) {
        return jsonErr(err);
      }
    },
  );

  server.registerTool(
    "remote_dag_status",
    {
      description: "Status snapshot of a dispatched DAG (node states + goalIds).",
      inputSchema: { dagId: z.string().min(1) },
    },
    async ({ dagId }) => {
      try {
        const dag = orch.getDag(dagId);
        if (!dag) {
          return jsonOk({ error: `no such dagId: ${dagId}` });
        }
        return jsonOk(dag.snapshot());
      } catch (err) {
        return jsonErr(err);
      }
    },
  );

  server.registerTool(
    "remote_reports",
    {
      description:
        'Report history: action="list" (all saved reports), "get" (one report ' +
        'by filename), or "diff" (compare file vs fileB by goalId; includes regressions list).',
      inputSchema: {
        action: z.enum(["list", "get", "diff"]).optional(),
        file: z.string().optional(),
        fileB: z.string().optional(),
      },
    },
    async ({ action, file, fileB }) => {
      try {
        const act = action ?? "list";
        if (act === "list") {
          return jsonOk({ reports: await orch.runStore.listReports() });
        }
        if (act === "get") {
          if (!file) return jsonOk({ error: "file required for action=get" });
          return jsonOk(await orch.runStore.loadReport(file));
        }
        if (act === "diff") {
          if (!file || !fileB) {
            return jsonOk({ error: "file and fileB required for action=diff" });
          }
          const [a, b] = await Promise.all([
            orch.runStore.loadReport(file),
            orch.runStore.loadReport(fileB),
          ]);
          return jsonOk(RunStore.diffReports(a, b));
        }
        return jsonOk({ error: "action must be list|get|diff" });
      } catch (err) {
        return jsonErr(err);
      }
    },
  );

  return { server, registry, orch, master, transport };
}

export async function startMcp(opts: RaoServerOptions = {}): Promise<void> {
  const { server } = await buildRaoServer(opts);
  await server.connect(new StdioServerTransport());
  // stdio transport: keep the process alive; logs go to stderr only
}
