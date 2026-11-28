/**
 * P3-a tests: cross-device DAG scheduling, report history/diff, and the MCP
 * tool surface (via InMemoryTransport — the exact protocol the host agent
 * speaks) including the operationId idempotency ledger semantics.
 */
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { Orchestrator, RunStore, type GoalChannel, type Transport } from "../src/orchestrator.js";
import { DeviceRegistry } from "../src/devices.js";
import { buildRaoServer } from "../src/mcp/server.js";
import { emptyResult } from "../src/orchestrator.js";

/* ---------------- shared fakes ---------------- */

const TERM_EVENTS: Record<string, string> = {
  success: "success", partial: "partial", failed: "failed",
  timeout: "timeout", cancelled: "cancelled",
};

/** Fake spawn-mode transport: result status derived from the objective text
 *  ("fail:<x>" -> failed, "partial:<x>" -> partial, else success). */
class ScriptedTransport implements Transport {
  opened: Array<{ goalId: string; objective: string }> = [];

  async openSession(goalId: string, _device: unknown, goal: { objective: string }): Promise<GoalChannel> {
    this.opened.push({ goalId, objective: goal.objective });
    const status = goal.objective.startsWith("fail:")
      ? "failed"
      : goal.objective.startsWith("partial:")
        ? "partial"
        : "success";
    const ch = new ScriptedChannel(goalId);
    void ch.pushScript(status);
    return ch;
  }
}

class ScriptedChannel implements GoalChannel {
  goalId: string;
  events: any[] = [];
  event_seqs: Array<number | null> = [];
  stderr_text: string[] = [];
  exit_code: number | null = null;
  killed = false;
  closed = false;
  supports_resume = false;

  constructor(goalId: string) {
    this.goalId = goalId;
  }

  async pushScript(status: string): Promise<void> {
    this.push("started");
    this.push("progress", { phase: "running" });
    this.push("result", {
      status, summary: `scripted ${status}`,
      steps: [], artifacts: [], assumptions: [], steerNotes: [],
      metrics: { stepAttempts: 1, retries: 0, durationMs: 1 },
    });
    this.exit_code = status === "cancelled" ? 4 : status === "timeout" ? 3 : 0;
  }

  push(kind: string, data: Record<string, unknown> = {}): void {
    this.events.push({
      protocol: "rao/1", kind, goalId: this.goalId, ts: Date.now(), data,
    });
    this.event_seqs.push(null);
  }

  async waitEvent(since: number, timeoutMs: number): Promise<[any, number]> {
    if (this.events.length > since) return [this.events[since], since + 1];
    await new Promise((r) => setTimeout(r, Math.min(timeoutMs, 50)));
    return this.events.length > since
      ? [this.events[since], since + 1]
      : [null, since];
  }

  writeLine(): boolean {
    return !this.closed;
  }
  alive(): boolean {
    return !this.closed && this.exit_code === null;
  }
  kill(): void {
    this.killed = true;
    this.closed = true;
  }
  close(): void {
    this.closed = true;
  }
  async waitExit(): Promise<number | null> {
    return this.exit_code;
  }
}

async function mkOrch(dir: string, deviceOver: Record<string, unknown> = {}): Promise<{
  orch: Orchestrator; transport: ScriptedTransport;
}> {
  const cfgPath = join(dir, "devices.json");
  const { writeFile } = await import("node:fs/promises");
  await writeFile(cfgPath, JSON.stringify({
    settings: {},
    devices: [
      { id: "d1", host: "127.0.0.1", port: 22, ...deviceOver },
      { id: "d2", host: "127.0.0.2", port: 22 },
    ],
  }));
  const registry = new DeviceRegistry(cfgPath);
  await registry.reload();
  const transport = new ScriptedTransport();
  const orch = new Orchestrator({
    registry, transport, runsDir: join(dir, "runs"),
    maxConcurrentGoals: 4,
  });
  return { orch, transport };
}

function mkGoal(objective: string): Record<string, unknown> {
  return {
    protocol: "rao/1",
    objective,
    agent: "builtin",
    plan: { planner: "main-agent", steps: [{ id: "s1", type: "shell", command: "echo x" }] },
    constraints: { timeoutMs: 15000 },
  };
}

/* ---------------- DAG scheduling ---------------- */

describe("DAG scheduling", () => {
  let dir: string;

  beforeEach(async () => {
    dir = await mkdtemp(join(tmpdir(), "rao-dag-"));
    process.env.RAO_STATE_DIR = dir;
    delete process.env.RAO_MERGE_NODES;
  });

  afterEach(async () => {
    delete process.env.RAO_STATE_DIR;
    await rm(dir, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
  });

  async function waitDagDone(orch: Orchestrator, dagId: string, timeoutMs = 15000): Promise<void> {
    const dag = orch.getDag(dagId)!;
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const snap = dag.snapshot() as { nodes: Array<{ state: string }> };
      if (snap.nodes.every((n) => !["pending", "queued", "running"].includes(n.state))) return;
      await new Promise((r) => setTimeout(r, 50));
    }
    throw new Error("dag did not finish");
  }

  it("diamond DAG: all-success path runs in topological order", async () => {
    const { orch } = await mkOrch(dir);
    const dag = orch.dispatchDag(
      [
        { key: "a", deviceId: "d1", goal: mkGoal("step-a") },
        { key: "b", deviceId: "d1", goal: mkGoal("step-b") },
        { key: "c", deviceId: "d2", goal: mkGoal("step-c") },
        { key: "d", deviceId: "d2", goal: mkGoal("step-d") },
      ],
      [["a", "b"], ["a", "c"], ["b", "d"], ["c", "d"]],
    );
    await waitDagDone(orch, dag.dagId);
    const snap = dag.snapshot() as { nodes: Array<{ key: string; state: string; goalId: string | null }> };
    const byKey = Object.fromEntries(snap.nodes.map((n) => [n.key, n]));
    expect(byKey.a.state).toBe("done");
    expect(byKey.b.state).toBe("done");
    expect(byKey.c.state).toBe("done");
    expect(byKey.d.state).toBe("done");
    // every node got a real dispatched goal
    for (const n of Object.values(byKey)) expect(n.goalId).toBeTruthy();
    // dag snapshot persisted
    const { readFile, readdir } = await import("node:fs/promises");
    const files = await readdir(join(dir, "runs"));
    const dagFile = files.find((f) => f.startsWith(`dag-${dag.dagId}`));
    expect(dagFile).toBeTruthy();
    const saved = JSON.parse(await readFile(join(dir, "runs", dagFile!), "utf8"));
    expect(saved.policy).toBe("success");
  });

  it("failed dependency transitively skips downstream nodes", async () => {
    const { orch } = await mkOrch(dir);
    const dag = orch.dispatchDag(
      [
        { key: "a", deviceId: "d1", goal: mkGoal("fail:boom") },
        { key: "b", deviceId: "d1", goal: mkGoal("step-b") },
        { key: "c", deviceId: "d2", goal: mkGoal("step-c") },
      ],
      [["a", "b"], ["b", "c"]],
    );
    await waitDagDone(orch, dag.dagId);
    const snap = dag.snapshot() as { nodes: Array<{ key: string; state: string; reason: string | null; status: string | null }> };
    const byKey = Object.fromEntries(snap.nodes.map((n) => [n.key, n]));
    expect(byKey.a.state).toBe("failed"); // node state mirrors the RUN state (Python parity)
    expect(byKey.a.status).toBe("failed");
    expect(byKey.b.state).toBe("skipped");
    expect(byKey.b.reason).toContain("dependency a");
    expect(byKey.c.state).toBe("skipped"); // transitive
  });

  it("lenient policy lets partial dependencies through; success policy skips", async () => {
    const { orch } = await mkOrch(dir);
    const dagStrict = orch.dispatchDag(
      [
        { key: "a", deviceId: "d1", goal: mkGoal("partial:half") },
        { key: "b", deviceId: "d1", goal: mkGoal("step-b") },
      ],
      [["a", "b"]],
      "success",
    );
    await waitDagDone(orch, dagStrict.dagId);
    const strictSnap = dagStrict.snapshot() as { nodes: Array<{ key: string; state: string }> };
    expect(strictSnap.nodes.find((n) => n.key === "b")!.state).toBe("skipped");

    const dagLenient = orch.dispatchDag(
      [
        { key: "a", deviceId: "d2", goal: mkGoal("partial:half") },
        { key: "b", deviceId: "d2", goal: mkGoal("step-b") },
      ],
      [["a", "b"]],
      "lenient",
    );
    await waitDagDone(orch, dagLenient.dagId);
    const lenientSnap = dagLenient.snapshot() as { nodes: Array<{ key: string; state: string }> };
    expect(lenientSnap.nodes.find((n) => n.key === "b")!.state).toBe("done");
  });

  it("validates: duplicate keys, unknown device, self-edge, unknown edge, cycles", async () => {
    const { orch } = await mkOrch(dir);
    const g = mkGoal("x");
    expect(() => orch.dispatchDag(
      [{ key: "k", deviceId: "d1", goal: g }, { key: "k", deviceId: "d1", goal: g }], [],
    )).toThrow(/unique/);
    expect(() => orch.dispatchDag(
      [{ key: "k", deviceId: "ghost", goal: g }], [],
    )).toThrow(/unknown deviceId/);
    expect(() => orch.dispatchDag(
      [{ key: "k", deviceId: "d1", goal: g }], [["k", "k"]],
    )).toThrow(/self-edge/);
    expect(() => orch.dispatchDag(
      [{ key: "k", deviceId: "d1", goal: g }], [["k", "nope"]],
    )).toThrow(/edge/);
    expect(() => orch.dispatchDag(
      [
        { key: "a", deviceId: "d1", goal: g },
        { key: "b", deviceId: "d1", goal: g },
      ],
      [["a", "b"], ["b", "a"]],
    )).toThrow(/cycle/);
    expect(() => orch.dispatchDag(
      [{ key: "k", deviceId: "d1", goal: { protocol: "rao/9" } }], [],
    )).toThrow(/RAO_DAG_INVALID/);
  });
});

/* ---------------- reports ---------------- */

describe("RunStore reports", () => {
  let dir: string;

  beforeEach(async () => {
    dir = await mkdtemp(join(tmpdir(), "rao-reports-"));
  });

  afterEach(async () => {
    await rm(dir, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
  });

  it("collect persists a report; list/get/diff round trip with regressions", async () => {
    const store = new RunStore(join(dir, "runs"));
    const base = {
      generatedMs: 1,
      matrix: [
        { goalId: "g1", status: "success", summary: "ok" },
        { goalId: "g2", status: "failed", summary: "boom" },
      ],
      successCount: 1,
      failCount: 1,
    };
    await store.saveReport({ ...base, reportFile: undefined });

    const report = {
      generatedMs: 2,
      matrix: [
        { goalId: "g1", status: "failed", summary: "regressed" },
        { goalId: "g2", status: "success", summary: "fixed" },
        { goalId: "g3", status: "success", summary: "new" },
      ],
      successCount: 2,
      failCount: 1,
    };
    await store.saveReport(report);

    const reports = await store.listReports();
    expect(reports.length).toBe(2);
    expect(reports[0].goals).toBe(2);

    const loaded = await store.loadReport(reports[1].file as string);
    expect(loaded.generatedMs).toBe(2);

    const diff = RunStore.diffReports(base, report);
    expect(diff.counts).toEqual({
      base: { success: 1, fail: 1 },
      newer: { success: 2, fail: 1 },
    });
    expect((diff.changed as unknown[]).length).toBe(3);
    const regressions = diff.regressions as Array<{ goalId: string; from: string; to: string }>;
    expect(regressions).toHaveLength(1);
    expect(regressions[0]).toMatchObject({ goalId: "g1", from: "success", to: "failed" });
  });

  it("loadReport rejects path traversal", async () => {
    const store = new RunStore(join(dir, "runs"));
    await expect(store.loadReport("../escape.json")).rejects.toThrow(/bare filename/);
    await expect(store.loadReport("sub/dir.json")).rejects.toThrow(/bare filename/);
    await expect(store.loadReport("missing.json")).rejects.toThrow(/no such report/);
  });

  it("collect persists the report and attaches reportFile", async () => {
    const { orch } = await mkOrch(dir);
    const [d] = orch.dispatch(["d1"], mkGoal("collect-me"));
    const report = await orch.collect([d.goalId], 15000);
    expect(report.successCount).toBe(1);
    expect(report.reportFile).toBeTruthy();
    const { statSync } = await import("node:fs");
    expect(statSync(join(dir, "runs", String(report.reportFile).replace(/\\/g, "/").split("/").pop()!)).isFile()).toBe(true);
  });
});

/* ---------------- MCP tool surface ---------------- */

function mkMcpGoal(objective: string): Record<string, unknown> {
  return mkGoal(objective);
}

describe("MCP server tools (InMemoryTransport)", () => {
  let dir: string;

  beforeEach(async () => {
    dir = await mkdtemp(join(tmpdir(), "rao-mcp-"));
    process.env.RAO_STATE_DIR = dir;
    process.env.RAO_MASTER = "0"; // no master listener in unit tests
  });

  afterEach(async () => {
    delete process.env.RAO_STATE_DIR;
    delete process.env.RAO_MASTER;
    await rm(dir, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
  });

  async function startClient(): Promise<{
    client: Client; orch: Orchestrator; transport: ScriptedTransport;
  }> {
    const cfgPath = join(dir, "devices.json");
    const { writeFile } = await import("node:fs/promises");
    await writeFile(cfgPath, JSON.stringify({
      settings: { runsDir: join(dir, "runs"), transportMode: "spawn" },
      devices: [
        { id: "d1", host: "127.0.0.1", port: 22 },
        { id: "d2", host: "127.0.0.2", port: 22 },
      ],
    }));
    const { server, orch, transport } = await buildRaoServer({
      configPath: cfgPath,
      noMaster: true,
      transport: new ScriptedTransport(),
    });
    const client = new Client({ name: "test-client", version: "0.0.0" });
    const [clientT, serverT] = InMemoryTransport.createLinkedPair();
    await Promise.all([
      server.connect(serverT),
      client.connect(clientT),
    ]);
    return { client, orch, transport };
  }

  async function call(client: Client, name: string, args: Record<string, unknown> = {}): Promise<any> {
    const res = await client.callTool({ name, arguments: args });
    const text = (res.content as Array<{ type: string; text: string }>)
      ?.map((c) => c.text).join("");
    try {
      return JSON.parse(text);
    } catch {
      return { raw: text };
    }
  }

  it("tools/list exposes the 9 tools", async () => {
    const { client } = await startClient();
    const tools = await client.listTools();
    const names = tools.tools.map((t) => t.name).sort();
    expect(names).toEqual([
      "devices_list", "remote_cancel", "remote_collect", "remote_dag_status",
      "remote_dispatch", "remote_dispatch_dag", "remote_reports",
      "remote_status", "remote_steer",
    ]);
  });

  it("devices_list describes devices with honest execution matrix", async () => {
    const { client } = await startClient();
    const res = await call(client, "devices_list");
    expect(res.devices).toHaveLength(2);
    expect(res.devices[0].execution.is_agent).toBe(false);
    expect(res.devices[0].id).toBe("d1");
  });

  it("remote_dispatch: ledger dedupe on same operationId; mismatch rejection", async () => {
    const { client, transport } = await startClient();
    const args = {
      operationId: "op-dispatch-1",
      deviceIds: ["d1"],
      goal: mkMcpGoal("mcp-dispatch"),
    };
    const r1 = await call(client, "remote_dispatch", args);
    expect(r1.dispatches).toHaveLength(1);
    const gid1 = r1.dispatches[0].goalId;

    // same key + same params -> same receipt (no second dispatch)
    const r2 = await call(client, "remote_dispatch", { ...args });
    expect(r2.dispatches[0].goalId).toBe(gid1);
    expect(transport.opened).toHaveLength(1); // only executed once

    // same key + CHANGED params -> fingerprint-mismatch rejection
    const r3 = await call(client, "remote_dispatch", {
      ...args,
      goal: mkMcpGoal("changed-objective"),
    });
    expect(r3.error).toContain("fingerprint mismatch");

    // fresh key -> executes again
    const r4 = await call(client, "remote_dispatch", {
      ...args,
      operationId: "op-dispatch-2",
    });
    expect(r4.dispatches[0].goalId).not.toBe(gid1);
    expect(transport.opened).toHaveLength(2);
  });

  it("remote_status/steer/cancel/collect flow", async () => {
    const { client } = await startClient();
    const d = await call(client, "remote_dispatch", {
      operationId: "op-flow-1",
      deviceIds: ["d1"],
      goal: mkMcpGoal("flow-test"),
    });
    const gid = d.dispatches[0].goalId;

    const st = await call(client, "remote_status", { goalId: gid });
    expect(st.goalId).toBe(gid);
    expect(st.state).toBe("done");

    const collect = await call(client, "remote_collect", { goalIds: [gid] });
    expect(collect.successCount).toBe(1);
    expect(collect.reportFile).toBeTruthy();

    // status of unknown id -> error payload
    const unknown = await call(client, "remote_status", { goalId: "nope" });
    expect(unknown.error).toContain("no such goalId");

    // steer unknown -> error
    const steer = await call(client, "remote_steer", {
      operationId: "op-steer-1", goalId: "nope", message: "x",
    });
    expect(steer.error).toContain("no such goalId");

    // cancel on terminal goal -> no-op with sticky state
    const cancel = await call(client, "remote_cancel", {
      operationId: "op-cancel-1", goalId: gid, reason: "late",
    });
    expect(cancel.cancelled).toBe(false);
    expect(cancel.state).toBe("done");
  });

  it("remote_dispatch_dag + remote_dag_status", async () => {
    const { client, orch } = await startClient();
    const r = await call(client, "remote_dispatch_dag", {
      operationId: "op-dag-1",
      nodes: [
        { key: "a", deviceId: "d1", goal: mkMcpGoal("dag-a") },
        { key: "b", deviceId: "d2", goal: mkMcpGoal("dag-b") },
      ],
      edges: [["a", "b"]],
    });
    expect(r.dagId).toBeTruthy();
    const status = await call(client, "remote_dag_status", { dagId: r.dagId });
    expect(status.dagId).toBe(r.dagId);
    expect(status.nodes).toHaveLength(2);

    // ledger dedupe: same operationId + same graph -> same dagId
    const r2 = await call(client, "remote_dispatch_dag", {
      operationId: "op-dag-1",
      nodes: [
        { key: "a", deviceId: "d1", goal: mkMcpGoal("dag-a") },
        { key: "b", deviceId: "d2", goal: mkMcpGoal("dag-b") },
      ],
      edges: [["a", "b"]],
    });
    expect(r2.dagId).toBe(r.dagId);

    // unknown dag -> error payload
    const bad = await call(client, "remote_dag_status", { dagId: "ghost" });
    expect(bad.error).toContain("no such dagId");
    void orch;
  });

  it("remote_reports list/get/diff", async () => {
    const { client } = await startClient();
    const d = await call(client, "remote_dispatch", {
      operationId: "op-rep-1",
      deviceIds: ["d1"],
      goal: mkMcpGoal("report-me"),
    });
    await call(client, "remote_collect", { goalIds: [d.dispatches[0].goalId] });
    const list = await call(client, "remote_reports", { action: "list" });
    expect(list.reports.length).toBeGreaterThanOrEqual(1);
    const file = list.reports[list.reports.length - 1].file;
    const got = await call(client, "remote_reports", { action: "get", file });
    expect(got.matrix).toHaveLength(1);
    const diff = await call(client, "remote_reports", {
      action: "diff", file, fileB: file,
    });
    expect(diff.changed).toHaveLength(0);
    const bad = await call(client, "remote_reports", { action: "get", file: "../x" });
    expect(bad.error).toContain("bare filename");
  });

  it("dispatch with invalid goal returns a structured error", async () => {
    const { client } = await startClient();
    const bad = await call(client, "remote_dispatch", {
      operationId: "op-bad-1",
      deviceIds: ["d1"],
      goal: { objective: "x", agent: "claude", plan: { planner: "main-agent", steps: [{ id: "s", type: "shell", command: "x" }] } },
    });
    expect(bad.error).toBeTruthy();
    const none = await call(client, "remote_dispatch", {
      operationId: "op-bad-2",
      deviceIds: ["ghost-device"],
      goal: mkMcpGoal("y"),
    });
    expect(none.error).toContain("unknown deviceId");
  });
});

/* keep the emptyResult import referenced (used by ScriptedChannel shape) */
void emptyResult;
