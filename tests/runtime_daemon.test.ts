/**
 * Daemon tests (rda_daemon.mjs) — ported from the Python daemon suite:
 * auth-first TCP, submit/attach/status/list/inject, one-shot control plane
 * (inject-one), stdio<->TCP bridge, node-mode master link, shutdown.
 * Every daemon runs against an isolated state dir.
 */
import { spawn, type ChildProcess } from "node:child_process";
import { connect, createServer } from "node:net";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";

const DAEMON = fileURLToPath(new URL("../src/runtime/rda_daemon.mjs", import.meta.url));

/* ---------------- helpers ---------------- */

interface LineConn {
  lines: any[];
  send: (obj: unknown) => void;
  close: () => void;
  waitFor: (pred: (m: any) => boolean, timeoutMs?: number) => Promise<any>;
}

function dial(port: number): Promise<LineConn> {
  return new Promise((resolve) => {
    const sock = connect({ host: "127.0.0.1", port });
    const lines: any[] = [];
    const waiters: Array<{
      pred: (m: any) => boolean;
      resolve: (m: any) => void;
      timer: NodeJS.Timeout;
    }> = [];
    const rl = createInterface({ input: sock, terminal: false });
    rl.on("error", () => {}); // readline re-emits socket errors on the interface
    rl.on("line", (raw) => {
      const line = raw.trim();
      if (!line) return;
      let msg;
      try { msg = JSON.parse(line); } catch { return; }
      lines.push(msg);
      for (let i = 0; i < waiters.length; i++) {
        if (waiters[i].pred(msg)) {
          clearTimeout(waiters[i].timer);
          waiters[i].resolve(msg);
          waiters.splice(i, 1);
          break;
        }
      }
    });
    sock.on("error", () => { /* resolve waiters via timeout */ });
    resolve({
      lines,
      send: (obj) => sock.write(JSON.stringify(obj) + "\n"),
      close: () => sock.destroy(),
      waitFor: (pred, timeoutMs = 10000) => {
        const found = lines.find(pred);
        if (found) return Promise.resolve(found);
        return new Promise((resolve, reject) => {
          const timer = setTimeout(
            () => reject(new Error(`waitFor timeout: ${JSON.stringify(pred)}`)),
            timeoutMs,
          );
          waiters.push({ pred, resolve, timer });
        });
      },
    });
  });
}

function mkGoal(over: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    protocol: "rao/1",
    goalId: `g-${Math.random().toString(36).slice(2, 10)}`,
    deviceId: "local",
    objective: "daemon test goal",
    constraints: { timeoutMs: 15000 },
    ...over,
  };
}

async function runCli(home: string, args: string[]): Promise<{
  code: number | null; stdout: string; stderr: string;
}> {
  return new Promise((resolve) => {
    const child = spawn(process.execPath, [DAEMON, "--home", home, ...args]);
    let stdout = "";
    let stderr = "";
    child.stdout!.on("data", (c) => { stdout += c; });
    child.stderr!.on("data", (c) => { stderr += c; });
    child.on("close", (code) => resolve({ code, stdout, stderr }));
  });
}

/* ---------------- shared daemon (main suite) ---------------- */

let dir: string;
let port: number;
let token: string;

beforeAll(async () => {
  dir = await mkdtemp(join(tmpdir(), "rao-daemon-"));
  const r = await runCli(dir, ["start", "--port", "0"]);
  expect(r.stdout).toContain("started on port");
  port = Number(r.stdout.match(/port (\d+)/)?.[1]);
  expect(port).toBeGreaterThan(0);
  // daemon.json carries the token for control-plane clients
  const { readFileSync } = await import("node:fs");
  const dj = JSON.parse(readFileSync(join(dir, "daemon.json"), "utf8"));
  token = dj.token;
  expect(dj.codeHash).toMatch(/^[0-9a-f]{64}$/);
}, 30000);

afterAll(async () => {
  await runCli(dir, ["stop"]);
  await rm(dir, { recursive: true, force: true }).catch(() => {});
}, 30000);

beforeEach(async () => {
  // fresh state dir per assertion block is unnecessary; the shared daemon
  // uses unique goalIds per test
});

async function authedConn(): Promise<LineConn> {
  const conn = await dial(port);
  conn.send({ cmd: "auth", token });
  await conn.waitFor((m) => m.type === "ready");
  return conn;
}

describe("daemon: auth-first wire", () => {
  it("rejects bad auth", async () => {
    const conn = await dial(port);
    conn.send({ cmd: "auth", token: "wrong" });
    const reply = await conn.waitFor((m) => m.type === "error");
    expect(reply.message).toBe("auth required");
    conn.close();
  });

  it("ping → pong", async () => {
    const conn = await authedConn();
    conn.send({ cmd: "ping" });
    const pong = await conn.waitFor((m) => m.type === "pong");
    expect(pong.ts).toBeGreaterThan(0);
    conn.close();
  });

  it("unknown cmd → error", async () => {
    const conn = await authedConn();
    conn.send({ cmd: "bogus" });
    const err = await conn.waitFor((m) => m.type === "error");
    expect(err.message).toContain("unknown cmd");
    conn.close();
  });
});

describe("daemon: submit/attach/status/list", () => {
  it("submit → ack → live events → goal_status(success)", async () => {
    const conn = await authedConn();
    const goal = mkGoal({
      plan: { planner: "main-agent", steps: [{ id: "s1", type: "shell", command: "echo ok-daemon" }] },
    });
    conn.send({ cmd: "submit", goal });
    const ack = await conn.waitFor((m) => m.type === "ack");
    expect(ack.goalId).toBe(goal.goalId);
    const ev = await conn.waitFor(
      (m) => m.type === "event" && m.event.kind === "started");
    expect(ev.seq).toBeGreaterThanOrEqual(1);
    expect(ev.goalId).toBe(goal.goalId);
    const status = await conn.waitFor((m) => m.type === "goal_status");
    expect(status.status).toBe("success");
    expect(status.exit).toBe(0);
    conn.close();
  });

  it("attach replays buffered events with stable seqs", async () => {
    const conn1 = await authedConn();
    const goal = mkGoal({
      plan: { planner: "main-agent", steps: [
        { id: "s1", type: "shell", command: "echo attach-test" },
      ] },
    });
    conn1.send({ cmd: "submit", goal });
    await conn1.waitFor((m) => m.type === "goal_status");
    conn1.close();

    const conn2 = await authedConn();
    conn2.send({ cmd: "attach", goalId: goal.goalId });
    await conn2.waitFor((m) => m.type === "attached");
    // the daemon sends: attached → replayed events → goal_status (last);
    // wait for the tail marker before inspecting the replay
    const st = await conn2.waitFor((m) => m.type === "goal_status");
    expect(st.status).toBe("success");
    const evs = conn2.lines.filter((m) => m.type === "event");
    expect(evs.length).toBeGreaterThanOrEqual(3);
    // seq numbering is contiguous from 1 across the replay
    expect(evs.map((e) => e.seq)).toEqual(
      evs.map((_, i) => i + 1));
    conn2.close();
  });

  it("attach unknown goalId → error", async () => {
    const conn = await authedConn();
    conn.send({ cmd: "attach", goalId: "no-such-goal" });
    const err = await conn.waitFor((m) => m.type === "error");
    expect(err.message).toContain("unknown goalId");
    conn.close();
  });

  it("status → goal_snapshot with recent events", async () => {
    const conn = await authedConn();
    const goal = mkGoal({
      plan: { planner: "main-agent", steps: [{ id: "s1", type: "shell", command: "echo snap" }] },
    });
    conn.send({ cmd: "submit", goal });
    await conn.waitFor((m) => m.type === "goal_status");
    conn.send({ cmd: "status", goalId: goal.goalId });
    const snap = await conn.waitFor((m) => m.type === "goal_snapshot");
    expect(snap.status).toBe("success");
    expect(snap.events.length).toBeGreaterThan(0);
    conn.close();
  });

  it("list → goal_list containing submitted goals", async () => {
    const conn = await authedConn();
    conn.send({ cmd: "list" });
    const list = await conn.waitFor((m) => m.type === "goal_list");
    expect(Array.isArray(list.goals)).toBe(true);
    expect(list.goals.length).toBeGreaterThanOrEqual(1);
    conn.close();
  });

  it("submit duplicate running goalId → error", async () => {
    const conn = await authedConn();
    const goal = mkGoal({
      plan: { planner: "main-agent", steps: [
        { id: "w", type: "wait_file", path: join(dir, "no-show"), timeoutMs: 6000 },
      ] },
    });
    conn.send({ cmd: "submit", goal });
    await conn.waitFor((m) => m.type === "ack");
    conn.send({ cmd: "submit", goal });
    const err = await conn.waitFor((m) => m.type === "error");
    expect(err.message).toContain("already running");
    conn.send({ cmd: "inject", goalId: goal.goalId, payload: { type: "cancel" } });
    await conn.waitFor((m) => m.type === "goal_status" && m.goalId === goal.goalId);
    conn.close();
  });

  it("invalid goal → error RAO_GOAL_INVALID", async () => {
    const conn = await authedConn();
    conn.send({ cmd: "submit", goal: { protocol: "rao/9" } });
    const err = await conn.waitFor((m) => m.type === "error");
    expect(err.message).toContain("RAO_GOAL_INVALID");
    conn.close();
  });
});

describe("daemon: control plane (inject + inject-one)", () => {
  it("inject cancel mid-goal → delivered receipt → goal_status(cancelled)", async () => {
    const conn = await authedConn();
    const goal = mkGoal({
      plan: { planner: "main-agent", steps: [
        { id: "w", type: "wait_file", path: join(dir, "never-1"), timeoutMs: 12000 },
      ] },
    });
    conn.send({ cmd: "submit", goal });
    await conn.waitFor((m) => m.type === "ack");
    // wait until the engine is up (started event seen)
    await conn.waitFor((m) => m.type === "event" && m.event.kind === "started");
    conn.send({ cmd: "inject", goalId: goal.goalId, payload: { type: "cancel", reason: "t" } });
    const receipt = await conn.waitFor((m) => m.type === "injected");
    expect(receipt.queued).toBe("delivered");
    const status = await conn.waitFor(
      (m) => m.type === "goal_status" && m.goalId === goal.goalId);
    expect(status.status).toBe("cancelled");
    expect(status.exit).toBe(4);
    conn.close();
  });

  it("inject-one CLI: delivered → exit 0; unknown goal → exit 1", async () => {
    const conn = await authedConn();
    const goal = mkGoal({
      plan: { planner: "main-agent", steps: [
        { id: "w", type: "wait_file", path: join(dir, "never-2"), timeoutMs: 12000 },
      ] },
    });
    conn.send({ cmd: "submit", goal });
    await conn.waitFor((m) => m.type === "event" && m.event.kind === "started");

    const r1 = await runCli(dir, [
      "inject-one", "--goal-id", goal.goalId,
      "--payload", JSON.stringify({ type: "cancel", reason: "cli" }),
    ]);
    expect(r1.code).toBe(0);
    expect(JSON.parse(r1.stdout).queued).toBe("delivered");
    const status = await conn.waitFor(
      (m) => m.type === "goal_status" && m.goalId === goal.goalId);
    expect(status.status).toBe("cancelled");
    conn.close();

    const r2 = await runCli(dir, [
      "inject-one", "--goal-id", "ghost", "--payload", "{}",
    ]);
    expect(r2.code).toBe(1);
    expect(JSON.parse(r2.stdout).queued).toBe("dropped");
  });

  it("status-one CLI prints the snapshot", async () => {
    const conn = await authedConn();
    const goal = mkGoal({
      plan: { planner: "main-agent", steps: [{ id: "s1", type: "shell", command: "echo so" }] },
    });
    conn.send({ cmd: "submit", goal });
    await conn.waitFor(
      (m) => m.type === "goal_status" && m.goalId === goal.goalId);
    conn.close();
    const r = await runCli(dir, ["status-one", "--goal-id", goal.goalId]);
    expect(r.code).toBe(0);
    const snap = JSON.parse(r.stdout);
    expect(snap.type).toBe("goal_snapshot");
    expect(snap.status).toBe("success");
  });
});

describe("daemon: bridge (stdio <-> TCP)", () => {
  it("bridges submit/events/goal_status over stdio", async () => {
    const child: ChildProcess = spawn(process.execPath, [
      DAEMON, "--home", dir, "client-bridge",
    ], { stdio: ["pipe", "pipe", "pipe"] });
    const lines: any[] = [];
    let buf = "";
    child.stdout!.on("data", (c) => {
      buf += c.toString("utf8");
      for (;;) {
        const nl = buf.indexOf("\n");
        if (nl < 0) break;
        const ln = buf.slice(0, nl).trim();
        buf = buf.slice(nl + 1);
        if (!ln) continue;
        try { lines.push(JSON.parse(ln)); } catch { /* drop */ }
      }
    });
    const goal = mkGoal({
      plan: { planner: "main-agent", steps: [{ id: "s1", type: "shell", command: "echo via-bridge" }] },
    });
    child.stdin!.write(JSON.stringify({ cmd: "submit", goal }) + "\n");
    const deadline = Date.now() + 15000;
    while (Date.now() < deadline) {
      if (lines.some((m) => m.type === "goal_status" && m.goalId === goal.goalId)) break;
      await new Promise((r) => setTimeout(r, 50));
    }
    child.kill();
    expect(lines.some((m) => m.type === "ack" && m.goalId === goal.goalId)).toBe(true);
    expect(lines.some((m) => m.type === "event")).toBe(true);
    expect(lines.some((m) => m.type === "goal_status" && m.status === "success")).toBe(true);
  });
});

/* ---------------- node mode: master link ---------------- */

describe("daemon: node mode (master link)", () => {
  it("dials the master, registers, executes a pushed goal, streams events", async () => {
    const nodeDir = await mkdtemp(join(tmpdir(), "rao-node-"));
    const received: any[] = [];
    const nodeConns: any[] = [];
    const master = createServer((sock) => {
      const rl = createInterface({ input: sock, terminal: false });
      rl.on("error", () => {}); // readline re-emits socket errors on the interface
      const send = (obj: unknown) => sock.write(JSON.stringify(obj) + "\n");
      nodeConns.push({ sock, send });
      rl.on("line", (raw) => {
        const line = raw.trim();
        if (!line) return;
        let msg;
        try { msg = JSON.parse(line); } catch { return; }
        received.push(msg);
        if (msg.cmd === "register") {
          expect(msg.token).toBe("enroll-tok");
          expect(msg.nodeId).toBeTruthy();
          send({ type: "registered" });
          // push a goal once registered
          const goal = mkGoal({
            plan: { planner: "main-agent", steps: [
              { id: "s1", type: "shell", command: "echo node-mode-ok" },
            ] },
          });
          setTimeout(() => send({ cmd: "goal", goal }), 100);
        }
      });
      sock.on("error", () => { /* ignore */ });
    });
    await new Promise<void>((resolve) => master.listen(0, "127.0.0.1", resolve));
    const mport = (master.address() as { port: number }).port;

    // daemon serves in node mode (its local TCP port is irrelevant to the master)
    const child: ChildProcess = spawn(process.execPath, [
      DAEMON, "--home", nodeDir, "serve", "--port", "0",
      "--master", `127.0.0.1:${mport}`, "--token", "enroll-tok",
    ], { stdio: ["ignore", "pipe", "pipe"] });
    let stderr = "";
    child.stderr!.on("data", (c) => { stderr += c; });

    const deadline = Date.now() + 15000;
    let accepted = null;
    let status = null;
    while (Date.now() < deadline) {
      accepted = received.find((m) => m.type === "goal_accepted");
      status = received.find((m) => m.type === "goal_status");
      if (accepted && status) break;
      await new Promise((r) => setTimeout(r, 50));
    }
    // events streamed to the master
    const events = received.filter((m) => m.type === "event");
    child.kill();
    master.close();
    await rm(nodeDir, { recursive: true, force: true }).catch(() => {});
    expect(accepted).not.toBeNull();
    expect(status?.status).toBe("success");
    expect(events.length).toBeGreaterThanOrEqual(2);
    void stderr;
  }, 25000);
});
