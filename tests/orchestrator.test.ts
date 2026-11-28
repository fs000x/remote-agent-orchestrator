/**
 * Orchestrator tests — ported semantics from the Python T-suite:
 * lifecycle, sticky terminal states, budget enforcement (fail-closed),
 * concurrency slots, daemon reconnect with seq dedupe, collect aggregation.
 * All transports are fakes; no network.
 */
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  Orchestrator,
  type GoalChannel,
  type Transport,
} from "../src/orchestrator.js";
import { DeviceRegistry, type DeviceRecord } from "../src/devices.js";
import { ErrorCode, RaoError, type RaoEvent } from "../src/protocol.js";

/* ---------------- fakes ---------------- */

class FakeChannel implements GoalChannel {
  goalId: string;
  events: RaoEvent[] = [];
  event_seqs: Array<number | null> = [];
  stderr_text: string[] = [];
  exit_code: number | null = null;
  killed = false;
  closed = false;
  dead = false;
  supports_resume: boolean;

  constructor(goalId: string, supportsResume: boolean) {
    this.goalId = goalId;
    this.supports_resume = supportsResume;
  }

  private waiters: Array<{
    since: number;
    resolve: (v: [RaoEvent | null, number]) => void;
    timer: NodeJS.Timeout;
  }> = [];

  push(
    kind: RaoEvent["kind"],
    data: Record<string, unknown>,
    seq: number | null,
  ): void {
    this.events.push({
      protocol: "rao/1",
      kind,
      goalId: this.goalId,
      ts: Date.now(),
      data,
    });
    this.event_seqs.push(seq);
    for (const w of [...this.waiters]) {
      if (this.events.length > w.since) {
        clearTimeout(w.timer);
        // one event per poll: newIndex = since + 1 (never skip ahead)
        w.resolve([this.events[w.since], w.since + 1]);
        this.waiters.splice(this.waiters.indexOf(w), 1);
      }
    }
  }

  waitEvent(
    since: number,
    timeoutMs: number,
  ): Promise<[RaoEvent | null, number]> {
    if (this.events.length > since) {
      return Promise.resolve([this.events[since], since + 1]);
    }
    return new Promise((resolve) => {
      const entry = {
        since,
        resolve,
        timer: setTimeout(() => {
          this.waiters = this.waiters.filter((w) => w !== entry);
          resolve([null, since]);
        }, timeoutMs),
      };
      this.waiters.push(entry);
    });
  }

  writeLine(): boolean {
    return !this.dead && !this.closed;
  }

  alive(): boolean {
    return !this.dead && this.exit_code === null && !this.closed;
  }

  /** simulate link loss (daemon reconnects) */
  die(): void {
    this.dead = true;
  }

  kill(): void {
    this.killed = true;
    this.exit_code = this.exit_code ?? 137;
  }

  close(): void {
    this.closed = true;
  }

  waitExit(timeoutMs: number): Promise<number | null> {
    if (this.exit_code !== null) return Promise.resolve(this.exit_code);
    return new Promise((resolve) => {
      const t = setInterval(() => {
        if (this.exit_code !== null) {
          clearInterval(t);
          resolve(this.exit_code);
        }
      }, 20);
      setTimeout(() => {
        clearInterval(t);
        resolve(null);
      }, timeoutMs);
    });
  }
}

class FakeTransport implements Transport {
  channels = new Map<string, FakeChannel>();
  provisions = 0;
  opened = 0;
  resumed = 0;
  steers: Array<{ goalId: string; payload: Record<string, unknown> }> = [];
  failFirstOpen = false;
  /** every resumed channel is born dead (resume-budget test) */
  resumeDead = false;

  constructor(readonly supportsResume: boolean) {}

  async provision(): Promise<void> {
    this.provisions++;
  }

  async openSession(goalId: string): Promise<GoalChannel> {
    this.opened++;
    if (this.failFirstOpen && this.opened === 1) {
      throw new RaoError(ErrorCode.ConnectionFailed, "boom");
    }
    const c = new FakeChannel(goalId, this.supportsResume);
    this.channels.set(goalId, c);
    return c;
  }

  /** daemon-style reconnect: fresh channel replaying the buffered events */
  async openResumeSession(goalId: string): Promise<GoalChannel> {
    this.resumed++;
    const old = this.channels.get(goalId);
    if (!old) throw new RaoError(ErrorCode.GoalNotFound, "no session");
    const c = new FakeChannel(goalId, this.supportsResume);
    c.events = [...old.events];
    c.event_seqs = [...old.event_seqs];
    if (this.resumeDead) c.die();
    this.channels.set(goalId, c);
    return c;
  }

  async steer(
    _device: DeviceRecord,
    goalId: string,
    payload: Record<string, unknown>,
  ): Promise<boolean> {
    this.steers.push({ goalId, payload });
    return true;
  }
}

function mkEvent(
  goalId: string,
  kind: RaoEvent["kind"],
  data: Record<string, unknown> = {},
): RaoEvent {
  return { protocol: "rao/1", kind, goalId, ts: Date.now(), data };
}
void mkEvent;

function mkGoal(over: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    protocol: "rao/1",
    objective: "do the thing",
    agent: "builtin",
    plan: {
      planner: "main-agent",
      steps: [{ id: "s1", type: "shell", command: "echo hi" }],
    },
    constraints: { timeoutMs: 15_000, maxSteps: 40 },
    ...over,
  };
}

const SUCCESS = (summary: string) => ({
  status: "success",
  summary,
  steps: [],
  artifacts: [],
  assumptions: [],
  steerNotes: [],
  metrics: { stepAttempts: 1, retries: 0, durationMs: 1 },
});

/** poll until the async openSession has produced the channel */
async function waitChannel(
  transport: FakeTransport,
  goalId: string,
  timeoutMs = 5_000,
): Promise<FakeChannel> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const c = transport.channels.get(goalId);
    if (c) return c;
    if (Date.now() > deadline) {
      throw new Error(`channel for ${goalId} never opened`);
    }
    await new Promise((r) => setTimeout(r, 20));
  }
}

/* ---------------- harness ---------------- */

let dir: string;

beforeEach(async () => {
  dir = await mkdtemp(join(tmpdir(), "rao-orch-"));
  process.env.RAO_STATE_DIR = dir; // isolate node-merge from the real ~/.rao
  delete process.env.RAO_MERGE_NODES;
});

afterEach(async () => {
  delete process.env.RAO_STATE_DIR;
  delete process.env.RAO_MERGE_NODES;
  await rm(dir, { recursive: true, force: true });
});

async function mkOrch(opts: {
  transport: Transport;
  deviceOver?: Record<string, unknown>;
  maxConcurrentGoals?: number;
  resumeMax?: number;
}): Promise<Orchestrator> {
  const cfgPath = join(dir, "devices.json");
  await writeFile(
    cfgPath,
    JSON.stringify({
      settings: {},
      devices: [
        {
          id: "d1",
          host: "127.0.0.1",
          port: 22,
          user: "diablo",
          maxConcurrentGoals: 2,
          ...opts.deviceOver,
        },
        { id: "d2", host: "127.0.0.2", port: 22, user: "u2" },
      ],
    }),
  );
  const registry = new DeviceRegistry(cfgPath);
  await registry.reload();
  return new Orchestrator({
    registry,
    transport: opts.transport,
    runsDir: join(dir, "runs"),
    maxConcurrentGoals: opts.maxConcurrentGoals,
    resumeMax: opts.resumeMax,
  });
}

/* ---------------- tests ---------------- */

describe("orchestrator: happy paths", () => {
  it("daemon mode: events drive state to done, result recorded, persisted", async () => {
    const transport = new FakeTransport(true);
    const orch = await mkOrch({ transport });
    const [d] = orch.dispatch(["d1"], mkGoal());
    const chan = await waitChannel(transport, d.goalId);
    chan.push("started", {}, 1);
    chan.push("progress", { phase: "running", stepId: "s1" }, 2);
    chan.push(
      "result",
      {
        status: "success",
        summary: "ok",
        steps: [{ id: "s1", ok: true }],
        artifacts: [],
        assumptions: [],
        steerNotes: [],
        metrics: { stepAttempts: 1, retries: 0, durationMs: 10 },
      },
      3,
    );
    const run = orch.getRun(d.goalId)!;
    await run.waitTerminal(5_000);
    expect(run.state).toBe("done");
    expect(run.result?.status).toBe("success");
    expect(run.attemptCount).toBe(1);
    const rec = await orch.runStore.load(d.goalId);
    expect(rec).not.toBeNull();
    expect(rec?.state).toBe("done");

    const collected = await orch.collect([d.goalId]);
    expect(collected.successCount).toBe(1);
    expect(collected.failCount).toBe(0);
  });

  it("spawn mode: result event finishes the run", async () => {
    const transport = new FakeTransport(false);
    const orch = await mkOrch({ transport });
    const [d] = orch.dispatch(["d1"], mkGoal());
    const chan = await waitChannel(transport, d.goalId);
    chan.push("started", {}, null);
    chan.push(
      "result",
      {
        status: "partial",
        summary: "mostly ok",
        steps: [],
        artifacts: [],
        assumptions: ["x"],
        steerNotes: [],
        metrics: { stepAttempts: 2, retries: 1, durationMs: 5 },
      },
      null,
    );
    await orch.getRun(d.goalId)!.waitTerminal(5_000);
    const run = orch.getRun(d.goalId)!;
    expect(run.state).toBe("done");
    expect(run.result?.status).toBe("partial");
  });

  it("ask flow: ask_waiting until activity resumes", async () => {
    const transport = new FakeTransport(true);
    const orch = await mkOrch({ transport });
    const [d] = orch.dispatch(["d1"], mkGoal());
    const run = orch.getRun(d.goalId)!;
    const chan = await waitChannel(transport, d.goalId);
    chan.push("ask", { askId: "a1", question: "which branch?" }, 1);
    await vi.waitFor(() => expect(run.state).toBe("ask_waiting"));
    const ok = await orch.steerGoal(d.goalId, undefined, "a1", "main");
    expect(ok).toBe(true);
    chan.push("progress", { phase: "running" }, 2);
    await vi.waitFor(() => expect(run.state).toBe("running"));
    chan.push("result", { ...SUCCESS("done"), metrics: { stepAttempts: 1, retries: 0, durationMs: 1 } }, 3);
    await run.waitTerminal(5_000);
    expect(run.state).toBe("done");
  });
});

describe("orchestrator: enforcement (sticky, fail-closed)", () => {
  it("goal deadline -> timeout, force-cancel via control plane, late RESULT ignored", async () => {
    const transport = new FakeTransport(true);
    const orch = await mkOrch({ transport });
    const [d] = orch.dispatch(
      ["d1"],
      mkGoal({ constraints: { timeoutMs: 1200, maxSteps: 40 } }),
    );
    const run = orch.getRun(d.goalId)!;
    const chan = await waitChannel(transport, d.goalId);
    chan.push("started", {}, 1);
    // keep the link noisy so the silence watchdog never fires
    const noise = setInterval(() => {
      if (chan.alive()) chan.push("progress", { tick: 1 }, null);
    }, 200);
    try {
      await run.waitTerminal(10_000);
    } finally {
      clearInterval(noise);
    }
    expect(run.state).toBe("timeout");
    expect(run.result?.status).toBe("timeout");
    expect(run.result?.summary).toContain("deadline");
    // sticky: a late result(cancelled) must NOT overwrite the decision
    chan.push("result", { status: "cancelled", summary: "late", steps: [], artifacts: [], assumptions: [], steerNotes: [], metrics: { stepAttempts: 0, retries: 0, durationMs: 0 } }, 999);
    expect(run.state).toBe("timeout");
    // enforcement used the control plane (fresh connection), not the bridge
    expect(transport.steers.some((s) => s.payload.type === "cancel")).toBe(true);
  });

  it("step budget exceeded -> timeout via orchestrator backstop", async () => {
    const transport = new FakeTransport(true);
    const orch = await mkOrch({ transport });
    const [d] = orch.dispatch(
      ["d1"],
      mkGoal({ constraints: { timeoutMs: 20_000, maxSteps: 2 } }),
    );
    const run = orch.getRun(d.goalId)!;
    const chan = await waitChannel(transport, d.goalId);
    chan.push("progress", { phase: "running" }, 1);
    chan.push("progress", { phase: "running" }, 2);
    chan.push("progress", { phase: "running" }, 3); // attempt 3 > maxSteps 2
    await run.waitTerminal(5_000);
    expect(run.state).toBe("timeout");
    expect(run.attemptCount).toBe(3);
    expect(run.error).toContain("BUDGET_EXCEEDED");
  });

  it("spawn mode: exit without result -> failed synthesis", async () => {
    const transport = new FakeTransport(false);
    const orch = await mkOrch({ transport });
    const [d] = orch.dispatch(["d1"], mkGoal());
    const run = orch.getRun(d.goalId)!;
    const chan = await waitChannel(transport, d.goalId);
    chan.push("started", {}, null);
    chan.exit_code = 1;
    await run.waitTerminal(5_000);
    expect(run.state).toBe("failed");
    expect(run.result?.summary).toContain("exited (code=1)");
  });

  it("openSession failure with RaoError -> failed with code prefix", async () => {
    const transport = new FakeTransport(true);
    transport.failFirstOpen = true;
    const orch = await mkOrch({ transport });
    const [d] = orch.dispatch(["d1"], mkGoal());
    const run = orch.getRun(d.goalId)!;
    await run.waitTerminal(5_000);
    expect(run.state).toBe("failed");
    expect(run.error).toContain("CONNECTION_FAILED");
  });
});

describe("orchestrator: daemon reconnect", () => {
  it("link loss -> resume replays buffered events (seq dedupe) -> run completes", async () => {
    const transport = new FakeTransport(true);
    const orch = await mkOrch({ transport, resumeMax: 3 });
    const [d] = orch.dispatch(["d1"], mkGoal());
    const run = orch.getRun(d.goalId)!;
    const chan = await waitChannel(transport, d.goalId);
    chan.push("started", {}, 1);
    chan.push("progress", { phase: "running" }, 2);
    chan.die(); // transport lost mid-goal
    // reconnect lands shortly after; resumed channel replays events 1..2
    const iv = setInterval(() => {
      const c2 = transport.channels.get(d.goalId);
      if (transport.resumed > 0 && c2 && c2.alive()) {
        c2.push("progress", { phase: "running", stepId: "s1" }, 3);
        c2.push("result", { ...SUCCESS("after resume"), metrics: { stepAttempts: 2, retries: 0, durationMs: 7 } }, 4);
      }
    }, 50);
    try {
      await run.waitTerminal(15_000);
    } finally {
      clearInterval(iv);
    }
    expect(run.state).toBe("done");
    expect(run.result?.summary).toBe("after resume");
    expect(transport.resumed).toBeGreaterThanOrEqual(1);
    // events were not double-counted despite replay
    expect(run.attemptCount).toBe(2);
  });

  it("resume budget exhausted -> failed", async () => {
    const transport = new FakeTransport(true);
    transport.resumeDead = true; // every resume is born dead
    const orch = await mkOrch({ transport, resumeMax: 1 });
    const [d] = orch.dispatch(["d1"], mkGoal());
    const run = orch.getRun(d.goalId)!;
    const chan = await waitChannel(transport, d.goalId);
    chan.die();
    await run.waitTerminal(15_000);
    expect(run.state).toBe("failed");
    expect(run.error).toContain("resume budget exhausted");
  });
});

describe("orchestrator: concurrency slots", () => {
  it("per-device limit serializes goals on one device", async () => {
    const transport = new FakeTransport(true);
    const orch = await mkOrch({
      transport,
      deviceOver: { maxConcurrentGoals: 1 },
    });
    const g = mkGoal();
    const [a] = orch.dispatch(["d1"], g);
    await waitChannel(transport, a.goalId); // A now holds the only slot
    const [b] = orch.dispatch(["d1"], g); // queued behind A
    const chanA = transport.channels.get(a.goalId)!;
    // goal A holds its slot for 300ms before finishing
    setTimeout(() => chanA.push("result", SUCCESS("A"), 1), 300);
    await vi.waitFor(() => expect(transport.opened).toBe(2), {
      timeout: 8_000,
      interval: 50,
    });
    // B only opened after A's pump finished (slot released)
    const runA = orch.getRun(a.goalId)!;
    expect(runA.isTerminal()).toBe(true);
    transport.channels
      .get(b.goalId)!
      .push("result", SUCCESS("B"), 1);
    await Promise.all([
      runA.waitTerminal(5_000),
      orch.getRun(b.goalId)!.waitTerminal(5_000),
    ]);
    const collected = await orch.collect([a.goalId, b.goalId]);
    expect(collected.successCount).toBe(2);
  });

  it("global limit serializes across devices", async () => {
    const transport = new FakeTransport(true);
    const orch = await mkOrch({ transport, maxConcurrentGoals: 1 });
    const g = mkGoal();
    const [a] = orch.dispatch(["d1"], g);
    await waitChannel(transport, a.goalId);
    const [b] = orch.dispatch(["d2"], g);
    setTimeout(
      () => transport.channels.get(a.goalId)!.push("result", SUCCESS("A"), 1),
      300,
    );
    await vi.waitFor(() => expect(transport.opened).toBe(2), {
      timeout: 8_000,
      interval: 50,
    });
    transport.channels.get(b.goalId)!.push("result", SUCCESS("B"), 1);
    await Promise.all([
      orch.getRun(a.goalId)!.waitTerminal(5_000),
      orch.getRun(b.goalId)!.waitTerminal(5_000),
    ]);
    const collected = await orch.collect([a.goalId, b.goalId]);
    expect(collected.successCount).toBe(2);
  });

  it("cancel while queued: session never opened", async () => {
    const transport = new FakeTransport(true);
    const orch = await mkOrch({
      transport,
      maxConcurrentGoals: 1,
      deviceOver: { maxConcurrentGoals: 1 },
    });
    const g = mkGoal();
    const [a] = orch.dispatch(["d1"], g);
    await waitChannel(transport, a.goalId); // A holds the only slot
    const [b] = orch.dispatch(["d1"], g); // queued
    expect(transport.opened).toBe(1);
    const ok = await orch.cancelGoal(b.goalId, "user");
    expect(ok).toBe(true);
    const runB = orch.getRun(b.goalId)!;
    await runB.thread;
    expect(runB.state).toBe("cancelled");
    expect(runB.result?.summary).toContain("cancelled");
    expect(transport.opened).toBe(1); // B never opened a session
    // cleanup: let A finish
    transport.channels.get(a.goalId)!.push("result", SUCCESS("A"), 1);
    await orch.getRun(a.goalId)!.waitTerminal(5_000);
  });
});

describe("orchestrator: dispatch validation + collect", () => {
  it("rejects empty/oversized batches and unknown devices", async () => {
    const transport = new FakeTransport(true);
    const orch = await mkOrch({ transport });
    expect(() => orch.dispatch([], mkGoal())).toThrow(RaoError);
    expect(() =>
      orch.dispatch(Array.from({ length: 17 }, () => "d1"), mkGoal()),
    ).toThrow(RaoError);
    try {
      orch.dispatch(["nope"], mkGoal());
      expect.unreachable();
    } catch (err) {
      expect((err as RaoError).code).toBe(ErrorCode.DeviceNotFound);
    }
  });

  it("rejects wrong protocol and claude+plan goals", async () => {
    const transport = new FakeTransport(true);
    const orch = await mkOrch({ transport });
    expect(() => orch.dispatch(["d1"], mkGoal({ protocol: "rao/9" }))).toThrow();
    expect(() => orch.dispatch(["d1"], mkGoal({ agent: "claude" }))).toThrow();
  });

  it("collect: unknown goalId is neither success nor fail", async () => {
    const transport = new FakeTransport(true);
    const orch = await mkOrch({ transport });
    const collected = await orch.collect(["missing-id"], 1_000);
    expect(collected.matrix[0].status).toBe("unknown");
    expect(collected.failCount).toBe(0);
    expect(collected.successCount).toBe(0);
  });

  it("status() snapshot shape; status of unknown -> GOAL_NOT_FOUND", async () => {
    const transport = new FakeTransport(true);
    const orch = await mkOrch({ transport });
    const [d] = orch.dispatch(["d1"], mkGoal());
    await waitChannel(transport, d.goalId);
    const snap = orch.status(d.goalId);
    expect(snap.goalId).toBe(d.goalId);
    expect(snap.state).toBe("running");
    expect(() => orch.status("nope")).toThrow(RaoError);
    transport.channels.get(d.goalId)!.push("result", SUCCESS("A"), 1);
    await orch.getRun(d.goalId)!.waitTerminal(5_000);
    expect(orch.status(d.goalId).state).toBe("done");
  });
});
