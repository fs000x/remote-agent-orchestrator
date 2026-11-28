/**
 * Runtime engine tests (rda.mjs + protocol.mjs) — ported from the Python
 * tests/rda_local suite. Process-level tests drive the real CLI (goal on
 * stdin, events on stdout, injections on stdin); embedded tests drive
 * GoalEngine directly with a captured sink.
 */
import { spawn } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { GoalEngine, EXIT_TIMEOUT } from "../src/runtime/rda.mjs";

const RDA_PATH = fileURLToPath(new URL("../src/runtime/rda.mjs", import.meta.url));

/* ---------------- helpers ---------------- */

export interface Inject { afterMs: number; line: string }

function runRda(
  goal: unknown,
  injections: Inject[] = [],
): Promise<{ events: any[]; exitCode: number | null }> {
  return new Promise((resolve) => {
    const child = spawn(process.execPath, [RDA_PATH, "run-goal"], {
      stdio: ["pipe", "pipe", "pipe"],
    });
    const events: any[] = [];
    const timers: NodeJS.Timeout[] = [];
    let buf = "";
    child.stdout!.on("data", (c) => {
      buf += c.toString("utf8");
      for (;;) {
        const nl = buf.indexOf("\n");
        if (nl < 0) break;
        const line = buf.slice(0, nl).trim();
        buf = buf.slice(nl + 1);
        if (!line) continue;
        try { events.push(JSON.parse(line)); } catch { /* drop */ }
      }
    });
    child.stdin!.write(JSON.stringify(goal) + "\n");
    for (const inj of injections) {
      timers.push(setTimeout(() => {
        if (child.stdin!.writable) child.stdin!.write(inj.line + "\n");
      }, inj.afterMs));
    }
    child.on("close", (code) => {
      for (const t of timers) clearTimeout(t);
      resolve({ events, exitCode: code });
    });
  });
}

function kinds(events: any[]): string[] {
  return events.map((e) => e.kind);
}

function resultOf(events: any[]): any {
  return events.find((e) => e.kind === "result")?.data;
}

function mkGoal(over: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    protocol: "rao/1",
    goalId: "t-goal",
    deviceId: "local",
    objective: "test objective",
    constraints: { timeoutMs: 15000 },
    ...over,
  };
}

/* ---------------- harness ---------------- */

let dir: string;

beforeEach(async () => {
  dir = await mkdtemp(join(tmpdir(), "rao-rda-"));
});

afterEach(async () => {
  await rm(dir, { recursive: true, force: true });
});

/* ---------------- process-level (wire protocol) ---------------- */

describe("rda.mjs: wire protocol", () => {
  it("success goal: started → progress → step_result → result(success), exit 0", async () => {
    const { events, exitCode } = await runRda(mkGoal({
      plan: {
        planner: "main-agent",
        steps: [{
          id: "s1", type: "shell", command: "echo marker-123",
          success: { exitCode: 0, stdoutContains: "marker-123" },
        }],
      },
    }));
    expect(kinds(events)).toEqual(["started", "progress", "step_result", "result"]);
    expect(resultOf(events).status).toBe("success");
    expect(resultOf(events).steps[0].status).toBe("ok");
    expect(resultOf(events).metrics.stepAttempts).toBe(1);
    expect(exitCode).toBe(0);
    // every event is protocol-tagged and sequenced by ts
    for (const ev of events) {
      expect(ev.protocol).toBe("rao/1");
      expect(ev.goalId).toBe("t-goal");
    }
  });

  it("failed required step → result(failed), exit 0 (exit code only signals timeout/cancel)", async () => {
    const { events, exitCode } = await runRda(mkGoal({
      plan: {
        planner: "main-agent",
        steps: [{ id: "s1", type: "shell", command: 'node -e "process.exit(3)"' }],
      },
    }));
    expect(resultOf(events).status).toBe("failed");
    expect(kinds(events)).toContain("error");
    expect(exitCode).toBe(0);
  });

  it("forbidden command pattern blocks the step", async () => {
    const { events } = await runRda(mkGoal({
      constraints: { timeoutMs: 15000, forbidden: ["rm -rf"] },
      plan: {
        planner: "main-agent",
        steps: [{ id: "s1", type: "shell", command: "echo rm -rf /" }],
      },
    }));
    expect(kinds(events)).toContain("warning");
    expect(resultOf(events).status).toBe("failed");
    const warn = events.find((e) => e.kind === "warning");
    expect(warn.data.message).toContain("forbidden");
  });

  it("retry then fallback success counts retries in metrics", async () => {
    const { events } = await runRda(mkGoal({
      plan: {
        planner: "main-agent",
        steps: [{
          id: "s1", type: "shell",
          command: "echo retry-bait",
          success: { exitCode: 0, stdoutContains: "never-matches" },
          retries: 1, retryBackoffMs: 100,
          fallbacks: [{ id: "fb1", type: "shell", command: "echo fallback-ok" }],
        }],
      },
    }));
    expect(resultOf(events).status).toBe("success");
    // fallback variant succeeded on its first attempt; the retry attempt
    // on the primary variant is counted in metrics.retries
    expect(resultOf(events).steps[0]).toMatchObject({
      status: "fallback_ok", attempts: 1,
    });
    expect(resultOf(events).metrics.retries).toBe(1);
  });

  it("artifact step collects existing files into result.artifacts", async () => {
    const f = join(dir, "out.txt");
    await writeFile(f, "payload");
    const { events } = await runRda(mkGoal({
      workspace: dir,
      plan: {
        planner: "main-agent",
        steps: [{ id: "a1", type: "artifact", paths: ["out.txt"] }],
      },
    }));
    expect(resultOf(events).status).toBe("success");
    expect(resultOf(events).artifacts).toHaveLength(1);
    expect(resultOf(events).artifacts[0].size).toBe(7);
  });

  it("invalid goal → RAO_GOAL_INVALID error event, exit 2", async () => {
    const { events, exitCode } = await runRda(mkGoal({
      protocol: "rao/9",
      plan: undefined,
    }));
    expect(events[0].kind).toBe("error");
    expect(events[0].data.message).toContain("RAO_GOAL_INVALID");
    expect(exitCode).toBe(2);
  });

  it("plan-less goal → recon → ask; steered plan executes", async () => {
    const steer = JSON.stringify({
      type: "steer",
      askId: "plan-req-1",
      answer: {
        plan: {
          planner: "main-agent",
          steps: [{ id: "p1", type: "shell", command: "echo steered-ok" }],
        },
      },
    });
    const { events, exitCode } = await runRda(mkGoal(), [
      { afterMs: 800, line: steer },
    ]);
    expect(kinds(events)).toContain("recon");
    expect(kinds(events)).toContain("ask");
    expect(resultOf(events).status).toBe("success");
    expect(resultOf(events).assumptions).toContain("plan received via steer at runtime");
    expect(exitCode).toBe(0);
  });

  it("cancel injection mid-goal → result(cancelled), exit 4", async () => {
    const { events, exitCode } = await runRda(mkGoal({
      plan: {
        planner: "main-agent",
        steps: [{
          id: "w1", type: "wait_file",
          path: join(dir, "never-appears"), timeoutMs: 9000,
        }],
      },
    }), [{ afterMs: 800, line: JSON.stringify({ type: "cancel", reason: "test" }) }]);
    expect(resultOf(events).status).toBe("cancelled");
    expect(exitCode).toBe(4);
  });

  it("ping injection → progress ping pong (long wait_file keeps engine alive)", async () => {
    const { events } = await runRda(mkGoal({
      plan: {
        planner: "main-agent",
        steps: [{
          id: "w1", type: "wait_file",
          path: join(dir, "nope"), timeoutMs: 3000,
        }],
      },
    }), [{ afterMs: 400, line: JSON.stringify({ type: "ping" }) }]);
    const pings = events.filter(
      (e) => e.kind === "progress" && e.data.ping === "pong");
    expect(pings.length).toBeGreaterThanOrEqual(1);
  });
});

/* ---------------- embedded engine (injection mechanics) ---------------- */

describe("GoalEngine embedded", () => {
  it("steer skip:s2 → s2 skipped → partial success", async () => {
    const events: any[] = [];
    const engine = new GoalEngine(mkGoal({
      plan: {
        planner: "main-agent",
        steps: [
          { id: "s1", type: "shell", command: "echo one" },
          { id: "s2", type: "wait_file", path: join(dir, "nope"), timeoutMs: 8000 },
        ],
      },
    }) as any, { sink: (e) => events.push(e) });
    engine.inject({ type: "steer", message: "skip:s2" });
    const code = await engine.run();
    const res = events.find((e) => e.kind === "result").data;
    expect(res.status).toBe("partial");
    expect(res.summary).toContain("skipped optional");
    expect(res.steerNotes).toEqual([{ directive: "skip:s2" }]);
    expect(code).toBe(0);
  });

  it("budget exhausted (maxSteps=2, 3 steps) → timeout, exit EXIT_TIMEOUT", async () => {
    const events: any[] = [];
    const engine = new GoalEngine(mkGoal({
      constraints: { timeoutMs: 15000, maxSteps: 2 },
      plan: {
        planner: "main-agent",
        steps: [
          { id: "s1", type: "shell", command: "echo 1" },
          { id: "s2", type: "shell", command: "echo 2" },
          { id: "s3", type: "shell", command: "echo 3" },
        ],
      },
    }) as any, { sink: (e) => events.push(e) });
    const code = await engine.run();
    expect(code).toBe(EXIT_TIMEOUT);
    const res = events.find((e) => e.kind === "result").data;
    expect(res.status).toBe("timeout");
    expect(res.summary).toBe("step budget exhausted");
    expect(res.metrics.stepAttempts).toBe(2);
    expect(events.some(
      (e) => e.kind === "error" && e.data.message.includes("RAO_BUDGET_EXCEEDED"),
    )).toBe(true);
  });

  it("embedded inject cancel → cancelled", async () => {
    const events: any[] = [];
    const engine = new GoalEngine(mkGoal({
      plan: {
        planner: "main-agent",
        steps: [{
          id: "w1", type: "wait_file",
          path: join(dir, "nope"), timeoutMs: 8000,
        }],
      },
    }) as any, { sink: (e) => events.push(e) });
    setTimeout(() => engine.inject({ type: "cancel", reason: "embedded" }), 300);
    const code = await engine.run();
    expect(code).toBe(4);
    expect(events.find((e) => e.kind === "result").data.status).toBe("cancelled");
  });

  it("steer run: directive appends an extra optional step", async () => {
    const events: any[] = [];
    const engine = new GoalEngine(mkGoal({
      plan: {
        planner: "main-agent",
        steps: [{ id: "s1", type: "shell", command: "echo base" }],
      },
    }) as any, { sink: (e) => events.push(e) });
    engine.inject({ type: "steer", message: "run:echo extra-run-ok" });
    const code = await engine.run();
    expect(code).toBe(0);
    const res = events.find((e) => e.kind === "result").data;
    expect(res.status).toBe("success");
    expect(res.steerNotes).toEqual([{ directive: "run:echo extra-run-ok" }]);
    // 3 attempts total: base + running-phase event mapping = 2 steps executed
    expect(res.metrics.stepAttempts).toBeGreaterThanOrEqual(2);
    const extra = res.steps.find((s: any) => s.id === "steer-0");
    expect(extra.status).toBe("ok");
  });

  it("goal deadline passes between steps → timeout", async () => {
    // deadline enforcement (like Python) happens at the top of the step
    // loop: quick successful steps until the wall clock passes the budget
    const events: any[] = [];
    const engine = new GoalEngine(mkGoal({
      constraints: { timeoutMs: 700 },
      plan: {
        planner: "main-agent",
        steps: [
          { id: "s1", type: "shell", command: 'node -e "setTimeout(()=>process.exit(0),250)"', success: { exitCode: 0 } },
          { id: "s2", type: "shell", command: 'node -e "setTimeout(()=>process.exit(0),250)"', success: { exitCode: 0 } },
          { id: "s3", type: "shell", command: 'node -e "setTimeout(()=>process.exit(0),250)"', success: { exitCode: 0 } },
          { id: "s4", type: "shell", command: 'node -e "setTimeout(()=>process.exit(0),250)"', success: { exitCode: 0 } },
        ],
      },
    }) as any, { sink: (e) => events.push(e) });
    const code = await engine.run();
    expect(code).toBe(3);
    const res = events.find((e) => e.kind === "result").data;
    expect(res.status).toBe("timeout");
    expect(res.summary).toBe("goal deadline exceeded");
  });
});
