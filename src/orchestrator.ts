/**
 * Orchestrator core: goal lifecycle, global/per-device concurrency slots,
 * budget enforcement (fail-closed, orchestrator-side backstop), event-mirror
 * reconcile, RunStore persistence, collect aggregation.
 *
 * Ported from src/rao/orchestrator.py (v0.11.1) with identical semantics:
 *  - LLM decides, deterministic code enforces;
 *  - terminal states are sticky (first decision wins; late RESULT ignored);
 *  - enforcement decides the state BEFORE force-cancel;
 *  - daemon mode reconnects and dedupes replayed events by seq.
 */
import { EventEmitter } from "node:events";
import { randomUUID } from "node:crypto";
import { mkdir, readdir, readFile, rename, writeFile } from "node:fs/promises";
import { join } from "node:path";
import {
  ErrorCode,
  LIMITS,
  PROTOCOL_VERSION,
  DispatchedGoalSchema,
  GoalSchema,
  RaoError,
  isTerminalState,
  type DispatchedGoal,
  type RaoEvent,
  type RunStateValue,
} from "./protocol.js";
import type { DeviceRecord, DeviceRegistry } from "./devices.js";

/* ------------------------------------------------------------------ *
 * Result contract (RDA -> orchestrator, `result` event payload)
 * ------------------------------------------------------------------ */
export interface RunResult {
  status: "success" | "partial" | "failed" | "timeout" | "cancelled";
  summary: string;
  steps: Array<Record<string, unknown>>;
  artifacts: Array<Record<string, unknown>>;
  assumptions: string[];
  steerNotes: Array<Record<string, unknown>>;
  metrics: { stepAttempts: number; retries: number; durationMs: number };
}

export function emptyResult(
  status: RunResult["status"],
  summary: string,
  stepAttempts = 0,
  durationMs = 0,
): RunResult {
  return {
    status,
    summary,
    steps: [],
    artifacts: [],
    assumptions: [],
    steerNotes: [],
    metrics: { stepAttempts, retries: 0, durationMs },
  };
}

/* ------------------------------------------------------------------ *
 * Channel + transport abstractions (fake-able in tests)
 * ------------------------------------------------------------------ */
export interface GoalChannel {
  goalId: string;
  /** ordered event buffer (replayed from seq 0 after daemon reconnect) */
  events: RaoEvent[];
  /** per-event daemon seq (null when the transport cannot number them) */
  event_seqs: Array<number | null>;
  stderr_text: string[];
  exit_code: number | null;
  killed: boolean;
  supports_resume: boolean;
  /** resolve when an event exists at index >= since; [null, since] on timeout */
  waitEvent(since: number, timeoutMs: number): Promise<[RaoEvent | null, number]>;
  /** send steer/cancel into the live session; false when undeliverable */
  writeLine(obj: Record<string, unknown>): Promise<boolean> | boolean;
  alive(): boolean;
  kill(): void;
  close(): void;
  /** null when still running after timeoutMs */
  waitExit(timeoutMs: number): Promise<number | null>;
}

export interface Transport {
  openSession(
    goalId: string,
    device: DeviceRecord,
    goal: DispatchedGoal,
  ): Promise<GoalChannel>;
  /** daemon-mode reconnect: replays the goal's buffered events */
  openResumeSession?(goalId: string, device: DeviceRecord): Promise<GoalChannel>;
  /** idempotent runtime preparation (provision compares codeHash) */
  provision?(device: DeviceRecord): Promise<unknown>;
  /** control-plane steer/cancel on a fresh connection (immune to bridge state) */
  steer?(device: DeviceRecord, goalId: string, payload: Record<string, unknown>): Promise<boolean>;
  /** authoritative event buffer (mirror reconcile at finalize); null = unknown */
  goalEvents?(device: DeviceRecord, goalId: string): Promise<RaoEvent[] | null>;
}

/* ------------------------------------------------------------------ *
 * RunStore (atomic json-per-goal persistence + report history/diff)
 * ------------------------------------------------------------------ */
export class RunStore {
  constructor(private readonly dir: string) {}

  async save(run: GoalRun): Promise<void> {
    await mkdir(this.dir, { recursive: true });
    const rec = {
      goal: run.goal,
      device: { id: run.device.id, host: run.device.host, user: run.device.user },
      state: run.state,
      error: run.error,
      result: run.result,
      events: run.chan?.events ?? [],
      stderrTail: (run.chan?.stderr_text ?? []).slice(-100),
      dispatchedMs: run.dispatchedMs,
      finishedMs: Date.now(),
    };
    const path = join(this.dir, `${run.goalId}.json`);
    const tmp = `${path}.tmp`;
    await writeFile(tmp, JSON.stringify(rec, null, 1), "utf8");
    await rename(tmp, path);
  }

  /** goals from earlier processes fall back to the persisted record */
  async load(goalId: string): Promise<Record<string, unknown> | null> {
    try {
      const raw = await readFile(join(this.dir, `${goalId}.json`), "utf8");
      return JSON.parse(raw) as Record<string, unknown>;
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code === "ENOENT") return null;
      return null; // corrupt record behaves as missing (Python parity)
    }
  }

  /* -- reports (history & diff) ------------------------------------------ */
  async listReports(): Promise<Array<Record<string, unknown>>> {
    await mkdir(this.dir, { recursive: true });
    const out: Array<Record<string, unknown>> = [];
    for (const name of (await readdir(this.dir)).sort()) {
      if (!name.startsWith("report-") || !name.endsWith(".json")) continue;
      try {
        const data = JSON.parse(await readFile(join(this.dir, name), "utf8")) as Record<string, unknown>;
        out.push({
          file: name,
          generatedMs: data.generatedMs,
          successCount: data.successCount,
          failCount: data.failCount,
          goals: Array.isArray(data.matrix) ? data.matrix.length : 0,
        });
      } catch { /* skip unreadable */ }
    }
    return out;
  }

  async loadReport(name: string): Promise<Record<string, unknown>> {
    if (name.includes("/") || name.includes("\\") || name.includes("..")) {
      throw new RaoError(ErrorCode.InvalidMessage, "report name must be a bare filename");
    }
    const path = join(this.dir, name);
    try {
      return JSON.parse(await readFile(path, "utf8")) as Record<string, unknown>;
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code === "ENOENT") {
        throw new RaoError(ErrorCode.GoalNotFound, `no such report: ${name}`);
      }
      throw err;
    }
  }

  async saveReport(report: Record<string, unknown>): Promise<string> {
    await mkdir(this.dir, { recursive: true });
    const path = join(this.dir, `report-${Date.now()}.json`);
    const tmp = `${path}.tmp`;
    await writeFile(tmp, JSON.stringify(report, null, 1), "utf8");
    await rename(tmp, path);
    return path;
  }

  async saveDag(dag: DagRun): Promise<string> {
    await mkdir(this.dir, { recursive: true });
    const path = join(this.dir, `dag-${dag.dagId}.json`);
    const tmp = `${path}.tmp`;
    await writeFile(tmp, JSON.stringify(dag.snapshot(), null, 1), "utf8");
    await rename(tmp, path);
    return path;
  }

  /** Diff two report dicts by goalId; includes regressions. */
  static diffReports(
    base: Record<string, unknown>,
    newer: Record<string, unknown>,
  ): Record<string, unknown> {
    const index = (rep: Record<string, unknown>) => {
      const m = new Map<string, Record<string, unknown>>();
      for (const row of (rep.matrix ?? []) as Array<Record<string, unknown>>) {
        m.set(String(row.goalId), row);
      }
      return m;
    };
    const ia = index(base);
    const ib = index(newer);
    const changes: Array<Record<string, unknown>> = [];
    for (const gid of [...new Set([...ia.keys(), ...ib.keys()])].sort()) {
      const sa = ia.get(gid)?.status;
      const sb = ib.get(gid)?.status;
      if (sa !== sb) {
        changes.push({
          goalId: gid,
          from: sa ?? "(absent)",
          to: sb ?? "(absent)",
          summary: (ib.get(gid) ?? ia.get(gid))?.summary,
        });
      }
    }
    return {
      baseFile: base.reportFile,
      newerFile: newer.reportFile,
      counts: {
        base: { success: base.successCount, fail: base.failCount },
        newer: { success: newer.successCount, fail: newer.failCount },
      },
      changed: changes,
      regressions: changes.filter(
        (c) => ["success", "partial"].includes(String(c.from)) &&
          !["success", "partial", "(absent)"].includes(String(c.to)),
      ),
    };
  }
}

/* ------------------------------------------------------------------ *
 * GoalRun
 * ------------------------------------------------------------------ */
const KILL_GRACE_MS = 5_000;
const PUMP_SLICE_MS = 1_000;

const RESULT_STATE: Record<string, RunStateValue> = {
  success: "done",
  partial: "done",
  failed: "failed",
  timeout: "timeout",
  cancelled: "cancelled",
};

const UNASK_KINDS = ["progress", "milestone", "step_result", "recon", "warning"];

export class GoalRun {
  readonly goal: DispatchedGoal;
  readonly device: DeviceRecord;
  readonly orch: Orchestrator;
  readonly goalId: string;
  state: RunStateValue = "queued";
  error: string | null = null;
  result: RunResult | null = null;
  chan: GoalChannel | null = null;
  readonly dispatchedMs = Date.now();
  readonly deadlineMs: number;
  cancelRequested = false;
  finalized = false;
  attemptCount = 0;
  slotAcquired = false;
  private lastSeq = 0;
  private readonly wake = new EventEmitter();
  readonly thread: Promise<void>;

  constructor(goal: DispatchedGoal, device: DeviceRecord, orch: Orchestrator) {
    this.goal = goal;
    this.device = device;
    this.orch = orch;
    this.goalId = goal.goalId;
    const timeout = goal.constraints?.timeoutMs ?? LIMITS.goalTimeoutMs;
    this.deadlineMs = this.dispatchedMs + timeout;
    // mirrors Python: the run loop starts with the run object
    this.thread = this.runLoop().catch(() => {});
  }

  /* ---------------- lifecycle ---------------- */

  private async runLoop(): Promise<void> {
    try {
      await this.orch.withSlot(this, async () => {
        if (this.cancelRequested) {
          this.result = emptyResult("cancelled", "cancelled before start");
          this.setState("cancelled");
          return;
        }
        this.setState("running");
        if (this.orch.transport.provision) {
          await this.orch.transport.provision(this.device);
        }
        try {
          this.chan = await this.orch.transport.openSession(
            this.goalId,
            this.device,
            this.goal,
          );
        } catch (err) {
          if (err instanceof RaoError) {
            this.error = `${err.code}: ${err.message}`;
            this.setState("failed");
            return;
          }
          throw err;
        }
        if (this.cancelRequested) {
          this.chan.close();
          this.result = emptyResult("cancelled", "cancelled at session start");
          this.setState("cancelled");
          return;
        }
        if (this.chan.supports_resume) await this.pumpDaemon();
        else await this.pumpSpawn();
      });
    } catch (err) {
      this.error = `orchestrator error: ${String(err)}`;
      this.setState("failed");
    } finally {
      await this.finalize();
    }
  }

  /** spawn mode: single blocking session, no resume */
  private async pumpSpawn(): Promise<void> {
    const chan = this.chan;
    if (!chan) throw new RaoError(ErrorCode.InternalError, "no channel");
    const silenceMs =
      this.goal.constraints?.eventSilenceMs ?? LIMITS.eventSilenceMs;
    let idx = 0;
    let lastEventMs = Date.now();
    for (;;) {
      const [ev, newIdx] = await chan.waitEvent(idx, PUMP_SLICE_MS);
      const now = Date.now();
      if (ev) {
        idx = newIdx;
        lastEventMs = now;
        if (this.applyEvent(ev)) return;
      }
      // enforcement backstop (sticky decisions made BEFORE force-cancel)
      if (await this.enforceBudget(now)) return;
      if (now - lastEventMs > silenceMs) {
        this.setState("failed");
        await this.forceCancel("event silence watchdog");
        return;
      }
      if (chan.exit_code !== null && idx >= chan.events.length) break;
    }
    // exited without a result event
    const cancelled = chan.killed || this.cancelRequested;
    this.result = emptyResult(
      cancelled ? "cancelled" : "failed",
      `RDA exited (code=${chan.exit_code}) without a result event`,
      this.attemptCount,
      Date.now() - this.dispatchedMs,
    );
    this.setState(cancelled ? "cancelled" : "failed");
  }

  /** daemon mode: reconnect + attach on loss, replayed events deduped by seq */
  private async pumpDaemon(): Promise<void> {
    const silenceMs =
      this.goal.constraints?.eventSilenceMs ?? LIMITS.eventSilenceMs;
    let attempt = 0;
    for (;;) {
      let idx = 0;
      let lastEventMs = Date.now();
      try {
        for (;;) {
          const chan = this.chan;
          if (!chan) {
            throw new RaoError(ErrorCode.TransportLost, "channel missing");
          }
          const [ev, newIdx] = await chan.waitEvent(idx, PUMP_SLICE_MS);
          const now = Date.now();
          if (ev) {
            idx = newIdx;
            const seq = chan.event_seqs[idx - 1] ?? null;
            if (seq !== null && seq <= this.lastSeq) continue; // replayed dup
            if (seq !== null) this.lastSeq = seq;
            lastEventMs = now;
            if (this.applyEvent(ev)) return;
          }
          if (await this.enforceBudget(now)) return;
          if (!chan.alive() && ev === null) {
            throw new RaoError(ErrorCode.TransportLost, "link lost");
          }
          if (now - lastEventMs > silenceMs) {
            this.setState("failed");
            await this.forceCancel("event silence watchdog");
            return;
          }
        }
      } catch (err) {
        if (
          !(err instanceof RaoError && err.code === ErrorCode.TransportLost)
        ) {
          throw err;
        }
      }
      if (this.isTerminal() || this.cancelRequested) return;
      attempt++;
      if (attempt > this.orch.resumeMax) {
        this.error = `resume budget exhausted after ${attempt} attempts`;
        this.setState("failed");
        return;
      }
      const backoff = Math.min(
        LIMITS.resumeBackoffMs * attempt,
        LIMITS.resumeBackoffCapMs,
      );
      await sleep(backoff);
      if (!this.orch.transport.openResumeSession) continue;
      try {
        this.chan?.close();
        this.chan = await this.orch.transport.openResumeSession(
          this.goalId,
          this.device,
        );
      } catch {
        /* next iteration retries */
      }
    }
  }

  /** returns true when a terminal `result` event was applied */
  private applyEvent(ev: RaoEvent): boolean {
    const kind = ev.kind;
    if (kind === "ask") {
      this.setState("ask_waiting");
      return false;
    }
    if (UNASK_KINDS.includes(kind)) this.setState("running");
    if (
      kind === "progress" &&
      (ev.data as Record<string, unknown>)?.phase === "running"
    ) {
      this.attemptCount++;
    }
    if (kind === "result") {
      if (this.isTerminal()) return true; // deadline/cancel already decided
      this.result = ev.data as unknown as RunResult;
      this.setState(RESULT_STATE[this.result.status] ?? "failed");
      return true;
    }
    return false;
  }

  /** orchestrator-side backstop; true when it terminated the run.
   *  Awaits force-cancel so the decision (state + error) is complete
   *  before the pump returns and finalize synthesizes the result. */
  private async enforceBudget(now: number): Promise<boolean> {
    const maxSteps = this.goal.constraints?.maxSteps ?? LIMITS.maxSteps;
    if (this.attemptCount > maxSteps) {
      this.setState("timeout");
      await this.forceCancel(`BUDGET_EXCEEDED (orchestrator: steps>${maxSteps})`);
      return true;
    }
    if (now >= this.deadlineMs) {
      this.setState("timeout");
      await this.forceCancel("goal deadline exceeded (orchestrator)");
      return true;
    }
    return false;
  }

  /* ---------------- control ---------------- */

  async forceCancel(reason: string): Promise<void> {
    if (this.orch.transport.steer) {
      try {
        await this.orch.transport.steer(
          this.device,
          this.goalId,
          { type: "cancel", reason },
        );
      } catch {
        /* fall through to channel kill */
      }
    } else if (this.chan) {
      try {
        await this.chan.writeLine({ type: "cancel", reason });
      } catch {
        /* kill below */
      }
    }
    if (this.chan) {
      const exited = await this.chan.waitExit(KILL_GRACE_MS);
      if (exited === null) this.chan.kill();
    }
    this.error = reason;
  }

  setState(s: RunStateValue): void {
    if (isTerminalState(this.state)) return; // sticky: first decision wins
    this.state = s;
    this.wake.emit("state", s);
  }

  isTerminal(): boolean {
    return isTerminalState(this.state);
  }

  async waitTerminal(timeoutMs: number): Promise<boolean> {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      if (this.isTerminal() && this.finalized) return true;
      if (Date.now() >= deadline) {
        return this.isTerminal() && this.finalized;
      }
      await sleep(50);
    }
  }

  async steer(
    message?: string,
    askId?: string,
    answer?: unknown,
  ): Promise<boolean> {
    const payload: Record<string, unknown> = { type: "steer" };
    if (askId != null) {
      payload.askId = askId;
      payload.answer = answer;
    } else {
      payload.message = message;
    }
    // control-plane preferred (fresh connection, immune to bridge state)
    if (this.orch.transport.steer) {
      for (let attempt = 0; attempt < LIMITS.steerRetries; attempt++) {
        try {
          if (
            await this.orch.transport.steer(this.device, this.goalId, payload)
          ) {
            return true;
          }
        } catch {
          /* retry */
        }
        if (this.isTerminal()) return false;
        await sleep(1_000);
      }
      return false;
    }
    if (this.chan) return await this.chan.writeLine(payload);
    return false;
  }

  async cancel(reason = "user requested"): Promise<boolean> {
    if (this.isTerminal()) return false;
    this.cancelRequested = true;
    this.setState("cancelled"); // sticky: late RESULT won't override
    if (this.orch.transport.steer) {
      try {
        await this.orch.transport.steer(
          this.device,
          this.goalId,
          { type: "cancel", reason },
        );
      } catch {
        /* kill below */
      }
    } else if (this.chan) {
      try {
        await this.chan.writeLine({ type: "cancel", reason });
      } catch {
        /* kill below */
      }
    }
    if (this.chan) {
      const exited = await this.chan.waitExit(KILL_GRACE_MS);
      if (exited === null) this.chan.kill();
    }
    this.error = reason;
    return true;
  }

  snapshot(maxEvents = 50): Record<string, unknown> {
    return {
      goalId: this.goalId,
      deviceId: this.goal.deviceId,
      state: this.state,
      error: this.error,
      attemptCount: this.attemptCount,
      result: this.result,
      events: this.chan?.events.slice(-maxEvents) ?? [],
      stderrTail: this.chan?.stderr_text.slice(-20) ?? [],
    };
  }

  private async finalize(): Promise<void> {
    if (this.result === null) {
      const status: RunResult["status"] =
        this.state === "timeout"
          ? "timeout"
          : this.state === "cancelled"
            ? "cancelled"
            : this.state === "done"
              ? "success"
              : "failed";
      this.result = emptyResult(
        status,
        this.error ?? "run ended without a result event",
        this.attemptCount,
        Date.now() - this.dispatchedMs,
      );
    }
    // event-mirror reconcile from the daemon's authoritative buffer
    if (this.orch.transport.goalEvents && this.chan) {
      try {
        const evs = await this.orch.transport.goalEvents(
          this.device,
          this.goalId,
        );
        if (evs && evs.length > this.chan.events.length) {
          this.chan.events = evs;
          this.chan.event_seqs = evs.map((_, i) => i + 1);
        }
      } catch {
        /* best effort */
      }
    }
    this.chan?.close();
    try {
      await this.orch.runStore.save(this);
    } catch {
      /* persistence is best-effort; the run result is already decided */
    }
    this.finalized = true;
    this.wake.emit("finalized", this.state);
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

/* ------------------------------------------------------------------ *
 * Orchestrator
 * ------------------------------------------------------------------ */
export interface DispatchEntry {
  goalId: string;
  deviceId: string;
  state: RunStateValue;
}

/* ------------------------------------------------------------------ *
 * Cross-device goal DAG: nodes dispatch in topological order; a node
 * starts only when its dependencies satisfy the policy (DESIGN.md §11.3).
 * ------------------------------------------------------------------ */
export const DAG_POLICIES = ["success", "lenient"] as const;
export type DagPolicy = (typeof DAG_POLICIES)[number];
const DAG_POLL_MS = 300;

interface DagNodeRec {
  key: string;
  deviceId: string;
  goal: Record<string, unknown>;
  goalId: string | null;
  state: "pending" | "queued" | "running" | "done" | "failed" | "timeout" | "cancelled" | "skipped";
  status: string | null;
  reason: string | null;
  run: GoalRun | null;
}

export class DagRun {
  readonly dagId: string;
  readonly policy: DagPolicy;
  readonly edges: Array<[string, string]>;
  readonly nodes = new Map<string, DagNodeRec>();
  private readonly deps = new Map<string, string[]>();
  private readonly orch: Orchestrator;
  readonly thread: Promise<void>;

  constructor(
    dagId: string,
    nodes: Array<{ key: string; deviceId: string; goal: Record<string, unknown> }>,
    edges: Array<[string, string]>,
    policy: DagPolicy,
    orch: Orchestrator,
  ) {
    this.dagId = dagId;
    this.policy = policy;
    this.orch = orch;
    for (const n of nodes) {
      this.nodes.set(n.key, {
        key: n.key, deviceId: n.deviceId, goal: n.goal,
        goalId: null, state: "pending", status: null, reason: null, run: null,
      });
    }
    this.edges = edges.map((e) => [e[0], e[1]]);
    for (const [frm, to] of this.edges) {
      const arr = this.deps.get(to) ?? [];
      arr.push(frm);
      this.deps.set(to, arr);
    }
    this.thread = this.schedule().catch(() => {});
  }

  /* -- graph helpers ------------------------------------------------------ */
  private depStatusOk(dep: DagNodeRec): boolean {
    if (this.policy === "lenient") {
      return dep.status === "success" || dep.status === "partial";
    }
    return dep.status === "success";
  }

  /** all nodes transitively depending on any key in `keys` */
  private downstream(keys: string[]): Set<string> {
    const out = new Set<string>();
    const frontier = [...keys];
    while (frontier.length) {
      const k = frontier.pop()!;
      for (const [frm, to] of this.edges) {
        if (frm === k && !out.has(to)) {
          out.add(to);
          frontier.push(to);
        }
      }
    }
    return out;
  }

  /* -- scheduler ------------------------------------------------------------ */
  private async schedule(): Promise<void> {
    try {
      for (;;) {
        let progressed = false;
        const terminal = ["failed", "timeout", "cancelled", "skipped"];
        // 1. propagate failures to downstream (transitive skip)
        const failedKeys = [...this.nodes.values()]
          .filter((n) => terminal.includes(n.state))
          .map((n) => n.key);
        for (const k of this.downstream(failedKeys)) {
          const n = this.nodes.get(k)!;
          if (n.state === "pending") {
            n.state = "skipped";
            // name the actual failed dependency (Python referenced the
            // skipped node itself — pure diagnostics, improved here)
            const badDep =
              (this.deps.get(k) ?? []).find(
                (d) => terminal.includes(this.nodes.get(d)!.state),
              ) ?? k;
            n.reason = `dependency ${badDep} did not succeed`;
            progressed = true;
          }
        }
        // 2. policy-violating deps: skip pending nodes whose deps are all
        //    terminal but at least one status violates the policy
        const allTerminal = ["done", ...terminal];
        for (const n of this.nodes.values()) {
          if (n.state !== "pending") continue;
          const deps = this.deps.get(n.key) ?? [];
          if (deps.length &&
              deps.every((d) => allTerminal.includes(this.nodes.get(d)!.state))) {
            const bad = deps.filter(
              (d) => !this.depStatusOk(this.nodes.get(d)!));
            if (bad.length) {
              const badKey = bad[0]!;
              n.state = "skipped";
              n.reason =
                `dependency ${badKey} status=${this.nodes.get(badKey)!.status} violates policy=${this.policy}`;
              progressed = true;
            }
          }
        }
        // 3. launch ready nodes
        for (const n of this.nodes.values()) {
          if (n.state !== "pending") continue;
          const deps = this.deps.get(n.key) ?? [];
          const ready = !deps.length || deps.every(
            (d) => this.nodes.get(d)!.state === "done" &&
              this.depStatusOk(this.nodes.get(d)!));
          if (ready) {
            const g = { ...n.goal, goalId: randomUUID(), deviceId: n.deviceId };
            const [d] = this.orch.dispatch([n.deviceId], g);
            if (d) {
              n.goalId = d.goalId;
              n.state = "queued";
              n.run = this.orch.getRun(d.goalId) ?? null;
              progressed = true;
            }
          }
        }
        // 4. refresh terminal nodes
        for (const n of this.nodes.values()) {
          if ((n.state === "queued" || n.state === "running") && n.run) {
            if (n.run.isTerminal()) {
              n.state = n.run.state === "done" ? "done" : n.run.state as DagNodeRec["state"];
              n.status = n.run.result?.status ?? null;
              progressed = true;
            } else if (n.state === "queued") {
              n.state = "running";
            }
          }
        }
        if ([...this.nodes.values()].every(
          (n) => ["done", ...terminal].includes(n.state),
        )) {
          break;
        }
        if (!progressed) await sleep(DAG_POLL_MS);
      }
    } catch (err) {
      // scheduler bug: fail remaining pending nodes
      for (const n of this.nodes.values()) {
        if (["pending", "queued", "running"].includes(n.state)) {
          n.state = "failed";
          n.reason = `dag scheduler error: ${String(err)}`;
        }
      }
    } finally {
      try {
        await this.orch.runStore.saveDag(this);
      } catch { /* persistence best-effort */ }
    }
  }

  snapshot(): Record<string, unknown> {
    return {
      dagId: this.dagId,
      policy: this.policy,
      edges: this.edges,
      nodes: [...this.nodes.values()].map((n) => ({
        key: n.key, deviceId: n.deviceId, goalId: n.goalId,
        state: n.state, status: n.status, reason: n.reason,
      })),
    };
  }
}

export class Orchestrator {
  readonly registry: DeviceRegistry;
  readonly transport: Transport;
  readonly runsDir: string;
  readonly maxConcurrentGoals: number;
  readonly resumeMax: number;
  readonly runStore: RunStore;
  readonly runs = new Map<string, GoalRun>();
  readonly dags = new Map<string, DagRun>();

  constructor(opts: {
    registry: DeviceRegistry;
    transport: Transport;
    runsDir?: string;
    maxConcurrentGoals?: number;
    resumeMax?: number;
  }) {
    this.registry = opts.registry;
    this.transport = opts.transport;
    this.runsDir = opts.runsDir ?? join(process.cwd(), "runs");
    this.maxConcurrentGoals =
      opts.maxConcurrentGoals ?? LIMITS.globalConcurrency;
    this.resumeMax = opts.resumeMax ?? LIMITS.resumeAttempts;
    this.runStore = new RunStore(this.runsDir);
  }

  /* ---------------- slots ---------------- */

  private deviceInflight(deviceId: string): number {
    let n = 0;
    for (const run of this.runs.values()) {
      if (
        run.goal.deviceId === deviceId &&
        run.slotAcquired &&
        !run.isTerminal()
      ) {
        n++;
      }
    }
    return n;
  }

  private globalInflight(): number {
    let n = 0;
    for (const run of this.runs.values()) {
      if (run.slotAcquired && !run.isTerminal()) n++;
    }
    return n;
  }

  /** @internal — GoalRun acquires its concurrency slot through this */
  async withSlot(
    run: GoalRun,
    fn: () => Promise<void>,
  ): Promise<void> {
    const perDevice =
      this.registry.get(run.goal.deviceId)?.maxConcurrentGoals ??
      LIMITS.deviceConcurrency;
    for (;;) {
      if (
        run.cancelRequested ||
        run.isTerminal() ||
        (this.globalInflight() < this.maxConcurrentGoals &&
          this.deviceInflight(run.goal.deviceId) < perDevice)
      ) {
        break;
      }
      await sleep(50);
    }
    if (run.cancelRequested || run.isTerminal()) {
      // never took a slot; the callback emits the cancelled result
      await fn();
      return;
    }
    run.slotAcquired = true;
    try {
      await fn();
    } finally {
      run.slotAcquired = false;
    }
  }

  /* ---------------- dispatch ---------------- */

  dispatch(deviceIds: string[], goal: unknown): DispatchEntry[] {
    if (!Array.isArray(deviceIds) || deviceIds.length === 0) {
      throw new RaoError(ErrorCode.InvalidMessage, "no target devices");
    }
    if (deviceIds.length > LIMITS.dispatchBatch) {
      throw new RaoError(
        ErrorCode.RateLimited,
        `dispatch batch exceeds limit (${deviceIds.length} > ${LIMITS.dispatchBatch})`,
      );
    }
    const parsed = GoalSchema.parse(goal);
    const base = { ...parsed, protocol: PROTOCOL_VERSION };
    const out: DispatchEntry[] = [];
    for (const deviceId of deviceIds) {
      const device = this.registry.require(deviceId);
      const g = DispatchedGoalSchema.parse({
        ...base,
        goalId: randomUUID(),
        deviceId,
      });
      const run = new GoalRun(g, device, this);
      this.runs.set(g.goalId, run);
      out.push({ goalId: g.goalId, deviceId, state: run.state });
    }
    return out;
  }

  /* ---------------- DAG dispatch (v0.2) ---------------- */

  dispatchDag(
    nodes: Array<{ key: string; deviceId: string; goal: unknown }>,
    edges: Array<[string, string]>,
    policy: DagPolicy = "success",
  ): DagRun {
    if (!DAG_POLICIES.includes(policy)) {
      throw new RaoError(
        ErrorCode.InvalidMessage,
        `policy must be one of ${DAG_POLICIES.join(",")}`,
      );
    }
    const keys = nodes.map((n) => n.key);
    if (!keys.length || new Set(keys).size !== keys.length) {
      throw new RaoError(
        ErrorCode.InvalidMessage,
        "RAO_DAG_INVALID: node keys must be non-empty and unique",
      );
    }
    const keyset = new Set(keys);
    for (const n of nodes) {
      if (!this.registry.get(n.deviceId)) {
        throw new RaoError(ErrorCode.DeviceNotFound, `unknown deviceId: ${n.deviceId}`);
      }
      const g = { protocol: PROTOCOL_VERSION, ...(n.goal as Record<string, unknown>) };
      const probe = DispatchedGoalSchema.safeParse({ ...g, goalId: "probe", deviceId: n.deviceId });
      if (!probe.success) {
        throw new RaoError(
          ErrorCode.InvalidMessage,
          `RAO_DAG_INVALID node '${n.key}': ${probe.error.issues.map((i) => i.message).join("; ")}`,
        );
      }
    }
    for (const e of edges) {
      if (!Array.isArray(e) || e.length !== 2 ||
          !keyset.has(e[0]) || !keyset.has(e[1])) {
        throw new RaoError(ErrorCode.InvalidMessage, `RAO_DAG_INVALID edge: ${JSON.stringify(e)}`);
      }
      if (e[0] === e[1]) {
        throw new RaoError(ErrorCode.InvalidMessage, `RAO_DAG_INVALID self-edge: ${JSON.stringify(e)}`);
      }
    }
    // cycle detection (Kahn)
    const indeg = new Map<string, number>(keys.map((k) => [k, 0]));
    const adj = new Map<string, string[]>();
    for (const [frm, to] of edges) {
      indeg.set(to, (indeg.get(to) ?? 0) + 1);
      const arr = adj.get(frm) ?? [];
      arr.push(to);
      adj.set(frm, arr);
    }
    const queue = keys.filter((k) => indeg.get(k) === 0);
    let seen = 0;
    while (queue.length) {
      const k = queue.pop()!;
      seen++;
      for (const nxt of adj.get(k) ?? []) {
        const d = (indeg.get(nxt) ?? 0) - 1;
        indeg.set(nxt, d);
        if (d === 0) queue.push(nxt);
      }
    }
    if (seen !== keys.length) {
      throw new RaoError(ErrorCode.InvalidMessage, "RAO_DAG_INVALID: cycle detected");
    }
    const dagId = randomUUID();
    const dag = new DagRun(
      dagId,
      nodes.map((n) => ({
        key: n.key,
        deviceId: n.deviceId,
        goal: { protocol: PROTOCOL_VERSION, ...(n.goal as Record<string, unknown>) },
      })),
      edges,
      policy,
      this,
    );
    this.dags.set(dagId, dag);
    return dag;
  }

  getDag(dagId: string): DagRun | undefined {
    return this.dags.get(dagId);
  }

  /* ---------------- queries / control ---------------- */

  getRun(goalId: string): GoalRun | undefined {
    return this.runs.get(goalId);
  }

  runsForDevice(deviceId: string): GoalRun[] {
    return [...this.runs.values()].filter(
      (r) => r.goal.deviceId === deviceId,
    );
  }

  status(goalId: string): Record<string, unknown> {
    const run = this.runs.get(goalId);
    if (!run) {
      throw new RaoError(ErrorCode.GoalNotFound, `unknown goalId: ${goalId}`);
    }
    return run.snapshot();
  }

  async steerGoal(
    goalId: string,
    message?: string,
    askId?: string,
    answer?: unknown,
  ): Promise<boolean> {
    const run = this.runs.get(goalId);
    if (!run) {
      throw new RaoError(ErrorCode.GoalNotFound, `unknown goalId: ${goalId}`);
    }
    return run.steer(message, askId, answer);
  }

  async cancelGoal(
    goalId: string,
    reason = "user requested",
  ): Promise<boolean> {
    const run = this.runs.get(goalId);
    if (!run) {
      throw new RaoError(ErrorCode.GoalNotFound, `unknown goalId: ${goalId}`);
    }
    return run.cancel(reason);
  }

  /* ---------------- collect ---------------- */

  async collect(
    goalIds: string[],
    timeoutMs = 120_000,
  ): Promise<{
    generatedMs: number;
    matrix: Array<Record<string, unknown>>;
    successCount: number;
    failCount: number;
    reportFile: string;
  }> {
    const deadline = Date.now() + timeoutMs;
    const matrix: Array<Record<string, unknown>> = [];
    for (const goalId of goalIds) {
      const run = this.runs.get(goalId);
      if (!run) {
        // goal from an earlier process: fall back to the persisted record
        const rec = await this.runStore.load(goalId);
        if (rec) {
          const res = (rec.result ?? {}) as Record<string, unknown>;
          matrix.push({
            goalId,
            deviceId: ((rec.goal ?? {}) as Record<string, unknown>).deviceId,
            status: res.status ?? rec.state,
            summary: res.summary,
            steps: res.steps ?? [],
            artifacts: res.artifacts ?? [],
            assumptions: res.assumptions ?? [],
            steerNotes: res.steerNotes ?? [],
            metrics: res.metrics ?? {},
          });
        } else {
          matrix.push({
            goalId,
            status: "unknown",
            summary: "no such goalId (not running, not persisted)",
          });
        }
        continue;
      }
      await run.waitTerminal(Math.max(500, deadline - Date.now()));
      matrix.push({
        goalId,
        deviceId: run.goal.deviceId,
        status: run.result?.status ?? run.state,
        summary: run.result?.summary ?? run.error,
        steps: run.result?.steps ?? [],
        artifacts: run.result?.artifacts ?? [],
        assumptions: run.result?.assumptions ?? [],
        steerNotes: run.result?.steerNotes ?? [],
        metrics: run.result?.metrics ?? {},
      });
    }
    const report: Record<string, unknown> = {
      generatedMs: Date.now(),
      matrix,
      successCount: matrix.filter((m) =>
        ["success", "partial"].includes(String(m.status)),
      ).length,
      failCount: matrix.filter(
        (m) => !["success", "partial", "unknown"].includes(String(m.status)),
      ).length,
    };
    try {
      report.reportFile = await this.runStore.saveReport(report);
    } catch {
      /* report persistence is best-effort */
    }
    return report as {
      generatedMs: number;
      matrix: Array<Record<string, unknown>>;
      successCount: number;
      failCount: number;
      reportFile: string;
    };
  }
}
