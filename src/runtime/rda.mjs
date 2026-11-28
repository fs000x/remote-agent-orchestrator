#!/usr/bin/env node
/**
 * RDA — Remote Device Agent (goal execution engine, v1). Node >= 18.
 *
 * Usage: node rda.mjs run-goal
 * Protocol: one JSON line (goal) on stdin, then JSON-line events on stdout;
 * further stdin lines are injections (steer/cancel/ping). See DESIGN.md §4/§9.
 *
 * ZERO dependencies: this file is deployed verbatim to remote devices
 * (alongside ./protocol.mjs); it must never import from the TypeScript src.
 *
 * Ported from src/rao/runtime/rda.py (v0.11.1) with identical wire behavior.
 */
import { spawn } from "node:child_process";
import {
  accessSync, constants as FS_CONSTANTS, existsSync, readdirSync,
  readFileSync, rmSync, statSync, writeFileSync,
} from "node:fs";
import { tmpdir, homedir } from "node:os";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { pathToFileURL } from "node:url";
import {
  DEFAULTS,
  KIND_ASK, KIND_ERROR, KIND_PROGRESS, KIND_RECON, KIND_RESULT,
  KIND_STARTED, KIND_STEP_RESULT, KIND_WARNING,
  PROTOCOL_VERSION, ST_CANCELLED, ST_FAILED, ST_PARTIAL, ST_SUCCESS,
  ST_TIMEOUT, commandViolatesForbidden, makeEvent, nowMs, stepEffectiveTimeout,
  validateGoal,
} from "./protocol.mjs";

export const RDA_VERSION = "0.1.0";
export const EXIT_OK = 0, EXIT_TIMEOUT = 3, EXIT_CANCELLED = 4, EXIT_ERROR = 1;
const POLL_INTERVAL_MS = 200;

function log(msg) {
  process.stderr.write(`[rda] ${msg}\n`);
}

function stdoutSink(ev) {
  process.stdout.write(JSON.stringify(ev) + "\n");
}

/* ------------------------------------------------------------------ *
 * shell runner (Python subprocess.run(shell=True, capture_output=True,
 * timeout=..., stdin=DEVNULL) equivalent)
 * ------------------------------------------------------------------ */
function shellInvocation(cmd) {
  if (process.platform === "win32") {
    const comspec = process.env.ComSpec || "cmd.exe";
    return { file: comspec, args: ["/d", "/s", "/c", cmd], verbatim: true };
  }
  return { file: "/bin/sh", args: ["-c", cmd], verbatim: false };
}

export function runShell(cmd, opts = {}) {
  const { cwd, timeoutMs = 120000, maxBuffer = 2 * 1024 * 1024 } = opts;
  return new Promise((resolve) => {
    const sh = shellInvocation(cmd);
    let child;
    try {
      child = spawn(sh.file, sh.args, {
        cwd: cwd || undefined,
        stdio: ["ignore", "pipe", "pipe"], // DEVNULL stdin (B-series fix)
        windowsVerbatimArguments: sh.verbatim,
        windowsHide: true,
      });
    } catch (err) {
      resolve({ returncode: -1, stdout: "", stderr: String(err), timedOut: false });
      return;
    }
    let out = Buffer.alloc(0);
    let errb = Buffer.alloc(0);
    let truncated = false;
    const collect = (buf, chunk) => {
      if (buf.length >= maxBuffer) { truncated = true; return buf; }
      return Buffer.concat([buf, chunk]);
    };
    child.stdout?.on("data", (c) => { out = collect(out, c); });
    child.stderr?.on("data", (c) => { errb = collect(errb, c); });
    let timedOut = false;
    const watcher = setTimeout(() => { timedOut = true; child.kill("SIGKILL"); }, timeoutMs);
    child.on("error", (err) => {
      clearTimeout(watcher);
      resolve({ returncode: -1, stdout: "", stderr: String(err), timedOut: false });
    });
    child.on("close", (code) => {
      clearTimeout(watcher);
      resolve({
        returncode: code ?? -1,
        stdout: out.toString("utf8") + (truncated ? "\n[truncated]" : ""),
        stderr: errb.toString("utf8"),
        timedOut,
      });
    });
  });
}

/* PATH scan (which) with Windows PATHEXT handling */
function which(name) {
  const pathEnv = process.env.PATH || "";
  const exts = process.platform === "win32"
    ? (process.env.PATHEXT || ".EXE;.CMD;.BAT").split(";")
    : [""];
  for (const d of pathEnv.split(process.platform === "win32" ? ";" : ":")) {
    if (!d) continue;
    for (const ext of exts) {
      const cand = join(d, name + ext.toLowerCase());
      try {
        accessSync(cand, FS_CONSTANTS.X_OK);
        if (statSync(cand).isFile()) return cand;
      } catch { /* next */ }
    }
  }
  return null;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

/* ------------------------------------------------------------------ *
 * GoalEngine
 * ------------------------------------------------------------------ */
export class GoalEngine {
  /**
   * @param goal validated goal object
   * @param opts.sink event sink (default stdout JSON lines) — daemon embeds
   *        its channel here; @param opts.stateDir runtime state directory
   */
  constructor(goal, opts = {}) {
    this.goal = goal;
    this.goalId = goal.goalId;
    this.constraints = goal.constraints ?? {};
    this.deadlineMs = nowMs() + Number(
      this.constraints.timeoutMs ?? DEFAULTS.timeoutMs);
    this.stepAttempts = 0;
    this.maxSteps = Number(this.constraints.maxSteps ?? DEFAULTS.maxSteps);
    this.sink = opts.sink ?? stdoutSink;
    this.stateDir = opts.stateDir ?? join(homedir(), ".rao");
    this.injections = []; // drained by pollInjections(); feed via inject()
    this.cancelled = false;
    this.timedOut = false;
    this.askResponses = new Map(); // askId -> answer payload
    this.steerNotes = [];          // free-text guidance, echoed in result
    this.skipIds = new Set();      // steer directive skip:<id>
    this.extraRuns = [];           // steer directive run:<command>
    this.assumptions = [];
    this.stepsReport = [];
    this.artifacts = [];
    this.totalRetries = 0;
    this.finalStatus = null;
    this.startedMs = nowMs();
  }

  /** programmatic injection feed (daemon mode / tests) */
  inject(obj) {
    this.injections.push(obj);
  }

  /* --- plumbing --- */
  emit(kind, data, extra = {}) {
    const ev = makeEvent(kind, this.goalId, data, extra);
    this.sink(ev);
    return ev;
  }

  deadlinePassed() {
    return nowMs() >= this.deadlineMs;
  }

  budgetLeft() {
    return this.stepAttempts < this.maxSteps;
  }

  /** Drain injections; returns false when the engine must stop. */
  pollInjections() {
    for (;;) {
      const inj = this.injections.shift();
      if (!inj) return true;
      const t = inj.type;
      if (t === "cancel") {
        this.cancelled = true;
        return false;
      }
      if (t === "ping") {
        this.emit(KIND_PROGRESS, { ping: "pong", stepAttempts: this.stepAttempts });
      } else if (t === "steer") {
        this.applySteer(inj);
      }
    }
  }

  applySteer(inj) {
    const askId = inj.askId;
    if (askId) {
      this.askResponses.set(askId, inj.answer);
      return;
    }
    const msg = inj.message;
    if (typeof msg !== "string" || !msg.trim()) return;
    const m = msg.trim();
    if (m.toLowerCase().startsWith("skip:")) {
      this.skipIds.add(m.slice(5).trim());
      this.steerNotes.push({ directive: m });
      return;
    }
    if (m.toLowerCase().startsWith("run:")) {
      this.extraRuns.push({
        id: `steer-${this.extraRuns.length}`,
        type: "shell",
        command: m.slice(4).trim(),
        optional: true,
      });
      this.steerNotes.push({ directive: m });
      return;
    }
    this.steerNotes.push({ message: m });
  }

  /* --- event wait --- */
  async waitAsk(askId, timeoutS) {
    const deadline = Date.now() + timeoutS * 1000;
    while (Date.now() < deadline) {
      if (this.askResponses.has(askId)) {
        const a = this.askResponses.get(askId);
        this.askResponses.delete(askId);
        return a;
      }
      if (!this.pollInjections()) return null;
      if (this.deadlinePassed()) {
        this.timedOut = true;
        return null;
      }
      await sleep(POLL_INTERVAL_MS);
    }
    return null;
  }

  /* --- criteria --- */
  criteriaMet(step, res) {
    const crit = step.success ?? { exitCode: 0 };
    if ("exitCode" in crit && res.returncode !== crit.exitCode) return false;
    if ("stdoutContains" in crit && !(res.stdout || "").includes(crit.stdoutContains)) return false;
    if ("stderrContains" in crit && !(res.stderr || "").includes(crit.stderrContains)) return false;
    if ("fileExists" in crit) {
      const base = step.cwd || process.cwd();
      if (!existsSync(join(base, crit.fileExists))) return false;
    }
    return true;
  }

  collectArtifacts(step, res, attemptData) {
    const base = step.cwd || process.cwd();
    for (const rel of step.artifacts ?? []) {
      const p = join(base, rel);
      try {
        if (existsSync(p)) {
          this.artifacts.push({ path: p, size: statSync(p).size, stepId: step.id });
        }
      } catch { /* unreadable artifact skipped */ }
    }
    if (res) {
      const tail = (res.stdout || "").slice(-400);
      if (tail.trim()) attemptData.stdoutTail = tail;
      const errtail = (res.stderr || "").slice(-400);
      if (errtail.trim()) attemptData.stderrTail = errtail;
    }
  }

  /* --- step execution --- */
  async runStep(step) {
    const sid = step.id ?? "?";
    if (this.skipIds.has(sid)) {
      this.emit(KIND_STEP_RESULT, { stepId: sid, status: "skipped", reason: "steer skip directive" });
      const rep = { id: sid, status: "skipped" };
      this.stepsReport.push(rep);
      return rep;
    }
    const retries = Number(step.retries ?? 0);
    const backoff = Number(step.retryBackoffMs ?? DEFAULTS.retryBackoffMs);
    const variants = [[step, "ok"]].concat(
      (step.fallbacks ?? []).map((fb, i) => [fb, `fallback:${i}`]));
    let rep = { id: sid, status: "failed" };

    for (const [variant, vLabel] of variants) {
      const tries = vLabel.startsWith("fallback") ? 1 : retries + 1;
      for (let attempt = 1; attempt <= tries; attempt++) {
        if (this.cancelled || this.timedOut || !this.budgetLeft()) break;
        this.stepAttempts++;
        if (attempt > 1) this.totalRetries++;
        let attemptData = {
          stepId: sid, attempt, variant: vLabel, phase: "running",
        };
        this.emit(KIND_PROGRESS, attemptData);
        attemptData = { ...attemptData };
        const ok = await this.runVariant(variant, sid, attemptData);
        attemptData.phase = "done";
        attemptData.status = ok ? "ok" : "failed";
        this.emit(KIND_STEP_RESULT, attemptData);
        if (ok) {
          rep = {
            id: sid,
            status: vLabel === "ok" ? "ok" : "fallback_ok",
            variant: vLabel,
            attempts: attempt,
          };
          this.stepsReport.push(rep);
          return rep;
        }
        if (attempt < tries && !this.cancelled && !this.deadlinePassed()) {
          await sleep(backoff);
        }
      }
      if (this.cancelled || this.timedOut) break;
    }

    const optional = Boolean(step.optional);
    rep = { id: sid, status: optional ? "skipped" : "failed" };
    this.stepsReport.push(rep);
    this.emit(KIND_ERROR, {
      message: `step ${sid} ${optional ? "skipped (optional)" : "failed"}`,
      fatal: !optional,
    });
    return rep;
  }

  async runVariant(step, sid, attemptData) {
    const stype = step.type;
    const timeoutS = stepEffectiveTimeout(step, this.constraints);
    if (stype === "shell") {
      const cmd = step.command ?? "";
      const violated = commandViolatesForbidden(cmd, this.constraints.forbidden ?? []);
      if (violated) {
        attemptData.status = "failed";
        attemptData.reason = `forbidden command pattern: ${violated}`;
        this.emit(KIND_WARNING, { message: `step ${sid} blocked: matches forbidden ${JSON.stringify(violated)}` });
        return false;
      }
      const ws = this.goal.workspace;
      const base = step.cwd || (ws && existsSync(ws) ? ws : undefined);
      const res = await runShell(cmd, {
        cwd: base,
        timeoutMs: timeoutS,
      });
      if (res.timedOut) {
        attemptData.reason = "step timeout";
        return false;
      }
      if (this.cancelled) {
        attemptData.reason = "cancelled during step";
        return false;
      }
      this.collectArtifacts(step, res, attemptData);
      return this.criteriaMet(step, res);
    }
    if (stype === "wait_file") {
      const path = step.path;
      const deadline = Date.now() + timeoutS;
      while (Date.now() < deadline) {
        if (existsSync(path)) return true;
        if (!this.pollInjections() || this.cancelled) {
          attemptData.reason = "cancelled while waiting";
          return false;
        }
        await sleep(500);
      }
      attemptData.reason = `wait_file timeout: ${path}`;
      return false;
    }
    if (stype === "artifact") {
      const base = step.cwd || this.goal.workspace || ".";
      const missing = (step.paths ?? [])
        .filter((p) => !existsSync(join(base, p)));
      if (missing.length) {
        attemptData.reason = `missing artifacts: ${missing.join(", ")}`;
        return false;
      }
      for (const p of step.paths ?? []) {
        const full = join(base, p);
        try {
          this.artifacts.push({ path: full, size: statSync(full).size, stepId: sid });
        } catch { /* unreadable artifact skipped */ }
      }
      return true;
    }
    log(`unknown step type ${JSON.stringify(stype)}`);
    return false;
  }

  /* --- ask step --- */
  async runAskStep(step) {
    const askId = `ask-${step.id ?? "x"}`;
    this.emit(KIND_ASK, {
      question: step.question ?? "",
      context: step.context ?? {},
    }, { askId });
    const answer = await this.waitAsk(askId, 3600);
    if (answer === null || answer === undefined) {
      const rep = { id: step.id, status: "failed", reason: "ask unanswered" };
      this.stepsReport.push(rep);
      this.emit(KIND_ERROR, { message: `ask ${askId} unanswered`, fatal: true });
      return rep;
    }
    const rep = { id: step.id, status: "ok", answer };
    this.stepsReport.push(rep);
    return rep;
  }

  /* --- claude agent backend (whole-goal delegation) --- */
  async runClaudeBackend() {
    let claudeBin = which("claude");
    if (!claudeBin) {
      // claude self-updates can break the PATH symlink (npm native-binary
      // distribution) — probe common install locations; the in-package
      // binary is preferred (self-update refreshes it in place)
      const cands = [
        "/usr/lib/node_modules/@anthropic-ai/claude-code/bin/claude.exe",
        "/usr/local/bin/claude",
        "/usr/bin/claude",
        join(homedir(), ".local/bin/claude"),
        join(homedir(), ".claude/local/claude"),
      ];
      for (const cand of cands) {
        try {
          if (existsSync(cand)) { claudeBin = cand; break; }
        } catch { /* next */ }
      }
    }
    if (!claudeBin) {
      this.emit(KIND_ERROR, {
        message: "claude CLI not found on device; agent backend unavailable",
        fatal: true,
      });
      return this.finish(ST_FAILED, "claude CLI not found on device");
    }
    const c = this.constraints;
    const maxTurns = Number(c.maxSteps ?? DEFAULTS.maxSteps);
    const timeoutMs = Number(c.timeoutMs ?? DEFAULTS.timeoutMs);
    const cmd = [
      claudeBin, "-p", this.goal.objective ?? "",
      "--output-format", "stream-json", "--verbose",
      "--max-turns", String(maxTurns),
    ];
    // autonomous tool use requires skipping interactive permission prompts
    // in headless mode (opt-in via constraints.claudeAutoApprove)
    if (c.claudeAutoApprove) cmd.push("--dangerously-skip-permissions");
    const forbidden = c.forbidden ?? [];
    if (forbidden.length) {
      cmd.push(
        "--append-system-prompt",
        "Forbidden operations (never perform): " + forbidden.join("; "),
      );
    }
    const cwd = this.goal.workspace || undefined;
    this.emit(KIND_PROGRESS, {
      backend: "claude", phase: "running",
      maxTurns, timeoutMs,
    });
    let proc;
    try {
      proc = spawn(claudeBin, cmd, {
        cwd,
        stdio: ["ignore", "pipe", "pipe"], // DEVNULL stdin
        windowsHide: true,
      });
    } catch (err) {
      this.emit(KIND_ERROR, { message: `claude spawn failed: ${err}`, fatal: true });
      return this.finish(ST_FAILED, "claude spawn failed");
    }

    const toolUses = [];
    const outcome = await new Promise((resolve) => {
      let buf = "";
      let final = null;
      let killed = false;
      let settled = false;
      const done = () => {
        if (settled) return;
        settled = true;
        clearInterval(iv);
        clearTimeout(watcher);
        resolve({ final, killed });
      };
      const watcher = setTimeout(() => proc.kill("SIGKILL"), timeoutMs);
      const iv = setInterval(() => {
        if (!this.pollInjections() || this.cancelled) {
          killed = true;
          proc.kill("SIGKILL");
        }
      }, 500);
      proc.stdout?.on("data", (chunk) => {
        buf += chunk.toString("utf8");
        for (;;) {
          const nl = buf.indexOf("\n");
          if (nl < 0) break;
          const line = buf.slice(0, nl).trim();
          buf = buf.slice(nl + 1);
          if (!line) continue;
          let msg;
          try { msg = JSON.parse(line); } catch { continue; }
          if (msg.type === "assistant") {
            for (const blk of msg.message?.content ?? []) {
              if (blk.type === "tool_use") {
                toolUses.push(blk.name);
                this.emit(KIND_PROGRESS, {
                  backend: "claude", tool: blk.name, phase: "tool_use",
                });
              }
            }
          } else if (msg.type === "result") {
            final = msg;
          }
        }
      });
      proc.on("error", () => { final = final ?? null; });
      proc.on("close", () => done());
    });

    if (outcome.killed || this.cancelled) {
      return this.finish(ST_CANCELLED, "claude backend cancelled");
    }
    const final = outcome.final;
    if (!final) {
      return this.finish(ST_FAILED, "claude ended without result (timeout or crash)");
    }
    const subtype = final.subtype ?? "";
    if (subtype !== "success") {
      return this.finish(ST_FAILED, `claude result: ${subtype}`);
    }
    const text = String(final.result ?? "").slice(0, 800);
    this.artifacts.push({ path: "<claude-result>", kind: "summary", stepId: "claude" });
    return this.finish(
      ST_SUCCESS,
      text || `claude completed with ${toolUses.length} tool uses`,
    );
  }

  /* --- recon --- */
  ensureWorkspaceSteps(ews) {
    const path = ews.path;
    const branch = ews.branch || "";
    const steps = [];
    if (existsSync(join(path, ".git"))) {
      steps.push({
        id: "ensure-ws-fetch", type: "shell",
        command: `git -C ${shq(path)} fetch origin --prune`,
        success: { exitCode: 0 }, retries: 1, retryBackoffMs: 1000,
      });
      if (branch) {
        steps.push({
          id: "ensure-ws-checkout", type: "shell",
          command: `git -C ${shq(path)} checkout ${shq(branch)}`,
          success: { exitCode: 0 },
        });
        steps.push({
          id: "ensure-ws-ff", type: "shell",
          command: `git -C ${shq(path)} merge --ff-only origin/${shq(branch)}`,
          success: { exitCode: 0 }, optional: true,
        });
      }
    } else {
      const cmd = `git clone ${branch ? `--branch ${shq(branch)} ` : ""}${shq(ews.repoUrl)} ${shq(path)}`;
      steps.push({
        id: "ensure-ws-clone", type: "shell",
        command: cmd.trim(), success: { exitCode: 0 },
        retries: 1, retryBackoffMs: 2000,
      });
    }
    return { steps, branch };
  }

  async ensureWorkspace(ews) {
    const { steps, branch } = this.ensureWorkspaceSteps(ews);
    let last = { status: "ok" };
    for (const s of steps) {
      const rep = await this.runStep(s);
      last = rep;
      if (rep.status === "failed") return rep;
      if (rep.status === "skipped" && s.id === "ensure-ws-ff") {
        this.assumptions.push(
          `workspace ${ews.path} diverged from origin/${branch}; left as-is`);
      }
    }
    if (branch) {
      this.assumptions.push(`workspace pinned to branch ${branch}`);
    }
    return last;
  }

  async tryRemotePlanner(facts) {
    // planner slot: goal.planner='remote-llm' — run the planner command
    // configured in <state-dir>/planner.json {"command": ...} (templates
    // {objective} and {recon_file}); must print a plan JSON on stdout.
    // Any failure falls back to the recon+ask flow (recorded as assumption).
    let cmdTmpl = null;
    try {
      const pjson = JSON.parse(readFileSync(join(this.stateDir, "planner.json"), "utf8"));
      cmdTmpl = pjson.command ?? null;
    } catch { /* no planner configured */ }
    if (!cmdTmpl) {
      this.assumptions.push(
        "planner=remote-llm but no planner command configured " +
        "(planner.json missing in state dir); fell back to ask flow");
      return null;
    }
    let reconPath = "";
    try {
      reconPath = join(tmpdir(), `rda-recon-${Date.now()}-${Math.random().toString(36).slice(2)}.json`);
      writeFileSync(reconPath, JSON.stringify({
        objective: this.goal.objective,
        recon: facts,
      }));
      const cmd = cmdTmpl
        .replaceAll("{objective}", String(this.goal.objective))
        .replaceAll("{recon_file}", reconPath);
      const res = await runShell(cmd, { timeoutMs: 180000 });
      const out = (res.stdout || "").trim();
      if (res.returncode !== 0) {
        this.assumptions.push(`remote planner rc=${res.returncode}; fell back to ask flow`);
        return null;
      }
      const start = out.indexOf("{");
      const end = out.lastIndexOf("}");
      if (start < 0 || end <= start) {
        throw new Error("planner stdout has no JSON object");
      }
      const plan = JSON.parse(out.slice(start, end + 1));
      const synthetic = {
        protocol: PROTOCOL_VERSION, goalId: this.goalId, deviceId: "x",
        objective: "validate planned program", plan,
      };
      const [ok, err] = validateGoal(synthetic);
      if (!ok) throw new Error(`planner plan invalid: ${err}`);
      this.assumptions.push("plan produced by remote planner");
      return plan;
    } catch (err) {
      this.assumptions.push(`remote planner failed (${err}); fell back to ask flow`);
      return null;
    } finally {
      try {
        if (reconPath && existsSync(reconPath)) rmSync(reconPath, { force: true });
      } catch { /* best effort */ }
    }
  }

  async runRecon() {
    const osMod = await import("node:os");
    const facts = {};
    try {
      facts.os = `${osMod.type()} ${osMod.release()}`;
      facts.arch = osMod.arch();
      facts.node = process.version;
      facts.hostname = osMod.hostname();
      facts.cpus = osMod.cpus().length;
      try {
        if (existsSync("/proc/meminfo")) {
          for (const line of readFileSync("/proc/meminfo", "utf8").split("\n")) {
            if (line.startsWith("MemAvailable")) {
              facts.memAvailableKB = Number(line.split(/\s+/)[1]);
              break;
            }
          }
        }
      } catch { /* ignore */ }
      const tools = {};
      for (const t of ["git", "make", "gcc", "python3", "node", "docker", "adb"]) {
        tools[t] = Boolean(which(t));
      }
      facts.tools = tools;
      // agent/ZCode-Server detection (capability reporting)
      const agents = {};
      agents.claude = Boolean(which("claude"));
      agents.codex = Boolean(which("codex"));
      const zs = {
        installed: existsSync(join(homedir(), ".zcode", "server")),
        version: null, running: false, pid: null,
      };
      if (zs.installed) {
        try {
          const st = JSON.parse(readFileSync(
            join(homedir(), ".zcode", "server", "run", "status.json"), "utf8"));
          zs.version = st.version ?? null;
          zs.pid = st.pid ?? null;
          zs.running = st.state === "ready" && Boolean(st.pid) &&
            existsSync(`/proc/${st.pid}`);
        } catch { /* ignore */ }
      }
      agents.zcode_server = zs;
      facts.agents = agents;
      const ws = this.goal.workspace;
      if (ws && existsSync(ws) && statSync(ws).isDirectory()) {
        const entries = readdirSync(ws).sort().slice(0, 30);
        const gitState = {};
        try {
          const r = await runShell("git status --porcelain -b | head -5", { cwd: ws, timeoutMs: 10000 });
          gitState.statusTail = (r.stdout || "").trim();
          const r2 = await runShell("git log --oneline -1", { cwd: ws, timeoutMs: 10000 });
          gitState.head = (r2.stdout || "").trim();
        } catch { /* ignore */ }
        facts.workspace = { path: ws, entries, git: gitState };
      }
    } catch (err) {
      facts.error = String(err);
    }
    return facts;
  }

  /* --- main loop --- */
  async run() {
    try {
      const ws = this.goal.workspace;
      if (ws && existsSync(ws)) {
        try { process.chdir(ws); } catch { /* keep cwd */ }
      }
      const osMod = await import("node:os");
      this.emit(KIND_STARTED, {
        rdaVersion: RDA_VERSION,
        node: process.version,
        host: osMod.hostname(),
        pid: process.pid,
      });

      if (this.goal.ensureWorkspace) {
        const rep = await this.ensureWorkspace(this.goal.ensureWorkspace);
        if (rep.status === "failed") {
          return this.finish(ST_FAILED, `ensureWorkspace failed: ${rep.reason}`);
        }
      }

      if ((this.goal.agent ?? "builtin") === "claude") {
        return await this.runClaudeBackend();
      }

      let plan = this.goal.plan ?? null;
      let reconFacts = null;
      if (!plan || !(plan.steps ?? []).length) {
        reconFacts = await this.runRecon();
        this.emit(KIND_RECON, { facts: reconFacts });
        if (this.goal.planner === "remote-llm") {
          plan = await this.tryRemotePlanner(reconFacts) ?? null;
        }
      }
      if (!plan || !(plan.steps ?? []).length) {
        const askId = "plan-req-1";
        this.emit(KIND_ASK, {
          question: "goal has no plan; send steer with plan steps after reviewing recon",
          context: { recon: reconFacts ?? {} },
        }, { askId });
        const answer = await this.waitAsk(askId, 3600);
        if (answer === null || answer === undefined) {
          return this.finish(
            this.timedOut ? ST_TIMEOUT : ST_FAILED,
            "no plan received for plan-less goal");
        }
        const newPlan = (answer ?? {}).plan;
        const synthetic = {
          protocol: PROTOCOL_VERSION, goalId: this.goalId, deviceId: "x",
          objective: "validate steered plan", plan: newPlan,
        };
        const [ok, err] = validateGoal(synthetic);
        if (!ok) {
          return this.finish(ST_FAILED, `steered plan invalid: ${err}`);
        }
        plan = newPlan;
        this.assumptions.push("plan received via steer at runtime");
      }

      const steps = [...(plan.steps ?? [])];
      let queueSteps = steps.concat(this.extraRuns); // steer run: directives append
      let failedRequired = false;
      let executed = 0;
      while (executed < queueSteps.length) {
        if (!this.pollInjections()) break;
        if (this.timedOut || this.deadlinePassed()) {
          this.timedOut = true;
          break;
        }
        if (!this.budgetLeft()) {
          this.emit(KIND_ERROR, {
            message: `RAO_BUDGET_EXCEEDED: maxSteps=${this.maxSteps}`,
            fatal: true,
          });
          return this.finish(ST_TIMEOUT, "step budget exhausted");
        }
        const step = queueSteps[executed];
        executed++;
        const rep = step.type === "ask"
          ? await this.runAskStep(step)
          : await this.runStep(step);
        if (rep.status === "failed") {
          failedRequired = true;
          break;
        }
        // new steer run: directives may have appeared
        if (queueSteps.length !== steps.length + this.extraRuns.length) {
          queueSteps = steps.concat(this.extraRuns);
        }
      }

      if (this.cancelled) return this.finish(ST_CANCELLED, "cancelled via steer");
      if (this.timedOut) return this.finish(ST_TIMEOUT, "goal deadline exceeded");
      if (failedRequired) return this.finish(ST_FAILED, "required step failed");
      const doneOk = this.stepsReport.filter((r) => r.status === "ok");
      const skipped = this.stepsReport.filter((r) => r.status === "skipped");
      if (skipped.length && doneOk.length) {
        return this.finish(ST_PARTIAL, "completed with skipped optional steps");
      }
      return this.finish(ST_SUCCESS, "all steps ok");
    } catch (err) {
      // engine bug / unexpected
      log(`engine error: ${err?.stack || err}`);
      try {
        this.emit(KIND_ERROR, { message: `rda engine error: ${err}`, fatal: true });
      } catch { /* sink broken */ }
      return this.finish(ST_FAILED, `engine error: ${err}`);
    }
  }

  finish(status, summary) {
    const data = {
      status, summary,
      steps: this.stepsReport,
      artifacts: this.artifacts,
      assumptions: this.assumptions,
      steerNotes: this.steerNotes,
      metrics: {
        stepAttempts: this.stepAttempts,
        retries: this.totalRetries,
        durationMs: Math.max(0, nowMs() - this.startedMs),
      },
    };
    this.emit(KIND_RESULT, data);
    this.finalStatus = status;
    return status === ST_TIMEOUT ? EXIT_TIMEOUT
      : status === ST_CANCELLED ? EXIT_CANCELLED
        : EXIT_OK;
  }
}

/** Platform-aware shell quoting (POSIX: single quotes; Windows: double quotes) */
export function shq(s) {
  if (process.platform === "win32") return `"${s}"`;
  return `'${String(s).replaceAll("'", `'\\''`)}'`;
}

/* ------------------------------------------------------------------ *
 * CLI entry
 * ------------------------------------------------------------------ */
function readFirstLine(stream) {
  return new Promise((resolve) => {
    const rl = createInterface({ input: stream, terminal: false });
    rl.once("line", (l) => resolve({ rl, line: l }));
  });
}

export async function main(argv = process.argv.slice(2)) {
  const a = [...argv];
  let stateDir = null;
  const i = a.indexOf("--state-dir");
  if (i >= 0) {
    stateDir = a[i + 1] ?? null;
    a.splice(i, 2);
  }
  if (a.length < 1 || a[0] !== "run-goal") {
    process.stderr.write("usage: node rda.mjs run-goal [--state-dir DIR]\n");
    return 2;
  }
  const { rl, line } = await readFirstLine(process.stdin);
  const raw = (line || "").trim();
  if (!raw) {
    process.stderr.write("no goal received on stdin\n");
    rl.close();
    return 2;
  }
  let goal;
  try {
    goal = JSON.parse(raw);
  } catch (err) {
    process.stderr.write(`goal json invalid: ${err}\n`);
    rl.close();
    return 2;
  }
  const [ok, err] = validateGoal(goal);
  if (!ok) {
    const gid = typeof goal === "object" && goal !== null
      ? goal.goalId ?? "?" : "?";
    process.stderr.write(`goal invalid: ${err}\n`);
    process.stdout.write(JSON.stringify(makeEvent(
      KIND_ERROR, gid, { message: `RAO_GOAL_INVALID: ${err}`, fatal: true })) + "\n");
    rl.close();
    return 2;
  }
  const engine = new GoalEngine(goal, { stateDir });
  // post-goal stdin lines are injections (steer/cancel/ping)
  rl.on("line", (l) => {
    const t = (l || "").trim();
    if (!t) return;
    try {
      engine.inject(JSON.parse(t));
    } catch {
      log(`non-json injection dropped: ${t.slice(0, 120)}`);
    }
  });
  const code = await engine.run();
  rl.close();
  return code;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().then((code) => { process.exitCode = code; });
}
