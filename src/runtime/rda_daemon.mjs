#!/usr/bin/env node
/**
 * RDA daemon — persistent goal engine host on a device. Node >= 18, ZERO
 * dependencies (deployed verbatim alongside rda.mjs + protocol.mjs).
 *
 * Subcommands:
 *   serve [--port N] [--master host:port] [--token TOKEN] [--home DIR]
 *                         run the daemon (TCP 127.0.0.1, token auth)
 *   start [--port N] ...  spawn serve detached, wait for daemon.json
 *   ping                  check daemon liveness (exit 0 = alive)
 *   stop                  ask the daemon to shut down
 *   status-one --goal-id  print one goal snapshot
 *   inject-one --goal-id --payload JSON   one-shot control-plane inject
 *   client-bridge         stdio <-> TCP bridge (used over SSH by orchestrator)
 *
 * State dir ($RAO_STATE_DIR or ~/.rao, or --home): daemon.json
 * {pid, port, token, startedMs, heartbeatMs}, daemon.log,
 * goals/<goalId>.json (per-goal buffered events for resume).
 *
 * Wire protocol (JSON lines over TCP; FIRST line must be auth):
 *   -> {"cmd":"auth","token":"..."}          <- {"type":"ready"}
 *   -> {"cmd":"submit","goal":{...}}         <- {"type":"ack","goalId":...}
 *      then live events: {"type":"event","goalId":...,"event":{...},"seq":N}
 *      terminal:            {"type":"goal_status","goalId":...,"status":...,"exit":N}
 *   -> {"cmd":"attach","goalId":"..."}       <- {"type":"attached",...} + buffered
 *                                               events + live tail + goal_status
 *   -> {"cmd":"status","goalId":"..."}       <- {"type":"goal_snapshot",...}
 *   -> {"cmd":"list"}                        <- {"type":"goal_list",...}
 *   -> {"cmd":"inject","goalId","payload":{...}} <- {"type":"injected",...}
 *   -> {"cmd":"ping"}                        <- {"type":"pong","ts":...}
 *   -> {"cmd":"shutdown"}                    <- {"type":"bye"} (daemon exits)
 *
 * Ported from src/rao/runtime/rda_daemon.py (v0.11.1).
 */
import { createHash, randomBytes } from "node:crypto";
import { connect, createServer } from "node:net";
import {
  chmodSync, closeSync, mkdirSync, openSync, readFileSync,
  renameSync, rmSync, writeFileSync,
} from "node:fs";
import { hostname, platform, homedir, userInfo } from "node:os";
import { dirname, join } from "node:path";
import { createInterface } from "node:readline";
import { fileURLToPath, pathToFileURL } from "node:url";
import {
  KIND_ERROR, makeEvent, newGoalId, nowMs, validateGoal,
} from "./protocol.mjs";
import { GoalEngine } from "./rda.mjs";

export const DAEMON_VERSION = "0.3.0";
const HEARTBEAT_MS = 5000;
const EVENT_BUFFER_MAX = 5000;

// Honour RAO_STATE_DIR like the orchestrator does; without this the daemon
// writes daemon.json/goals into the developer's real ~/.rao even when the
// caller asked for an isolated state dir (test isolation depends on it).
let RAO_DIR = process.env.RAO_STATE_DIR || join(homedir(), ".rao");
let DAEMON_JSON = join(RAO_DIR, "daemon.json");
let DAEMON_LOG = join(RAO_DIR, "daemon.log");
let GOALS_DIR = join(RAO_DIR, "goals");

function setHome(home) {
  RAO_DIR = home;
  DAEMON_JSON = join(RAO_DIR, "daemon.json");
  DAEMON_LOG = join(RAO_DIR, "daemon.log");
  GOALS_DIR = join(RAO_DIR, "goals");
}

function log(msg) {
  process.stderr.write(`[rda-daemon] ${msg}\n`);
}

function loadDaemonJson() {
  try {
    return JSON.parse(readFileSync(DAEMON_JSON, "utf8"));
  } catch {
    return null;
  }
}

/* ------------------------------------------------------------------ *
 * GoalRecord: buffered events + persistence + subscriber fan-out
 * ------------------------------------------------------------------ */
class GoalRecord {
  constructor(goal) {
    this.goal = goal;
    this.goalId = goal.goalId;
    this.events = [];
    this.totalEver = 0;   // seq counter (1-based, survives buffer trim)
    this.trimmed = 0;     // events dropped from the front of the buffer
    this.status = "running";
    this.exitCode = null;
    this.startedMs = nowMs();
    this.finishedMs = null;
    this.subscribers = []; // list of send(msg) sinks
    this.engine = null;
    this.pendingInjections = []; // injections arriving before engine ready
  }

  /** GoalEngine event sink: buffer + fan out */
  sink(ev) {
    this.totalEver++;
    const seq = this.totalEver;
    this.events.push(ev);
    if (this.events.length > EVENT_BUFFER_MAX) {
      const removed = this.events.length - EVENT_BUFFER_MAX;
      this.events.splice(0, removed);
      this.trimmed += removed;
    }
    for (const send of [...this.subscribers]) {
      try {
        send({ type: "event", goalId: this.goalId, event: ev, seq });
      } catch { /* dead subscriber */ }
    }
  }

  finish(exitCode) {
    let res = null;
    for (let i = this.events.length - 1; i >= 0; i--) {
      if (this.events[i].kind === "result") { res = this.events[i]; break; }
    }
    this.status = res ? res.data?.status ?? "failed" : "failed";
    this.exitCode = exitCode;
    this.finishedMs = nowMs();
    this.persist(); // goal_status implies the record is on disk
    const payload = {
      type: "goal_status", goalId: this.goalId,
      status: this.status, exit: exitCode,
    };
    for (const send of [...this.subscribers]) {
      try { send(payload); } catch { /* dead subscriber */ }
    }
  }

  persist() {
    try {
      mkdirSync(GOALS_DIR, { recursive: true });
      const path = join(GOALS_DIR, `${this.goalId}.json`);
      const tmp = `${path}.tmp`;
      writeFileSync(tmp, JSON.stringify({
        goal: this.goal, status: this.status, exit: this.exitCode,
        startedMs: this.startedMs, finishedMs: this.finishedMs,
        events: this.events,
      }));
      renameSync(tmp, path);
    } catch (err) {
      log(`persist goal ${this.goalId} failed: ${err}`);
    }
  }
}

/* ------------------------------------------------------------------ *
 * DaemonState
 * ------------------------------------------------------------------ */
class DaemonState {
  constructor(token) {
    this.token = token;
    this.goals = new Map();
    this.shuttingDown = false;
  }

  submit(goal) {
    const [ok, err] = validateGoal(goal);
    if (!ok) throw new Error(`RAO_GOAL_INVALID: ${err}`);
    const gid = goal.goalId;
    const existing = this.goals.get(gid);
    if (existing && existing.status === "running") {
      throw new Error(`goal ${gid} already running`);
    }
    const rec = new GoalRecord(goal);
    this.goals.set(gid, rec);
    (async () => {
      let code = 1;
      try {
        const engine = new GoalEngine(goal, { sink: (ev) => rec.sink(ev) });
        rec.engine = engine;
        for (const payload of rec.pendingInjections) engine.inject(payload);
        rec.pendingInjections = [];
        code = await engine.run();
      } catch (err) {
        log(`engine crash ${gid}: ${err?.stack || err}`);
        try {
          rec.sink(makeEvent(KIND_ERROR, gid, {
            message: `daemon engine crash: ${err}`, fatal: true,
          }));
        } catch { /* sink broken */ }
        code = 1;
      }
      rec.finish(code);
    })();
    return rec;
  }

  startHeartbeat() {
    const iv = setInterval(() => {
      if (this.shuttingDown) return;
      try {
        const data = loadDaemonJson() ?? {};
        data.pid = process.pid;
        data.heartbeatMs = nowMs();
        data.goals = this.goals.size;
        const tmp = `${DAEMON_JSON}.tmp`;
        writeFileSync(tmp, JSON.stringify(data));
        renameSync(tmp, DAEMON_JSON);
      } catch (err) {
        log(`heartbeat failed: ${err}`);
      }
    }, HEARTBEAT_MS);
    iv.unref?.();
    return iv;
  }
}

/* ------------------------------------------------------------------ *
 * TCP server (auth-first JSON lines)
 * ------------------------------------------------------------------ */
function makeSend(sock) {
  return (obj) => {
    if (!sock.destroyed) sock.write(JSON.stringify(obj) + "\n");
  };
}

function serveState(state, port) {
  const server = createServer((sock) => {
    sock.setNoDelay?.(true);
    const send = makeSend(sock);
    const liveGoalIds = new Set();
    let authed = false;
    const rl = createInterface({ input: sock, terminal: false });
    rl.on("error", () => { /* socket error already handled on sock */ });
    rl.on("line", (raw) => {
      const line = raw.trim();
      if (!line) return;
      let msg;
      try { msg = JSON.parse(line); } catch {
        if (authed) send({ type: "error", message: "bad json" });
        return;
      }
      if (!authed) {
        if (msg.cmd !== "auth" || msg.token !== state.token) {
          send({ type: "error", message: "auth required" });
          sock.destroy();
          return;
        }
        authed = true;
        send({ type: "ready", version: DAEMON_VERSION });
        return;
      }
      const cmd = msg.cmd;
      log(`handler: cmd=${cmd} goalId=${msg.goalId ?? msg.goal?.goalId ?? "-"}`);
      try {
        if (cmd === "ping") {
          send({ type: "pong", ts: nowMs() });
        } else if (cmd === "submit") {
          const rec = state.submit(msg.goal);
          send({ type: "ack", goalId: rec.goalId });
          // live stream this goal to THIS connection
          rec.subscribers.push(send);
          liveGoalIds.add(rec.goalId);
        } else if (cmd === "attach") {
          const gid = msg.goalId;
          const rec = state.goals.get(gid);
          if (!rec) {
            send({ type: "error", message: `unknown goalId ${gid}` });
            return;
          }
          send({ type: "attached", goalId: gid, status: rec.status });
          log(`attach ${gid}: status=${rec.status} buffered=${rec.events.length}`);
          const base = rec.trimmed;
          const evs = [...rec.events];
          for (let i = 0; i < evs.length; i++) {
            send({ type: "event", goalId: gid, event: evs[i], seq: base + i + 1 });
          }
          log(`attach ${gid}: replayed ${evs.length} events`);
          if (rec.status !== "running") {
            send({ type: "goal_status", goalId: gid, status: rec.status, exit: rec.exitCode });
          } else {
            rec.subscribers.push(send);
          }
        } else if (cmd === "status") {
          const rec = state.goals.get(msg.goalId);
          if (!rec) {
            send({ type: "error", message: `unknown goalId ${msg.goalId}` });
            return;
          }
          send({
            type: "goal_snapshot", goalId: msg.goalId, status: rec.status,
            exit: rec.exitCode, events: rec.events.slice(-50),
          });
        } else if (cmd === "list") {
          const rows = [...state.goals.values()].map((g) => ({
            goalId: g.goalId, status: g.status, exit: g.exitCode,
          }));
          send({ type: "goal_list", goals: rows });
        } else if (cmd === "inject") {
          const gid = msg.goalId;
          const rec = state.goals.get(gid);
          const ack = msg.ack;
          if (!rec) {
            send({ type: "injected", goalId: gid, queued: "dropped", reason: "unknown goalId", ack });
            return;
          }
          if (rec.status !== "running") {
            send({ type: "injected", goalId: gid, queued: "dropped", reason: "goal already finished", ack });
            return;
          }
          if (!rec.engine) {
            // engine not started yet: hold and flush on start
            rec.pendingInjections.push(msg.payload ?? {});
            send({ type: "injected", goalId: gid, queued: "pending", ack });
            return;
          }
          rec.engine.inject(msg.payload ?? {});
          send({ type: "injected", goalId: gid, queued: "delivered", ack });
        } else if (cmd === "shutdown") {
          send({ type: "bye" });
          state.shuttingDown = true;
          setTimeout(() => {
            try { rmSync(DAEMON_JSON, { force: true }); } catch { /* ignore */ }
            process.exit(0);
          }, 100);
        } else {
          send({ type: "error", message: `unknown cmd ${JSON.stringify(cmd)}` });
        }
      } catch (err) {
        try { send({ type: "error", message: String(err?.message ?? err) }); } catch { return; }
      }
    });
    sock.on("close", () => {
      // unsubscribe this connection's sends from any live goals
      for (const gid of liveGoalIds) {
        const rec = state.goals.get(gid);
        if (rec) {
          const idx = rec.subscribers.indexOf(send);
          if (idx >= 0) rec.subscribers.splice(idx, 1);
        }
      }
    });
    sock.on("error", () => { /* client vanished */ });
  });
  return server;
}

/* ------------------------------------------------------------------ *
 * v0.10 node mode: outbound link to the rao master
 * ------------------------------------------------------------------ */
async function masterLinkLoop(state, host, mport, enrollToken, nodeId) {
  let backoff = 500;
  for (;;) {
    try {
      await new Promise((resolveLink) => {
        // NOTE: no connect({timeout}) — that installs an IDLE timeout and
        // would drop the link during quiet goal phases; connect guarding
        // is done via the one-shot timer below instead.
        const sock = connect({ host, port: mport }, () => {
          clearTimeout(connectGuard);
          sock.setNoDelay?.(true);
        });
        const connectGuard = setTimeout(() => fail("connect timeout"), 10000);
        const send = makeSend(sock);
        let authed = false;
        const rl = createInterface({ input: sock, terminal: false });
        rl.on("error", () => { /* socket error already handled on sock */ });
        let closed = false;
        const fail = (why) => {
          if (closed) return;
          closed = true;
          clearTimeout(connectGuard);
          log(`master link lost: ${why}`);
          // drop our subscriptions on this dead link
          for (const rec of state.goals.values()) {
            const i = rec.subscribers.indexOf(send);
            if (i >= 0) rec.subscribers.splice(i, 1);
          }
          sock.destroy();
          resolveLink();
        };
        sock.on("error", (err) => fail(String(err)));
        sock.on("close", () => fail("EOF"));
        rl.on("line", (raw) => {
          const line = raw.trim();
          if (!line) return;
          let msg;
          try { msg = JSON.parse(line); } catch { return; }
          if (!authed) {
            if (msg.type !== "registered") {
              log(`register rejected: ${JSON.stringify(msg)}`);
              fail("register rejected");
              return;
            }
            authed = true;
            log(`registered with master ${host}:${mport}`);
            backoff = 500;
            // resubscribe running goals to this connection
            for (const rec of state.goals.values()) {
              if (rec.status === "running") rec.subscribers.push(send);
            }
            return;
          }
          const cmd = msg.cmd;
          try {
            if (cmd === "goal") {
              const gid = msg.goal?.goalId || newGoalId();
              const g = { ...msg.goal, goalId: gid };
              const rec = state.submit(g);
              rec.subscribers.push(send); // events stream to master
              send({ type: "goal_accepted", goalId: gid });
            } else if (cmd === "attach") {
              const gid = msg.goalId;
              const rec = state.goals.get(gid);
              if (!rec) {
                send({ type: "error", goalId: gid, message: "unknown goalId" });
                return;
              }
              const base = rec.trimmed;
              const evs = [...rec.events];
              for (let i = 0; i < evs.length; i++) {
                send({ type: "event", goalId: gid, event: evs[i], seq: base + i + 1 });
              }
              if (rec.status !== "running") {
                send({ type: "goal_status", goalId: gid, status: rec.status, exit: rec.exitCode });
              } else {
                rec.subscribers.push(send);
              }
            } else if (cmd === "inject") {
              const rec = state.goals.get(msg.goalId);
              const payload = msg.payload ?? {};
              if (!rec || !rec.engine) {
                send({ type: "receipt", goalId: msg.goalId, ack: msg.ack, queued: "dropped", reason: "engine not ready" });
              } else {
                rec.engine.inject(payload);
                send({ type: "receipt", goalId: msg.goalId, ack: msg.ack, queued: "delivered" });
              }
            } else if (cmd === "ping") {
              send({ type: "pong", ts: nowMs() });
            }
          } catch (err) {
            try { send({ type: "error", message: String(err?.message ?? err) }); } catch { /* link dead */ }
          }
        });
        // outbound: register first thing
        const caps = {
          os: platform(),
          node: process.version,
          user: process.env.USER || process.env.USERNAME ||
            (() => { try { return userInfo().username; } catch { return "?"; } })(),
        };
        send({
          cmd: "register", nodeId, token: enrollToken, capabilities: caps,
        });
      });
    } catch (err) {
      log(`master link lost: ${err?.stack || err}`);
    }
    await new Promise((r) => setTimeout(r, backoff));
    backoff = Math.min(backoff * 2, 30000);
  }
}

/* ------------------------------------------------------------------ *
 * serve / start
 * ------------------------------------------------------------------ */
function pickFreePort() {
  return new Promise((resolve) => {
    const s = createServer();
    s.listen(0, "127.0.0.1", () => {
      const { port } = s.address();
      s.close(() => resolve(port));
    });
  });
}

function computeCodeHash() {
  const base = dirname(fileURLToPath(import.meta.url));
  const h = createHash("sha256");
  // hash the WHOLE runtime (an rda.mjs change alone must trigger daemon
  // restart, else the in-memory GoalEngine stays stale)
  for (const name of ["rda_daemon.mjs", "rda.mjs", "protocol.mjs"].sort()) {
    const fp = join(base, name);
    try {
      h.update(name);
      h.update(readFileSync(fp));
    } catch { /* file missing (partial deploy) */ }
  }
  return h.digest("hex");
}

export async function cmdServe(port, { master, nodeToken } = {}) {
  mkdirSync(RAO_DIR, { recursive: true });
  const token = randomBytes(16).toString("hex");
  if (!port) port = await pickFreePort();
  const state = new DaemonState(token);
  const server = serveState(state, port);
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(port, "127.0.0.1", resolve);
  });
  writeFileSync(DAEMON_JSON, JSON.stringify({
    pid: process.pid, port, token,
    version: DAEMON_VERSION,
    codeHash: computeCodeHash(),
    startedMs: nowMs(),
  }));
  if (platform() !== "win32") {
    try { chmodSync(DAEMON_JSON, 0o600); } catch { /* ignore */ }
  }
  state.startHeartbeat();
  if (master) {
    const idx = master.lastIndexOf(":");
    const host = idx > 0 ? master.slice(0, idx) : "127.0.0.1";
    const mport = Number(master.slice(idx + 1)) || 9700;
    masterLinkLoop(state, host, mport, nodeToken, hostname()).catch(() => {});
  }
  log(`serving on 127.0.0.1:${port}`);
  const cleanup = () => {
    try { rmSync(DAEMON_JSON, { force: true }); } catch { /* ignore */ }
  };
  process.on("SIGTERM", () => { cleanup(); process.exit(0); });
  process.on("SIGINT", () => { cleanup(); process.exit(0); });
  return 0;
}

/* one-shot local TCP client: auth + messages, collect reply lines.
 * Early-exits when the expected reply type for the last command arrives;
 * otherwise reads until quiet timeout. Never rejects on timeouts. */
function localClient(port, token, messages, readTimeoutMs = 5000) {
  return new Promise((resolve) => {
    const lines = [];
    const sock = connect({ host: "127.0.0.1", port, timeout: 5000 });
    let buf = "";
    const finish = () => {
      try { sock.destroy(); } catch { /* ignore */ }
      resolve(lines);
    };
    sock.on("error", () => resolve(lines));
    sock.on("timeout", () => finish());
    sock.on("close", () => resolve(lines));
    sock.on("connect", () => {
      const payload = [JSON.stringify({ cmd: "auth", token })]
        .concat(messages.map((m) => JSON.stringify(m)))
        .join("\n") + "\n";
      sock.write(payload);
    });
    sock.on("data", (chunk) => {
      buf += chunk.toString("utf8");
      for (;;) {
        const nl = buf.indexOf("\n");
        if (nl < 0) break;
        const ln = buf.slice(0, nl).trim();
        buf = buf.slice(nl + 1);
        if (!ln) continue;
        let msg;
        try { msg = JSON.parse(ln); } catch { continue; }
        lines.push(msg);
        const expected = EXPECTED_REPLY[messages[messages.length - 1]?.cmd];
        if (expected && msg.type === expected) { finish(); return; }
        if (messages.some((m) => m.cmd === "submit") &&
            msg.type === "goal_status") { finish(); return; }
      }
    });
  });
}

const EXPECTED_REPLY = { ping: "pong", stop: "bye", shutdown: "bye", start: null };

export async function isAlive(dj) {
  if (!dj) return false;
  const lines = await localClient(dj.port, dj.token, [{ cmd: "ping" }], 3000);
  return lines.some((m) => m.type === "pong");
}

export async function cmdStart(port, { master, nodeToken } = {}) {
  mkdirSync(RAO_DIR, { recursive: true });
  const dj = loadDaemonJson();
  if (dj && (await isAlive(dj))) {
    process.stdout.write(`already running on port ${dj.port}\n`);
    return 0;
  }
  const { spawn } = await import("node:child_process");
  const self = fileURLToPath(import.meta.url);
  const args = [self, "serve", "--port", String(port || 0)];
  if (master) args.push("--master", master);
  if (nodeToken) args.push("--token", nodeToken);
  // ALWAYS propagate the resolved state dir: RAO_STATE_DIR is inherited via
  // env, but an explicit --home (or a resolved default) must reach the child
  // or parent/child disagree on where daemon.json lives
  args.push("--home", RAO_DIR);
  const logFd = openSync(DAEMON_LOG, "a");
  const child = spawn(process.execPath, args, {
    detached: true,
    stdio: ["ignore", logFd, logFd],
  });
  child.unref();
  closeSync(logFd);
  const deadline = Date.now() + 10000;
  while (Date.now() < deadline) {
    const d = loadDaemonJson();
    if (d && (await isAlive(d))) {
      process.stdout.write(`started on port ${d.port}\n`);
      return 0;
    }
    await new Promise((r) => setTimeout(r, 300));
  }
  process.stdout.write(`start timed out; see ${DAEMON_LOG}\n`);
  return 1;
}

export async function cmdPing() {
  const dj = loadDaemonJson();
  if (dj && (await isAlive(dj))) {
    process.stdout.write(`alive port=${dj.port}\n`);
    return 0;
  }
  process.stdout.write("not running\n");
  return 1;
}

export async function cmdInjectOne(goalId, payloadJson) {
  // control-plane: one-shot inject over a fresh local connection.
  // Prints the receipt JSON line; exit 0 only when queued == delivered.
  const dj = loadDaemonJson();
  if (!dj) {
    process.stdout.write("not running\n");
    return 1;
  }
  let lines;
  try {
    const payload = JSON.parse(payloadJson);
    lines = await localClient(dj.port, dj.token, [
      { cmd: "inject", goalId, payload },
    ], 5000);
  } catch (err) {
    process.stdout.write(JSON.stringify({ type: "error", message: String(err) }) + "\n");
    return 1;
  }
  const receipt = lines.find((m) => m.type === "injected");
  if (!receipt) {
    const err = lines.find((m) => m.type === "error");
    process.stdout.write(JSON.stringify({
      type: "injected", goalId, queued: "dropped",
      reason: err?.message ?? "no receipt",
    }) + "\n");
    return 1;
  }
  process.stdout.write(JSON.stringify(receipt) + "\n");
  return receipt.queued === "delivered" ? 0 : 1;
}

export async function cmdStop() {
  const dj = loadDaemonJson();
  if (!dj) {
    process.stdout.write("not running\n");
    return 0;
  }
  try {
    await localClient(dj.port, dj.token, [{ cmd: "shutdown" }], 5000);
  } catch { /* ignore */ }
  const deadline = Date.now() + 5000;
  while (Date.now() < deadline) {
    if (!(await isAlive(dj))) {
      process.stdout.write("stopped\n");
      return 0;
    }
    await new Promise((r) => setTimeout(r, 300));
  }
  process.stdout.write("stop timed out\n");
  return 1;
}

export async function cmdStatusOne(goalId) {
  const dj = loadDaemonJson();
  if (!dj) {
    process.stdout.write(JSON.stringify({ type: "error", message: "daemon not running" }) + "\n");
    return 1;
  }
  let lines;
  try {
    lines = await localClient(dj.port, dj.token, [
      { cmd: "status", goalId },
    ], 5000);
  } catch (err) {
    process.stdout.write(JSON.stringify({ type: "error", message: String(err) }) + "\n");
    return 1;
  }
  const snap = lines.find((m) => m.type === "goal_snapshot");
  process.stdout.write(JSON.stringify(snap ?? { type: "error", message: "no snapshot" }) + "\n");
  return snap ? 0 : 1;
}

/* ------------------------------------------------------------------ *
 * stdio <-> daemon TCP bridge (remote side of the SSH channel)
 * ------------------------------------------------------------------ */
export async function cmdBridge() {
  const dj = loadDaemonJson();
  const alive = dj ? await isAlive(dj) : false;
  if (!(dj && alive)) {
    process.stderr.write(`[bridge] startup check failed: dj=${Boolean(dj)} alive=${alive}\n`);
    process.stdout.write(JSON.stringify({ type: "error", message: "daemon not running" }) + "\n");
    return 3;
  }
  await new Promise((resolveBridge) => {
    // NOTE: no connect({timeout}) here — that option installs an IDLE
    // timeout which kills the bridge during quiet goal phases (wait_file
    // etc.). Python parity: pump reads block indefinitely (settimeout(None)).
    const sock = connect({ host: "127.0.0.1", port: dj.port });
    let readySeen = false;
    let stdinEnded = false;
    const connectGuard = setTimeout(() => die("connect timeout", 3), 5000);
    const die = (why, code) => {
      clearTimeout(connectGuard);
      process.stderr.write(`[bridge] pump exit (${why})\n`);
      try { sock.destroy(); } catch { /* ignore */ }
      resolveBridge(code);
    };
    sock.on("error", (err) => die(`socket error: ${err}`, 3));
    sock.on("close", () => {
      // reader lost the daemon stream: kill the bridge so the SSH
      // channel EOFs and the orchestrator reconnects with attach
      if (!stdinEnded) die("EOF from daemon or error", 3);
    });
    sock.on("connect", () => {
      clearTimeout(connectGuard);
      sock.setNoDelay?.(true);
      sock.write(JSON.stringify({ cmd: "auth", token: dj.token }) + "\n");
    });
    sock.on("data", (chunk) => {
      // first line is the ready reply — relay everything as-is
      if (!readySeen) readySeen = true;
      process.stdout.write(chunk);
    });
    // raw byte pump; node streams have no read-ahead buffering issue
    process.stdin.on("data", (chunk) => {
      if (!sock.destroyed) sock.write(chunk);
    });
    process.stdin.on("end", () => {
      stdinEnded = true;
      process.stdout.write("");
      try { sock.destroy(); } catch { /* ignore */ }
      resolveBridge(0);
    });
    process.stdin.on("error", () => { /* ignore */ });
  });
  return 0;
}

/* ------------------------------------------------------------------ *
 * CLI
 * ------------------------------------------------------------------ */
export async function main(argv = process.argv.slice(2)) {
  let home = null;
  const hi = argv.indexOf("--home");
  if (hi >= 0) {
    home = argv[hi + 1] ?? null;
    argv = argv.slice(0, hi).concat(argv.slice(hi + 2));
  }
  if (home) setHome(home);
  const [cmd, ...rest] = argv;
  const flag = (name) => {
    const i = rest.indexOf(name);
    if (i < 0) return undefined;
    const v = rest[i + 1];
    rest.splice(i, 2);
    return v;
  };
  if (cmd === "serve") {
    const port = Number(flag("--port") ?? 0);
    const master = flag("--master");
    const nodeToken = flag("--token");
    return cmdServe(port, { master, nodeToken });
  }
  if (cmd === "start") {
    const port = Number(flag("--port") ?? 0);
    const master = flag("--master");
    const nodeToken = flag("--token");
    return cmdStart(port, { master, nodeToken });
  }
  if (cmd === "ping") return cmdPing();
  if (cmd === "stop") return cmdStop();
  if (cmd === "inject-one") {
    const goalId = flag("--goal-id");
    let payloadJson = flag("--payload");
    const b64 = flag("--payload-b64");
    if (b64) payloadJson = Buffer.from(b64, "base64").toString("utf8");
    if (!goalId) {
      process.stderr.write("inject-one requires --goal-id\n");
      return 2;
    }
    return cmdInjectOne(goalId, payloadJson ?? "{}");
  }
  if (cmd === "status-one") {
    const goalId = flag("--goal-id");
    if (!goalId) {
      process.stderr.write("status-one requires --goal-id\n");
      return 2;
    }
    return cmdStatusOne(goalId);
  }
  if (cmd === "client-bridge") return cmdBridge();
  process.stderr.write(
    "usage: node rda_daemon.mjs [--home DIR] <serve|start|ping|stop|status-one|inject-one|client-bridge>\n");
  return 2;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().then((code) => { process.exitCode = code; });
}
