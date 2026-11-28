/**
 * rao master — node-mode control endpoint (v0.10). A TCP listener that
 * registered rao nodes dial INTO (outbound from the node); control/data
 * plane for node devices runs over this connection. SSH devices keep using
 * SshDaemonTransport unchanged (coexistence per PROTOCOL-NODE.md).
 *
 * Messages (JSON Lines), node -> master:
 *   {"cmd":"register","nodeId":...,"token":...,"capabilities":{...}}
 *   {"type":"event","goalId":G,"event":{...},"seq":N}   (forwarded by node)
 *   {"type":"receipt","goalId":G,"ack":A,"queued":Q}    (inject receipt)
 *   {"type":"goal_status","goalId":G,"status":...,"exit":N}
 * master -> node:
 *   {"cmd":"goal","goal":{...}}
 *   {"cmd":"attach","goalId":G}
 *   {"cmd":"inject","goalId":G,"payload":{...},"ack":A}
 *
 * Ported from src/rao/master.py (v0.11.1).
 */
import { createServer, type Server } from "node:net";
import { randomBytes } from "node:crypto";
import { chmodSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { RAO_VERSION } from "./version.js";
// @ts-ignore — canonical runtime implementation is plain .mjs (zero-dep)
import {
  NoiseError, noiseResponderHandshake, noiseSplit,
} from "./runtime/noise.mjs";
import type { DeviceRecord } from "./devices.js";
import type { DispatchedGoal } from "./protocol.js";
import type { GoalChannel, Transport } from "./orchestrator.js";

export function raoStateDir(): string {
  return process.env.RAO_STATE_DIR || join(homedir(), ".rao");
}

export function masterTokenPath(): string {
  return join(raoStateDir(), "master-token");
}

export function masterNodesPath(): string {
  return join(raoStateDir(), "nodes.json");
}

export function ensureMasterToken(): string {
  const path = masterTokenPath();
  try {
    const token = readFileSync(path, "utf8").trim();
    if (token) return token;
  } catch { /* generate below */ }
  const token = randomBytes(20).toString("hex");
  mkdirSync(raoStateDir(), { recursive: true });
  writeFileSync(path, token + "\n");
  try {
    if (process.platform !== "win32") chmodSync(path, 0o600);
  } catch { /* ignore */ }
  return token;
}

interface NodeEntry {
  info: Record<string, unknown>;
  lastSeen: number;
  sock: import("node:net").Socket | null;
  peer: string;
  /** Noise_IK session when the node registered via the secure path */
  session: NoiseSession | null;
}

type WrappedMsg = Record<string, unknown> & { goalId?: string };

/** raw x25519 keypair (32-byte buffers) — matches src/runtime/noise.mjs */
export interface KeyPairRaw { priv: Buffer; pub: Buffer }

/** session shape returned by src/runtime/noise.mjs noiseSplit() */
interface NoiseSession {
  send(plaintext: unknown): Record<string, unknown>;
  recv(frame: Record<string, unknown>): Buffer;
}

export class NodeGoalChannel implements GoalChannel {
  readonly goalId: string;
  private readonly nodeId: string;
  private readonly master: MasterServer;
  /** @internal wrapped messages (events + receipts + status), read by MasterServer routing */
  readonly inbox: WrappedMsg[] = [];
  closed = false;
  exit_code: number | null = null;
  killed = false;
  supports_resume = true;
  /** @internal last goal_status, read by MasterServer.goalFinished */
  status: string | null = null;
  private waiters: Array<{
    since: number;
    resolve: (v: [import("./protocol.js").RaoEvent | null, number]) => void;
    timer: NodeJS.Timeout;
  }> = [];
  /** _DaemonLink-compatible attributes used by GoalRun._pump_daemon */
  stderr_text: string[] = [];
  events: import("./protocol.js").RaoEvent[] = [];
  event_seqs: Array<number | null> = [];

  constructor(goalId: string, nodeId: string, master: MasterServer) {
    this.goalId = goalId;
    this.nodeId = nodeId;
    this.master = master;
  }

  /** dispatch the goal to the node (over its master connection) */
  sendGoal(goalObj: DispatchedGoal): void {
    this.master.sendToNode(this.nodeId, { cmd: "goal", goal: goalObj });
  }

  sendAttach(goalId: string): void {
    this.master.sendToNode(this.nodeId, { cmd: "attach", goalId });
  }

  /** master router feeds a wrapped message here */
  push(msg: WrappedMsg): void {
    const t = msg.type as string | undefined;
    if (t === "event") {
      const ev = (msg.event ?? {}) as import("./protocol.js").RaoEvent;
      this.events.push(ev);
      this.event_seqs.push((msg.seq as number | undefined) ?? null);
      const w = this.waiters[0];
      if (w && this.events.length > w.since) {
        clearTimeout(w.timer);
        w.resolve([this.events[w.since]!, w.since + 1]);
        this.waiters.shift();
      }
    } else if (t === "goal_status") {
      this.status = msg.status as string;
      this.exit_code = (msg.exit as number) ?? 0;
    }
    this.inbox.push(msg);
  }

  waitEvent(
    since: number,
    timeoutMs: number,
  ): Promise<[import("./protocol.js").RaoEvent | null, number]> {
    if (this.events.length > since) {
      return Promise.resolve([this.events[since]!, since + 1]);
    }
    if (this.closed) return Promise.resolve([null, since]);
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

  /** inject via the node connection; waits for the receipt */
  writeLine(obj: Record<string, unknown>): Promise<boolean> {
    return this.master.inject(this.nodeId, this.goalId, obj);
  }

  alive(): boolean {
    return !this.closed && this.master.nodeAlive(this.nodeId);
  }

  waitExit(timeoutMs: number): Promise<number | null> {
    if (this.master.goalFinished(this.goalId)) return Promise.resolve(0);
    return new Promise((resolve) => {
      const t = setInterval(() => {
        if (this.master.goalFinished(this.goalId) || this.closed) {
          clearInterval(t);
          resolve(0);
        }
      }, 100);
      setTimeout(() => {
        clearInterval(t);
        resolve(null);
      }, timeoutMs);
    });
  }

  kill(): void {
    this.killed = true;
    this.closed = true;
  }

  close(): void {
    this.closed = true;
    this.master.detach(this.goalId);
  }
}

export class MasterServer {
  readonly port: number;
  readonly enrollmentToken: string;
  private readonly nodes = new Map<string, NodeEntry>();
  private readonly goalChannels = new Map<string, NodeGoalChannel>();
  private server: Server | null = null;
  private readonly registry: { reload(): Promise<void> } | null;
  /** E2EE (Noise_IK): master static keypair + enrolled node static pubs */
  readonly noiseStatic: KeyPairRaw | null;
  readonly noiseNodeKeys: Map<string, Buffer>;
  /** handshake-replay guard (per process) */
  readonly seenEphemerals = new Set<string>();
  /** pairing one-time codes: code -> {nodeId, expiresMs} (one-time, 5 min) */
  private readonly pairingCodes = new Map<string, { nodeId: string; expiresMs: number }>();
  /** enrolled node static pubs (persisted at nodes-noise.json) */
  private readonly enrolledNoise = new Map<string, Buffer>();
  private readonly noiseEnrollPath = join(raoStateDir(), "nodes-noise.json");

  constructor(opts: {
    port?: number;
    registry?: { reload(): Promise<void> };
    /** enable Noise_IK for nodes enrolled in noiseNodeKeys */
    noiseStatic?: KeyPairRaw;
    noiseNodeKeys?: Map<string, Buffer>;
  } = {}) {
    this.port = opts.port ?? 0;
    this.registry = opts.registry ?? null;
    this.noiseStatic = opts.noiseStatic ?? null;
    this.noiseNodeKeys = opts.noiseNodeKeys ?? new Map();
    this.enrollmentToken = ensureMasterToken();
    this.loadNoiseEnrollments();
  }

  /* -- device pairing v1 (one-time code -> key possession) ---------------- */

  /** admin-side: mint a one-time pairing code for nodeId (5 min TTL) */
  createPairingCode(nodeId: string): { code: string; nodeId: string; expiresMs: number } {
    const code = String(Math.floor(100000 + Math.random() * 900000));
    this.pairingCodes.set(code, { nodeId, expiresMs: Date.now() + 5 * 60_000 });
    // opportunistic cleanup of expired codes
    for (const [c, rec] of this.pairingCodes) {
      if (rec.expiresMs < Date.now()) this.pairingCodes.delete(c);
    }
    return { code, nodeId, expiresMs: Date.now() + 5 * 60_000 };
  }

  /** device-side: consume a code; verifies key POSSESSION (the device
   *  generated its noise keypair locally and presents the public half) */
  private consumePairingCode(code: string, nodeId: string, noisePub: Buffer): void {
    const rec = this.pairingCodes.get(code);
    if (!rec) throw new Error("invalid pairing code");
    if (rec.expiresMs < Date.now()) {
      this.pairingCodes.delete(code);
      throw new Error("pairing code expired");
    }
    if (rec.nodeId !== nodeId) {
      throw new Error("pairing code was issued for a different nodeId");
    }
    this.pairingCodes.delete(code); // one-time
    this.enrolledNoise.set(nodeId, noisePub);
    this.noiseNodeKeys.set(nodeId, noisePub);
    mkdirSync(raoStateDir(), { recursive: true });
    const data: Record<string, string> = {};
    for (const [nid, pub] of this.enrolledNoise) {
      data[nid] = pub.toString("base64");
    }
    writeFileSync(this.noiseEnrollPath, JSON.stringify(data, null, 2));
  }

  private loadNoiseEnrollments(): void {
    try {
      const data = JSON.parse(readFileSync(this.noiseEnrollPath, "utf8")) as Record<string, string>;
      for (const [nid, pubB64] of Object.entries(data)) {
        this.enrolledNoise.set(nid, Buffer.from(pubB64, "base64"));
        this.noiseNodeKeys.set(nid, Buffer.from(pubB64, "base64"));
      }
    } catch { /* no enrollments yet */ }
  }

  /* -- node connection plumbing ------------------------------------------ */

  sendToNode(nodeId: string, obj: Record<string, unknown>): void {
    const node = this.nodes.get(nodeId);
    if (!node || node.sock === null) {
      throw new Error(`node ${nodeId} not connected`);
    }
    if (node.session) {
      node.sock.write(JSON.stringify(node.session.send(obj)) + "\n");
      return;
    }
    node.sock.write(JSON.stringify(obj) + "\n");
  }

  nodeAlive(nodeId: string): boolean {
    const node = this.nodes.get(nodeId);
    return Boolean(node && node.sock !== null);
  }

  persistNodes(): void {
    const data: Record<string, unknown> = {};
    for (const [nid, node] of this.nodes) {
      data[nid] = {
        capabilities: node.info,
        peer: node.peer,
        lastSeen: node.lastSeen,
      };
    }
    mkdirSync(raoStateDir(), { recursive: true });
    writeFileSync(
      masterNodesPath(),
      JSON.stringify(data, null, 2),
    );
  }

  /** devices.json merge feed (DeviceRegistry.mergeRegisteredNodes) */
  registeredDevices(): DeviceRecord[] {
    const out: DeviceRecord[] = [];
    for (const [nid, node] of this.nodes) {
      const info = node.info as Record<string, unknown>;
      out.push({
        id: nid,
        host: (info.host as string) ?? node.peer,
        port: null,
        user: info.user as string | undefined,
        workspace: info.workspace as string | undefined,
        transport: "node",
        source: "node",
        maxConcurrentGoals:
          (info.maxConcurrentGoals as number | undefined) ?? 2,
        online: node.sock !== null,
        execution: info as Record<string, unknown>,
      });
    }
    return out;
  }

  /* -- goal channel routing ------------------------------------------------ */

  getNodeChannel(nodeId: string, goalId: string): NodeGoalChannel {
    let ch = this.goalChannels.get(goalId);
    if (!ch) {
      ch = new NodeGoalChannel(goalId, nodeId, this);
      this.goalChannels.set(goalId, ch);
    }
    if (!this.nodeAlive(nodeId)) {
      throw new Error(`node ${nodeId} not connected`);
    }
    return ch;
  }

  routeInbound(msg: WrappedMsg): void {
    const gid = msg.goalId ?? undefined;
    if (!gid) return;
    this.goalChannels.get(gid)?.push(msg);
  }

  /** control-plane inject over the node connection; waits for receipt.
   *  Retries on timeout — the node's pending queue makes re-delivery safe. */
  async inject(
    nodeId: string,
    goalId: string,
    payload: Record<string, unknown>,
    waitAckMs = 5000,
    retries = 3,
  ): Promise<boolean> {
    for (let attempt = 0; attempt < retries; attempt++) {
      const ch = this.goalChannels.get(goalId);
      if (!ch) return false;
      const ack = `ack-${Date.now()}-${attempt}`;
      try {
        this.sendToNode(nodeId, { cmd: "inject", goalId, payload, ack });
      } catch {
        return false;
      }
      const got = await new Promise<WrappedMsg | null>((resolve) => {
        const pending = ch.inbox.find(
          (m) => m.type === "receipt" && m.ack === ack);
        if (pending) { resolve(pending); return; }
        const timer = setTimeout(() => resolve(null), waitAckMs);
        const iv = setInterval(() => {
          const m = ch.inbox.find(
            (x) => x.type === "receipt" && x.ack === ack);
          if (m) { clearInterval(iv); clearTimeout(timer); resolve(m); }
        }, 50);
        setTimeout(() => clearInterval(iv), waitAckMs + 500);
      });
      if (got) return got.queued === "delivered";
      if (ch.closed) return false;
      await new Promise((r) => setTimeout(r, 1000)); // receipt lost: retry
    }
    return false;
  }

  detach(goalId: string): void {
    this.goalChannels.delete(goalId);
  }

  goalFinished(goalId: string): boolean {
    const ch = this.goalChannels.get(goalId);
    if (!ch) return false;
    return ch.status !== null ||
      ch.inbox.some((m) => (m.event as { kind?: string } | undefined)?.kind === "result");
  }

  /* -- lifecycle ------------------------------------------------------------ */

  start(port?: number): Promise<number> {
    const p = port ?? this.port;
    return new Promise((resolve, reject) => {
      const server = createServer((sock) => {
        this.handleConnection(sock);
      });
      server.on("error", reject);
      server.listen(p, "0.0.0.0", () => {
        const addr = server.address() as { port: number };
        this.server = server;
        resolve(addr.port);
      });
    });
  }

  private handleConnection(sock: import("node:net").Socket): void {
    sock.setNoDelay?.(true);
    const rl = createInterface({ input: sock, terminal: false });
    rl.on("error", () => { /* socket error handled below */ });
    let nodeId: string | null = null;
    let session: NoiseSession | null = null;
    const peer = sock.remoteAddress ?? "?";
    const writeLine = (obj: Record<string, unknown>): void => {
      // all master->node traffic goes through the session when E2EE is on
      if (session) {
        sock.write(JSON.stringify(session.send(obj)) + "\n");
      } else {
        sock.write(JSON.stringify(obj) + "\n");
      }
    };
    rl.on("line", (raw) => {
      const line = raw.trim();
      if (!line) return;
      if (session && nodeId !== null) {
        // encrypted data frame → unwrap, then route
        let frame: Record<string, unknown>;
        try { frame = JSON.parse(line); } catch { return; }
        let msg: WrappedMsg;
        try {
          msg = JSON.parse(session.recv(frame).toString("utf8")) as WrappedMsg;
        } catch {
          // replay/tamper: fail-closed — drop the link
          sock.destroy();
          return;
        }
        this.routeInbound(msg);
        const node = this.nodes.get(nodeId);
        if (node) node.lastSeen = Date.now();
        return;
      }
      let msg: WrappedMsg;
      try { msg = JSON.parse(line); } catch { return; }
      if (nodeId === null) {
        // one-shot control lines before any register: pairing endpoints
        if (msg.cmd === "pair-create") {
          // admin-only: possession of the local master token
          try {
            if (msg.adminToken !== this.enrollmentToken) {
              throw new Error("admin token mismatch");
            }
            const made = this.createPairingCode(String(msg.nodeId ?? ""));
            sock.write(JSON.stringify({
              type: "pair-code", code: made.code, nodeId: made.nodeId,
              expiresMs: made.expiresMs, masterVersion: RAO_VERSION,
            }) + "\n");
          } catch (err) {
            sock.write(JSON.stringify({
              type: "error", message: String((err as Error)?.message ?? err),
            }) + "\n");
          }
          sock.end();
          return;
        }
        if (msg.cmd === "pair") {
          // device presents its locally-generated noise public key
          try {
            const code = String(msg.code ?? "");
            const nid = String(msg.nodeId ?? "");
            const noisePub = Buffer.from(String(msg.noisePub ?? ""), "base64");
            if (!nid || noisePub.length !== 32) {
              throw new Error("nodeId and 32-byte noisePub required");
            }
            this.consumePairingCode(code, nid, noisePub);
            sock.write(JSON.stringify({
              type: "paired", nodeId: nid,
              masterNoisePub: (this.noiseStatic?.pub ?? Buffer.alloc(0)).toString("base64"),
              enrollmentToken: this.enrollmentToken,
              masterVersion: RAO_VERSION,
            }) + "\n");
          } catch (err) {
            sock.write(JSON.stringify({
              type: "error", message: String((err as Error)?.message ?? err),
            }) + "\n");
          }
          sock.end();
          return;
        }
        // first line must be a valid register
        if (msg.cmd !== "register" || msg.token !== this.enrollmentToken) {
          sock.write(JSON.stringify({
            type: "error", message: "register with valid token first",
          }) + "\n");
          sock.destroy();
          return;
        }
        const nid = msg.nodeId as string | undefined;
        if (!nid) {
          sock.write(JSON.stringify({ type: "error", message: "nodeId required" }) + "\n");
          sock.destroy();
          return;
        }
        // optional E2EE upgrade (Noise_IK): the node initiates with ik1;
        // prologue binds the nodeId so a captured handshake cannot be
        // replayed onto another identity's link
        if (msg.sec === "noise-ik-v1") {
          const nodeStaticPub = this.noiseNodeKeys?.get(nid);
          if (!nodeStaticPub || !this.noiseStatic) {
            sock.write(JSON.stringify({
              type: "error", message: "noise not enrolled for this node",
            }) + "\n");
            sock.destroy();
            return;
          }
          try {
            const res = noiseResponderHandshake(
              this.noiseStatic,
              `rao-node-link|v1|${nid}`,
              msg as unknown as Record<string, unknown>,
              this.seenEphemerals,
              Buffer.from(JSON.stringify({
                registered: true, masterVersion: RAO_VERSION,
              })),
            );
            // capabilities arrive ENCRYPTED (res.payload) — msg.pl is ciphertext
            const capabilities = JSON.parse(res.payload.toString("utf8"));
            // identity binding: fail-closed unless the initiator static
            // matches the enrolled node key
            if (!res.initiatorStaticPub.equals(nodeStaticPub)) {
              throw new NoiseError("initiator identity mismatch");
            }
            session = noiseSplit(res.hs, false);
            const reply = res.reply as Record<string, unknown>;
            reply.type = "registered";
            reply.nodeId = nid;
            sock.write(JSON.stringify(reply) + "\n");
            const prev = this.nodes.get(nid);
            if (prev && prev.sock !== null) {
              try { prev.sock.destroy(); } catch { /* stale */ }
            }
            this.nodes.set(nid, {
              info: capabilities as Record<string, unknown>,
              lastSeen: Date.now(),
              sock,
              peer,
              session,
            });
            nodeId = nid;
            this.persistNodes();
            void this.registry?.reload().catch(() => { /* registry optional */ });
            return;
          } catch (err) {
            sock.write(JSON.stringify({
              type: "error",
              message: `noise handshake failed: ${String((err as Error)?.message ?? err)}`,
            }) + "\n");
            sock.destroy();
            return;
          }
        }
        const prev = this.nodes.get(nid);
        if (prev && prev.sock !== null) {
          try { prev.sock.destroy(); } catch { /* stale */ }
        }
        this.nodes.set(nid, {
          info: (msg.capabilities ?? {}) as Record<string, unknown>,
          lastSeen: Date.now(),
          sock,
          peer,
          session: null,
        });
        nodeId = nid;
        sock.write(JSON.stringify({
          type: "registered", nodeId: nid, masterVersion: RAO_VERSION,
        }) + "\n");
        this.persistNodes();
        void this.registry?.reload().catch(() => { /* registry optional */ });
        return;
      }
      this.routeInbound(msg);
      const node = this.nodes.get(nodeId);
      if (node) node.lastSeen = Date.now();
    });
    sock.on("close", () => {
      if (nodeId) {
        const node = this.nodes.get(nodeId);
        if (node && node.sock === sock) node.sock = null; // offline, keep record
      }
    });
    sock.on("error", () => { /* close will follow */ });
  }

  stop(): void {
    this.server?.close();
  }
}

/** start the master listener; resolves with the bound port */
export async function startMaster(
  port = 9700,
  registry?: { reload(): Promise<void> },
): Promise<MasterServer> {
  const m = new MasterServer({ port, registry });
  await m.start(port);
  return m;
}

/* ------------------------------------------------------------------------ *
 * NodeTransport: rides the master connection for node-mode devices.
 * ------------------------------------------------------------------------ */
export class NodeTransport implements Transport {
  constructor(private readonly master: MasterServer) {}

  async openSession(
    goalId: string,
    device: DeviceRecord,
    goal: DispatchedGoal,
  ): Promise<NodeGoalChannel> {
    const ch = this.master.getNodeChannel(device.id, goalId);
    ch.sendGoal(goal);
    return ch;
  }

  async openResumeSession(
    goalId: string,
    device: DeviceRecord,
  ): Promise<NodeGoalChannel> {
    const ch = this.master.getNodeChannel(device.id, goalId);
    ch.sendAttach(goalId);
    return ch;
  }

  steer(
    device: DeviceRecord,
    goalId: string,
    payload: Record<string, unknown>,
  ): Promise<boolean> {
    return this.master.inject(device.id, goalId, payload);
  }

  async provision(): Promise<{ checked: boolean; deployed: boolean }> {
    return { checked: true, deployed: false }; // node self-manages its runtime
  }
}

/* ------------------------------------------------------------------------ *
 * MultiTransport: routes per device (transport=="node" -> NodeTransport).
 * ------------------------------------------------------------------------ */
export class MultiTransport implements Transport {
  constructor(
    private readonly ssh: Transport,
    private readonly node: NodeTransport | null,
  ) {}

  private forDevice(device: DeviceRecord): Transport | null {
    if (device.transport === "node") return this.node;
    return this.ssh;
  }

  async openSession(
    goalId: string,
    device: DeviceRecord,
    goal: DispatchedGoal,
  ): Promise<GoalChannel> {
    const t = this.forDevice(device);
    if (!t) throw new Error(`no transport for device ${device.id}`);
    return t.openSession(goalId, device, goal);
  }

  async openResumeSession(
    goalId: string,
    device: DeviceRecord,
  ): Promise<GoalChannel> {
    const t = this.forDevice(device);
    if (!t?.openResumeSession) {
      throw new Error(`transport cannot resume for device ${device.id}`);
    }
    return t.openResumeSession(goalId, device);
  }

  async provision(device: DeviceRecord): Promise<unknown> {
    const t = this.forDevice(device);
    return t?.provision ? t.provision(device) : { checked: false };
  }

  async steer(
    device: DeviceRecord,
    goalId: string,
    payload: Record<string, unknown>,
  ): Promise<boolean> {
    const t = this.forDevice(device);
    if (t?.steer) return t.steer(device, goalId, payload);
    return false;
  }

  async goalEvents(
    device: DeviceRecord,
    goalId: string,
  ): Promise<import("./protocol.js").RaoEvent[] | null> {
    const t = this.forDevice(device);
    if (t?.goalEvents) return t.goalEvents(device, goalId);
    return null;
  }
}
