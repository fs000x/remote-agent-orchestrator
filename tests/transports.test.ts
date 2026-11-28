/**
 * Transport layer tests: LineStream framing, GoalChannelBase waiter
 * contract, DaemonChannelBase handshake/routing/ack-inject, MasterServer +
 * NodeTransport over a real dial-in node, MultiTransport routing,
 * provision local hashes. End-to-end: Orchestrator -> TcpDaemonTransport
 * -> real rda_daemon.mjs child process.
 */
import { spawn } from "node:child_process";
import { connect } from "node:net";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { LineStream } from "../src/transports/lineStream.js";
import { GoalChannelBase } from "../src/transports/channel.js";
import { DaemonChannelBase } from "../src/transports/daemonChannel.js";
import { TcpDaemonTransport } from "../src/transports/tcpDaemon.js";
import {
  MasterServer,
  MultiTransport,
  NodeTransport,
} from "../src/master.js";
import { Orchestrator } from "../src/orchestrator.js";
import { DeviceRegistry } from "../src/devices.js";
import { localHashes, RDA_FILES, runtimeDir } from "../src/provision.js";
import { validateEvent } from "../src/protocol.js";
import { existsSync } from "node:fs";
import type { RaoEvent } from "../src/protocol.js";

/* ---------------- LineStream ---------------- */

describe("LineStream", () => {
  it("frames complete lines, buffers partials, drops noise", async () => {
    const ls = new LineStream();
    ls.push('{"a":1}\n{"b":');
    const p1 = ls.next(null);
    ls.push('2}\nnot json at all\n{"c":3}\n');
    expect(await p1).toEqual({ a: 1 });
    expect(await ls.next(null)).toEqual({ b: 2 });
    expect(await ls.next(null)).toEqual({ c: 3 });
    expect(ls.noise).toEqual(["not json at all"]);
  });

  it("EOF delivers queued then null; timeout returns null", async () => {
    const ls = new LineStream();
    ls.push('{"x":1}\n');
    ls.end();
    expect(await ls.next(null)).toEqual({ x: 1 });
    expect(await ls.next(null)).toBeNull();
    const empty = new LineStream();
    const t0 = Date.now();
    expect(await empty.next(50)).toBeNull();
    expect(Date.now() - t0).toBeGreaterThanOrEqual(40);
  });
});

/* ---------------- GoalChannelBase ---------------- */

class TestChannel extends GoalChannelBase {
  handle(line: string): boolean {
    return this.handleLine(line);
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
}

function ev(goalId: string, kind: RaoEvent["kind"], data: Record<string, unknown> = {}): string {
  return JSON.stringify({ protocol: "rao/1", kind, goalId, ts: Date.now(), data });
}

describe("GoalChannelBase", () => {
  it("handleLine validates protocol events; bad lines to stderr", () => {
    const ch = new TestChannel("g1");
    expect(ch.handle(ev("g1", "started"))).toBe(true);
    expect(ch.handle(ev("g2", "started"))).toBe(true); // other goalId: still valid
    expect(ch.handle("garbage")).toBe(false);
    expect(ch.handle(ev("g1", "bogus-kind"))).toBe(false);
    expect(ch.events.length).toBe(2);
    expect(ch.stderr_text.length).toBe(2);
  });

  it("waitEvent returns one event per poll (never skips buffered)", async () => {
    const ch = new TestChannel("g1");
    ch.handle(ev("g1", "started"));
    ch.handle(ev("g1", "recon", { facts: {} }));
    ch.handle(ev("g1", "result", { status: "success" }));
    const [e0, i0] = await ch.waitEvent(0, 100);
    expect(e0?.kind).toBe("started");
    expect(i0).toBe(1);
    const [e1, i1] = await ch.waitEvent(i0, 100);
    expect(e1?.kind).toBe("recon");
    expect(i1).toBe(2);
    const [e2, i2] = await ch.waitEvent(i1, 100);
    expect(e2?.kind).toBe("result");
    expect(i2).toBe(3);
  });

  it("waitEvent blocks until push, then unblocks with [ev, since+1]", async () => {
    const ch = new TestChannel("g1");
    const p = ch.waitEvent(0, 2000);
    setTimeout(() => ch.handle(ev("g1", "started")), 80);
    const [e, i] = await p;
    expect(e?.kind).toBe("started");
    expect(i).toBe(1);
  });

  it("waitExit polls exit_code; close unblocks", async () => {
    const ch = new TestChannel("g1");
    const p = ch.waitExit(3000);
    setTimeout(() => ch.setExitCode(4), 80);
    expect(await p).toBe(4);
  });
});

/* ---------------- DaemonChannelBase ---------------- */

type Route = (msg: Record<string, unknown>) => void;

class FakeDaemonChannel extends DaemonChannelBase {
  sent: Record<string, unknown>[] = [];
  private onSend: Route | null = null;

  constructor(handshake: "bridge" | "direct" = "direct") {
    super("g-ack", handshake, "tok");
  }

  setRoute(r: Route): void {
    this.onSend = r;
    // replay queued handshake messages
    for (const m of this.sent) r(m);
  }

  protected async openLink(): Promise<void> { /* nothing */ }
  protected sendLine(obj: Record<string, unknown>): void {
    this.sent.push(obj);
    if (this.onSend) this.onSend(obj);
  }
  protected linkAlive(): boolean { return true; }
  protected closeLink(): void { this.stream.end(); }

  /** test helper: daemon pushes a wrapped message */
  fromDaemon(msg: Record<string, unknown>): void {
    this.stream.push(JSON.stringify(msg) + "\n");
  }
}

function daemonGoal() {
  return {
    protocol: "rao/1", goalId: "g-ack", deviceId: "d",
    objective: "x",
    plan: { planner: "main-agent", steps: [{ id: "s1", type: "shell", command: "echo hi" }] },
  };
}

describe("DaemonChannelBase", () => {
  it("handshake: direct auth + submit → ack → pump routes events", async () => {
    const ch = new FakeDaemonChannel("direct");
    const p = ch.connectAndStart(daemonGoal(), false);
    ch.setRoute((msg) => {
            if (msg.cmd === "auth") ch.fromDaemon({ type: "ready", version: "0.3.0" });
      else if (msg.cmd === "submit") {
                ch.fromDaemon({ type: "ack", goalId: "g-ack" });
        ch.fromDaemon({ type: "event", goalId: "g-ack", event: ev0("started"), seq: 1 });
              }
    });
    await p;
    await new Promise((r) => setTimeout(r, 100));
        const submit = ch.sent.find((m) => m.cmd === "submit")!;
    expect(submit.goal).toEqual(daemonGoal());
                    expect(ch.events[0].kind).toBe("started");
    expect(ch.event_seqs[0]).toBe(1);
  });

  it("handshake: resume attach replays", async () => {
    const ch = new FakeDaemonChannel("direct");
    const p = ch.connectAndStart(null, true);
    ch.setRoute((msg) => {
      if (msg.cmd === "auth") ch.fromDaemon({ type: "ready" });
      else if (msg.cmd === "attach") {
        ch.fromDaemon({ type: "attached", goalId: "g-ack", status: "running" });
        ch.fromDaemon({ type: "event", goalId: "g-ack", event: ev0("progress", { phase: "running" }), seq: 2 });
      }
    });
    await p;
    expect(ch.sent.some((m) => m.cmd === "attach")).toBe(true);
    expect(ch.events[0].kind).toBe("progress");
  });

  it("failed handshake (no ready) → TRANSPORT_LOST", async () => {
    const ch = new FakeDaemonChannel("direct");
    const p = ch.connectAndStart(daemonGoal(), false).catch((e) => e);
    ch.setRoute(() => {}); // never replies
    const err = await p as Error;
    expect(err.message).toContain("not ready");
  });

  it("goal_status safety net synthesizes result when missing", async () => {
    const ch = new FakeDaemonChannel("direct");
    const p = ch.connectAndStart(daemonGoal(), false);
    ch.setRoute((msg) => {
      if (msg.cmd === "auth") ch.fromDaemon({ type: "ready" });
      else if (msg.cmd === "submit") ch.fromDaemon({ type: "ack", goalId: "g-ack" });
    });
    await p;
    ch.fromDaemon({ type: "goal_status", goalId: "g-ack", status: "timeout", exit: 3 });
    ch.close(); // end the stream so pumpDone can resolve
    await ch.pumpDone();
    expect(ch.exit_code).toBe(3);
    const res = ch.events.find((e) => e.kind === "result");
    expect(res?.data.status).toBe("timeout");
    expect(res?.data.summary).toContain("goal_status");
  });

  it("inject waits for receipt: delivered/pending true, dropped false, timeout false", async () => {
    const ch = new FakeDaemonChannel("direct");
    const p = ch.connectAndStart(daemonGoal(), false);
    ch.setRoute((msg) => {
      if (msg.cmd === "auth") ch.fromDaemon({ type: "ready" });
      else if (msg.cmd === "submit") ch.fromDaemon({ type: "ack", goalId: "g-ack" });
      else if (msg.cmd === "inject") {
        // 'delivered' for cancel; 'dropped' for the steered message;
        // NO reply for the 'no reply' message (timeout path)
        const t = (msg.payload as Record<string, unknown>).type;
        const m = (msg.payload as Record<string, unknown>).message;
        if (t === "cancel" || m === "hi") {
          const ok = t === "cancel";
          setTimeout(() => ch.fromDaemon({
            type: "injected", goalId: "g-ack", ack: msg.ack,
            queued: ok ? "delivered" : "dropped",
          }), 20);
        }
      }
    });
    await p;
    expect(await ch.inject({ type: "cancel" })).toBe(true);
    expect(await ch.inject({ type: "steer", message: "hi" })).toBe(false);
    const t0 = Date.now();
    expect(await ch.inject({ type: "steer", message: "no reply" }, 120)).toBe(false);
    expect(Date.now() - t0).toBeGreaterThanOrEqual(100);
  });
});

function ev0(kind: RaoEvent["kind"], data: Record<string, unknown> = {}): RaoEvent {
  return { protocol: "rao/1", kind, goalId: "g-ack", ts: Date.now(), data };
}

/* ---------------- E2E: orchestrator -> TcpDaemonTransport -> real daemon ---------------- */

describe("E2E: orchestrator over TcpDaemonTransport to a real daemon", () => {
  let dir: string;
  let port = 0;
  let token = "";

  beforeAll(async () => {
    dir = await mkdtemp(join(tmpdir(), "rao-tcp-e2e-"));
    const daemonPath = fileURLToPath(new URL("../src/runtime/rda_daemon.mjs", import.meta.url));
    const child = spawn(process.execPath, [
      daemonPath, "--home", dir, "start", "--port", "0",
    ]);
    let out = "";
    child.stdout!.on("data", (c) => { out += c; });
    const code = await new Promise<number | null>((r) => child.on("close", r));
    expect(code).toBe(0);
    port = Number(out.match(/port (\d+)/)?.[1]);
    const { readFileSync } = await import("node:fs");
    token = JSON.parse(readFileSync(join(dir, "daemon.json"), "utf8")).token;
  }, 30000);

  afterAll(async () => {
    const daemonPath = fileURLToPath(new URL("../src/runtime/rda_daemon.mjs", import.meta.url));
    await new Promise<void>((r) => {
      const child = spawn(process.execPath, [daemonPath, "--home", dir, "stop"]);
      child.on("close", () => r());
    });
    await rm(dir, { recursive: true, force: true }).catch(() => {});
  }, 30000);

  it("dispatch → events → collect(success); device concurrency via registry", async () => {
    const cfg = join(dir, "devices.json");
    const { writeFile } = await import("node:fs/promises");
    await writeFile(cfg, JSON.stringify({
      settings: {},
      devices: [{ id: "local-daemon", host: "127.0.0.1", daemonPort: port }],
    }));
    const registry = new DeviceRegistry(cfg);
    await registry.reload();
    const transport = new TcpDaemonTransport(() => token);
    const orch = new Orchestrator({
      registry, transport,
      runsDir: join(dir, "runs"),
      maxConcurrentGoals: 2,
    });
    const [d] = orch.dispatch(["local-daemon"], {
      protocol: "rao/1",
      objective: "echo through the whole stack",
      agent: "builtin",
      constraints: { timeoutMs: 15000 },
      plan: {
        planner: "main-agent",
        steps: [{ id: "s1", type: "shell", command: "echo e2e-ok" }],
      },
    });
    const collected = await orch.collect([d.goalId], 20000);
    expect(collected.successCount).toBe(1);
    const run = orch.getRun(d.goalId)!;
    expect(run.state).toBe("done");
    expect(run.result?.summary).toBe("all steps ok");
    expect(run.attemptCount).toBe(1);
    // daemon seq numbers flowed through to the channel mirror
    expect(run.chan!.event_seqs[0]).toBe(1);
  }, 30000);

  it("steer/cancel round trip through the orchestrator", async () => {
    const cfg = join(dir, "devices.json");
    const registry = new DeviceRegistry(cfg);
    await registry.reload();
    const transport = new TcpDaemonTransport(() => token);
    const orch = new Orchestrator({ registry, transport, runsDir: join(dir, "runs2") });
    const [d] = orch.dispatch(["local-daemon"], {
      protocol: "rao/1",
      objective: "wait forever until cancelled",
      agent: "builtin",
      constraints: { timeoutMs: 20000 },
      plan: {
        planner: "main-agent",
        steps: [{ id: "w", type: "wait_file", path: join(dir, "never"), timeoutMs: 15000 }],
      },
    });
    // wait until the engine is running, then cancel via control plane
    const run = orch.getRun(d.goalId)!;
    await vi.waitFor(() => expect(run.attemptCount).toBe(1), { timeout: 10000 });
    expect(await orch.cancelGoal(d.goalId, "e2e cancel")).toBe(true);
    await run.waitTerminal(10000);
    expect(run.state).toBe("cancelled");
  }, 30000);
});

// vitest import for waitFor in the E2E block
import { vi } from "vitest";

/* ---------------- MasterServer + NodeTransport ---------------- */

describe("MasterServer + NodeTransport", () => {
  let savedStateDir: string | undefined;
  let nodeStateDir = "";
  beforeAll(async () => {
    savedStateDir = process.env.RAO_STATE_DIR;
    nodeStateDir = await mkdtemp(join(tmpdir(), "rao-master-"));
    process.env.RAO_STATE_DIR = nodeStateDir; // keep master-token/nodes.json out of ~/.rao
  });
  afterAll(async () => {
    if (savedStateDir === undefined) delete process.env.RAO_STATE_DIR;
    else process.env.RAO_STATE_DIR = savedStateDir;
    await rm(nodeStateDir, { recursive: true, force: true }).catch(() => {});
  });

  it("node dials in, receives pushed goal, streams events + receipts", async () => {
    const master = new MasterServer({ port: 0 });
    const bound = await master.start(0);
    expect(bound).toBeGreaterThan(0);
    expect(master.enrollmentToken).toMatch(/^[0-9a-f]{40}$/);

    // fake node: dial + register
    const nodeSock = connect({ host: "127.0.0.1", port: bound });
    const rl = createInterface({ input: nodeSock, terminal: false });
    rl.on("error", () => {});
    const nodeSent: any[] = [];
    const send = (obj: unknown) => {
      nodeSent.push(obj);
      nodeSock.write(JSON.stringify(obj) + "\n");
    };
    const nodeLines: any[] = [];
    rl.on("line", (raw) => {
      const t = raw.trim();
      if (!t) return;
      try { nodeLines.push(JSON.parse(t)); } catch { /* ignore */ }
    });
    const registered = new Promise<void>((resolve) => {
      const iv = setInterval(() => {
        if (nodeLines.some((m) => m.type === "registered")) {
          clearInterval(iv);
          resolve();
        }
      }, 20);
    });
    send({ cmd: "register", nodeId: "node-1", token: master.enrollmentToken, capabilities: { user: "u", workspace: "/w" } });
    await registered;
    expect(master.nodeAlive("node-1")).toBe(true);
    // registry feed sees the node
    expect(master.registeredDevices()[0]).toMatchObject({
      id: "node-1", transport: "node", source: "node",
    });

    // dispatch through NodeTransport
    const nt = new NodeTransport(master);
    const goal = {
      protocol: "rao/1", goalId: "g-node", deviceId: "node-1",
      objective: "x",
      plan: { planner: "main-agent", steps: [{ id: "s1", type: "shell", command: "echo hi" }] },
    } as any;
    const chan = await nt.openSession("g-node", { id: "node-1" } as any, goal);
    // node received the goal command
    await new Promise((r) => setTimeout(r, 100));
    expect(nodeLines.some((m) => (m as any).cmd === "goal")).toBe(true);
    // node streams an event back
    send({ type: "event", goalId: "g-node", event: ev0("started"), seq: 1 });
    await new Promise((r) => setTimeout(r, 100));
    expect(chan.events.length).toBe(1);
    expect(chan.alive()).toBe(true);

    // inject → receipt
    const injectPromise = nt.steer({ id: "node-1" } as any, "g-node", { type: "cancel" });
    await new Promise((r) => setTimeout(r, 100));
    const inj = nodeLines.find((m) => (m as any).cmd === "inject" && (m as any).payload?.type === "cancel") as any;
    expect(inj).toBeTruthy();
    send({ type: "receipt", goalId: "g-node", ack: inj.ack, queued: "delivered" });
    expect(await injectPromise).toBe(true);

    // goal_status finishes the channel
    send({ type: "goal_status", goalId: "g-node", status: "cancelled", exit: 4 });
    await new Promise((r) => setTimeout(r, 100));
    expect(await chan.waitExit(2000)).toBe(0);
    expect(master.goalFinished("g-node")).toBe(true);
    chan.close(); // detaches the goal from the master (ported semantics)
    nodeSock.destroy();
    master.stop();
  }, 15000);

  it("bad register token is rejected", async () => {
    const master = new MasterServer({ port: 0 });
    const bound = await master.start(0);
    const nodeSock = connect({ host: "127.0.0.1", port: bound });
    const rl = createInterface({ input: nodeSock, terminal: false });
    rl.on("error", () => {});
    const reply = await new Promise<any>((resolve) => {
      rl.on("line", (raw) => {
        try { resolve(JSON.parse(raw)); } catch { /* ignore */ }
      });
      nodeSock.write(JSON.stringify({ cmd: "register", nodeId: "x", token: "WRONG" }) + "\n");
    });
    expect(reply.type).toBe("error");
    nodeSock.destroy();
    master.stop();
  }, 10000);
});

/* ---------------- MultiTransport routing ---------------- */

describe("MultiTransport", () => {
  class StubTransport {
    opened: string[] = [];
    steered: string[] = [];
    async openSession(goalId: string, device: any): Promise<GoalChannel> {
      this.opened.push(`${goalId}:${device.id}`);
      throw new Error("stub");
    }
    async steer(device: any, goalId: string): Promise<boolean> {
      this.steered.push(`${goalId}:${device.id}`);
      return true;
    }
  }

  it("routes node devices to NodeTransport, others to ssh", async () => {
    const ssh = new StubTransport();
    const node = new StubTransport();
    const multi = new MultiTransport(ssh as any, node as any);
    await expect(multi.openSession("g", { id: "d1", transport: "ssh" } as any, {} as any)).rejects.toThrow("stub");
    await expect(multi.openSession("g", { id: "n1", transport: "node" } as any, {} as any)).rejects.toThrow("stub");
    expect(ssh.opened).toEqual(["g:d1"]);
    expect(node.opened).toEqual(["g:n1"]);
    expect(await multi.steer({ id: "n1", transport: "node" } as any, "g", {})).toBe(true);
    expect(node.steered).toEqual(["g:n1"]);
    // ssh stub has no provision → checked:false
    expect(await multi.provision({ id: "d1", transport: "ssh" } as any)).toEqual({ checked: false });
  });
});

/* ---------------- provision (local side) ---------------- */

describe("provision", () => {
  it("localHashes covers the runtime triple + combined hash", () => {
    const h = localHashes();
    for (const f of RDA_FILES) {
      expect(h[f]).toMatch(/^[0-9a-f]{64}$/);
    }
    expect(h.__combined__).toMatch(/^[0-9a-f]{64}$/);
    for (const f of RDA_FILES) {
      expect(existsSync(join(runtimeDir(), f))).toBe(true);
    }
  });
});

/* ---------------- validateEvent ---------------- */

describe("validateEvent", () => {
  it("accepts protocol events, rejects wrong version/kind/data", () => {
    expect(validateEvent({ protocol: "rao/1", kind: "started", goalId: "g", ts: 1, data: {} })[0]).toBe(true);
    expect(validateEvent({ protocol: "rao/9", kind: "started", goalId: "g", ts: 1, data: {} })[0]).toBe(false);
    expect(validateEvent({ protocol: "rao/1", kind: "nope", goalId: "g", ts: 1, data: {} })[0]).toBe(false);
    expect(validateEvent({ protocol: "rao/1", kind: "started", goalId: "g", ts: 1, data: "str" })[0]).toBe(false);
  });
});

beforeEach(() => {
  delete process.env.RAO_MERGE_NODES;
});
