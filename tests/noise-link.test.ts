/**
 * P4-a integration: Noise_IK over a real master link — node dials the
 * MasterServer with the secure register path; events/injects flow as
 * encrypted data frames; wrong identity and replays fail closed.
 */
import { connect } from "node:net";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { MasterServer } from "../src/master.js";
import {
  NoiseError, initiatorHandshake, noiseSplit, x25519Generate,
  x25519PublicFromPrivate,
} from "../src/runtime/noise.mjs";

let dir: string;

beforeEach(async () => {
  dir = await mkdtemp(join(tmpdir(), "rao-noise-link-"));
  process.env.RAO_STATE_DIR = dir;
});

afterEach(async () => {
  delete process.env.RAO_STATE_DIR;
  await rm(dir, {
    recursive: true, force: true, maxRetries: 10, retryDelay: 100,
  });
});

/** scripted secure node: dial + noise register + frame helpers */
function secureNode(
  port: number,
  nodeId: string,
  token: string,
  keypair: { priv: Buffer; pub: Buffer },
  masterPub: Buffer,
) {
  const init = initiatorHandshake(
    keypair, masterPub, `rao-node-link|v1|${nodeId}`,
    Buffer.from(JSON.stringify({ user: "u", workspace: "/w" })),
  );
  const sock = connect({ host: "127.0.0.1", port });
  const lines: any[] = [];
  let session: ReturnType<typeof noiseSplit> | null = null;
  let handshakeDone: ((ok: boolean, err?: string) => void) | null = null;
  const handshake = new Promise<boolean>((resolve) => {
    handshakeDone = (ok) => resolve(ok);
  });
  const rl = createInterface({ input: sock, terminal: false });
  rl.on("error", () => {});
  rl.on("line", (raw) => {
    const t = raw.trim();
    if (!t) return;
    let m: any;
    try { m = JSON.parse(t); } catch { return; }
    if (!session && m.type === "registered" && m.e) {
      try {
        const res = init.finish(m);
        session = res.session;
        lines.push(m);
        handshakeDone?.(true);
      } catch (err) {
        handshakeDone?.(false, (err as Error).message);
      }
      return;
    }
    if (session) {
      try {
        lines.push(JSON.parse(session.recv(m).toString("utf8")));
      } catch { /* dropped */ }
      return;
    }
    if (!session && m.type === "error" && handshakeDone) {
      const done = handshakeDone;
      handshakeDone = null;
      done(false);
    }
    lines.push(m);
  });
  const raw = (obj: unknown) => sock.write(JSON.stringify(obj) + "\n");
  raw({
    cmd: "register", nodeId, token, sec: "noise-ik-v1",
    ...init.frame,
  });
  return {
    handshake,
    lines,
    send: (obj: unknown) => {
      if (session) raw(session.send(obj));
      else raw(obj);
    },
    raw,
    destroy: () => sock.destroy(),
  };
}

function ev(kind: string, goalId: string): Record<string, unknown> {
  return { protocol: "rao/1", kind: kind as never, goalId, ts: Date.now(), data: {} };
}

describe("master link with Noise_IK (secure register)", () => {
  it("handshake → encrypted events → encrypted master commands", async () => {
    const masterStatic = x25519Generate();
    const nodeStatic = x25519Generate();
    const master = new MasterServer({
      port: 0,
      noiseStatic: masterStatic,
      noiseNodeKeys: new Map([["node-1", nodeStatic.pub]]),
    });
    const port = await master.start(0);

    const node = secureNode(port, "node-1", master.enrollmentToken, nodeStatic, masterStatic.pub);
    const ok = await node.handshake;
    if (!ok) console.error("NODE LINES:", JSON.stringify(node.lines));
    expect(ok).toBe(true);
    expect(master.nodeAlive("node-1")).toBe(true);
    // the master decrypted the capabilities payload
    expect(master.registeredDevices()[0]?.user).toBe("u");

    // channel + encrypted event flow (node → master)
    const ch = master.getNodeChannel("node-1", "g-n");
    node.send({ type: "event", goalId: "g-n", event: ev("started", "g-n"), seq: 1 });
    await new Promise((r) => setTimeout(r, 150));
    expect(ch.events.length).toBe(1);

    // master → node command arrives as an encrypted frame
    ch.sendGoal({ goalId: "g-n" } as never);
    await new Promise((r) => setTimeout(r, 150));
    const goalCmd = node.lines.find((m) => m.cmd === "goal");
    expect(goalCmd).toBeTruthy();
    expect(goalCmd.goal.goalId).toBe("g-n");

    // inject → encrypted receipt
    const injectPromise = master.inject("node-1", "g-n", { type: "cancel" });
    await new Promise((r) => setTimeout(r, 150));
    const inj = node.lines.find((m) => m.cmd === "inject") as any;
    expect(inj).toBeTruthy();
    node.send({ type: "receipt", goalId: "g-n", ack: inj.ack, queued: "delivered" });
    expect(await injectPromise).toBe(true);

    node.destroy();
    master.stop();
  }, 15000);

  it("node with an unenrolled static key fails closed", async () => {
    const masterStatic = x25519Generate();
    const impostor = x25519Generate();
    const master = new MasterServer({
      port: 0,
      noiseStatic: masterStatic,
      noiseNodeKeys: new Map([["node-1", x25519Generate().pub]]), // different key
    });
    const port = await master.start(0);
    const node = secureNode(port, "node-1", master.enrollmentToken, impostor, masterStatic.pub);
    const ok = await node.handshake;
    expect(ok).toBe(false); // identity mismatch → handshake rejected
    expect(master.nodeAlive("node-1")).toBe(false);
    node.destroy();
    master.stop();
  }, 15000);

  it("replaying a captured ik1 (same ephemeral) fails closed", async () => {
    const masterStatic = x25519Generate();
    const nodeStatic = x25519Generate();
    const master = new MasterServer({
      port: 0,
      noiseStatic: masterStatic,
      noiseNodeKeys: new Map([["node-1", nodeStatic.pub]]),
    });
    const port = await master.start(0);
    const init = initiatorHandshake(
      nodeStatic, masterStatic.pub, "rao-node-link|v1|node-1",
      Buffer.from("{}"),
    );
    const frame = {
      cmd: "register", nodeId: "node-1", token: master.enrollmentToken,
      sec: "noise-ik-v1", ...init.frame,
    };
    // first connection: consumed
    const sock1 = connect({ host: "127.0.0.1", port });
    sock1.write(JSON.stringify(frame) + "\n");
    await new Promise((r) => setTimeout(r, 200));
    sock1.destroy();
    // replay the exact same ik1 → ephemeral reuse → rejected
    const reply = await new Promise<any>((resolve) => {
      const sock2 = connect({ host: "127.0.0.1", port });
      const rl = createInterface({ input: sock2, terminal: false });
      rl.on("error", () => {});
      rl.on("line", (raw) => {
        try { resolve(JSON.parse(raw)); } catch { /* ignore */ }
      });
      sock2.write(JSON.stringify(frame) + "\n");
      setTimeout(() => resolve(null), 3000);
    });
    expect(reply?.type).toBe("error");
    master.stop();
  }, 15000);

  it("nodes without an enrolled noise key cannot use the secure path", async () => {
    const masterStatic = x25519Generate();
    const nodeStatic = x25519Generate();
    const master = new MasterServer({
      port: 0,
      noiseStatic: masterStatic,
      noiseNodeKeys: new Map(), // nothing enrolled
    });
    const port = await master.start(0);
    const node = secureNode(port, "node-1", master.enrollmentToken, nodeStatic, masterStatic.pub);
    const ok = await node.handshake;
    expect(ok).toBe(false);
    node.destroy();
    master.stop();
  }, 15000);

  it("data-frame tamper kills the link (fail-closed)", async () => {
    const masterStatic = x25519Generate();
    const nodeStatic = x25519Generate();
    const master = new MasterServer({
      port: 0,
      noiseStatic: masterStatic,
      noiseNodeKeys: new Map([["node-1", nodeStatic.pub]]),
    });
    const port = await master.start(0);
    const node = secureNode(port, "node-1", master.enrollmentToken, nodeStatic, masterStatic.pub);
    expect(await node.handshake).toBe(true);
    // send a corrupted data frame — the master drops the link
    const f1 = (node as unknown as { _session?: unknown })._session;
    void f1;
    // craft a tampered frame with the node's real first counter (n=0)
    const tampered = { t: "d", n: 0, ct: Buffer.alloc(40, 1).toString("base64") };
    node.raw(tampered);
    await new Promise((r) => setTimeout(r, 200));
    expect(master.nodeAlive("node-1")).toBe(false); // link destroyed
    node.destroy();
    master.stop();
  }, 15000);
});

void NoiseError;
void x25519PublicFromPrivate;
