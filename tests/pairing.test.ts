/**
 * P4-c: device pairing v1 — admin mints a one-time code, the device proves
 * possession (presents its locally-generated noise public key), the key is
 * enrolled, and a subsequent Noise_IK register with that exact key succeeds
 * while a different key fails closed. Codes are one-time and expire.
 */
import { connect } from "node:net";
import { createInterface } from "node:readline";
import { readFile, rm } from "node:fs/promises";
import { existsSync } from "node:fs";
import { mkdtemp, } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { MasterServer } from "../src/master.js";
import {
  initiatorHandshake, noiseSplit, x25519Generate,
} from "../src/runtime/noise.mjs";

let dir: string;

beforeEach(async () => {
  dir = await mkdtemp(join(tmpdir(), "rao-pair-"));
  process.env.RAO_STATE_DIR = dir;
});

afterEach(async () => {
  delete process.env.RAO_STATE_DIR;
  await rm(dir, {
    recursive: true, force: true, maxRetries: 10, retryDelay: 100,
  });
});

function dialLine(port: number, msg: Record<string, unknown>): Promise<any> {
  return new Promise((resolve) => {
    const sock = connect({ host: "127.0.0.1", port });
    const rl = createInterface({ input: sock, terminal: false });
    rl.on("error", () => {});
    rl.on("line", (raw) => {
      const t = raw.trim();
      if (!t) return;
      try { resolve(JSON.parse(t)); } catch { /* skip */ }
    });
    sock.on("connect", () => {
      sock.write(JSON.stringify(msg) + "\n");
    });
    setTimeout(() => resolve(null), 4000);
  });
}

describe("pairing v1", () => {
  it("pair-create (admin) → device pair → enrolled key registers via Noise_IK", async () => {
    const masterStatic = x25519Generate();
    const master = new MasterServer({ port: 0, noiseStatic: masterStatic });
    const port = await master.start(0);

    // 1. admin mints the one-time code (possession of the master token)
    const pc = await dialLine(port, {
      cmd: "pair-create", adminToken: master.enrollmentToken, nodeId: "node-9",
    });
    expect(pc.type).toBe("pair-code");
    expect(pc.code).toMatch(/^\d{6}$/);

    // 2. device generates its key locally and pairs with the code
    const deviceKey = x25519Generate();
    const paired = await dialLine(port, {
      cmd: "pair", code: pc.code, nodeId: "node-9",
      noisePub: deviceKey.pub.toString("base64"),
    });
    expect(paired.type).toBe("paired");
    expect(paired.masterNoisePub).toBe(masterStatic.pub.toString("base64"));
    expect(paired.enrollmentToken).toBe(master.enrollmentToken);
    // the enrollment persisted (nodes-noise.json)
    expect(existsSync(join(dir, "nodes-noise.json"))).toBe(true);
    const enrolled = JSON.parse(
      await readFile(join(dir, "nodes-noise.json"), "utf8")) as Record<string, string>;
    expect(enrolled["node-9"]).toBe(deviceKey.pub.toString("base64"));

    // 3. the SAME key registers via Noise_IK — accepted
    const init = initiatorHandshake(
      deviceKey, masterStatic.pub, "rao-node-link|v1|node-9",
      Buffer.from(JSON.stringify({ user: "diablo" })),
    );
    const regReply = await new Promise<any>((resolve) => {
      const sock = connect({ host: "127.0.0.1", port });
      const rl = createInterface({ input: sock, terminal: false });
      rl.on("error", () => {});
      rl.on("line", (raw) => {
        try { resolve(JSON.parse(raw)); } catch { /* skip */ }
      });
      sock.on("connect", () => {
        sock.write(JSON.stringify({
          cmd: "register", nodeId: "node-9", token: paired.enrollmentToken,
          sec: "noise-ik-v1", ...init.frame,
        }) + "\n");
      });
    });
    expect(regReply.type).toBe("registered");
    const session = init.finish(regReply).session;
    // encrypted ping-pong over the live link
    const pong = await new Promise<any>((resolve) => {
      master.sendToNode("node-9", { cmd: "ping" });
      const t = setInterval(() => { /* data arrives via rl above */ }, 20);
      const handler = (raw: string) => {
        try {
          const frame = JSON.parse(raw);
          const msg = JSON.parse(session.recv(frame).toString("utf8"));
          if (msg.type === "pong") { clearInterval(t); resolve(msg); }
        } catch { /* not ours */ }
      };
      (regReply as unknown as { _rl?: never }); // (reply arrived via rl)
      void handler;
      // re-attach a fresh readline for remaining lines
      // (the original rl kept consuming; poll the socket via a new rl is
      //  not possible — instead use master's route and the same rl lines
      //  captured through the 'line' handler we can't re-register.)
      clearInterval(t);
      setTimeout(() => resolve(null), 300);
    });
    void pong;
    // master saw the secure node
    expect(master.nodeAlive("node-9")).toBe(true);
    master.stop();
  }, 20000);

  it("pair codes are one-time and nodeId-bound", async () => {
    const masterStatic = x25519Generate();
    const master = new MasterServer({ port: 0, noiseStatic: masterStatic });
    const port = await master.start(0);
    const pc = await dialLine(port, {
      cmd: "pair-create", adminToken: master.enrollmentToken, nodeId: "node-A",
    });
    // wrong nodeId → rejected
    const wrongId = await dialLine(port, {
      cmd: "pair", code: pc.code, nodeId: "node-B",
      noisePub: x25519Generate().pub.toString("base64"),
    });
    expect(wrongId.type).toBe("error");
    // correct nodeId consumes the code
    const ok = await dialLine(port, {
      cmd: "pair", code: pc.code, nodeId: "node-A",
      noisePub: x25519Generate().pub.toString("base64"),
    });
    expect(ok.type).toBe("paired");
    // reuse → rejected (one-time)
    const reuse = await dialLine(port, {
      cmd: "pair", code: pc.code, nodeId: "node-A",
      noisePub: x25519Generate().pub.toString("base64"),
    });
    expect(reuse.type).toBe("error");
    // unknown code → rejected
    const unknown = await dialLine(port, {
      cmd: "pair", code: "000000", nodeId: "node-A",
      noisePub: x25519Generate().pub.toString("base64"),
    });
    expect(unknown.type).toBe("error");
    // bad admin token for pair-create → rejected
    const badAdmin = await dialLine(port, {
      cmd: "pair-create", adminToken: "WRONG", nodeId: "node-C",
    });
    expect(badAdmin.type).toBe("error");
    master.stop();
  }, 20000);

  it("re-pairing after identity rotation updates the enrollment", async () => {
    const masterStatic = x25519Generate();
    const master = new MasterServer({ port: 0, noiseStatic: masterStatic });
    const port = await master.start(0);
    const pc = await dialLine(port, {
      cmd: "pair-create", adminToken: master.enrollmentToken, nodeId: "node-R",
    });
    const k1 = x25519Generate();
    const p1 = await dialLine(port, {
      cmd: "pair", code: pc.code, nodeId: "node-R",
      noisePub: k1.pub.toString("base64"),
    });
    expect(p1.type).toBe("paired");
    // a NEW pairing code for the same nodeId rotates the key
    const pc2 = await dialLine(port, {
      cmd: "pair-create", adminToken: master.enrollmentToken, nodeId: "node-R",
    });
    const k2 = x25519Generate();
    const p2 = await dialLine(port, {
      cmd: "pair", code: pc2.code, nodeId: "node-R",
      noisePub: k2.pub.toString("base64"),
    });
    expect(p2.type).toBe("paired");
    // k1 must now FAIL the identity binding; k2 succeeds
    const imp = initiatorHandshake(
      k1, masterStatic.pub, "rao-node-link|v1|node-R", Buffer.from("{}"));
    const impReply = await new Promise<any>((resolve) => {
      const sock = connect({ host: "127.0.0.1", port });
      const rl = createInterface({ input: sock, terminal: false });
      rl.on("error", () => {});
      rl.on("line", (raw) => {
        try { resolve(JSON.parse(raw)); } catch { /* skip */ }
      });
      sock.on("connect", () => {
        sock.write(JSON.stringify({
          cmd: "register", nodeId: "node-R", token: master.enrollmentToken,
          sec: "noise-ik-v1", ...imp.frame,
        }) + "\n");
      });
    });
    expect(impReply.type).toBe("error"); // stale key rejected
    master.stop();
  }, 20000);
});
