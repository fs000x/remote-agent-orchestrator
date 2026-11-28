/**
 * Noise_IK tests: full handshake + session flow, prologue/identity binding
 * (fail-closed), handshake replay rejection, data-frame replay/tamper
 * rejection, tampered handshake rejection, and tampered ik2 rejection.
 */
import { describe, expect, it } from "vitest";
import {
  NoiseError, x25519Generate as generateKeyPair, initiatorHandshake,
  noiseResponderHandshake, noiseSplit,
} from "../src/runtime/noise.mjs";

const PAYLOAD = Buffer.from(JSON.stringify({ cmd: "hello", nodeId: "node-1" }));

function prologueOf(connectionId: string, ids: string[]): string {
  return `conn=${connectionId};devices=${ids.join(",")}`;
}

function handshakePair(
  prologue: string,
  payload = PAYLOAD,
  replyPayload = Buffer.from("registered"),
) {
  const initiator = generateKeyPair();
  const responder = generateKeyPair();
  const seen = new Set<string>();
  const client = initiatorHandshake(initiator, responder.pub, prologue, payload);
  const server = noiseResponderHandshake(
    responder, prologue, client.frame, seen, replyPayload);
  const clientSide = client.finish(server.reply);
  return {
    initiator, responder, seen, client, server, clientSide,
    serverSession: noiseSplit(server.hs, false),
  };
}

describe("noise-ik: handshake + session", () => {
  it("full handshake derives matching session keys; frames round trip", () => {
    const { clientSide, server, client, serverSession } = handshakePair(prologueOf("c1", ["node-1"]));
    // payload exchange
    expect(clientSide.payload.toString()).toBe("registered");
    expect(server.payload).toEqual(PAYLOAD);
    // the responder learned the initiator's static identity exactly
    expect(server.initiatorStaticPub).toEqual(client.initiatorPub);
    // bidirectional frames
    const f1 = clientSide.session.send("goal submitted");
    const p1 = serverSession.recv(f1);
    expect(p1.toString()).toBe("goal submitted");
    const f2 = serverSession.send({ status: "ok" });
    const p2 = clientSide.session.recv(f2);
    expect(JSON.parse(p2.toString())).toEqual({ status: "ok" });
    // counters advance independently per direction
    const f3 = clientSide.session.send("second");
    expect(f3.n).toBe(1);
    expect(serverSession.recv(f3).toString()).toBe("second");
  });

  it("prologue mismatch (different connectionId) fails closed", () => {
    const initiator = generateKeyPair();
    const responder = generateKeyPair();
    const client = initiatorHandshake(
      initiator, responder.pub, prologueOf("c1", ["node-1"]), PAYLOAD);
    expect(() => noiseResponderHandshake(
      responder, prologueOf("c2", ["node-1"]), client.frame,
      new Set(), Buffer.from("registered"),
    )).toThrow(NoiseError);
  });

  it("wrong responder identity fails closed (initiator side)", () => {
    const initiator = generateKeyPair();
    const responder = generateKeyPair();
    const attacker = generateKeyPair();
    const client = initiatorHandshake(
      initiator, responder.pub, prologueOf("c1", ["node-1"]), PAYLOAD);
    // attacker (holding a DIFFERENT static key) answers — cannot derive es
    expect(() => {
      const atk = noiseResponderHandshake(
        attacker, prologueOf("c1", ["node-1"]), client.frame,
        new Set(), Buffer.from("registered"));
      client.finish(atk.reply);
    }).toThrow(NoiseError);
  });

  it("handshake replay (same ephemeral) is rejected", () => {
    const { client, server, seen } = handshakePair(prologueOf("c1", ["node-1"]));
    void client;
    void server;
    const initiator2 = generateKeyPair();
    // replay the SAME frame through a fresh responder view: seen-set blocks it
    const client2 = initiatorHandshake(
      initiator2, generateKeyPair().pub, prologueOf("c1", ["node-1"]), PAYLOAD);
    // simulate: the same ephemeral seen twice on one responder
    const ephHex = Buffer.from(client2.frame.e as string, "base64").toString("hex");
    seen.add(ephHex);
    expect(() => noiseResponderHandshake(
      generateKeyPair(), prologueOf("c1", ["node-1"]),
      { ...client2.frame, e: Buffer.from(ephHex, "hex").toString("base64") },
      seen, Buffer.from("registered"),
    )).toThrow(/replay/);
  });

  it("tampered ik1 static block fails closed", () => {
    const initiator = generateKeyPair();
    const responder = generateKeyPair();
    const client = initiatorHandshake(
      initiator, responder.pub, prologueOf("c1", ["node-1"]), PAYLOAD);
    const tampered = { ...client.frame, s: Buffer.alloc(64, 7).toString("base64") };
    expect(() => noiseResponderHandshake(
      responder, prologueOf("c1", ["node-1"]), tampered,
      new Set(), Buffer.from("registered"),
    )).toThrow(NoiseError);
  });

  it("tampered ik2 payload fails closed (initiator side)", () => {
    const initiator = generateKeyPair();
    const responder = generateKeyPair();
    const client = initiatorHandshake(
      initiator, responder.pub, prologueOf("c1", ["node-1"]), PAYLOAD);
    const server = noiseResponderHandshake(
      responder, prologueOf("c1", ["node-1"]), client.frame,
      new Set(), Buffer.from("registered"));
    const tamperedReply = { ...server.reply, pl: Buffer.alloc(48, 9).toString("base64") };
    expect(() => client.finish(tamperedReply)).toThrow(NoiseError);
  });

  it("data-frame replay and reorder fail closed", () => {
    const { clientSide, serverSession } = handshakePair(prologueOf("c1", ["node-1"]));
    const f0 = clientSide.session.send("frame-0");
    const f1 = clientSide.session.send("frame-1");
    expect(serverSession.recv(f0).toString()).toBe("frame-0");
    expect(serverSession.recv(f1).toString()).toBe("frame-1");
    // replay f0 → counter mismatch
    expect(() => serverSession.recv(f0)).toThrow(NoiseError);
    // reorder (f2 then f1) → mismatch
    const f2 = clientSide.session.send("frame-2");
    const f3 = clientSide.session.send("frame-3");
    expect(() => serverSession.recv(f3)).toThrow(NoiseError);
    expect(serverSession.recv(f2).toString()).toBe("frame-2");
  });

  it("data-frame tamper fails closed (AEAD)", () => {
    const { clientSide, server } = handshakePair(prologueOf("c1", ["node-1"]));
    const f = clientSide.session.send("secret");
    const ct = Buffer.from(f.ct as string, "base64");
    ct[0] ^= 0xff;
    expect(() => serverSession.recv({ t: "d", n: f.n, ct: ct.toString("base64") }))
      .toThrow();
  });
});
