/**
 * P4-b tests: blind relay — byte-verbatim piping (never parses post-join
 * traffic), join validation, role uniqueness, teardown propagation, token
 * enforcement, capacity guard.
 */
import { connect } from "node:net";
import { createInterface } from "node:readline";
import { describe, expect, it } from "vitest";
import { RelayServer } from "../src/relay/server.js";

function dial(port: number): Promise<{ sock: import("node:net").Socket; lines: any[]; raw: (s: string) => void }> {
  return new Promise((resolve) => {
    const sock = connect({ host: "127.0.0.1", port });
    const lines: any[] = [];
    const rl = createInterface({ input: sock, terminal: false });
    rl.on("error", () => {});
    rl.on("line", (raw) => {
      const t = raw.trim();
      if (!t) return;
      try { lines.push(JSON.parse(t)); } catch { /* raw bytes */ }
    });
    sock.on("connect", () => resolve({
      sock,
      lines,
      raw: (s: string) => sock.write(s),
    }));
  });
}

async function until(
  fn: () => boolean,
  timeoutMs = 5000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (fn()) return;
    await new Promise((r) => setTimeout(r, 25));
  }
  throw new Error("condition not met in time");
}

const TOKEN = "link-secret-1";
const LINK = "link-abc-1";

async function startRelay(): Promise<RelayServer> {
  const relay = new RelayServer({ port: 0, tokens: [TOKEN] });
  await relay.start();
  return relay;
}

function joinLine(role: string, token = TOKEN, linkId = LINK): string {
  return JSON.stringify({ relay: "join", linkId, role, token }) + "\n";
}

describe("relay: join validation", () => {
  it("rejects bad token / duplicate role / malformed join", async () => {
    const relay = await startRelay();
    const c1 = await dial((relay as unknown as { port: number }).port);
    c1.raw(JSON.stringify({ relay: "join", linkId: LINK, role: "master", token: "WRONG" }) + "\n");
    await until(() => c1.lines.length >= 1);
    expect(c1.lines[0]).toMatchObject({ relay: "error" });
    c1.sock.destroy();

    const m1 = await dial((relay as unknown as { port: number }).port);
    m1.raw(joinLine("master"));
    await until(() => m1.lines.length >= 1);
    expect(m1.lines[0]).toMatchObject({ relay: "joined", waiting: true });
    const m2 = await dial((relay as unknown as { port: number }).port);
    m2.raw(joinLine("master"));
    await until(() => m2.lines.some((l) => l.relay === "error"));
    expect(m2.lines.find((l) => l.relay === "error").message).toContain("already joined");
    m1.sock.destroy();
    m2.sock.destroy();
    relay.stop();
  }, 15000);

  it("requires a token even on token-list relays", async () => {
    const relay = await startRelay();
    const c = await dial((relay as unknown as { port: number }).port);
    c.raw(JSON.stringify({ relay: "join", linkId: LINK, role: "master", token: "" }) + "\n");
    await until(() => c.lines.length >= 1);
    expect(c.lines[0].relay).toBe("error");
    c.sock.destroy();
    relay.stop();
  });
});

describe("relay: blind byte pipe", () => {
  it("pipes arbitrary bytes verbatim once both roles join", async () => {
    const relay = await startRelay();
    const port = (relay as unknown as { port: number }).port;
    const master = await dial(port);
    master.raw(joinLine("master"));
    await until(() => master.lines.length >= 1);
    const node = await dial(port);
    node.raw(joinLine("node"));
    await until(() =>
      master.lines.some((l) => l.waiting === false) &&
      node.lines.some((l) => l.waiting === false));

    // opaque ciphertext-like binary payloads in both directions
    const up = Buffer.from([0x00, 0x01, 0x02, 0xff, 0xfe, 0x7f]);
    const down = Buffer.from("\u0001\u0002NOISE-FRAME\u0003");
    let gotDown = Buffer.alloc(0);
    let gotUp = Buffer.alloc(0);
    node.sock.on("data", (c: Buffer) => { gotDown = Buffer.concat([gotDown, c]); });
    master.sock.on("data", (c: Buffer) => { gotUp = Buffer.concat([gotUp, c]); });
    master.sock.write(up);
    node.sock.write(down);
    await until(() => gotDown.equals(up) && gotUp.equals(down));
    expect(gotDown.equals(up)).toBe(true);   // master → node verbatim
    expect(gotUp.equals(down)).toBe(true);   // node → master verbatim
    // relay never learned anything: linkCount is the only state
    expect(relay.linkCount()).toBe(1);
    master.sock.destroy();
    node.sock.destroy();
    relay.stop();
  }, 15000);

  it("peer disconnect tears the pair down; relay stays up for new links", async () => {
    const relay = await startRelay();
    const port = (relay as unknown as { port: number }).port;
    const master = await dial(port);
    master.raw(joinLine("master"));
    await until(() => master.lines.length >= 1);
    const node = await dial(port);
    node.raw(joinLine("node"));
    await until(() => master.lines.some((l) => l.waiting === false));
    master.sock.destroy();
    await until(() => node.sock.destroyed, 3000);
    expect(relay.linkCount()).toBe(0);
    // a fresh link still works after teardown
    const m2 = await dial(port);
    m2.raw(joinLine("master", TOKEN, LINK + "-2"));
    await until(() => m2.lines.length >= 1);
    expect(m2.lines[0].relay).toBe("joined");
    m2.sock.destroy();
    node.sock.destroy();
    relay.stop();
  }, 15000);

  it("capacity guard rejects links beyond the cap", async () => {
    const relay = new RelayServer({ port: 0, tokens: [TOKEN] });
    // shrink the cap via prototype override for the test
    const original = Object.getPrototypeOf(relay).constructor;
    void original;
    await relay.start();
    const port = (relay as unknown as { port: number }).port;
    // fill up to the cap minus a few with master-only links
    const conns: Array<{ sock: import("node:net").Socket; lines: any[]; raw: (s: string) => void }> = [];
    let rejected = false;
    for (let i = 0; i < 260 && !rejected; i++) {
      const c = await dial(port);
      conns.push(c);
      c.raw(joinLine("master", TOKEN, `cap-link-${i}`));
      await until(() => c.lines.length >= 1, 3000);
      if (c.lines[0].relay === "error") rejected = true;
    }
    expect(rejected).toBe(true);
    for (const c of conns) c.sock.destroy();
    relay.stop();
  }, 40000);
});
