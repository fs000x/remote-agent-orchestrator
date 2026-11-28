/**
 * Device registry tests: fresh-clone tolerance (missing devices.json),
 * local + node merge (local wins, RAO_MERGE_NODES=0 opt-out), stable
 * DEVICE_NOT_FOUND errors, credential resolution order, honest execution
 * matrix (`is_agent: false` for the builtin executor).
 */
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
  DeviceRegistry,
  passwordOf,
} from "../src/devices.js";
import { ErrorCode } from "../src/protocol.js";

let dir: string;
let envBackup: Record<string, string | undefined>;

beforeEach(async () => {
  dir = await mkdtemp(join(tmpdir(), "rao-devices-"));
  envBackup = {
    RAO_STATE_DIR: process.env.RAO_STATE_DIR,
    RAO_MERGE_NODES: process.env.RAO_MERGE_NODES,
  };
  process.env.RAO_STATE_DIR = dir;
  delete process.env.RAO_MERGE_NODES;
});

afterEach(async () => {
  for (const [k, v] of Object.entries(envBackup)) {
    if (v === undefined) delete process.env[k];
    else process.env[k] = v;
  }
  await rm(dir, { recursive: true, force: true });
});

async function writeConfig(devices: unknown[], settings = {}): Promise<string> {
  const path = join(dir, "devices.json");
  await writeFile(path, JSON.stringify({ settings, devices }));
  return path;
}

describe("registry: loading", () => {
  it("fresh clone: missing devices.json tolerated (empty registry)", async () => {
    const reg = new DeviceRegistry(join(dir, "does-not-exist.json"));
    await reg.reload();
    expect(reg.listIds()).toEqual([]);
  });

  it("loads devices and settings; sorts ids", async () => {
    const path = await writeConfig([
      { id: "b-device", host: "127.0.0.2", port: 22 },
      { id: "a-device", host: "127.0.0.1", port: 22, user: "u" },
    ]);
    const reg = new DeviceRegistry(path);
    await reg.reload();
    expect(reg.listIds()).toEqual(["a-device", "b-device"]);
    expect(reg.get("a-device")?.user).toBe("u");
  });

  it("corrupt JSON (not ENOENT) is a hard error", async () => {
    const path = join(dir, "broken.json");
    await writeFile(path, "{ not json");
    const reg = new DeviceRegistry(path);
    await expect(reg.reload()).rejects.toThrow();
  });

  it("require() unknown id -> DEVICE_NOT_FOUND with config path", async () => {
    const path = await writeConfig([]);
    const reg = new DeviceRegistry(path);
    await reg.reload();
    try {
      reg.require("ghost");
      expect.unreachable();
    } catch (err) {
      expect((err as { code?: string }).code).toBe(ErrorCode.DeviceNotFound);
      expect((err as Error).message).toContain(path);
    }
  });
});

describe("registry: node merge", () => {
  it("registered nodes appear automatically as transport=node", async () => {
    await writeFile(
      join(dir, "nodes.json"),
      JSON.stringify({
        "node-1": {
          peer: "192.168.0.100",
          capabilities: {
            user: "diablo",
            workspace: "/home/diablo/git500/zcode_prj",
            maxConcurrentGoals: 3,
          },
        },
      }),
    );
    const path = await writeConfig([
      { id: "local-1", host: "10.0.0.1", port: 22 },
    ]);
    const reg = new DeviceRegistry(path);
    await reg.reload();
    expect(reg.listIds()).toEqual(["local-1", "node-1"]);
    const n = reg.get("node-1")!;
    expect(n.transport).toBe("node");
    expect(n.source).toBe("node");
    expect(n.user).toBe("diablo");
    expect(n.maxConcurrentGoals).toBe(3);
    expect(n.host).toBe("192.168.0.100");
  });

  it("local entry wins on id collision; RAO_MERGE_NODES=0 opts out", async () => {
    await writeFile(
      join(dir, "nodes.json"),
      JSON.stringify({ shared: { peer: "from-nodes", capabilities: {} } }),
    );
    const path = await writeConfig([
      { id: "shared", host: "from-config", port: 22 },
    ]);
    const reg = new DeviceRegistry(path);
    await reg.reload();
    expect(reg.get("shared")?.host).toBe("from-config");

    process.env.RAO_MERGE_NODES = "0";
    const reg2 = new DeviceRegistry(await writeConfig([]));
    await reg2.reload();
    expect(reg2.listIds()).toEqual([]);
  });
});

describe("registry: credentials + description", () => {
  it("passwordOf: config-file credential (keyring absent)", async () => {
    const path = await writeConfig([
      {
        id: "d1",
        host: "h",
        port: 22,
        auth: { type: "password", password: "1" },
      },
    ]);
    const reg = new DeviceRegistry(path);
    await reg.reload();
    expect(passwordOf(reg.require("d1"))).toBe("1");
  });

  it("describeAll: honest execution matrix (builtin is NOT an agent)", async () => {
    const path = await writeConfig([
      { id: "d1", host: "127.0.0.1", port: 22 },
    ]);
    const reg = new DeviceRegistry(path);
    await reg.reload();
    const all = await reg.describeAll(false);
    expect(all).toHaveLength(1);
    expect(all[0].execution).toMatchObject({
      executor: true,
      is_agent: false,
    });
  });
});
