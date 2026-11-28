/**
 * P3-b tests: agent detection/injection (backup → merge → rollback) and the
 * rao CLI device management. Hermetic: HOME/USERPROFILE + RAO_STATE_DIR all
 * point into a temp dir, so default-path injections never touch real configs.
 */
import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
  detectAgents, getAgentDef, injectAgent, isInjected, mcpEntry,
  uninstallAgent,
} from "../src/agents.js";
import {
  cmdDevicesAdd, cmdDevicesList, cmdDevicesRemove,
} from "../src/cli.js";

let dir: string;
let savedEnv: {
  RAO_STATE_DIR?: string; USERPROFILE?: string; HOME?: string;
};

beforeEach(async () => {
  dir = await mkdtemp(join(tmpdir(), "rao-agents-"));
  savedEnv = {
    RAO_STATE_DIR: process.env.RAO_STATE_DIR,
    USERPROFILE: process.env.USERPROFILE,
    HOME: process.env.HOME,
  };
  process.env.RAO_STATE_DIR = dir;
  // homedir() reads USERPROFILE on win32 and HOME on posix
  process.env.USERPROFILE = dir;
  process.env.HOME = dir;
});

afterEach(async () => {
  if (savedEnv.RAO_STATE_DIR === undefined) delete process.env.RAO_STATE_DIR;
  else process.env.RAO_STATE_DIR = savedEnv.RAO_STATE_DIR;
  if (savedEnv.USERPROFILE === undefined) delete process.env.USERPROFILE;
  else process.env.USERPROFILE = savedEnv.USERPROFILE;
  if (savedEnv.HOME === undefined) delete process.env.HOME;
  else process.env.HOME = savedEnv.HOME;
  await rm(dir, {
    recursive: true, force: true, maxRetries: 10, retryDelay: 100,
  });
});

describe("agents: MCP entry", () => {
  it("points at the compiled CLI with the mcp subcommand", () => {
    const entry = mcpEntry();
    expect(entry.command).toBe(process.execPath);
    expect((entry.args as string[])[1]).toBe("mcp");
    expect((entry.args as string[])[0]).toMatch(/dist[\\\/]cli\.js$/);
    expect(entry._note).toBe("installed by rao");
  });
});

describe("agents: inject / uninstall lifecycle", () => {
  it("inject zcode: backup → merge → skill → manifest", async () => {
    const cfg = join(dir, ".zcode", "cli", "config.json");
    await mkdir(join(dir, ".zcode", "cli"), { recursive: true });
    await writeFile(cfg, JSON.stringify({
      existing: true,
      mcp: { servers: { other: { command: "keep-me" } } },
    }));

    const r = injectAgent("zcode");
    expect(r.ok).toBe(true);
    expect(r.actions.some((a) => a.startsWith("backup -> "))).toBe(true);
    expect(r.actions).toContain("mcp entry written (mcp.servers)");

    const saved = JSON.parse(await readFile(cfg, "utf8"));
    expect(saved.existing).toBe(true); // merge preserved existing keys
    expect(saved.mcp.servers.other.command).toBe("keep-me");
    expect(saved.mcp.servers["remote-agent-orchestrator"].command).toBe(process.execPath);
    // backup file exists under the state dir
    const backup = r.actions.find((a) => a.startsWith("backup -> "))!.slice("backup -> ".length);
    expect(existsSync(backup)).toBe(true);
    // skill copied
    const skillPath = join(dir, ".zcode", "skills", "cross-device-orchestrator", "SKILL.md");
    expect(existsSync(skillPath)).toBe(true);
    expect(r.skillPath).toBe(skillPath);
    // manifest recorded
    const manifest = JSON.parse(await readFile(join(dir, "manifest.json"), "utf8"));
    expect(manifest.zcode.config).toBe(cfg);
    expect(isInjected("zcode")).toBe(true);
  });

  it("second inject is idempotent (skips identical entry + skill)", async () => {
    const cfg = join(dir, ".zcode", "cli", "config.json");
    await mkdir(join(dir, ".zcode", "cli"), { recursive: true });
    await writeFile(cfg, JSON.stringify({ mcp: { servers: {} } }));
    injectAgent("zcode");
    const r2 = injectAgent("zcode");
    expect(r2.actions).toContain("mcp entry already up-to-date (skipped)");
    expect(r2.actions).toContain("skill already up-to-date (skipped)");
  });

  it("uninstall restores the backup and removes the skill + manifest record", async () => {
    const cfg = join(dir, ".zcode", "cli", "config.json");
    await mkdir(join(dir, ".zcode", "cli"), { recursive: true });
    await writeFile(cfg, JSON.stringify({ preExisting: true, mcp: { servers: {} } }));
    injectAgent("zcode");
    expect(isInjected("zcode")).toBe(true);

    const r = uninstallAgent("zcode");
    expect(r.ok).toBe(true);
    expect(r.actions).toContain("config restored from backup");
    expect(r.actions.some((a) => a.startsWith("skill removed"))).toBe(true);
    // restored from backup: our entry gone, pre-existing content back
    const restored = JSON.parse(await readFile(cfg, "utf8"));
    expect(restored.preExisting).toBe(true);
    expect(restored.mcp.servers["remote-agent-orchestrator"]).toBeUndefined();
    expect(isInjected("zcode")).toBe(false);
    const manifest = JSON.parse(await readFile(join(dir, "manifest.json"), "utf8"));
    expect(manifest.zcode).toBeUndefined();
  });

  it("uninstall without a backup (config did not exist before) removes the entry", async () => {
    injectAgent("claude"); // no pre-existing ~/.claude.json
    expect(isInjected("claude")).toBe(true);
    const r = uninstallAgent("claude");
    expect(r.actions).toContain("mcp entry removed");
    expect(isInjected("claude")).toBe(false);
    // config file itself survives (with our entry removed)
    expect(existsSync(getAgentDef("claude").configPath)).toBe(true);
  });

  it("unsupported agent is rejected before any write", () => {
    expect(() => injectAgent("codex")).toThrow(/unsupported agent/);
    expect(() => uninstallAgent("nope")).toThrow(/unsupported agent/);
  });
});

describe("agents: detection", () => {
  it("never raises and covers the three hosts", () => {
    const all = detectAgents();
    expect(all.map((a) => a.name).sort()).toEqual(["claude", "codex", "zcode"]);
    for (const a of all) {
      expect(typeof a.installed).toBe("boolean");
    }
  });

  it("codex is detection-only", () => {
    const codex = detectAgents().find((a) => a.name === "codex")!;
    expect(codex.injected).toBe(false);
    expect(codex.note).toContain("planned");
  });
});

describe("cli: devices add/list/remove", () => {
  it("add writes credentials, replaces by id, remove deletes", async () => {
    const cfg = join(dir, "devices.json");
    await cmdDevicesAdd({
      id: "box1", host: "192.0.2.10", port: "22", user: "diablo",
      password: "1", workspace: "/home/diablo/ws", configPath: cfg,
    });
    const saved = JSON.parse(await readFile(cfg, "utf8"));
    expect(saved.devices).toHaveLength(1);
    expect(saved.devices[0]).toMatchObject({
      id: "box1", host: "192.0.2.10", user: "diablo",
      auth: { type: "password", password: "1" },
    });

    await cmdDevicesAdd({
      id: "box1", host: "192.0.2.11", port: "2222", user: "u2",
      password: "pw2", workspace: "", configPath: cfg,
    });
    const saved2 = JSON.parse(await readFile(cfg, "utf8"));
    expect(saved2.devices).toHaveLength(1);
    expect(saved2.devices[0].host).toBe("192.0.2.11");

    // list against TEST-NET (probe fails fast); only the exit code matters
    expect(await cmdDevicesList(cfg)).toBe(0);

    await cmdDevicesRemove("box1", cfg);
    const saved3 = JSON.parse(await readFile(cfg, "utf8"));
    expect(saved3.devices).toHaveLength(0);
  });
});

describe("agents: skill packaging", () => {
  it("skill source exists and names the skill", async () => {
    const { skillSourcePath } = await import("../src/agents.js");
    expect(existsSync(skillSourcePath())).toBe(true);
    const raw = await readFile(skillSourcePath(), "utf8");
    expect(raw.toLowerCase()).toContain("cross-device");
  });
});
