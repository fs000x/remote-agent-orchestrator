/**
 * Agent detection + plugin injection with backup/merge/rollback (v0.5 port).
 *
 * Supported hosts (v1): zcode, claude. Codex: detector stub only.
 *
 * Injection contract per host:
 *  - MCP registration: JSON config file, backup -> merge -> atomic write
 *  - Skill: copy the packaged SKILL.md into the host's skills directory
 *  - manifest at state_dir/manifest.json enables `rao uninstall` rollback
 */
import { copyFileSync, existsSync, mkdirSync, readFileSync, rmSync, statSync, writeFileSync, renameSync } from "node:fs";
import { homedir, tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { RAO_VERSION } from "./version.js";

export const MCP_SERVER_NAME = "remote-agent-orchestrator";
export const ENTRY_NOTE = "installed by rao";
export const SUPPORTED_AGENTS = ["zcode", "claude"] as const;
export type SupportedAgent = (typeof SUPPORTED_AGENTS)[number];

export function stateDir(override?: string): string {
  return override ?? process.env.RAO_STATE_DIR ?? join(homedir(), ".rao");
}

function packageRoot(): string {
  // walk up from this module until the package.json is found
  let dir = dirname(fileURLToPath(import.meta.url));
  for (;;) {
    if (existsSync(join(dir, "package.json"))) return dir;
    const parent = dirname(dir);
    if (parent === dir) {
      throw new Error("rao package root not found");
    }
    dir = parent;
  }
}

export function skillSourcePath(): string {
  return join(
    packageRoot(), "src", "data", "skills",
    "cross-device-orchestrator", "SKILL.md",
  );
}

export interface AgentDef {
  name: string;
  cli: string | null;
  configPath: string;
  skillsDir: string;
  mcpKeyPath: string[];
}

export function getAgentDef(
  name: string,
  configPath?: string,
  skillsDir?: string,
): AgentDef {
  const home = homedir();
  if (name === "zcode") {
    return {
      name: "zcode",
      cli: "zcode",
      configPath: configPath ?? join(home, ".zcode", "cli", "config.json"),
      skillsDir: skillsDir ?? join(home, ".zcode", "skills"),
      mcpKeyPath: ["mcp", "servers"],
    };
  }
  if (name === "codex") {
    return {
      name: "codex",
      cli: "codex",
      configPath: configPath ?? join(home, ".codex", "config.toml"),
      skillsDir: skillsDir ?? join(home, ".codex", "prompts"),
      mcpKeyPath: ["mcp_servers"],
    };
  }
  if (name === "claude") {
    return {
      name: "claude",
      cli: "claude",
      configPath: configPath ?? join(home, ".claude.json"),
      skillsDir: skillsDir ?? join(home, ".claude", "skills"),
      mcpKeyPath: ["mcpServers"],
    };
  }
  throw new Error(`unsupported agent: ${name}`);
}

function entryGet(cfg: Record<string, unknown>, keyPath: string[]): Record<string, unknown> {
  let node = cfg;
  for (const k of keyPath) {
    if (typeof node[k] !== "object" || node[k] === null) node[k] = {};
    node = node[k] as Record<string, unknown>;
  }
  return node;
}

function entryRemove(cfg: Record<string, unknown>, keyPath: string[]): void {
  let node = cfg;
  for (const k of keyPath.slice(0, -1)) {
    const next = node[k];
    if (typeof next !== "object" || next === null) return;
    node = next as Record<string, unknown>;
  }
  const last = keyPath[keyPath.length - 1];
  if (last !== undefined) delete node[last];
}

/** path of the compiled CLI entry (dist/cli.js) used in the MCP entry */
function cliEntryPath(): string {
  return join(packageRoot(), "dist", "cli.js");
}

export function mcpEntry(): Record<string, unknown> {
  return {
    command: process.execPath,
    args: [cliEntryPath(), "mcp"],
    _note: ENTRY_NOTE,
    _raoVersion: RAO_VERSION,
  };
}

function whichSync(cli: string): string | null {
  const pathEnv = process.env.PATH || "";
  void 0;
  const exts = process.platform === "win32"
    ? (process.env.PATHEXT || ".EXE;.CMD;.BAT").split(";")
    : [""];
  for (const d of pathEnv.split(process.platform === "win32" ? ";" : ":")) {
    if (!d) continue;
    for (const ext of exts) {
      const cand = join(d!, cli + ext.toLowerCase());
      try {
        if (statSync(cand).isFile()) return cand;
      } catch { /* next */ }
    }
  }
  return null;
}

export interface AgentDetection {
  name: string;
  installed: boolean;
  cliPath: string | null;
  configPath?: string;
  injected?: boolean;
  note?: string;
}

export function isInjected(name: string, configPath?: string): boolean {
  try {
    const d = getAgentDef(name, configPath);
    const cfg = JSON.parse(readFileSync(d.configPath, "utf8")) as Record<string, unknown>;
    let node: unknown = cfg;
    for (const k of d.mcpKeyPath) {
      node = typeof node === "object" && node !== null ? (node as Record<string, unknown>)[k] : {};
    }
    return typeof node === "object" && node !== null &&
      MCP_SERVER_NAME in (node as Record<string, unknown>);
  } catch {
    return false;
  }
}

function detectCodex(): AgentDetection {
  const home = homedir();
  const cliPath = whichSync("codex");
  const dirExists = existsSync(join(home, ".codex"));
  return {
    name: "codex",
    installed: Boolean(cliPath || dirExists),
    cliPath,
    injected: false,
    note: "injection not implemented yet (planned)",
  };
}

export function detectAgents(): AgentDetection[] {
  const out: AgentDetection[] = [detectCodex()];
  const home = homedir();
  for (const name of SUPPORTED_AGENTS) {
    const d = getAgentDef(name);
    let cliPath = d.cli ? whichSync(d.cli) : null;
    const configExists = existsSync(d.configPath);
    const dirExists = existsSync(dirname(d.configPath)) || existsSync(d.skillsDir);
    let installed = Boolean(cliPath) || configExists || dirExists;
    if (name === "zcode") {
      // installed desktop app ships the CLI outside PATH
      for (const cand of [
        "C:\\Program Files\\ZCode\\resources\\glm\\zcode.cjs",
        join(home, ".zcode", "cli"),
      ]) {
        if (existsSync(cand)) {
          cliPath = cliPath ?? cand;
          installed = true;
          break;
        }
      }
    }
    out.push({
      name,
      installed,
      cliPath,
      configPath: d.configPath,
      injected: isInjected(name),
    });
  }
  return out;
}

/* ------------------------------------------------------------------ *
 * inject / uninstall
 * ------------------------------------------------------------------ */
function atomicWrite(path: string, data: string): void {
  const tmp = `${path}.rao-tmp`;
  writeFileSync(tmp, data);
  renameSync(tmp, path);
}

function manifestPath(): string {
  return join(stateDir(), "manifest.json");
}

function loadManifest(): Record<string, Record<string, unknown>> {
  try {
    return JSON.parse(readFileSync(manifestPath(), "utf8"));
  } catch {
    return {};
  }
}

function saveManifest(manifest: Record<string, Record<string, unknown>>): void {
  mkdirSync(stateDir(), { recursive: true });
  atomicWrite(manifestPath(), JSON.stringify(manifest, null, 2));
}

export interface InjectResult {
  agent: string;
  config: string;
  actions: string[];
  skillPath: string;
  ok: boolean;
}

export function injectAgent(name: string): InjectResult {
  if (!SUPPORTED_AGENTS.includes(name as SupportedAgent)) {
    throw new Error(`unsupported agent for injection: ${name} (detection-only hosts cannot be injected)`);
  }
  const d = getAgentDef(name);
  const result: InjectResult = {
    agent: name, config: d.configPath, actions: [], skillPath: "", ok: false,
  };
  // backup
  const backups = join(stateDir(), "backups");
  mkdirSync(backups, { recursive: true });
  let backupPath: string | null = null;
  if (existsSync(d.configPath)) {
    backupPath = join(
      backups,
      `${name}-${Date.now()}-${d.configPath.split(/[\\/]/).pop()}`,
    );
    copyFileSync(d.configPath, backupPath);
    result.actions.push(`backup -> ${backupPath}`);
  }
  // merge
  let cfg: Record<string, unknown> = {};
  if (existsSync(d.configPath)) {
    cfg = JSON.parse(readFileSync(d.configPath, "utf8")) as Record<string, unknown>;
  }
  const entryNode = entryGet(cfg, d.mcpKeyPath);
  const desired = mcpEntry();
  if (JSON.stringify(entryNode[MCP_SERVER_NAME]) === JSON.stringify(desired)) {
    result.actions.push("mcp entry already up-to-date (skipped)");
  } else {
    entryNode[MCP_SERVER_NAME] = desired;
    mkdirSync(dirname(d.configPath) || ".", { recursive: true });
    atomicWrite(d.configPath, JSON.stringify(cfg, null, 2));
    result.actions.push(`mcp entry written (${d.mcpKeyPath.join(".")})`);
  }
  // skill
  const src = skillSourcePath();
  const dst = join(d.skillsDir, "cross-device-orchestrator", "SKILL.md");
  mkdirSync(dirname(dst), { recursive: true });
  const needSkill = !existsSync(dst) ||
    readFileSync(dst).toString("binary") !== readFileSync(src).toString("binary");
  if (needSkill) {
    copyFileSync(src, dst);
    result.actions.push(`skill -> ${dst}`);
  } else {
    result.actions.push("skill already up-to-date (skipped)");
  }
  result.skillPath = dst;
  // manifest for rollback
  const manifest = loadManifest();
  manifest[name] = {
    config: d.configPath, backup: backupPath, skill: dst, at: Date.now(),
  };
  saveManifest(manifest);
  result.ok = true;
  return result;
}

export function uninstallAgent(name: string): { agent: string; actions: string[]; ok: boolean } {
  if (!SUPPORTED_AGENTS.includes(name as SupportedAgent)) {
    throw new Error(`unsupported agent: ${name}`);
  }
  const d = getAgentDef(name);
  const result = { agent: name, actions: [] as string[], ok: false };
  const manifest = loadManifest();
  const rec = manifest[name];
  if (!rec) {
    result.actions.push("no injection record; nothing to do");
    result.ok = true;
    return result;
  }
  if (typeof rec.backup === "string" && existsSync(rec.backup)) {
    copyFileSync(rec.backup, d.configPath);
    result.actions.push("config restored from backup");
  } else {
    // no backup (config did not exist before): remove our entry
    if (existsSync(d.configPath)) {
      const cfg = JSON.parse(readFileSync(d.configPath, "utf8")) as Record<string, unknown>;
      entryRemove(cfg, d.mcpKeyPath);
      atomicWrite(d.configPath, JSON.stringify(cfg, null, 2));
      result.actions.push("mcp entry removed");
    } else {
      result.actions.push("config absent; nothing to remove");
    }
  }
  const skillDir = join(d.skillsDir, "cross-device-orchestrator");
  if (existsSync(skillDir)) {
    rmSync(skillDir, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
    result.actions.push(`skill removed (${skillDir})`);
  }
  delete manifest[name];
  saveManifest(manifest);
  result.ok = true;
  return result;
}

void tmpdir;
