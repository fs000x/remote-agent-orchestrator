/**
 * Device registry: local devices.json + node-registered devices merged.
 * No env-var configuration (v0.8+): everything lives in devices.json.
 */
import { readFileSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { homedir } from "node:os";
import { join } from "node:path";
import { createRequire } from "node:module";
import { RaoError, ErrorCode } from "./protocol.js";


/** CJS interop for optional dependencies (keyring) — absence degrades gracefully */
const require = createRequire(import.meta.url);

export interface DeviceRecord {
  id: string;
  host?: string;
  port?: number | null;
  /** daemon-mode: on-device loopback port (SSH transport reads it via daemon.json) */
  daemonPort?: number;
  user?: string;
  workspace?: string;
  rdaDir?: string;
  maxConcurrentGoals?: number;
  transport?: "ssh" | "node";
  source?: "local" | "node" | "zcode";
  auth?: { type: "password"; password: string };
  zcodeProfileId?: string;
  online?: boolean;
  execution?: Record<string, unknown>;
}

export interface RaoSettings {
  transportMode?: "auto" | "spawn" | "daemon";
  rdaDir?: string;
  runsDir?: string;
  masterPort?: number;
}

const DEFAULTS = {
  rdaDir: "$HOME/.rao/runtime",
  runsDir: "runs",
  masterPort: 9700,
  maxConcurrentGoals: 2,
};

export class DeviceRegistry {
  private readonly devices = new Map<string, DeviceRecord>();
  settings: RaoSettings = {};
  private readonly lock = new Set<Promise<void>>();
  path: string;

  constructor(configPath?: string) {
    this.path =
      configPath ?? join(process.cwd(), "config", "devices.json");
  }

  async reload(): Promise<void> {
    // serialize reloads
    const prev = [...this.lock][0];
    const run = (async () => {
      this.devices.clear();
      this.settings = {};
      try {
        const raw = await readFile(this.path, "utf8");
        const data = JSON.parse(raw) as {
          settings?: RaoSettings;
          devices?: DeviceRecord[];
        };
        this.settings = data.settings ?? {};
        for (const d of data.devices ?? []) {
          if (d?.id) {
            d.source = d.source ?? "local";
            this.devices.set(d.id, d);
          }
        }
      } catch (err) {
        if ((err as NodeJS.ErrnoException).code !== "ENOENT") throw err;
        // fresh clone: tolerate missing devices.json (rao devices add creates it)
      }
      this.mergeRegisteredNodes();
    })();
    this.lock.add(run);
    try {
      await run;
    } finally {
      this.lock.delete(run);
    }
  }

  /** nodes registered with the rao master appear automatically (transport=node).
   *  devices.json entries win on id collision. */
  private mergeRegisteredNodes(): void {
    if (process.env.RAO_MERGE_NODES === "0") return;
    const nodesPath = join(
      process.env.RAO_STATE_DIR || join(homedir(), ".rao"),
      "nodes.json",
    );
    let nodes: Record<
      string,
      { peer?: string; capabilities?: Record<string, unknown> }
    > = {};
    try {
      nodes = JSON.parse(readFileSync(nodesPath, "utf8"));
    } catch {
      return; // no nodes file yet
    }
    for (const [nid, rec] of Object.entries(nodes)) {
      if (this.devices.has(nid)) continue; // local entry wins
      const caps = rec.capabilities ?? {};
      this.devices.set(nid, {
        id: nid,
        host: rec.peer,
        port: null,
        user: typeof caps.user === "string" ? caps.user : undefined,
        workspace:
          typeof caps.workspace === "string" ? caps.workspace : undefined,
        transport: "node",
        source: "node",
        maxConcurrentGoals:
          typeof caps.maxConcurrentGoals === "number"
            ? caps.maxConcurrentGoals
            : DEFAULTS.maxConcurrentGoals,
      });
    }
  }

  listIds(): string[] {
    return [...this.devices.keys()].sort();
  }

  get(id: string): DeviceRecord | undefined {
    return this.devices.get(id);
  }

  require(id: string): DeviceRecord {
    const d = this.devices.get(id);
    if (!d) {
      throw new RaoError(
        ErrorCode.DeviceNotFound,
        `unknown deviceId: ${id} (config: ${this.path})`,
      );
    }
    return d;
  }

  /** Resolve the device password: keyring (optional) > config file. No env. */
  static passwordOf(d: DeviceRecord): string | undefined {
    try {
      // optional dependency — absence degrades to config-file credentials
      const keyring = require("keyring") as {
        getPassword(s: string, u: string): string | null;
      };
      const pw = keyring.getPassword("rao", d.id);
      if (pw) return pw;
    } catch {
      /* keyring not installed / backend unavailable */
    }
    return d.auth?.password;
  }

  async describeAll(probe = true): Promise<DeviceRecord[]> {
    const out: DeviceRecord[] = [];
    for (const id of this.listIds()) {
      const d = this.get(id)!;
      const entry: DeviceRecord = {
        ...d,
        online: undefined,
        execution: {
          executor: true,
          is_agent: false, // 内置执行器不是自主 Agent（诚实命名，PRODUCT.md）
          agent_claude: "unknown",
        },
      };
      if (probe && d.host && d.port != null) {
        const { probeTcp } = await import("./transports/probe.js");
        entry.online = await probeTcp(d.host, d.port);
      }
      out.push(entry);
    }
    return out;
  }
}

/** credential helper shared with transports */
export function passwordOf(d: DeviceRecord): string | undefined {
  return DeviceRegistry.passwordOf(d);
}

export function defaultRdaDir(d: DeviceRecord, fallback: string): string {
  return d.rdaDir || fallback;
}
