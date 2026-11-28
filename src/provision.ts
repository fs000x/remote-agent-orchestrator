/**
 * RDA auto-provisioning: ensure the remote runtime files match local before
 * dispatch (replaces the manual deploy step). Ported from provision.py —
 * the runtime triple is now rda.mjs/protocol.mjs/rda_daemon.mjs.
 */
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { sshExec, sftpPutFiles, connectClient, execOn } from "./transports/ssh2util.js";
import type { DeviceRecord } from "./devices.js";

export const RDA_FILES = ["rda.mjs", "protocol.mjs", "rda_daemon.mjs", "noise.mjs"] as const;

export function runtimeDir(): string {
  return join(dirname(fileURLToPath(import.meta.url)), "runtime");
}

export function localHashes(): Record<string, string> {
  const base = runtimeDir();
  const out: Record<string, string> = {};
  for (const name of RDA_FILES) {
    out[name] = createHash("sha256").update(readFileSync(join(base, name))).digest("hex");
  }
  // combined runtime hash (matches the daemon's daemon.json codeHash —
  // any single-file change must trigger a daemon restart)
  const h = createHash("sha256");
  for (const name of [...RDA_FILES].sort()) {
    h.update(name);
    h.update(readFileSync(join(base, name)));
  }
  out.__combined__ = h.digest("hex");
  return out;
}

/** Query remote file hashes. Returns {name: hash_or_null} */
export async function remoteHashes(
  device: DeviceRecord,
  rdaDir: string,
): Promise<Record<string, string | null>> {
  const checks = RDA_FILES.map(
    (f) => `sha256sum "${rdaDir}/${f}" 2>/dev/null | cut -d" " -f1`,
  ).join(" && ");
  const { code, stdout } = await sshExec(
    device, "cd / 2>/dev/null; " + checks, 15000);
  const lines = stdout.trim().split("\n").map((l) => l.trim()).filter(Boolean);
  const result: Record<string, string | null> = {};
  RDA_FILES.forEach((name, i) => {
    const h = lines[i];
    result[name] = code === 0 && h !== undefined && h.length === 64 ? h : null;
  });
  return result;
}

/** Probe remote RDA files; upload if missing or stale. */
export async function ensureRda(
  device: DeviceRecord,
  rdaDir: string,
): Promise<{ deployed: boolean; checked: boolean; uploaded?: string[]; stale?: string[]; error?: string }> {
  let local: Record<string, string>;
  try {
    local = localHashes();
  } catch (err) {
    return { deployed: false, checked: false, error: String(err) };
  }
  let remote: Record<string, string | null>;
  try {
    remote = await remoteHashes(device, rdaDir);
  } catch (err) {
    return { deployed: false, checked: false, error: String((err as Error).message ?? err) };
  }
  const stale = RDA_FILES.filter((name) => remote[name] !== local[name]);
  if (!stale.length) {
    return { deployed: false, checked: true, stale: [] };
  }
  const base = runtimeDir();
  const client = await connectClient(device);
  try {
    await execOn(client, `mkdir -p "${rdaDir}"`, 10000);
    const uploaded = await sftpPutFiles(
      client,
      stale.map((name) => ({
        localPath: join(base, name),
        remotePath: `${rdaDir}/${name}`,
      })),
    );
    return { deployed: true, checked: true, uploaded, stale };
  } finally {
    client.end();
  }
}
