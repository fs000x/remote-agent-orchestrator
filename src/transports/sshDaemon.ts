/**
 * SSH daemon transport (v0.3+): the on-device daemon is loopback-only; the
 * orchestrator reaches it through `client-bridge` piping the TCP socket over
 * the SSH channel (the bridge auths itself with the local daemon.json token).
 *
 * provision(): upload stale runtime files AND restart the daemon when its
 * code changed (a running daemon keeps old in-memory code). v0.10
 * single-instance guarantee: kill ALL serve processes, then start exactly
 * one — multiple daemons of different versions made goal bridges randomly
 * attach to a stale engine (field bug).
 */
import { ErrorCode, RaoError, type DispatchedGoal } from "../protocol.js";
import { passwordOf, type DeviceRecord } from "../devices.js";
import { connectClient, execOn, sshExec, type ClientChannel } from "./ssh2util.js";
import { DaemonChannelBase } from "./daemonChannel.js";
import { GoalChannelBase } from "./channel.js";
import type { Transport } from "../orchestrator.js";
import { ensureRda } from "../provision.js";

const BRIDGE_COMMAND = "cd {rdaDir} && node rda_daemon.mjs client-bridge";

export class SshDaemonChannel extends DaemonChannelBase {
  private client: import("ssh2").Client | null = null;
  private chan: ClientChannel | null = null;

  constructor(
    goalId: string,
    private readonly device: DeviceRecord,
    private readonly rdaDir: string,
  ) {
    super(goalId, "bridge");
  }

  protected async openLink(): Promise<void> {
    try {
      this.client = await connectClient(this.device);
    } catch (err) {
      throw err instanceof RaoError
        ? err
        : new RaoError(ErrorCode.HostOffline, `ssh bridge: ${String(err)}`, true);
    }
    let stream: ClientChannel;
    try {
      stream = await new Promise<ClientChannel>((resolve, reject) => {
        this.client!.exec(
          BRIDGE_COMMAND.replace("{rdaDir}", this.rdaDir),
          (err, ch) => (err ? reject(err) : resolve(ch)),
        );
      });
    } catch (err) {
      this.close();
      throw new RaoError(ErrorCode.TransportLost, `exec bridge: ${String(err)}`, true);
    }
    this.chan = stream;
    stream.stdout.on("data", (c: Buffer) => this.stream.push(c.toString("utf8")));
    stream.stderr.on("data", (c: Buffer) => {
      for (const ln of c.toString("utf8").split("\n")) {
        if (ln.trim()) this.addStderr(`[bridge] ${ln.trim().slice(0, 200)}`);
      }
    });
    stream.on("close", () => this.stream.end());
    stream.on("error", () => this.stream.end());
  }

  protected sendLine(obj: Record<string, unknown>): void {
    this.chan!.stdin.write(JSON.stringify(obj) + "\n");
  }

  protected linkAlive(): boolean {
    return this.chan !== null && !this.chan.destroyed && this.exit_code === null;
  }

  protected closeLink(): void {
    try { this.chan?.close(); } catch { /* gone */ }
    try { this.client?.end(); } catch { /* gone */ }
    this.stream.end();
  }
}

export class SshDaemonTransport implements Transport {
  constructor(private readonly rdaDir: string) {}

  private devRdaDir(device: DeviceRecord): string {
    return device.rdaDir || this.rdaDir;
  }

  /** upload stale files + guarantee exactly one fresh daemon */
  async provision(device: DeviceRecord): Promise<unknown> {
    const rd = this.devRdaDir(device);
    const r = await ensureRda(device, rd);
    if ("error" in r && r.error) {
      throw new RaoError(ErrorCode.HostOffline, `provision failed: ${r.error}`, true);
    }
    // SINGLE-INSTANCE guarantee (see header)
    await sshExec(
      device,
      "pkill -f 'rda_daemon.mjs serve' 2>/dev/null; true",
      10000,
    );
    const ping = `cd ${rd} && node rda_daemon.mjs ping`;
    for (let i = 0; i < 15; i++) {
      const { code } = await sshExec(device, ping, 10000);
      if (code !== 0) break;
      await new Promise((res) => setTimeout(res, 300));
    }
    await sshExec(device, `cd ${rd} && node rda_daemon.mjs start`, 25000);
    let up = false;
    for (let i = 0; i < 15; i++) {
      const { code } = await sshExec(device, ping, 10000);
      if (code === 0) { up = true; break; }
      await new Promise((res) => setTimeout(res, 300));
    }
    if (!up) {
      throw new RaoError(ErrorCode.HostOffline, "daemon failed to start", true);
    }
    return r;
  }

  async ensureDaemon(device: DeviceRecord): Promise<boolean> {
    try {
      await this.provision(device);
      return true;
    } catch (err) {
      if (err instanceof RaoError) return false;
      throw err;
    }
  }

  /** v0.9 control-plane steer: one-shot inject over a fresh on-device
   *  connection (bypasses the goal's event bridge entirely). */
  async steer(
    device: DeviceRecord,
    goalId: string,
    payload: Record<string, unknown>,
  ): Promise<boolean> {
    const rd = this.devRdaDir(device);
    const b64 = Buffer.from(JSON.stringify(payload), "utf8").toString("base64");
    const cmd = `cd ${rd} && node rda_daemon.mjs inject-one --goal-id ${goalId} --payload-b64 ${b64}`;
    const { code, stdout } = await sshExec(device, cmd, 15000);
    for (const line of stdout.trim().split("\n")) {
      try {
        const m = JSON.parse(line.trim());
        if (m.type === "injected") return m.queued === "delivered";
      } catch { /* skip noise */ }
    }
    void code;
    return false;
  }

  /** authoritative event list from the daemon (mirror reconcile) */
  async goalEvents(device: DeviceRecord, goalId: string): Promise<import("../protocol.js").RaoEvent[] | null> {
    const rd = this.devRdaDir(device);
    const cmd = `cd ${rd} && node rda_daemon.mjs status-one --goal-id ${goalId}`;
    const { stdout } = await sshExec(device, cmd, 15000);
    for (const line of stdout.trim().split("\n")) {
      try {
        const m = JSON.parse(line.trim());
        if (m.type === "goal_snapshot") return m.events ?? [];
      } catch { /* skip noise */ }
    }
    return null;
  }

  async openSession(
    goalId: string,
    device: DeviceRecord,
    goal: DispatchedGoal,
  ): Promise<SshDaemonChannel> {
    const chan = new SshDaemonChannel(goalId, device, this.devRdaDir(device));
    await chan.connectAndStart(goal, false);
    return chan;
  }

  async openResumeSession(
    goalId: string,
    device: DeviceRecord,
  ): Promise<SshDaemonChannel> {
    const chan = new SshDaemonChannel(goalId, device, this.devRdaDir(device));
    await chan.connectAndStart(null, true);
    return chan;
  }
}

/** retained parity export */
export const password_of = passwordOf;
