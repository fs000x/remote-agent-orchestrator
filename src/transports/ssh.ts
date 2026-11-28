/**
 * Spawn-mode SSH transport: one SSH connection + one exec channel per goal,
 * running `node rda.mjs run-goal` on the device. (The Python version used
 * python3 rda.py; the TS rewrite deploys the .mjs runtime — devices need
 * Node >= 18, see DEPLOYMENT.md.)
 */
import { ErrorCode, RaoError } from "../protocol.js";
import { passwordOf, type DeviceRecord } from "../devices.js";
import { connectClient, type ClientChannel } from "./ssh2util.js";
import { GoalChannelBase } from "./channel.js";
import type { DispatchedGoal } from "../protocol.js";
import type { Transport } from "../orchestrator.js";
import { ensureRda } from "../provision.js";

const RDA_COMMAND = "cd {rdaDir} && node rda.mjs run-goal";

export class SshGoalChannel extends GoalChannelBase {
  private client: import("ssh2").Client | null = null;
  private chan: ClientChannel | null = null;

  constructor(
    goalId: string,
    private readonly device: DeviceRecord,
    private readonly rdaDir: string,
  ) {
    super(goalId);
  }

  async connectAndStart(goalLine: unknown): Promise<this> {
    try {
      this.client = await connectClient(this.device);
    } catch (err) {
      throw err instanceof RaoError
        ? err
        : new RaoError(ErrorCode.HostOffline, String(err), true);
    }
    let stream: ClientChannel;
    try {
      stream = await new Promise<ClientChannel>((resolve, reject) => {
        this.client!.exec(
          RDA_COMMAND.replace("{rdaDir}", this.rdaDir),
          (err, ch) => (err ? reject(err) : resolve(ch)),
        );
      });
    } catch (err) {
      this.close();
      throw new RaoError(ErrorCode.TransportLost, `exec: ${String(err)}`, true);
    }
    this.chan = stream;
    stream.stdout.on("data", (c: Buffer) => {
      for (const ln of c.toString("utf8").split("\n")) this.handleLine(ln);
    });
    stream.stderr.on("data", (c: Buffer) => {
      for (const ln of c.toString("utf8").split("\n")) {
        if (ln.trim()) this.addStderr(ln.trim());
      }
    });
    stream.on("close", (code: number) => {
      this.setExitCode(code ?? -1);
    });
    stream.on("error", () => {
      if (this.exit_code === null) {
        this.pushSyntheticError("RAO_TRANSPORT_LOST: ssh stream error");
        this.setExitCode(-1);
      }
    });
    // goal must arrive before RDA waits for plan
    stream.stdin.write(JSON.stringify(goalLine) + "\n");
    return this;
  }

  writeLine(obj: Record<string, unknown>): boolean {
    if (this.closed || !this.chan || this.chan.destroyed) return false;
    try {
      this.chan.stdin.write(JSON.stringify(obj) + "\n");
      return true;
    } catch {
      return false;
    }
  }

  alive(): boolean {
    return this.chan !== null && !this.closed && this.exit_code === null;
  }

  kill(): void {
    this.killed = true;
    this.close();
  }

  close(): void {
    this.closed = true;
    try { this.chan?.close(); } catch { /* already gone */ }
    try { this.client?.end(); } catch { /* already gone */ }
  }
}

export class SshTransport implements Transport {
  constructor(private readonly rdaDir: string) {}

  async provision(device: DeviceRecord): Promise<unknown> {
    const r = await ensureRda(device, device.rdaDir || this.rdaDir);
    if ("error" in r && r.error) {
      throw new RaoError(
        ErrorCode.HostOffline, `provision failed: ${r.error}`, true);
    }
    return r;
  }

  async openSession(
    goalId: string,
    device: DeviceRecord,
    goal: DispatchedGoal,
  ): Promise<SshGoalChannel> {
    const chan = new SshGoalChannel(goalId, device, device.rdaDir || this.rdaDir);
    await chan.connectAndStart(goal);
    return chan;
  }

  /** spawn mode cannot resume */
  async openResumeSession(): Promise<never> {
    throw new RaoError(ErrorCode.TransportLost, "spawn mode cannot resume", true);
  }
}

/** retained for parity with the Python layer; TS registry resolves directly */
export const password_of = passwordOf;
