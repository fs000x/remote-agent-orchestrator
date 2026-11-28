/**
 * Direct-TCP daemon transport (loopback / local tests): auths with the
 * daemon token over a plain socket.
 */
import { connect } from "node:net";
import { ErrorCode, RaoError } from "../protocol.js";
import type { DeviceRecord, DeviceRegistry } from "../devices.js";
import { DaemonChannelBase } from "./daemonChannel.js";
import type { DispatchedGoal } from "../protocol.js";
import type { Transport } from "../orchestrator.js";

export class TcpDaemonChannel extends DaemonChannelBase {
  private sock: import("node:net").Socket | null = null;

  constructor(
    goalId: string,
    private readonly device: DeviceRecord,
    token: string,
  ) {
    super(goalId, "direct", token);
  }

  protected openLink(): Promise<void> {
    return new Promise((resolve, reject) => {
      const port = this.device.daemonPort ?? 0;
      const sock = connect(
        { host: this.device.host, port, timeout: 5000 },
        () => {
          sock.setNoDelay?.(true);
          resolve();
        },
      );
      sock.on("data", (c: Buffer) => this.stream.push(c.toString("utf8")));
      sock.on("error", (err) => {
        if (!this.closed) {
          this.addStderr(`[tcp] ${err.message}`);
          this.stream.end();
        }
      });
      sock.on("close", () => this.stream.end());
      this.sock = sock;
    });
  }

  protected sendLine(obj: Record<string, unknown>): void {
    this.sock!.write(JSON.stringify(obj) + "\n");
  }

  protected linkAlive(): boolean {
    return this.sock !== null && !this.sock.destroyed;
  }

  protected closeLink(): void {
    try { this.sock?.destroy(); } catch { /* gone */ }
    this.stream.end();
  }
}

export class TcpDaemonTransport implements Transport {
  constructor(
    private readonly token: string | ((device: DeviceRecord) => string | undefined),
    private readonly registry?: DeviceRegistry,
  ) {}

  private tokenFor(device: DeviceRecord): string {
    const t = typeof this.token === "function" ? this.token(device) : this.token;
    if (!t) {
      throw new RaoError(ErrorCode.AuthRequired,
        `no daemon token for device ${device.id}`);
    }
    return t;
  }

  async openSession(
    goalId: string,
    device: DeviceRecord,
    goal: DispatchedGoal,
  ): Promise<TcpDaemonChannel> {
    const chan = new TcpDaemonChannel(goalId, device, this.tokenFor(device));
    await chan.connectAndStart(goal, false);
    return chan;
  }

  async openResumeSession(
    goalId: string,
    device: DeviceRecord,
  ): Promise<TcpDaemonChannel> {
    const chan = new TcpDaemonChannel(goalId, device, this.tokenFor(device));
    await chan.connectAndStart(null, true);
    return chan;
  }
}
