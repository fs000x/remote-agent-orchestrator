/**
 * Daemon-mode channel base (v0.3+): auth handshake, submit/attach, event
 * routing with daemon seq numbers, inject receipts with ack correlation.
 *
 * handshake "bridge": SSH client-bridge (the bridge auths itself with the
 *   on-device daemon.json token; ready arrives immediately).
 * handshake "direct": TCP to the daemon (client auths with the token).
 */
import { ErrorCode, RaoError, validateEvent, type RaoEvent } from "../protocol.js";
import { GoalChannelBase } from "./channel.js";
import { LineStream, type WireMessage } from "./lineStream.js";

export abstract class DaemonChannelBase extends GoalChannelBase {
  supports_resume = true;
  protected readonly stream = new LineStream();
  private readonly acks = new Map<string, WireMessage>();
  private readonly ackWaiters = new Map<string, (m: WireMessage) => void>();
  private pumpPromise: Promise<void> | null = null;

  protected constructor(
    goalId: string,
    private readonly handshake: "bridge" | "direct",
    private readonly token?: string,
  ) {
    super(goalId);
  }

  /* -- link plumbing (concrete transports) ------------------------------- */

  /** open the link and start feeding this.stream (stdout only; stderr goes
   *  to stderr_text) — resolve when the link is ready to carry lines */
  protected abstract openLink(): Promise<void>;
  protected abstract sendLine(obj: Record<string, unknown>): void;
  protected abstract linkAlive(): boolean;
  protected abstract closeLink(): void;

  /* -- lifecycle ---------------------------------------------------------- */

  async connectAndStart(goalObj: unknown, resume = false): Promise<this> {
    await this.openLink();
    if (this.handshake === "direct") {
      this.sendLine({ cmd: "auth", token: this.token });
    }
    const ready = await this.stream.next(10000);
    if (!ready || ready.type !== "ready") {
      throw new RaoError(
        ErrorCode.TransportLost,
        `daemon bridge not ready: ${JSON.stringify(ready)}`,
      );
    }
    if (resume) {
      this.sendLine({ cmd: "attach", goalId: this.goalId });
      const ack = await this.stream.next(10000);
      if (!ack || ack.type !== "attached") {
        throw new RaoError(
        ErrorCode.TransportLost,
        `attach failed: ${JSON.stringify(ack)}`,
      );
      }
    } else {
      this.sendLine({ cmd: "submit", goal: goalObj });
      const ack = await this.stream.next(10000);
      if (!ack || ack.type !== "ack") {
        throw new RaoError(
        ErrorCode.TransportLost,
        `submit failed: ${JSON.stringify(ack)}`,
      );
      }
    }
    this.startPump();
    return this;
  }

  private startPump(): void {
    this.pumpPromise = (async () => {
      try {
        for (;;) {
          const msg = await this.stream.next(null);
          if (!msg) break;
          this.routeMsg(msg);
        }
      } catch {
        /* link died mid-read: waiters unblock via close()/waitExit */
      }
    })();
  }

  /** awaited only by tests; the orchestrator never blocks on the pump */
  pumpDone(): Promise<void> {
    return this.pumpPromise ?? Promise.resolve();
  }

  private routeMsg(msg: WireMessage): void {
    const mtype = msg.type as string | undefined;
    if (mtype === "event") {
      const ev = (msg.event ?? {}) as RaoEvent;
      const [ok, err] = validateEvent(ev);
      if (!ok) {
        this.addStderr(`[bad event] ${err}`);
        return;
      }
      this.pushEvent(ev, (msg.seq as number | undefined) ?? null);
    } else if (mtype === "goal_status") {
      // terminal signal from the daemon (post-result safety net)
      this.setExitCode((msg.exit as number) ?? 0);
      const status = msg.status as string;
      if (["timeout", "cancelled", "failed"].includes(status) &&
          !this.events.some((e) => e.kind === "result")) {
        this.pushEvent({
          protocol: "rao/1",
          kind: "result",
          goalId: this.goalId,
          ts: Date.now(),
          data: {
            status,
            summary: `daemon goal_status (${status})`,
            steps: [], artifacts: [], assumptions: [], steerNotes: [],
            metrics: {},
          },
        });
      }
    } else if (mtype === "injected") {
      const ack = msg.ack as string | undefined;
      const w = ack ? this.ackWaiters.get(ack) : undefined;
      if (w) {
        this.ackWaiters.delete(ack!);
        w(msg);
      } else if (ack) {
        this.acks.set(ack, msg);
      }
    } else if (mtype === "error") {
      this.addStderr(`[daemon error] ${String(msg.message ?? "")}`);
    } else {
      this.addStderr(`[daemon] ${JSON.stringify(msg).slice(0, 120)}`);
    }
  }

  /* -- GoalChannel API ----------------------------------------------------- */

  writeLine(obj: Record<string, unknown>): Promise<boolean> {
    return this.inject(obj);
  }

  /** Send an injection and WAIT for the daemon receipt. True only when the
   *  daemon delivered it to the engine (or holds it as pending). */
  async inject(obj: Record<string, unknown>, waitAckMs = 5000): Promise<boolean> {
    if (this.closed) return false;
    const ack = `ack-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    try {
      this.sendLine({
        cmd: "inject", goalId: this.goalId, payload: obj, ack,
      });
    } catch {
      return false;
    }
    const reply = await new Promise<WireMessage | null>((resolve) => {
      const pending = this.acks.get(ack);
      if (pending) {
        this.acks.delete(ack);
        resolve(pending);
        return;
      }
      const timer = setTimeout(() => {
        this.ackWaiters.delete(ack);
        resolve(null);
      }, waitAckMs);
      this.ackWaiters.set(ack, (m) => {
        clearTimeout(timer);
        resolve(m);
      });
    });
    return reply?.queued === "delivered" || reply?.queued === "pending";
  }

  alive(): boolean {
    return !this.closed && this.exit_code === null && this.linkAlive();
  }

  kill(): void {
    this.killed = true;
    this.close();
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    // pending inject() calls resolve false via their own timers
    this.ackWaiters.clear();
    this.closeLink();
  }
}
