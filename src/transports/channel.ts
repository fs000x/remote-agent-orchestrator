/**
 * GoalChannel base: event buffering (single-event-per-poll contract),
 * stderr capture, exit tracking. Concrete transports implement writeLine /
 * alive / close and feed pushEvent / setExitCode.
 */
import {
  PROTOCOL_VERSION, validateEvent, type RaoEvent,
} from "../protocol.js";
import type { GoalChannel } from "../orchestrator.js";

export abstract class GoalChannelBase implements GoalChannel {
  readonly goalId: string;
  events: RaoEvent[] = [];
  event_seqs: Array<number | null> = [];
  stderr_text: string[] = [];
  exit_code: number | null = null;
  killed = false;
  closed = false;
  supports_resume = false;

  private waiters: Array<{
    since: number;
    resolve: (v: [RaoEvent | null, number]) => void;
    timer: NodeJS.Timeout;
  }> = [];

  constructor(goalId: string) {
    this.goalId = goalId;
  }

  /** validated protocol event appended; resolves one waiter (index contract:
   *  newIndex = since + 1, never skipping buffered events) */
  pushEvent(ev: RaoEvent, seq: number | null = null): void {
    this.events.push(ev);
    this.event_seqs.push(seq);
    const w = this.waiters[0];
    if (w && this.events.length > w.since) {
      clearTimeout(w.timer);
      w.resolve([this.events[w.since]!, w.since + 1]);
      this.waiters.shift();
    }
  }

  /** synthesize a protocol error event (transport lost etc.) */
  pushSyntheticError(message: string): void {
    this.pushEvent({
      protocol: PROTOCOL_VERSION,
      kind: "error",
      goalId: this.goalId,
      ts: Date.now(),
      data: { message, fatal: true },
    });
  }

  addStderr(line: string): void {
    if (this.stderr_text.length < 4000) this.stderr_text.push(line);
  }

  setExitCode(code: number): void {
    if (this.exit_code === null) this.exit_code = code;
  }

  waitEvent(since: number, timeoutMs: number): Promise<[RaoEvent | null, number]> {
    if (this.events.length > since) {
      return Promise.resolve([this.events[since]!, since + 1]);
    }
    if (this.closed) return Promise.resolve([null, since]);
    return new Promise((resolve) => {
      const entry = {
        since,
        resolve,
        timer: setTimeout(() => {
          this.waiters = this.waiters.filter((w) => w !== entry);
          resolve([null, since]);
        }, timeoutMs),
      };
      this.waiters.push(entry);
    });
  }

  waitExit(timeoutMs: number): Promise<number | null> {
    if (this.exit_code !== null) return Promise.resolve(this.exit_code);
    return new Promise((resolve) => {
      const t = setInterval(() => {
        if (this.exit_code !== null || this.closed) {
          clearInterval(t);
          resolve(this.exit_code);
        }
      }, 50);
      setTimeout(() => {
        clearInterval(t);
        resolve(this.exit_code);
      }, timeoutMs);
    });
  }

  abstract writeLine(obj: Record<string, unknown>): Promise<boolean> | boolean;
  abstract alive(): boolean;
  abstract kill(): void;
  abstract close(): void;

  /** parse + validate one stdout line; returns true when accepted */
  protected handleLine(line: string): boolean {
    const t = line.trim();
    if (!t) return false;
    let ev: unknown;
    try {
      ev = JSON.parse(t);
    } catch {
      this.addStderr(`[non-json stdout] ${t.slice(0, 200)}`);
      return false;
    }
    const [ok, err] = validateEvent(ev as RaoEvent);
    if (!ok) {
      this.addStderr(`[bad event] ${err}`);
      return false;
    }
    this.pushEvent(ev as RaoEvent);
    return true;
  }
}
