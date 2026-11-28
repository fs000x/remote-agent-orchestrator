/**
 * JSON-line message stream: shared framing for daemon handshakes and
 * streaming readers (Python used one shared _line_buf for both).
 */
export type WireMessage = Record<string, unknown>;

export class LineStream {
  private buf = "";
  private queue: WireMessage[] = [];
  private waiters: Array<{
    resolve: (m: WireMessage | null) => void;
    timer: NodeJS.Timeout | null;
  }> = [];
  private eof = false;
  /** non-protocol bytes (bridge stderr noise, bad json) for diagnostics */
  noise: string[] = [];

  /** feed raw bytes (UTF-8 text) */
  push(chunk: string): void {
    if (this.eof) return;
    this.buf += chunk;
    for (;;) {
      const nl = this.buf.indexOf("\n");
      if (nl < 0) break;
      const line = this.buf.slice(0, nl).trim();
      this.buf = this.buf.slice(nl + 1);
      if (!line) continue;
      let msg: WireMessage;
      try {
        msg = JSON.parse(line) as WireMessage;
      } catch {
        this.noise.push(line.slice(0, 200));
        continue;
      }
      this.deliver(msg);
    }
  }

  /** EOF: queued messages still deliver; next() resolves null afterwards */
  end(): void {
    if (this.eof) return;
    // final partial line without newline (rare but possible)
    const rest = this.buf.trim();
    this.buf = "";
    if (rest) {
      try {
        this.deliver(JSON.parse(rest) as WireMessage);
      } catch {
        this.noise.push(rest.slice(0, 200));
      }
    }
    this.eof = true;
    for (;;) {
      const w = this.waiters.shift();
      if (!w) break;
      if (w.timer) clearTimeout(w.timer);
      w.resolve(this.queue.shift() ?? null);
    }
  }

  private deliver(msg: WireMessage): void {
    const w = this.waiters.shift();
    if (w) {
      if (w.timer) clearTimeout(w.timer);
      w.resolve(msg);
    } else {
      this.queue.push(msg);
    }
  }

  /**
   * Next parsed message. timeoutMs=null blocks until a message or EOF;
   * otherwise resolves null on quiet timeout (caller re-checks deadline).
   */
  next(timeoutMs: number | null): Promise<WireMessage | null> {
    const queued = this.queue.shift();
    if (queued !== undefined) return Promise.resolve(queued);
    if (this.eof) return Promise.resolve(null);
    return new Promise((resolve) => {
      const w = {
        resolve,
        timer: timeoutMs === null
          ? null
          : setTimeout(() => {
            const i = this.waiters.indexOf(w);
            if (i >= 0) this.waiters.splice(i, 1);
            resolve(null);
          }, timeoutMs),
      };
      this.waiters.push(w);
    });
  }
}
