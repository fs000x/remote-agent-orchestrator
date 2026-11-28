/**
 * rao-relay — blind relay server (borrowed from ds-harness-remote's
 * apps/server design). Forwards opaque bytes between two endpoints of a
 * link WITHOUT ever reading them: only the single JSON join line per
 * connection is parsed; everything after it is piped verbatim in both
 * directions. E2EE (Noise_IK) stays between the endpoints — the relay
 * operator sees only ciphertext.
 *
 * Join protocol (one JSON line, then raw bytes):
 *   {"relay":"join","linkId":"...","role":"master"|"node","token":"..."}
 *   <- {"relay":"joined","waiting":true}          (peer not yet connected)
 *   <- {"relay":"joined","waiting":false}         (peer connected; raw pipe starts)
 *   <- {"relay":"error","message":"..."}          (rejection; socket closes)
 *
 * When both roles of a linkId are connected, their sockets are piped to
 * each other. Either side closing tears the pair down; both sides simply
 * reconnect (their own backoff) and re-run join + handshake.
 *
 * Node >= 18, stdlib only. Run: `rao-relay [--port 9900] [--token SECRET]`.
 */
import { createServer, type Server, type Socket } from "node:net";
import { createHash, randomBytes } from "node:crypto";

const JOIN_LINE_MAX = 4096;
const DEFAULT_PORT = 9900;
/** max concurrent links (abuse guard; documented in DEPLOYMENT.md) */
const MAX_LINKS = 256;

interface LinkEnd {
  sock: Socket;
  buf: string;
  joined: boolean;
}

interface Link {
  master: LinkEnd | null;
  node: LinkEnd | null;
}

export class RelayServer {
  private readonly server: Server;
  private readonly links = new Map<string, Link>();
  private readonly tokens: Set<string>; // allowed link tokens (hashed compare)
  private port = 0;

  constructor(opts: { port?: number; tokens?: string[] } = {}) {
    this.port = opts.port ?? DEFAULT_PORT;
    this.tokens = new Set((opts.tokens ?? []).map(canonicalToken));
    this.server = createServer((sock) => this.handle(sock));
  }

  /** starts listening; resolves with the bound port */
  start(): Promise<number> {
    return new Promise((resolve, reject) => {
      this.server.once("error", reject);
      this.server.listen(this.port, "0.0.0.0", () => {
        const addr = this.server.address() as { port: number };
        this.port = addr.port;
        resolve(addr.port);
      });
    });
  }

  stop(): void {
    this.server.close();
    for (const link of this.links.values()) {
      link.master?.sock.destroy();
      link.node?.sock.destroy();
    }
    this.links.clear();
  }

  linkCount(): number {
    return this.links.size;
  }

  private handle(sock: Socket): void {
    sock.setNoDelay?.(true);
    const end: LinkEnd = { sock, buf: "", joined: false };
    sock.on("data", (chunk: Buffer) => {
      if (end.joined) return; // raw pipe mode: relay stays blind
      end.buf += chunk.toString("utf8");
      const nl = end.buf.indexOf("\n");
      if (nl < 0) {
        if (end.buf.length > JOIN_LINE_MAX) {
          this.reject(end, "join line too large");
        }
        return;
      }
      const line = end.buf.slice(0, nl).trim();
      end.buf = "";
      let msg: Record<string, unknown>;
      try {
        msg = JSON.parse(line);
      } catch {
        this.reject(end, "join line must be one JSON object");
        return;
      }
      this.join(end, msg);
    });
    sock.on("error", () => this.drop(end));
    sock.on("close", () => this.drop(end));
  }

  private reject(end: LinkEnd, message: string): void {
    try {
      end.sock.write(JSON.stringify({ relay: "error", message }) + "\n");
    } catch { /* gone */ }
    end.sock.destroy();
    this.drop(end);
  }

  private drop(end: LinkEnd): void {
    if (end.joined) {
      // find + clear our slot; close the peer so it reconnects
      for (const [linkId, link] of this.links) {
        if (link.master === end) {
          link.master = null;
          link.node?.sock.destroy();
        } else if (link.node === end) {
          link.node = null;
          link.master?.sock.destroy();
        }
        if (!link.master && !link.node) this.links.delete(linkId);
      }
    }
    try { end.sock.destroy(); } catch { /* gone */ }
  }

  private join(end: LinkEnd, msg: Record<string, unknown>): void {
    if (msg.relay !== "join") {
      this.reject(end, "first line must be a relay join");
      return;
    }
    const linkId = String(msg.linkId ?? "");
    const role = String(msg.role ?? "");
    const token = String(msg.token ?? "");
    if (!linkId || !["master", "node"].includes(role)) {
      this.reject(end, "linkId and role (master|node) required");
      return;
    }
    if (this.tokens.size && !this.tokens.has(canonicalToken(token))) {
      this.reject(end, "invalid link token");
      return;
    }
    if (!this.tokens.size) {
      // open relay without a token list: require a non-empty secret anyway
      // so cross-tenant links cannot accidentally share an id
      if (!token) {
        this.reject(end, "token required");
        return;
      }
    }
    let link = this.links.get(linkId);
    if (!link) {
      if (this.links.size >= MAX_LINKS) {
        this.reject(end, "relay link capacity reached");
        return;
      }
      link = { master: null, node: null };
      this.links.set(linkId, link);
    }
    const slot = role === "master" ? "master" : "node";
    if (link[slot]) {
      this.reject(end, `${role} already joined this link`);
      return;
    }
    end.joined = true;
    link[slot] = end;
    const peer = slot === "master" ? link.node : link.master;
    if (peer) {
      end.sock.write(JSON.stringify({ relay: "joined", waiting: false }) + "\n");
      peer.sock.write(JSON.stringify({ relay: "joined", waiting: false }) + "\n");
      this.pipe(linkId, link);
    } else {
      end.sock.write(JSON.stringify({ relay: "joined", waiting: true }) + "\n");
    }
  }

  /** blind byte pipe: sockets are wired verbatim in both directions */
  private pipe(linkId: string, link: Link): void {
    const wire = (from: Socket, to: Socket): void => {
      from.on("data", (chunk: Buffer) => {
        try { to.write(chunk); } catch { /* peer died; close handles it */ }
      });
    };
    const mSock = link.master!.sock;
    const nSock = link.node!.sock;
    wire(mSock, nSock);
    wire(nSock, mSock);
    const teardown = (which: "master" | "node"): void => {
      const l = this.links.get(linkId);
      if (!l) return;
      if (which === "master") l.node?.sock.destroy();
      else l.master?.sock.destroy();
      this.links.delete(linkId);
    };
    mSock.once("close", () => teardown("master"));
    nSock.once("close", () => teardown("node"));
    mSock.once("error", () => teardown("master"));
    nSock.once("error", () => teardown("node"));
  }
}

function canonicalToken(token: string): string {
  return createHash("sha256").update(token).digest("hex");
}

/** deterministic demo/link-id helper for CLI output */
export function newLinkId(): string {
  return randomBytes(12).toString("hex");
}

/* ------------------------------------------------------------------ *
 * CLI
 * ------------------------------------------------------------------ */
export async function main(argv = process.argv.slice(2)): Promise<number> {
  let port = DEFAULT_PORT;
  let tokenFlag: string | undefined;
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "--port") port = Number(argv[i + 1]);
    if (argv[i] === "--token") tokenFlag = argv[i + 1];
  }
  const relay = new RelayServer({ port, tokens: tokenFlag ? [tokenFlag] : [] });
  const bound = await relay.start();
  process.stdout.write(
    `rao-relay listening on 0.0.0.0:${bound}` +
    (tokenFlag ? " (token required)" : " (open: token still required per link)") + "\n",
  );
  const interval = setInterval(() => { /* keep the process alive */ }, 60000);
  interval.unref?.();
  process.on("SIGTERM", () => { relay.stop(); process.exit(0); });
  process.on("SIGINT", () => { relay.stop(); process.exit(0); });
  return 0;
}

if (process.argv[1] && import.meta.url.endsWith(process.argv[1].split(/[\\/]/).pop() ?? "…")) {
  main().then((code) => { process.exitCode = code; });
}
