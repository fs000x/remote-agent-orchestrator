/**
 * Noise_IK_25519_ChaChaPoly_SHA256 — E2EE for rao node links (canonical
 * runtime implementation; deployed verbatim, Node >= 18, ZERO dependencies).
 *
 * IK pattern: the initiator (node) already knows the responder's (master)
 * static public key from enrollment. The handshake PROLOGUE binds
 * `rao-node-link|v1|<nodeId>` so a captured handshake cannot be replayed
 * onto another identity's link — mismatch = fail-closed.
 *
 * Wire mapping (JSON lines, base64 fields):
 *   node -> master: {"cmd":"register","nodeId":...,"token":...,
 *                    "sec":"noise-ik-v1","e":...,"s":...,"pl":...}
 *   master -> node: {"type":"registered","nodeId":...,"e":...,"pl":...}
 *   data frames:    {"t":"d","n":<counter>,"ct":...}   (both directions)
 *
 * Replay protection:
 *  - the responder rejects a seen initiator ephemeral (handshake replay);
 *  - initiator identity is checked against the enrolled static key;
 *  - data-frame counters are strictly monotonic per direction.
 */

/* ------------------------------------------------------------------ *
 * primitives (node:crypto only)
 * ------------------------------------------------------------------ */
import {
  createCipheriv, createDecipheriv, createHash, createPrivateKey,
  createPublicKey, diffieHellman, generateKeyPairSync, hkdfSync,
} from "node:crypto";

const X25519_SPKI_PREFIX = Buffer.from("302a300506032b656e032100", "hex");
const X25519_PKCS8_PREFIX = Buffer.from("302e020100300506032b656e04220420", "hex");

export function x25519Generate() {
  const { publicKey, privateKey } = generateKeyPairSync("x25519");
  return {
    pub: Buffer.from(publicKey.export({ type: "spki", format: "der" }).subarray(12)),
    priv: Buffer.from(privateKey.export({ type: "pkcs8", format: "der" }).subarray(16)),
  };
}

export function x25519PublicFromPrivate(priv) {
  const keyObj = createPrivateKey({
    key: Buffer.concat([X25519_PKCS8_PREFIX, priv]),
    format: "der",
    type: "pkcs8",
  });
  return Buffer.from(
    createPublicKey(keyObj).export({ type: "spki", format: "der" }).subarray(12));
}

export function x25519Dh(priv, peerPub) {
  const privObj = createPrivateKey({
    key: Buffer.concat([X25519_PKCS8_PREFIX, priv]),
    format: "der",
    type: "pkcs8",
  });
  const pubObj = createPublicKey({
    key: Buffer.concat([X25519_SPKI_PREFIX, peerPub]),
    format: "der",
    type: "spki",
  });
  return Buffer.from(diffieHellman({ privateKey: privObj, publicKey: pubObj }));
}

export function chacha20poly1305Seal(key, nonce, plaintext) {
  const cipher = createCipheriv("chacha20-poly1305", key, nonce, { authTagLength: 16 });
  return Buffer.concat([cipher.update(plaintext), cipher.final(), cipher.getAuthTag()]);
}

export function chacha20poly1305Open(key, nonce, ciphertextWithTag) {
  const tag = ciphertextWithTag.subarray(ciphertextWithTag.length - 16);
  const body = ciphertextWithTag.subarray(0, ciphertextWithTag.length - 16);
  const decipher = createDecipheriv("chacha20-poly1305", key, nonce, { authTagLength: 16 });
  decipher.setAuthTag(tag);
  return Buffer.concat([decipher.update(body), decipher.final()]);
}

export function hkdfSha256(ikm, salt, info) {
  const out = Buffer.from(hkdfSync("sha256", ikm, salt, info, 64));
  return [out.subarray(0, 32), out.subarray(32)];
}

export function sha256(data) {
  return createHash("sha256").update(data).digest();
}

/* ------------------------------------------------------------------ *
 * handshake
 * ------------------------------------------------------------------ */
export const NOISE_PROTOCOL_NAME = "Noise_IK_25519_ChaChaPoly_SHA256";
export const RAO_NOISE_INFO = Buffer.from("rao-noise-v1", "utf8");

export class NoiseError extends Error {}

function noiseNonce(n) {
  const buf = Buffer.alloc(12); // 4 zero bytes || 8-byte LE counter
  buf.writeUInt32LE(n >>> 0, 4);
  return buf;
}

function initHs(prologue) {
  const h = sha256(Buffer.concat([
    Buffer.from(NOISE_PROTOCOL_NAME, "utf8"),
    Buffer.from(prologue, "utf8"),
  ]));
  return { h, ck: h, k: null };
}

function mixHash(hs, data) {
  hs.h = sha256(Buffer.concat([hs.h, data]));
}

function mixKey(hs, dh) {
  const [ck2, k2] = hkdfSha256(hs.ck, dh, RAO_NOISE_INFO);
  hs.ck = ck2;
  hs.k = k2;
}

// each handshake key encrypts exactly one message → nonce 0 (Noise style)
function seal(hs, plaintext) {
  return chacha20poly1305Seal(hs.k, noiseNonce(0), plaintext);
}

function open(hs, ciphertext) {
  return chacha20poly1305Open(hs.k, noiseNonce(0), ciphertext);
}

/**
 * Full initiator flow. Returns the ik1 frame fields plus a finisher that
 * consumes the master's ik2 reply and derives the session.
 */
export function initiatorHandshake(staticKey, responderStaticPub, prologue, payload) {
  const hs = initHs(prologue);
  const eph = x25519Generate();
  mixHash(hs, eph.pub);
  mixKey(hs, x25519Dh(eph.priv, responderStaticPub)); // es
  const sCt = seal(hs, staticKey.pub);
  mixHash(hs, sCt);
  mixKey(hs, x25519Dh(staticKey.priv, responderStaticPub)); // ss
  const plCt = seal(hs, payload);
  mixHash(hs, plCt);
  return {
    frame: {
      e: eph.pub.toString("base64"),
      s: sCt.toString("base64"),
      pl: plCt.toString("base64"),
    },
    initiatorPub: staticKey.pub,
    finish(reply) {
      if (!reply.e || !reply.pl) {
        throw new NoiseError("malformed ik2 frame");
      }
      const ephR = Buffer.from(reply.e, "base64");
      mixHash(hs, ephR);
      mixKey(hs, x25519Dh(eph.priv, ephR)); // ee
      mixKey(hs, x25519Dh(staticKey.priv, ephR)); // se
      let pl;
      try {
        pl = open(hs, Buffer.from(reply.pl, "base64"));
      } catch {
        throw new NoiseError("ik2 decryption failed (identity mismatch or tampering)");
      }
      mixHash(hs, pl);
      return { session: noiseSplit(hs, true), payload: pl };
    },
  };
}

/**
 * Responder side. Throws NoiseError on replay/prologue mismatch/tampering.
 * `seenEphemerals` (Set) rejects handshake replays for this process.
 */
export function noiseResponderHandshake(staticKey, prologue, frame, seenEphemerals, payload) {
  if (!frame.e || !frame.s || !frame.pl) {
    throw new NoiseError("malformed ik1 frame");
  }
  const ephPub = Buffer.from(frame.e, "base64");
  const ephKey = ephPub.toString("hex");
  if (seenEphemerals.has(ephKey)) {
    throw new NoiseError("handshake replay detected (ephemeral reuse)");
  }
  seenEphemerals.add(ephKey);
  const hs = initHs(prologue);
  mixHash(hs, ephPub);
  mixKey(hs, x25519Dh(staticKey.priv, ephPub)); // es
  const sCt = Buffer.from(frame.s, "base64");
  let initiatorPub;
  try {
    initiatorPub = open(hs, sCt);
  } catch {
    throw new NoiseError("initiator static decryption failed (identity/prologue mismatch or tampering)");
  }
  mixHash(hs, sCt);
  mixKey(hs, x25519Dh(staticKey.priv, initiatorPub)); // ss
  const plCt = Buffer.from(frame.pl, "base64");
  let pl;
  try {
    pl = open(hs, plCt);
  } catch {
    throw new NoiseError("payload decryption failed (identity/prologue mismatch or tampering)");
  }
  mixHash(hs, plCt);
  // message 2: responder ephemeral; ee; se; payload
  const ephR = x25519Generate();
  mixHash(hs, ephR.pub);
  mixKey(hs, x25519Dh(ephR.priv, ephPub)); // ee
  mixKey(hs, x25519Dh(ephR.priv, initiatorPub)); // se
  const pl2Ct = seal(hs, payload);
  mixHash(hs, pl2Ct);
  return {
    reply: {
      e: ephR.pub.toString("base64"),
      pl: pl2Ct.toString("base64"),
    },
    payload: pl,
    initiatorStaticPub: initiatorPub,
    hs,
  };
}

/** Derive session keys from the completed handshake state. */
export function noiseSplit(hs, isInitiator) {
  const [k1, k2] = hkdfSha256(hs.ck, Buffer.alloc(0), RAO_NOISE_INFO);
  // k1: initiator -> responder; k2: responder -> initiator
  return {
    sendN: 0,
    recvN: 0,
    sendKey: isInitiator ? k1 : k2,
    recvKey: isInitiator ? k2 : k1,
    send(plaintext) {
      let pt;
      if (typeof plaintext === "string") pt = Buffer.from(plaintext, "utf8");
      else if (Buffer.isBuffer(plaintext)) pt = plaintext;
      else pt = Buffer.from(JSON.stringify(plaintext), "utf8");
      const n = this.sendN++;
      return { t: "d", n, ct: chacha20poly1305Seal(this.sendKey, noiseNonce(n), pt).toString("base64") };
    },
    recv(frame) {
      if (!frame || frame.t !== "d") {
        throw new NoiseError(`expected data frame, got ${JSON.stringify(frame?.t)}`);
      }
      const n = frame.n;
      if (!Number.isInteger(n) || n !== this.recvN) {
        // strict monotonic counter: replay/reorder = fail-closed
        throw new NoiseError(`frame counter mismatch: expected ${this.recvN}, got ${String(n)}`);
      }
      const pt = chacha20poly1305Open(
        this.recvKey, noiseNonce(n), Buffer.from(frame.ct, "base64"));
      this.recvN++;
      return pt;
    },
  };
}
