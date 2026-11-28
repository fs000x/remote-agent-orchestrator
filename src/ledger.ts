/**
 * operationId idempotency ledger (borrowed from ds-harness-remote).
 *
 * Semantics (documented in PRODUCT/DEPLOYMENT, normative):
 *  - same key + same params  -> same receipt (no re-execution)
 *  - same key + changed args -> rejected (params fingerprint mismatch)
 *  - receipts are lost when the process resets; `unknown` receipt is NOT
 *    proof that an earlier operation did not execute — verify via
 *    remote_status / remote_collect instead of retrying with a new key.
 *  - `accepted` means queued, NOT completed.
 *
 * The ledger stores only hashes, target identifiers and receipts —
 * never message bodies.
 */
import { createHash } from "node:crypto";

export type ReceiptStatus = "pending" | "accepted" | "rejected" | "unknown";

export interface Receipt {
  operationId: string;
  status: ReceiptStatus;
  /** domain-specific result fields (goalId, deviceId, ...) */
  result?: Record<string, unknown>;
  /** monotonic ms when the receipt was recorded */
  at: number;
}

interface LedgerEntry {
  fingerprint: string;
  receipt: Receipt;
  pending?: Promise<Receipt>;
}

/** operations that mutate remote state and therefore require idempotency */
export const MUTATING_OPERATIONS = new Set([
  "remote_dispatch",
  "remote_steer",
  "remote_cancel",
  "remote_dispatch_dag",
]);

export function fingerprint(params: unknown): string {
  return createHash("sha256")
    .update(JSON.stringify(params, sortedReplacer))
    .digest("hex");
}

function sortedReplacer(_key: string, value: unknown): unknown {
  if (value && typeof value === "object" && !Array.isArray(value)) {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>).sort(([a], [b]) =>
        a < b ? -1 : 1,
      ),
    );
  }
  return value;
}

export class OperationLedger {
  private readonly ledger = new Map<string, LedgerEntry>();
  /** in-memory only: receipts are lost on reset by design (documented) */

  /** Returns the existing receipt when the fingerprint matches; throws
   *  RATE_LIMITED-style conflict when the same key is reused with different
   *  params. Returns undefined when the key is new. */
  check(operationId: string, params: unknown): Receipt | undefined {
    const fp = fingerprint(params);
    const old = this.ledger.get(operationId);
    if (old) {
      if (old.fingerprint !== fp) {
        throw new Error(
          `operationId ${operationId} was already used with different parameters (fingerprint mismatch); use a fresh operationId`,
        );
      }
      return old.receipt;
    }
    return undefined;
  }

  record(
    operationId: string,
    params: unknown,
    receipt: Omit<Receipt, "operationId" | "at"> & { at?: number },
  ): Receipt {
    const full: Receipt = {
      operationId,
      status: receipt.status,
      result: receipt.result,
      at: receipt.at ?? Date.now(),
    };
    this.ledger.set(operationId, {
      fingerprint: fingerprint(params),
      receipt: full,
    });
    return full;
  }

  /** Wrap an async mutation: dedupe + record, single in-flight per key.
   *  Callers receive the domain result fields merged at the top level
   *  (e.g. goalId/deviceId next to operationId/status/at). */
  async run<T extends Record<string, unknown> & { status: ReceiptStatus }>(
    operationId: string,
    params: unknown,
    fn: () => Promise<T>,
  ): Promise<T & { operationId: string; at: number }> {
    const fp = fingerprint(params);
    const old = this.ledger.get(operationId);
    if (old) {
      if (old.fingerprint !== fp) {
        throw new Error(
          `operationId ${operationId} was already used with different parameters (fingerprint mismatch); use a fresh operationId`,
        );
      }
      if (old.pending) {
        return (await old.pending) as unknown as T & {
          operationId: string;
          at: number;
        }; // join the in-flight op
      }
      const rec = old.receipt;
      return {
        ...(rec.result ?? {}),
        operationId: rec.operationId,
        status: rec.status,
        at: rec.at,
      } as unknown as T & { operationId: string; at: number };
    }

    const pending = (async () => {
      try {
        const r = await fn();
        const full: Receipt = {
          operationId,
          status: r.status,
          result: { ...r },
          at: Date.now(),
        };
        this.ledger.set(operationId, { fingerprint: fp, receipt: full });
        return { ...r, operationId, at: full.at } as T & {
          operationId: string;
          at: number;
        };
      } catch (err) {
        // nothing recorded on failure: a retry with the same key is allowed
        this.ledger.delete(operationId);
        throw err;
      }
    })();
    this.ledger.set(operationId, {
      fingerprint: fp,
      receipt: { operationId, status: "pending", at: Date.now() },
      pending,
    });
    return await pending;
  }

  /** Receipts are lost on process reset by design; call once at startup. */
  clear(): void {
    this.ledger.clear();
  }
}

/** Shared ledger instance for the MCP server process. */
export const globalLedger = new OperationLedger();
