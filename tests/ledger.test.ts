/**
 * operationId idempotency ledger tests (P1 borrow from ds-harness-remote).
 * Semantics under test:
 *  - same key + same params  -> same receipt, fn executed once
 *  - same key + changed args -> fingerprint-mismatch rejection
 *  - concurrent same-key calls join the single in-flight execution
 */
import { describe, expect, it, vi } from "vitest";
import {
  MUTATING_OPERATIONS,
  OperationLedger,
  fingerprint,
} from "../src/ledger.js";

describe("ledger: dedupe + receipts", () => {
  it("same key + same params -> same receipt, fn called once", async () => {
    const ledger = new OperationLedger();
    const fn = vi.fn(async () => ({
      status: "accepted" as const,
      goalId: "g-1",
    }));
    const params = { deviceId: "d1", objective: "x" };
    const r1 = await ledger.run("op-1", params, fn);
    const r2 = await ledger.run("op-1", params, fn);
    expect(fn).toHaveBeenCalledTimes(1);
    expect(r1.goalId).toBe("g-1");
    expect(r2).toEqual(r1);
    expect(r2.operationId).toBe("op-1");
    expect(r2.status).toBe("accepted");
  });

  it("same key + changed params -> fingerprint mismatch rejection", async () => {
    const ledger = new OperationLedger();
    await ledger.run("op-1", { deviceId: "d1" }, async () => ({
      status: "accepted" as const,
    }));
    expect(() => ledger.check("op-1", { deviceId: "d2" })).toThrow(
      /fingerprint mismatch/,
    );
    await expect(
      ledger.run("op-1", { deviceId: "d2" }, async () => ({
        status: "accepted" as const,
      })),
    ).rejects.toThrow(/fingerprint mismatch/);
  });

  it("concurrent same-key calls join the single in-flight execution", async () => {
    const ledger = new OperationLedger();
    const fn = vi.fn(async () => {
      await new Promise((r) => setTimeout(r, 50));
      return { status: "accepted" as const, n: 1 };
    });
    const [a, b] = await Promise.all([
      ledger.run("op-1", { p: 1 }, fn),
      ledger.run("op-1", { p: 1 }, fn),
    ]);
    expect(fn).toHaveBeenCalledTimes(1);
    expect(a).toEqual(b);
  });

  it("different keys do not interfere; params are key-scoped", async () => {
    const ledger = new OperationLedger();
    const fn = vi.fn(async () => ({ status: "accepted" as const }));
    await ledger.run("op-a", { p: 1 }, fn);
    await ledger.run("op-b", { p: 1 }, fn);
    expect(fn).toHaveBeenCalledTimes(2);
  });

  it("check/record low-level API: undefined for new key, receipt after record", () => {
    const ledger = new OperationLedger();
    const params = { deviceId: "d1" };
    expect(ledger.check("op-1", params)).toBeUndefined();
    const rec = ledger.record("op-1", params, {
      status: "rejected",
      result: { reason: "goal invalid" },
    });
    expect(rec.status).toBe("rejected");
    const seen = ledger.check("op-1", params);
    expect(seen?.status).toBe("rejected");
  });

  it("fingerprint is order-insensitive for object keys", () => {
    expect(fingerprint({ a: 1, b: { c: 2, d: 3 } })).toBe(
      fingerprint({ b: { d: 3, c: 2 }, a: 1 }),
    );
    expect(fingerprint({ a: 1 })).not.toBe(fingerprint({ a: 2 }));
  });

  it("clear() resets receipts (lost-on-reset is documented behavior)", async () => {
    const ledger = new OperationLedger();
    await ledger.run("op-1", {}, async () => ({ status: "accepted" as const }));
    ledger.clear();
    const fn = vi.fn(async () => ({ status: "accepted" as const }));
    await ledger.run("op-1", {}, fn);
    expect(fn).toHaveBeenCalledTimes(1);
  });
});

describe("ledger: mutating-operation registry", () => {
  it("covers the four mutating MCP tools", () => {
    for (const op of [
      "remote_dispatch",
      "remote_steer",
      "remote_cancel",
      "remote_dispatch_dag",
    ]) {
      expect(MUTATING_OPERATIONS.has(op)).toBe(true);
    }
    // read-only tools must never require idempotency keys
    expect(MUTATING_OPERATIONS.has("remote_status")).toBe(false);
    expect(MUTATING_OPERATIONS.has("remote_collect")).toBe(false);
    expect(MUTATING_OPERATIONS.has("devices_list")).toBe(false);
  });
});
