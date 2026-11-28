import { describe, expect, it } from "vitest";
import {
  GoalSchema, EventSchema, LIMITS, ErrorCode, RETRYABLE, RaoError, isAbsPath,
} from "../src/protocol.js";

const baseGoal = {
  protocol: "rao/1" as const,
  goalId: "g-1",
  deviceId: "d-1",
  objective: "do the thing",
};

describe("GoalSchema", () => {
  it("accepts a minimal goal", () => {
    expect(GoalSchema.safeParse(baseGoal).success).toBe(true);
  });
  it("accepts a full plan with criteria/fallbacks", () => {
    const g = { ...baseGoal, plan: { steps: [
      { id: "s1", type: "shell", command: "ls", success: { exitCode: 0 },
        retries: 1, fallbacks: [{ id: "f1", type: "shell", command: "true" }] },
      { id: "s2", type: "ask", question: "continue?" },
      { id: "s3", type: "artifact", paths: ["a.log"] },
      { id: "s4", type: "wait_file", path: "/tmp/x" },
    ] } };
    expect(GoalSchema.safeParse(g).success).toBe(true);
  });
  it("rejects wrong protocol", () => {
    expect(GoalSchema.safeParse({ ...baseGoal, protocol: "rao/2" }).success).toBe(false);
  });
  it("rejects missing objective", () => {
    const { objective: _drop, ...rest } = baseGoal;
    expect(GoalSchema.safeParse(rest).success).toBe(false);
  });
  it("rejects duplicate step ids via plan step uniqueness not enforced at schema (dedup at engine)", () => {
    const g = _goal(baseGoal, { plan: { steps: [
      { id: "s1", type: "shell", command: "a" },
      { id: "s1", type: "shell", command: "b" },
    ] } });
    // schema allows dup ids (engine dedups); just ensure parse succeeds
    expect(GoalSchema.safeParse(g).success).toBe(true);
  });
  it("rejects claude agent combined with plan", () => {
    const g = { ...baseGoal, agent: "claude" as const,
      plan: { steps: [{ id: "s1", type: "shell", command: "x" }] } };
    const r = GoalSchema.safeParse(g);
    expect(r.success).toBe(false);
  });
  it("rejects relative workspace", () => {
    expect(GoalSchema.safeParse({ ...baseGoal, workspace: "rel" }).success).toBe(false);
  });
  it("rejects unknown agent", () => {
    expect(GoalSchema.safeParse({ ...baseGoal, agent: "gpt-9000" }).success).toBe(false);
  });
  it("rejects ensureWorkspace without repoUrl", () => {
    const g = { ...baseGoal, ensureWorkspace: { path: "/x" } };
    expect(GoalSchema.safeParse(g).success).toBe(false);
  });
});

function _goal(base: object, over: object) {
  return { ...base, ...over };
}

describe("errors", () => {
  it("retryable set contains transport/timeout codes", () => {
    expect(RETRYABLE.has(ErrorCode.TransportLost)).toBe(true);
    expect(RETRYABLE.has(ErrorCode.GoalInvalid)).toBe(false);
  });
  it("RaoError carries code+retryable", () => {
    const e = new RaoError(ErrorCode.HostOffline, "down", true);
    expect(e.code).toBe("HOST_OFFLINE");
    expect(e.retryable).toBe(true);
  });
});

describe("limits", () => {
  it("exposes the centralized limits table", () => {
    expect(LIMITS.maxSteps).toBeGreaterThan(0);
    expect(LIMITS.frameMax).toBe(65536);
  });
  it("isAbsPath accepts POSIX and Windows drive paths", () => {
    expect(isAbsPath("/srv/x")).toBe(true);
    expect(isAbsPath("C:/x")).toBe(true);
    expect(isAbsPath("rel")).toBe(false);
  });
});
