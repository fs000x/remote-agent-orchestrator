/**
 * rao wire protocol v1 — zod schemas, error taxonomy, centralized limits.
 *
 * Borrowed discipline (from ds-harness-remote's protocol.md):
 *  - stable error codes grouped by domain, with `retryable` semantics
 *    ("retryable only means a later attempt may succeed; never auto-replay
 *     a non-idempotent mutation"),
 *  - a centralized limits table; violations MUST return a stable error
 *    (RATE_LIMITED / FRAME_TOO_LARGE) instead of silently caching.
 */
import { z } from "zod";

export const PROTOCOL_VERSION = "rao/1";

/* ------------------------------------------------------------------ *
 * Limits (single source of truth — violations return stable errors)
 * ------------------------------------------------------------------ */
export const LIMITS = {
  /** max steps per goal (incl. retries); claude backend maps to --max-turns */
  maxSteps: 40,
  /** default whole-goal hard timeout */
  goalTimeoutMs: 600_000,
  /** default per-step timeout */
  stepTimeoutMs: 120_000,
  /** event silence watchdog */
  eventSilenceMs: 90_000,
  /** steer acknowledgement wait */
  steerAckMs: 5_000,
  /** control-plane (inject-one) retry budget */
  steerRetries: 3,
  /** reconnect attempts per goal in daemon mode */
  resumeAttempts: 5,
  /** reconnect backoff base/cap */
  resumeBackoffMs: 1_000,
  resumeBackoffCapMs: 5_000,
  /** max goals dispatched per remote_dispatch call */
  dispatchBatch: 16,
  /** max concurrent goals per device */
  deviceConcurrency: 2,
  /** max concurrent goals globally */
  globalConcurrency: 5,
  /** objective length cap */
  objectiveMax: 8_192,
  /** run: steered command cap */
  steeredCommandMax: 4_096,
  /** wire frame cap (JSON line) */
  frameMax: 64 * 1024,
  /** daemon buffered events per goal */
  eventBuffer: 5_000,
} as const;

/* ------------------------------------------------------------------ *
 * Error taxonomy (stable codes; `retryable` semantics documented)
 * ------------------------------------------------------------------ */
export const ErrorCode = {
  // protocol / version
  InvalidMessage: "INVALID_MESSAGE",
  UnsupportedVersion: "UNSUPPORTED_VERSION",
  FrameTooLarge: "FRAME_TOO_LARGE",
  RateLimited: "RATE_LIMITED",
  MethodNotFound: "METHOD_NOT_FOUND",
  // auth / device
  AuthRequired: "AUTH_REQUIRED",
  AuthInvalid: "AUTH_INVALID",
  DeviceNotFound: "DEVICE_NOT_FOUND",
  // connection / transport
  HostOffline: "HOST_OFFLINE",
  ConnectionFailed: "CONNECTION_FAILED",
  ConnectionReplaced: "CONNECTION_REPLACED",
  TransportLost: "TRANSPORT_LOST",
  SlowConsumer: "SLOW_CONSUMER",
  // goals
  GoalInvalid: "GOAL_INVALID",
  GoalNotFound: "GOAL_NOT_FOUND",
  BudgetExceeded: "BUDGET_EXCEEDED",
  StepFailed: "STEP_FAILED",
  SteerDropped: "STEER_DROPPED",
  AgentBusy: "AGENT_BUSY",
  AgentUnavailable: "AGENT_UNAVAILABLE",
  RpcTimeout: "RPC_TIMEOUT",
  InternalError: "INTERNAL_ERROR",
} as const;
export type ErrorCodeValue = (typeof ErrorCode)[keyof typeof ErrorCode];

/** Which errors MAY succeed on a later attempt. Non-idempotent mutations
 *  must never be auto-replayed even when retryable=true. */
export const RETRYABLE = new Set<string>([
  ErrorCode.HostOffline,
  ErrorCode.ConnectionFailed,
  ErrorCode.TransportLost,
  ErrorCode.RpcTimeout,
  ErrorCode.RateLimited,
  ErrorCode.AgentBusy,
]);

export class RaoError extends Error {
  constructor(
    public readonly code: ErrorCodeValue,
    message: string,
    public readonly retryable = false,
    public readonly details?: Record<string, unknown>,
  ) {
    super(message);
    this.name = "RaoError";
  }
}

/* ------------------------------------------------------------------ *
 * Goal schema (v0.10 fields carried forward; agent backend added)
 * ------------------------------------------------------------------ */
export const StepSchema = z.discriminatedUnion("type", [
  z.object({
    id: z.string().regex(/^[A-Za-z0-9._-]{1,64}$/),
    type: z.literal("shell"),
    command: z.string().min(1),
    cwd: z.string().optional(),
    timeoutMs: z.number().int().positive().optional(),
    success: z
      .object({
        exitCode: z.number().int().optional(),
        stdoutContains: z.string().optional(),
        stderrContains: z.string().optional(),
        fileExists: z.string().optional(),
      })
      .partial()
      .optional(),
    retries: z.number().int().min(0).optional(),
    retryBackoffMs: z.number().int().min(0).optional(),
    fallbacks: z
      .array(z.object({ id: z.string(), type: z.literal("shell"), command: z.string() }))
      .optional(),
    optional: z.boolean().optional(),
    artifacts: z.array(z.string()).optional(),
  }),
  z.object({
    id: z.string(),
    type: z.literal("ask"),
    question: z.string(),
    context: z.record(z.unknown()).optional(),
  }),
  z.object({
    id: z.string(),
    type: z.literal("artifact"),
    paths: z.array(z.string()),
    cwd: z.string().optional(),
  }),
  z.object({
    id: z.string(),
    type: z.literal("wait_file"),
    path: z.string(),
    timeoutMs: z.number().int().positive().optional(),
  }),
]);
export type Step = z.infer<typeof StepSchema>;

export const GoalObjectSchema = z
  .object({
    protocol: z.literal(PROTOCOL_VERSION),
    goalId: z.string().min(1).optional(), // filled per-device by dispatch
    deviceId: z.string().min(1).optional(),
    objective: z.string().min(1).max(LIMITS.objectiveMax),
    workspace: z
      .string()
      .refine(isAbsPath, "workspace must be an absolute path")
      .optional(),
    agent: z.enum(["builtin", "claude"]).default("builtin"),
    // claude backend rejects task-programs (whole-goal delegation)
    ensureWorkspace: z
      .object({
        repoUrl: z.string().min(1),
        path: z.string().refine(isAbsPath),
        branch: z.string().optional(),
      })
      .optional(),
    constraints: z
      .object({
        maxSteps: z.number().int().positive().optional(),
        timeoutMs: z.number().int().positive().optional(),
        stepTimeoutMs: z.number().int().positive().optional(),
        eventSilenceMs: z.number().int().positive().optional(),
        forbidden: z.array(z.string()).optional(),
        claudeAutoApprove: z.boolean().optional(),
      })
      .optional(),
    plan: z
      .object({
        planner: z.literal("main-agent").default("main-agent"),
        steps: z.array(StepSchema).min(1),
      })
      .optional(),
    reporting: z.object({ finalArtifacts: z.boolean().optional() }).optional(),
  });
export type GoalInput = z.input<typeof GoalObjectSchema>;

/** superRefine 校验（claude+plan 互斥）独立为函数，便于 extend 后复用 */
export function refineClaudePlan(
  g: z.infer<typeof GoalObjectSchema>,
  ctx: z.RefinementCtx,
): void {
  if (g.agent === "claude" && g.plan) {
    ctx.addIssue({
      code: z.ZodIssueCode.custom,
      message: "goal.agent='claude' executes the whole objective; drop plan",
    });
  }
}

export const GoalSchema = GoalObjectSchema.superRefine(refineClaudePlan);
export type Goal = z.infer<typeof GoalSchema>;

/** Full goal (post-dispatch: goalId/deviceId assigned). */
export const DispatchedGoalSchema = GoalObjectSchema.extend({
  goalId: z.string().min(1),
  deviceId: z.string().min(1),
}).superRefine(refineClaudePlan);
export type DispatchedGoal = z.infer<typeof DispatchedGoalSchema>;

export function isAbsPath(p: string): boolean {
  return p.startsWith("/") || /^[A-Za-z]:[/\\]/.test(p);
}


/* ------------------------------------------------------------------ *
 * Events (RDA -> orchestrator)
 * ------------------------------------------------------------------ */
export const EventKind = z.enum([
  "started",
  "recon",
  "ask",
  "progress",
  "milestone",
  "step_result",
  "warning",
  "error",
  "result",
]);
export const EventSchema = z.object({
  protocol: z.literal(PROTOCOL_VERSION),
  kind: EventKind,
  goalId: z.string(),
  ts: z.number().int(),
  data: z.record(z.unknown()),
});
export type RaoEvent = z.infer<typeof EventSchema>;

/** Validate an event dict coming from RDA/daemon. Returns [ok, error]. */
export function validateEvent(ev: unknown): [boolean, string | null] {
  const r = EventSchema.safeParse(ev);
  return r.success
    ? [true, null]
    : [false, r.error.issues.map((i) => `${i.path.join(".")}: ${i.message}`).join("; ")];
}

/* ------------------------------------------------------------------ *
 * Run states
 * ------------------------------------------------------------------ */
export const RunState = z.enum([
  "queued",
  "running",
  "ask_waiting",
  "done",
  "failed",
  "timeout",
  "cancelled",
]);
export type RunStateValue = z.infer<typeof RunState>;
export const TERMINAL_STATES: readonly RunStateValue[] = [
  "done",
  "failed",
  "timeout",
  "cancelled",
];
export const isTerminalState = (s: RunStateValue): boolean =>
  TERMINAL_STATES.includes(s);
