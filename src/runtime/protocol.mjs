/**
 * RAO Wire Protocol v1 — shared schema/validation for the runtime deployed
 * to remote devices. Node >= 18, ZERO dependencies (verbatim deployment;
 * must never import from the TypeScript sources).
 *
 * Ported from src/rao/runtime/protocol.py (v0.11.1) with identical behavior.
 */

export const PROTOCOL_VERSION = "rao/1";

/* --- event kinds (RDA -> orchestrator) --- */
export const KIND_STARTED = "started";
export const KIND_RECON = "recon";
export const KIND_ASK = "ask";
export const KIND_PROGRESS = "progress";
export const KIND_MILESTONE = "milestone";
export const KIND_STEP_RESULT = "step_result";
export const KIND_WARNING = "warning";
export const KIND_ERROR = "error";
export const KIND_RESULT = "result";
export const KNOWN_KINDS = [
  KIND_STARTED, KIND_RECON, KIND_ASK, KIND_PROGRESS, KIND_MILESTONE,
  KIND_STEP_RESULT, KIND_WARNING, KIND_ERROR, KIND_RESULT,
];

/* --- result status --- */
export const ST_SUCCESS = "success";
export const ST_PARTIAL = "partial";
export const ST_FAILED = "failed";
export const ST_TIMEOUT = "timeout";
export const ST_CANCELLED = "cancelled";
export const KNOWN_STATUS = [ST_SUCCESS, ST_PARTIAL, ST_FAILED, ST_TIMEOUT, ST_CANCELLED];

/* --- goal run states (orchestrator side) --- */
export const RUN_QUEUED = "queued";
export const RUN_RUNNING = "running";
export const RUN_ASK_WAITING = "ask_waiting";
export const RUN_DONE = "done";
export const RUN_FAILED = "failed";
export const RUN_TIMEOUT = "timeout";
export const RUN_CANCELLED = "cancelled";

export const STEP_TYPES = ["shell", "ask", "artifact", "wait_file"];

export const DEFAULTS = {
  maxSteps: 40,
  timeoutMs: 600000,
  stepTimeoutMs: 120000,
  retryBackoffMs: 2000,
  eventSilenceMs: 90000,
};

const ID_RE = /^[A-Za-z0-9._-]{1,64}$/;
const WIN_ABS_RE = /^[A-Za-z]:[/\\]/;

export function newGoalId() {
  return crypto.randomUUID();
}

export function nowMs() {
  return Date.now();
}

export function makeEvent(kind, goalId, data, extra = {}) {
  return { protocol: PROTOCOL_VERSION, kind, goalId, ts: nowMs(), data, ...extra };
}

function checkStep(step, idx, seenIds) {
  if (typeof step !== "object" || step === null || Array.isArray(step)) {
    return `plan.steps[${idx}] must be an object`;
  }
  const sid = step.id;
  if (typeof sid !== "string" || !ID_RE.test(sid)) {
    return `plan.steps[${idx}].id invalid`;
  }
  if (seenIds.has(sid)) {
    return `plan.steps[${idx}].id duplicated: ${sid}`;
  }
  seenIds.add(sid);
  if (!STEP_TYPES.includes(step.type)) {
    return `plan.steps[${idx}].type must be one of ${STEP_TYPES.join(",")}`;
  }
  if (step.type === "shell" && typeof step.command !== "string") {
    return `plan.steps[${idx}] shell requires command`;
  }
  if (step.type === "ask" && typeof step.question !== "string") {
    return `plan.steps[${idx}] ask requires question`;
  }
  if (step.type === "wait_file") {
    const p = step.path;
    if (typeof p !== "string" || !p) {
      return `plan.steps[${idx}] wait_file requires path`;
    }
  }
  if (step.type === "artifact" && !Array.isArray(step.paths)) {
    return `plan.steps[${idx}] artifact requires paths list`;
  }
  const success = step.success ?? { exitCode: 0 };
  if (typeof success !== "object" || success === null || Array.isArray(success)) {
    return `plan.steps[${idx}].success must be object`;
  }
  for (const k of Object.keys(success)) {
    if (!["exitCode", "stdoutContains", "stderrContains", "fileExists"].includes(k)) {
      return `plan.steps[${idx}].success unknown key: ${k}`;
    }
  }
  for (const intK of ["retries", "retryBackoffMs", "timeoutMs"]) {
    if (intK in step && !Number.isInteger(step[intK])) {
      return `plan.steps[${idx}].${intK} must be int`;
    }
  }
  const fbs = step.fallbacks ?? [];
  if (!Array.isArray(fbs)) {
    return `plan.steps[${idx}].fallbacks must be list`;
  }
  for (let j = 0; j < fbs.length; j++) {
    const fb = fbs[j];
    if (typeof fb !== "object" || fb === null || !STEP_TYPES.includes(fb.type)) {
      return `plan.steps[${idx}].fallbacks[${j}] invalid`;
    }
    if (fb.type === "shell" && typeof fb.command !== "string") {
      return `plan.steps[${idx}].fallbacks[${j}] shell requires command`;
    }
  }
  return null;
}

export function isAbsPath(p) {
  // POSIX absolute OR Windows drive-absolute (protocol is cross-platform;
  // local Windows RDA runs are legitimate test targets).
  return typeof p === "string" && (p.startsWith("/") || WIN_ABS_RE.test(p));
}

export function validateGoal(goal) {
  if (typeof goal !== "object" || goal === null || Array.isArray(goal)) {
    return [false, "goal must be an object"];
  }
  if (goal.protocol !== PROTOCOL_VERSION) {
    return [false, `protocol must be ${PROTOCOL_VERSION}`];
  }
  if (typeof goal.goalId !== "string" || !goal.goalId) {
    return [false, "goalId required"];
  }
  if (typeof goal.deviceId !== "string" || !goal.deviceId) {
    return [false, "deviceId required"];
  }
  if (typeof goal.objective !== "string" || !goal.objective.trim()) {
    return [false, "objective required"];
  }
  const ws = goal.workspace;
  if (ws !== undefined && ws !== null && !isAbsPath(ws)) {
    return [false, "workspace must be absolute path when present"];
  }
  const c = goal.constraints ?? {};
  if (typeof c !== "object" || c === null || Array.isArray(c)) {
    return [false, "constraints must be object"];
  }
  for (const k of Object.keys(DEFAULTS)) {
    if (k in c && !Number.isInteger(c[k])) {
      return [false, `constraints.${k} must be int`];
    }
  }
  for (const f of c.forbidden ?? []) {
    if (typeof f !== "string") {
      return [false, "constraints.forbidden entries must be strings"];
    }
  }
  const planner = (goal.plan ?? {}).planner ?? "main-agent";
  if (planner !== "main-agent") {
    return [false, "plan.planner must be 'main-agent' (steered plans)"];
  }
  const gp = goal.planner ?? "main-agent";
  if (!["main-agent", "remote-llm"].includes(gp)) {
    return [false, "goal.planner must be 'main-agent' or 'remote-llm'"];
  }
  const backend = goal.agent ?? "builtin";
  if (!["builtin", "claude"].includes(backend)) {
    return [false, "goal.agent must be 'builtin' or 'claude' (v0.6)"];
  }
  if (backend === "claude" && "plan" in goal && goal.plan != null) {
    return [false, "goal.agent='claude' executes the whole objective; drop plan"];
  }
  const ews = goal.ensureWorkspace;
  if (ews != null) {
    if (typeof ews !== "object" || Array.isArray(ews)) {
      return [false, "ensureWorkspace must be an object"];
    }
    if (typeof ews.repoUrl !== "string" || !ews.repoUrl) {
      return [false, "ensureWorkspace.repoUrl required"];
    }
    if (!isAbsPath(ews.path)) {
      return [false, "ensureWorkspace.path must be absolute"];
    }
    if ("branch" in ews && typeof ews.branch !== "string") {
      return [false, "ensureWorkspace.branch must be a string"];
    }
  }
  if ("plan" in goal && goal.plan != null) {
    const steps = goal.plan.steps;
    if (!Array.isArray(steps) || steps.length === 0) {
      return [false, "plan.steps must be a non-empty list"];
    }
    const seen = new Set();
    for (let i = 0; i < steps.length; i++) {
      const err = checkStep(steps[i], i, seen);
      if (err) return [false, err];
    }
  }
  return [true, null];
}

export function validateEvent(ev) {
  if (typeof ev !== "object" || ev === null || Array.isArray(ev)) {
    return [false, "event must be object"];
  }
  if (ev.protocol !== PROTOCOL_VERSION) {
    return [false, "event protocol mismatch"];
  }
  if (!KNOWN_KINDS.includes(ev.kind)) {
    return [false, `unknown event kind: ${JSON.stringify(ev.kind)}`];
  }
  if (typeof ev.data !== "object" || ev.data === null || Array.isArray(ev.data)) {
    return [false, "event.data must be object"];
  }
  return [true, null];
}

export function commandViolatesForbidden(command, forbidden) {
  // soft constraint check (documented as non-sandbox)
  if (!command) return null;
  for (const pat of forbidden ?? []) {
    if (pat && command.includes(pat)) return pat;
  }
  return null;
}

export function stepEffectiveTimeout(step, goalConstraints) {
  return Number(step.timeoutMs) || Number(goalConstraints.stepTimeoutMs) || DEFAULTS.stepTimeoutMs;
}
