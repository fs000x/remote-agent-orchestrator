#!/usr/bin/env node
/**
 * Real-device verification (P3-c): run the T-series against a configured
 * device using the BUILT TypeScript stack (dist/).
 *
 *   node scripts/device-verify.mjs [--device diablo-100] [--skip-claude]
 *
 * T1 provision (runtime upload + single-instance daemon restart)
 * T2 daemon-mode echo goal → collect(success)
 * T3 steer skip directive mid-goal
 * T4 cancel mid-goal
 * T5 claude backend whole-goal delegation (opt-out with --skip-claude)
 * T6 report persistence via collect
 */
import { argv, exit, stdout } from "node:process";
import { rmSync } from "node:fs";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DeviceRegistry } from "../dist/devices.js";
import { Orchestrator } from "../dist/orchestrator.js";
import { SshDaemonTransport } from "../dist/transports/sshDaemon.js";
import { sshExec } from "../dist/transports/ssh2util.js";

const args = argv.slice(2);
const flag = (name) => {
  const i = args.indexOf(name);
  return i >= 0 ? args[i + 1] : undefined;
};
const deviceId = flag("--device") ?? "diablo-100";
const skipClaude = args.includes("--skip-claude");
const dir = mkdtempSync(join(tmpdir(), "rao-verify-"));

function out(msg) {
  stdout.write(`${msg}\n`);
}

function banner(t, msg) {
  out(`\n=== ${t} ${msg ?? ""} ===`);
}

function die(msg) {
  out(`FATAL: ${msg}`);
  rmSync(dir, { recursive: true, force: true });
  exit(1);
}

const registry = new DeviceRegistry();
await registry.reload();
const device = registry.get(deviceId)
  ?? die(`no device '${deviceId}' in config/devices.json`);
const transport = new SshDaemonTransport(
  registry.settings.rdaDir ?? "$HOME/.rao/runtime");

const orch = new Orchestrator({
  registry, transport,
  runsDir: join(dir, "runs"),
  maxConcurrentGoals: 3,
});

function mkGoal(objective, extra = {}) {
  return {
    protocol: "rao/1",
    objective,
    agent: "builtin",
    constraints: { timeoutMs: 60000, ...(extra.constraints ?? {}) },
    plan: {
      planner: "main-agent",
      steps: extra.steps ?? [{
        id: "s1", type: "shell", command: "echo verify-ok",
        success: { exitCode: 0, stdoutContains: "verify-ok" },
      }],
    },
  };
}

async function waitRun(goalId, timeoutMs) {
  const run = orch.getRun(goalId);
  const ok = await run.waitTerminal(timeoutMs);
  if (!ok) die(`goal ${goalId} did not finish (state=${run.state})`);
  return run;
}

/* T1: provision */
banner("T1", "provision (upload runtime + restart daemon)");
try {
  const r = await transport.provision(device);
  out(`provision: deployed=${r.deployed} uploaded=${JSON.stringify(r.uploaded ?? [])}`);
} catch (err) {
  die(`provision failed: ${err.message}`);
}
{
  const { code, stdout: ping } = await sshExec(
    device,
    `cd ${device.rdaDir} && node rda_daemon.mjs ping`, 15000);
  if (code !== 0) die(`daemon not alive after provision: ${ping}`);
  out(`daemon ping: ${ping.trim()}`);
}

/* T2: echo goal through the SSH daemon bridge */
banner("T2", "daemon-mode echo goal");
{
  const [d] = orch.dispatch([deviceId], mkGoal("verify: echo goal"));
  const run = await waitRun(d.goalId, 90000);
  out(`state=${run.state} status=${run.result?.status} summary=${run.result?.summary}`);
  if (run.result?.status !== "success") die("T2 failed");
  out(`events seen: ${run.chan?.events.length ?? 0} (seqs: ${JSON.stringify(run.chan?.event_seqs ?? [])})`);
}

/* T3: steer skip directive */
banner("T3", "steer skip directive");
{
  const [d] = orch.dispatch([deviceId], mkGoal("verify: steer skip", {
    constraints: { timeoutMs: 60000, maxSteps: 10 },
    steps: [
      { id: "s1", type: "shell", command: "sleep 3 && echo first-ok", success: { exitCode: 0, stdoutContains: "first-ok" } },
      { id: "w1", type: "wait_file", path: "/tmp/rao-verify-never", timeoutMs: 20000 },
    ],
  }));
  const run = orch.getRun(d.goalId);
  // wait until the first step is running, then inject the skip
  const t0 = Date.now();
  while (run.attemptCount < 1 && Date.now() - t0 < 20000) {
    await new Promise((r) => setTimeout(r, 300));
  }
  const steered = await orch.steerGoal(d.goalId, "skip:w1");
  out(`steer accepted=${steered}`);
  await run.waitTerminal(60000);
  out(`state=${run.state} status=${run.result?.status}`);
  out(`steerNotes=${JSON.stringify(run.result?.steerNotes ?? [])}`);
  if (run.result?.status !== "partial") die("T3 failed (expected partial)");
}

/* T4: cancel mid-goal */
banner("T4", "cancel mid-goal");
{
  const [d] = orch.dispatch([deviceId], mkGoal("verify: cancel", {
    steps: [{ id: "w1", type: "wait_file", path: "/tmp/rao-verify-never", timeoutMs: 30000 }],
  }));
  const run = orch.getRun(d.goalId);
  await new Promise((r) => setTimeout(r, 2500)); // let the engine start
  const cancelled = await orch.cancelGoal(d.goalId, "verify cancel");
  out(`cancel accepted=${cancelled}`);
  await run.waitTerminal(30000);
  out(`state=${run.state} status=${run.result?.status}`);
  if (run.state !== "cancelled") die("T4 failed");
}

/* T5: claude backend */
if (!skipClaude) {
  banner("T5", "claude backend (whole-goal delegation)");
  const [d] = orch.dispatch([deviceId], {
    protocol: "rao/1",
    objective: "用一句话回答：你在哪台机器上？运行 hostname 命令验证并把主机名写进回答。",
    agent: "claude",
    constraints: { timeoutMs: 300000, claudeAutoApprove: true },
  });
  const run = orch.getRun(d.goalId);
  const t0 = Date.now();
  while (Date.now() - t0 < 300000) {
    await run.waitTerminal(10000);
    if (run.isTerminal()) break;
    const toolEvts = (run.chan?.events ?? []).filter(
      (e) => e.kind === "progress" && e.data.phase === "tool_use");
    out(`...running (${Math.round((Date.now() - t0) / 1000)}s, tool_uses=${toolEvts.length})`);
  }
  await run.waitTerminal(5000);
  out(`state=${run.state} status=${run.result?.status}`);
  out(`summary=${run.result?.summary}`);
  if (run.result?.status !== "success") die("T5 failed");
} else {
  banner("T5", "SKIPPED (--skip-claude)");
}

/* T6: collect + report persistence */
banner("T6", "collect + report persistence");
{
  const [d] = orch.dispatch([deviceId], mkGoal("verify: report"));
  const report = await orch.collect([d.goalId], 90000);
  out(`successCount=${report.successCount} failCount=${report.failCount}`);
  out(`reportFile=${report.reportFile}`);
  if (report.successCount !== 1) die("T6 failed");
}

banner("ALL", "real-device verification PASSED");
rmSync(dir, { recursive: true, force: true });
exit(0);
