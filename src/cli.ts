#!/usr/bin/env node
/**
 * rao CLI: setup / devices / doctor / uninstall / run / mcp (v0.5 port).
 */
import { createInterface } from "node:readline";
import { existsSync, readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { dirname } from "node:path";
import { pathToFileURL } from "node:url";
import { DeviceRegistry } from "./devices.js";
import { Orchestrator } from "./orchestrator.js";
import { SshTransport } from "./transports/ssh.js";
import {
  detectAgents, injectAgent, uninstallAgent, SUPPORTED_AGENTS,
} from "./agents.js";
import { remoteHashes } from "./provision.js";
import { startMcp } from "./mcp/server.js";
import { RAO_VERSION } from "./version.js";
import { Command } from "commander";

function runsDir(): string {
  const reg = new DeviceRegistry();
  return reg.settings.runsDir ?? "runs";
}

function defaultRdaDir(): string {
  const reg = new DeviceRegistry();
  return reg.settings.rdaDir ?? "$HOME/.rao/runtime";
}

/* ------------------------------------------------------------------ *
 * devices
 * ------------------------------------------------------------------ */
export async function cmdDevicesList(configPath?: string): Promise<number> {
  const reg = new DeviceRegistry(configPath);
  await reg.reload();
  for (const d of await reg.describeAll(true)) {
    const authConfigured = Boolean(d.auth?.password) ? "yes" : "no";
    process.stdout.write(
      `${d.id.padEnd(16)} ${(d.host ?? "-").padEnd(15)}:${String(d.port ?? "-").padEnd(5)} ` +
      `user=${(d.user ?? "-").padEnd(10)} ws=${d.workspace ?? "-"} ` +
      `online=${d.online ? "yes" : "no"} creds=${authConfigured}\n`,
    );
  }
  return 0;
}

function ask(question: string, defaultValue?: string): Promise<string> {
  return new Promise((resolve) => {
    const rl = createInterface({ input: process.stdin, terminal: false });
    rl.question(
      `${question}${defaultValue ? ` [${defaultValue}]` : ""}: `,
      (v) => {
        rl.close();
        resolve(v.trim() || defaultValue || "");
      },
    );
  });
}

export async function cmdDevicesAdd(opts: {
  id?: string; host?: string; port?: string; user?: string;
  password?: string; workspace?: string; yes?: boolean;
  configPath?: string;
}): Promise<number> {
  const reg = new DeviceRegistry(opts.configPath);
  await reg.reload();
  const id = opts.id ?? (await ask("设备 id (如 my-linux-box)"));
  const host = opts.host ?? (await ask("主机地址"));
  const port = Number(opts.port ?? (await ask("端口", "22")));
  const user = opts.user ?? (await ask("SSH 用户"));
  const password = opts.password ?? (await ask("密码"));
  const workspace = opts.workspace ?? (await ask("workspace 路径(远端绝对路径,可空)"));
  const device = {
    id, host, port, user,
    workspace: workspace || undefined,
    maxConcurrentGoals: 2,
    auth: { type: "password" as const, password },
  };
  const path = reg.path;
  let data: { settings?: unknown; devices?: Array<Record<string, unknown>> } = { devices: [] };
  if (existsSync(path)) {
    data = JSON.parse(readFileSync(path, "utf8"));
  }
  data.devices = (data.devices ?? []).filter((d) => d.id !== id);
  data.devices.push(device);
  mkdirSync(dirname(path) || ".", { recursive: true });
  writeFileSync(path, JSON.stringify(data, null, 2));
  process.stdout.write(`device ${id} written to ${path}\n`);
  return 0;
}

export async function cmdDevicesRemove(id: string, configPath?: string): Promise<number> {
  const reg = new DeviceRegistry(configPath);
  const path = reg.path;
  const data = JSON.parse(readFileSync(path, "utf8")) as { devices?: Array<Record<string, unknown>> };
  const before = (data.devices ?? []).length;
  data.devices = (data.devices ?? []).filter((d) => d.id !== id);
  writeFileSync(path, JSON.stringify(data, null, 2));
  process.stdout.write(`removed ${before - (data.devices ?? []).length} device(s)\n`);
  return 0;
}

/* ------------------------------------------------------------------ *
 * setup / uninstall
 * ------------------------------------------------------------------ */
async function cmdSetup(agents: string, yes: boolean): Promise<number> {
  const wanted = agents.split(",").map((a) => a.trim()).filter(Boolean);
  const detected = detectAgents();
  process.stdout.write("=== Agent 检测 ===\n");
  for (const d of detected) {
    const state = d.injected ? "已注入" : "未注入";
    process.stdout.write(
      `  ${d.name.padEnd(8)} installed=${d.installed} cli=${d.cliPath ?? "-"} [${state}]\n`,
    );
  }
  const targets = wanted.length
    ? wanted
    : detected.filter((d) => d.installed && d.name !== "codex").map((d) => d.name);
  if (!yes) {
    const ans = await ask(`将注入 MCP+Skill 到: ${targets.join(",")}，继续? [Y/n]`);
    if (ans.trim().toLowerCase() === "n") {
      process.stdout.write("aborted\n");
      return 1;
    }
  }
  let rc = 0;
  for (const name of targets) {
    try {
      const r = injectAgent(name);
      process.stdout.write(`=== 注入 ${name} ===\n`);
      for (const a of r.actions) process.stdout.write(`  - ${a}\n`);
    } catch (err) {
      process.stdout.write(`=== 注入 ${name} 失败: ${String(err)} ===\n`);
      rc = 1;
    }
  }
  return rc;
}

function cmdUninstall(agents: string): number {
  const names = agents.split(",").map((a) => a.trim()).filter(Boolean);
  for (const name of names.length ? names : [...SUPPORTED_AGENTS]) {
    const r = uninstallAgent(name);
    process.stdout.write(`=== 回滚 ${name} ===\n`);
    for (const a of r.actions) process.stdout.write(`  - ${a}\n`);
  }
  return 0;
}

/* ------------------------------------------------------------------ *
 * doctor
 * ------------------------------------------------------------------ */
async function cmdDoctor(): Promise<number> {
  process.stdout.write("=== 本地 Agent ===\n");
  for (const d of detectAgents()) {
    process.stdout.write(
      `  ${d.name.padEnd(8)} installed=${d.installed} injected=${d.injected ? "yes" : "no"}\n`,
    );
  }
  process.stdout.write("=== 设备 ===\n");
  const reg = new DeviceRegistry();
  await reg.reload();
  const rdaDir = defaultRdaDir();
  for (const d of await reg.describeAll(true)) {
    const authConfigured = Boolean(d.auth?.password);
    process.stdout.write(
      `  ${d.id.padEnd(16)} online=${d.online ? "yes" : "no"} auth=${authConfigured ? "yes" : "no"}\n`,
    );
    if (!d.online || !d.host) continue;
    try {
      const device = reg.require(d.id);
      const rh = await remoteHashes(device, device.rdaDir || rdaDir);
      const ok = Object.values(rh).every((v) => Boolean(v));
      process.stdout.write(
        `    RDA 运行时: ${ok ? "就绪" : "缺失/过期（派发时自动补部署）"}\n`,
      );
      const { sshExec } = await import("./transports/ssh2util.js");
      const { stdout } = await sshExec(
        device,
        "which claude 2>/dev/null; which node 2>/dev/null; " +
        "test -f ~/.zcode/server/run/status.json && echo zcode-server",
      );
      const marks = stdout.trim().split("\n").map((l) => l.trim()).filter(Boolean);
      process.stdout.write(`    远端 Agent: ${marks.length ? marks.join(", ") : "claude=✗ node=✗ zcode-server=✗"}\n`);
    } catch (err) {
      process.stdout.write(`    远端检查失败: ${String(err)}\n`);
    }
  }
  return 0;
}

/* ------------------------------------------------------------------ *
 * run (one-shot remote execution without MCP)
 * ------------------------------------------------------------------ */
async function cmdRun(device: string, command: string, timeoutMs: number): Promise<number> {
  const reg = new DeviceRegistry();
  await reg.reload();
  const dev = reg.require(device);
  const orch = new Orchestrator({
    registry: reg,
    transport: new SshTransport(dev.rdaDir || defaultRdaDir()),
    runsDir: runsDir(),
  });
  const goal = {
    protocol: "rao/1",
    objective: `rao run: ${command}`,
    agent: "builtin",
    workspace: dev.workspace,
    constraints: { timeoutMs, stepTimeoutMs: timeoutMs },
    plan: {
      planner: "main-agent",
      steps: [{
        id: "s1", type: "shell", command,
        success: { exitCode: 0 },
      }],
    },
  };
  const [d] = orch.dispatch([device], goal);
  const run = orch.getRun(d!.goalId)!;
  const finished = await run.waitTerminal(timeoutMs + 30_000);
  if (!finished) {
    process.stdout.write(`[rao] goal did not finish in time; state=${run.state}\n`);
    return 1;
  }
  const res = (run.result ?? {}) as { status?: string; summary?: string };
  process.stdout.write(`[rao] status: ${res.status}\n`);
  if (res.summary) process.stdout.write(`[rao] summary: ${res.summary}\n`);
  for (const ev of run.chan?.events ?? []) {
    if (ev.kind === "step_result" && (ev.data as Record<string, unknown>).stdoutTail) {
      process.stdout.write(`[out] ${String((ev.data as Record<string, unknown>).stdoutTail).trim()}\n`);
    }
  }
  return res.status === "success" ? 0 : 1;
}

/* ------------------------------------------------------------------ *
 * main
 * ------------------------------------------------------------------ */
function parseNum(v: string): number {
  return Number(v);
}

export async function main(argv = process.argv.slice(2)): Promise<number> {
  const program = new Command();
  program
    .name("rao")
    .description("Remote Agent Orchestrator — local agent controls semi-autonomous agents on remote devices")
    .version(`rao ${RAO_VERSION}`);

  program
    .command("setup")
    .description("检测 Agent 并注入 MCP+Skill")
    .option("--agents <list>", "逗号分隔: zcode,claude（默认自动）")
    .option("--yes", "免确认", false)
    .action(async (opts: { agents?: string; yes: boolean }) => {
      process.exitCode = await cmdSetup(opts.agents ?? "", opts.yes);
    });

  const dev = program
    .command("devices")
    .description("远程设备配置管理");
  dev
    .command("list")
    .action(async () => {
      process.exitCode = await cmdDevicesList();
    });
  dev
    .command("add")
    .option("--id <id>").option("--host <host>").option("--port <port>")
    .option("--user <user>").option("--password <pw>")
    .option("--workspace <ws>").option("--yes", "免确认", false)
    .action(async (opts) => {
      process.exitCode = await cmdDevicesAdd(opts);
    });
  dev
    .command("remove")
    .argument("<id>")
    .action(async (id: string) => {
      process.exitCode = await cmdDevicesRemove(id);
    });

  program
    .command("doctor")
    .description("诊断：Agent/设备/远端能力")
    .action(async () => {
      process.exitCode = await cmdDoctor();
    });

  program
    .command("uninstall")
    .description("回滚注入")
    .option("--agents <list>", "逗号分隔（默认全部）")
    .action((opts: { agents?: string }) => {
      process.exitCode = cmdUninstall(opts.agents ?? "");
    });

  program
    .command("run")
    .description("一条命令在远程设备上执行并等待结果")
    .requiredOption("--device <id>")
    .option("--timeout-ms <n>", "整目标超时", parseNum, 300000)
    .argument("<command>", "要在远端执行的 shell 命令")
    .action(async (command: string, opts: { device: string; timeoutMs: number }) => {
      process.exitCode = await cmdRun(opts.device, command, opts.timeoutMs);
    });

  program
    .command("mcp")
    .description("启动 MCP server（stdio）")
    .action(async () => {
      await startMcp();
      // stdio server runs until the client disconnects
      await new Promise(() => { /* never */ });
    });

  await program.parseAsync(argv, { from: "user" });
  return typeof process.exitCode === "number" ? process.exitCode : 0;
}

if (process.argv[1] && import.meta.url ===
    pathToFileURL(process.argv[1]).href) {
  main().then((code) => { process.exitCode = code; }).catch((err) => {
    console.error(err);
    process.exitCode = 1;
  });
}
