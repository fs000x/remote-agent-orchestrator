# Remote Agent Orchestrator (RAO) 设计文档

- 版本: v1.0
- 日期: 2026-11-19
- 状态: 已定稿，进入实现
- 载体: ZCode MCP plugin（非源码扩展）

---

## 1. 目标与非目标

### 1.1 目标

让 ZCode 本地主 agent 在执行任务时，能**并行调度多台远程设备上的半自主 agent（RDA）**，
派发目标（goal）、流式回收事件、中途纠偏（steer）、聚合结果并产出跨设备报告。

### 1.2 非目标（roadmap，见 §11）

- R3 源码 PR 路线（session-control 上远端链路）
- RDA 常驻 daemon 化
- 设备间 DAG 依赖编排
- 回归对比 / 测试计划版本管理
- 远端独立 LLM 规划器（协议预留插槽）

---

## 2. 设计基线（与用户确认的决策）

| 决策点 | 结论 | 依据 |
|---|---|---|
| 载体 | ZCode MCP plugin，不改 ZCode 源码 | 用户确认"混合路线"；服务集合是内部 RPC，注入必须改源码，故桥接层后置为 PR |
| 远端 agent 自主性 | **半自主** | 用户选择；远端自适应执行 + 升级提问，规划由主 agent 承担（见 §3.3） |
| 多设备执行 | **并行** | 用户选择；wait-all + 允许部分成功 |
| 场景范围 | 只做 Agent↔Agent，无被测 APP / adb | 用户明确 |
| 网络 | 局域网直连 SSH | 现有设备 192.168.0.100 |
| workspace | 预置（a 方案）起步 | 用户采纳；`ensureWorkspace` 留 roadmap |
| 技术栈 | Python 3.10+（MCP Server）+ Python 3.8（RDA） | 本机 Python 3.13 + paramiko 已实测打通；ZCode 生态 TS 备选记入 roadmap |

---

## 3. 总体架构

### 3.1 组件图

```
ZCode 主 agent（LLM，唯一规划者）
  │ MCP 工具调用
  ▼
┌─────────────────────────────────────────────────┐
│ rao-mcp (本地, Python/FastMCP)                  │
│  ┌──────────────┐   ┌─────────────────────────┐ │
│  │ 6 Tools      │──▶│ Orchestrator            │ │
│  └──────────────┘   │  DispatchPool (并发池)   │ │
│                     │  EventMux   (事件聚合)   │ │
│                     │  RunStore    (持久化)    │ │
│                     └───────────┬─────────────┘ │
│                                 │               │
│                     ┌───────────▼─────────────┐ │
│                     │ SshTransport (paramiko) │ │
│                     └───────────┬─────────────┘ │
└─────────────────────────────────┼───────────────┘
                                  │ SSH exec (stdin/stdout, JSON Lines)
                    ┌─────────────▼──────────────┐
                    │ RDA (远端, Python 3.8)      │
                    │  GoalEngine:                │
                    │   recon → plan-req → steps  │
                    │   retry/fallback → events   │
                    │   ask/steer → result        │
                    └────────────────────────────┘
```

### 3.2 职责划分

| 组件 | 职责 | 不负责 |
|---|---|---|
| 主 agent (ZCode) | 理解意图、写 goal/plan、响应 ask、聚合报告语义 | 进程管理、传输细节 |
| rao-mcp (MCP Server) | 工具面、并发调度、事件缓冲、预算强制、运行记录 | 语义理解 |
| SshTransport | 连接、认证、进程生命周期、stdin 注入、stdout 流读 | 协议语义 |
| RDA (远端) | goal 执行引擎：步骤执行、判据、重试/回退、recon、事件、升级 | 自主规划（v1） |

### 3.3 半自主的定义（v1 决策）

远端设备没有独立 LLM（本机已核实：无 headless agent CLI，ZCode server 无 headless 会话入口）。
因此 v1 的半自主 = **自适应执行 + 结构化升级**：

1. **自适应执行**：RDA 按任务程序（task program）自主执行；步骤失败时按策略重试、
   尝试声明的回退变体、跳过可容错步骤——全程不需主 agent 参与。
2. **结构化升级**：RDA 遇到程序外情况（recon 后无计划、重试耗尽、显式 `ask` 步骤）
   发 `ask` 事件并暂停，等待主 agent 经 steer 通道回应。
3. **规划归属**：v1 由主 agent 在派发前写任务程序（可先派 recon goal 收集设备事实再写）。
   协议在 goal 中预留 `planner` 字段（v1 仅支持 `"main-agent"`），
   未来接远端 LLM CLI 时改为 `"remote-llm"`，RDA 收到无 plan 的 goal 时可自主规划。

> 设计原则：不因"半自主"引入远端 API key 依赖；主 agent 是系统唯一的 LLM。

---

## 4. 协议（RAO Wire Protocol v1）

所有消息为单行 JSON（JSON Lines）。方向：主→远（stdin 注入），远→主（stdout 行）。

### 4.1 Goal（主 → RDA，dispatch 时一次性写入 stdin）

```jsonc
{
  "protocol": "rao/1",
  "goalId": "uuid4",
  "deviceId": "diablo-100",
  "objective": "把仓库构建跑通并执行 smoke 测试",
  "workspace": "/home/diablo/git500/rk3399",
  "constraints": {
    "maxSteps": 40,            // 步骤预算（含重试）
    "timeoutMs": 600000,       // 整体超时
    "stepTimeoutMs": 120000,   // 单步默认超时
    "forbidden": ["git push", "rm -rf /", "sudo reboot"],
    "maxConcurrent": 1
  },
  "plan": {                    // 可省略 → 触发 recon + ask 规划回路
    "planner": "main-agent",
    "steps": [
      {
        "id": "s1",
        "type": "shell",                    // shell | ask | artifact | wait_file
        "command": "make -j2",
        "cwd": "/home/diablo/git500/rk3399",
        "timeoutMs": 120000,
        "success": { "exitCode": 0 },       // 判据
        "retries": 1,                       // 失败重试次数
        "retryBackoffMs": 2000,
        "fallbacks": [                      // 重试耗尽后的回退变体（可选）
          { "id": "s1f", "type": "shell", "command": "make -j1", "success": {"exitCode": 0} }
        ],
        "optional": false,                  // true: 失败跳过不致败
        "artifacts": ["build.log"]          // 步骤产物（相对 cwd）
      }
    ]
  },
  "reporting": { "events": ["milestone", "warning", "error", "ask"], "finalArtifacts": true }
}
```

### 4.2 事件（RDA → 主，stdout 逐行）

```jsonc
{ "protocol": "rao/1", "kind": "started",  "goalId": "...", "ts": 1234567890123,
  "data": { "rdaVersion": "0.1.0", "python": "3.8.10", "host": "diablo-pc" } }
{ "kind": "recon", "data": { "facts": { "os": "...", "tools": {...}, "workspace": {...} } } }
{ "kind": "ask",  "askId": "a1", "question": "需要任务程序", "context": { "recon": {...} } }
{ "kind": "progress", "data": { "stepId": "s1", "attempt": 1, "phase": "running" } }
{ "kind": "milestone", "data": { "stepId": "s1", "summary": "build ok" } }
{ "kind": "step_result", "data": { "stepId": "s1", "status": "ok|failed|skipped|fallback_ok",
  "exitCode": 0, "durationMs": 4200, "stdoutTail": "...", "stderrTail": "..." } }
{ "kind": "warning", "data": { "message": "..." } }
{ "kind": "error", "data": { "message": "...", "fatal": false } }
{ "kind": "result", "data": { "status": "success|failed|partial|timeout|cancelled",
  "summary": "...", "steps": [...], "artifacts": [{"path":"...","size":123}],
  "assumptions": ["..."], "metrics": { "steps": 5, "retries": 1, "durationMs": 42000 } } }
```

### 4.3 注入（主 → RDA，运行中经 stdin）

```jsonc
{ "type": "steer",  "askId": "a1", "answer": { "plan": {...} } }   // 回应 ask
{ "type": "steer",  "message": "跳过当前步骤，直接做 X" }            // 一般指导
{ "type": "cancel", "reason": "user requested" }                    // 取消
{ "type": "ping" }                                                   // 心跳探测
```

RDA 对一般 steer 的处理：当前步骤完成后，将 `message` 作为后续执行的约束注入
（v1 实现：记录到 `steerNotes`，引擎在选取下一步时优先考虑 `skip:<stepId>` /
`run:<command>` 指令形式；自由文本仅记录进 result 供主 agent 审计）。

### 4.4 错误码

| 码 | 含义 |
|---|---|
| `RAO_PROTOCOL_MISMATCH` | protocol 字段不是 rao/1 |
| `RAO_GOAL_INVALID` | goal schema 校验失败 |
| `RAO_BUDGET_EXCEEDED` | 步骤/时间预算耗尽 |
| `RAO_STEP_FAILED` | 步骤最终失败（含回退） |
| `RAO_TRANSPORT_LOST` | SSH 通道中断 |
| `RAO_DEVICE_UNREACHABLE` | TCP/认证失败 |

---

## 5. MCP 工具契约（6 个）

| 工具 | 输入 | 输出 | 行为 |
|---|---|---|---|
| `devices_list` | – | 设备数组（id/host/user/port/online/workspace） | 读配置 + TCP 22 探测（并发，1.5s 超时） |
| `remote_dispatch` | `{deviceIds[], goal}` | `{dispatches:[{goalId,deviceId}]}` | 校验 → 生成 goalId → 起线程 → 立即返回 |
| `remote_status` | `{goalId}` 或 `{deviceId}` | 状态 + 最近 N 条事件 | 单 goal 或按设备聚合 |
| `remote_steer` | `{goalId, message}` 或 `{goalId, askId, answer}` | `{accepted:true}` | 写 stdin；无 askId 为一般指导 |
| `remote_cancel` | `{goalId, reason?}` | `{cancelled:true}` | 注入 cancel，5s 不退则 kill 进程组 |
| `remote_collect` | `{goalIds[], timeoutMs?}` | 聚合报告（见 §7.3） | 等待完成（每 goal 各自超时），汇总矩阵 |

---

## 6. 并发与生命周期

### 6.1 DispatchPool

- 每个 goal 一个守护线程（`GoalRun`）：transport.open → spawn RDA → pump 事件 → 收尾。
- 并发上限：`rao.json` 全局 `maxConcurrentGoals`（默认 5）+ 单设备 `maxConcurrentGoals`
  （默认 2，设备只有 2 核）。超限的 dispatch 排队（`queued` 态）。
- goal 状态机：`queued → running → (ask_waiting ⇄ running) → done | failed | timeout | cancelled`。

### 6.2 预算与超时（强制在 Orchestrator 侧，不只依赖 RDA）

- 步数预算：由 RDA 计数上报，Orchestrator 校验超额即 cancel。
- 整体超时：Orchestrator 定时器到点 → cancel（注入失败则直接 kill）→ status=timeout，保留已收事件。
- 事件沉默超时：`eventSilenceMs`（默认 90s）无任何事件 → 判定卡死 → cancel。

### 6.3 RunStore（轻量持久化）

每次 dispatch 落盘 `<runsDir>/<goalId>.json`：goal + 事件流 + 最终 result。
`remote_collect` 聚合报告同时写 `<runsDir>/report-<ts>.json`。
供事后审计与回归对比（回归本身属 roadmap）。

---

## 7. 结果与报告

### 7.1 单 goal Result（§4.2 result 事件）由 Orchestrator 校验后入库。

### 7.2 status 语义

- `success`：全部步骤 ok（optional 步骤 failed 也算 success， assumptions 记录）
- `partial`：optional 步骤失败被跳过，或 steer 中止了部分范围
- `failed`：必需步骤最终失败 / RDA 崩溃
- `timeout`：预算耗尽被 Orchestrator 取消
- `cancelled`：用户取消

### 7.3 聚合报告（remote_collect 输出）

```
matrix:   [{deviceId, goalId, status, summary}]
common:   跨设备共性结论（主 agent 语义层撰写，collect 仅提供材料）
artifacts: 各 goal 工件路径清单
next:     建议后续动作（主 agent 撰写）
```

---

## 8. 凭据与安全

- 设备配置 `config/devices.json`（**gitignore**，提供 `devices.example.json`）：
  `{id, host, port, user, auth:{type:"password", password}, workspace, maxConcurrentGoals}`
- 环境变量覆盖：`RAO_DEVICE_<ID>_PASSWORD` 优先于配置文件。
- 已知妥协（v1）：密码在本地配置文件中明文/受环境变量保护；文件权限收紧 + 文档警告。
  v2 计划接 OS keyring（Windows Credential Manager）。
- RDA 侧：goal 中 `forbidden` 命令黑名单在 RDA 执行前做子串/前缀校验（v1 为软约束，
  文档明示不能替代 OS 沙箱；硬隔离属 roadmap）。
- 传输安全：SSH 本身（known_hosts 由 paramiko AutoAdd 记录到项目 known_hosts 文件）。
- ZCode server daemon 与 RAO 完全无交互（RAO 走 sshd 直连，见实机验证记录）。

---

## 9. RDA 设计（远端，Python 3.8 兼容）

### 9.1 进程模型

每 goal 一次性进程：`python3 rda.py run-goal`（goal 从 stdin 读入）。
无常驻 daemon（roadmap v2）。stderr 用于 RDA 自身日志，stdout 只允许协议事件行。

### 9.2 执行引擎

```
load goal → validate → emit started
  ├─ plan 缺失 → recon（内置事实收集）→ emit ask("需要任务程序") → 等 steer
  ├─ plan 存在 → 逐步骤执行:
  │    shell: subprocess.run(cwd, timeout, capture) → 判据校验
  │      ok → emit step_result → next
  │      fail → retries-- → backoff → retry
  │           retries 尽 → fallbacks 依序尝试
  │           全败 → optional? skip : emit error(fatal) → result(failed)
  │    ask: emit ask → 等 steer → 继续
  │    artifact: 收集文件清单（大小/mtime）→ 事件
  │    wait_file: 轮询文件出现/消失
  ├─ 全程: 步数/时间预算检查、stdin 注入轮询（steer/cancel/ping）
  └─ 终态: emit result → exit(0=正常收尾, 3=超时, 4=cancel)
```

### 9.3 recon 内置收集（无 plan 时的 ask 附件）

os/uname、python 版本、磁盘、内存、CPU 核数、workspace 目录摘要（顶层条目+git 状态）、
常见工具存在性（git/make/gcc/python3/node/docker）、近期登录。

---

## 10. 测试策略

| 层 | 内容 | 通过标准 |
|---|---|---|
| 单元 | schema 校验、状态机、预算、EventMux、错误归一 | pytest 全绿（transport 用 FakeTransport） |
| 协议集成 | Orchestrator ⇄ 本地伪 RDA（echo 协议器） | 事件流/steer/cancel 全路径 |
| 真机集成 | diablo@192.168.0.100 实测 | 见 §10.1 用例表，全部通过 |
| MCP 集成 | mcp stdio client 起 rao-mcp 调 6 工具 | 工具返回结构正确 |
| 端到端 | ZCode 配置 MCP 后由主 agent 实跑 | 真实 goal 闭环 |

### 10.1 真机用例（验收表）

| # | 用例 | 验收 |
|---|---|---|
| T1 | devices_list | 设备 online=true |
| T2 | 单 goal 闭环（写 facts.json 工件） | result=success，工件存在且内容正确 |
| T3 | 失败步骤 + retry + fallback | step_result 见 attempt=2 与 fallback_ok |
| T4 | ask/steer 回路（无 plan goal） | recon → ask → steer(plan) → success |
| T5 | 同设备双 goal 并发 | 两 goal 均 success，时间上重叠 |
| T6 | 运行中 cancel | status=cancelled，进程终止 |
| T7 | 整体超时（极小 timeoutMs） | status=timeout，事件保留 |
| T8 | 运行中一般 steer | steerNotes 出现在 result |
| T9 | forbidden 命令拦截 | 步骤被拒，result 含警告 |
| T10 | remote_collect 聚合 | matrix 完整，报告文件落盘 |

---

## 11. Roadmap（明确不做进 v1）

1. R3 正式路线：session-control 上远端链路（源码 PR），Transport 接口已隔离，届时新增
   `R3SessionControlTransport` 与垫片并存。
2. RDA daemon 化 + 心跳常驻 + 断线重接。
3. 设备间 DAG（goal 依赖声明与拓扑执行）。
4. 远端独立规划器（`planner: "remote-llm"`，接远端 LLM CLI）。
5. 凭据入 OS keyring；命令硬沙箱（bubblewrap/sandbox-ssh 思路）。
6. 回归对比与报告版本管理。

---

## 12. 决策记录（ADR 摘要）

| # | 决策 | 备选 | 理由 |
|---|---|---|---|
| A1 | MCP plugin 非源码 | fork ZCode | fork 维护成本不可接受；PR 后置 |
| A2 | Python/FastMCP | TS/ssh2 | 本机已实测 paramiko；Node 工具链本地未验证；Transport 接口隔离栈选择 |
| A3 | 每 goal 一次性 RDA 进程 | 常驻 daemon | 生命周期最简，无 daemon 管理复杂度；v2 再 daemon 化 |
| A4 | 规划由主 agent 承担 | 远端 LLM | 远端无 key/无 CLI（已核实）；协议留插槽 |
| A5 | 密码进 devices.json + env 覆盖 | keyring | v1 最简可行；roadmap 升级 |
| A6 | 预算双重强制（RDA+Orchestrator） | 仅 RDA | RDA 可能崩溃/被杀，Orchestrator 必须兜底 |
