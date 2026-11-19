---
name: cross-device-orchestrator
description: 使用 remote-agent-orchestrator 的 6 个 MCP 工具把任务并行派发到多台远程设备上的半自主 agent（RDA），监控进度、中途纠偏（steer）、聚合跨设备结果并产出报告。当用户要求在远程设备/多台机器/跨设备上执行、验证或排查任务时使用本技能。
---

# Cross-Device Orchestrator 工作流

通过 `remote-agent-orchestrator` MCP 工具集调度远程设备上的 RDA agent。
你是唯一的规划者；RDA 是远端的执行手（自适应执行 + 遇疑升级）。

## 工具一览

| 工具 | 用途 |
|---|---|
| `devices_list` | 列出设备与在线状态（每次编排前必调） |
| `remote_dispatch` | `{deviceIds[], goal}` 并行派发，立即返回 goalId[] |
| `remote_status` | 按 goalId（详细事件）或 deviceId（该设备全部 goal）查进度 |
| `remote_steer` | 回应 ask（`askId`+`answer`）或发指导（`message`，支持 `skip:<stepId>` / `run:<command>` 指令） |
| `remote_cancel` | 取消 goal |
| `remote_collect` | 等待完成并聚合报告（matrix + 报告文件路径） |

## 标准流程（五步）

1. **侦察**：`devices_list` 确认目标设备在线。对不熟悉的设备，先派一个
   **无 plan 的 goal**（只写 objective）——RDA 会自动 recon（收集系统/工具/
   workspace/git 状态）并发 `ask` 把事实摆给你，这比盲写 plan 更可靠。
2. **规划**：基于 recon 事实为**每台设备**写 goal：
   - `objective`：一句话目标（远端会看到）
   - `workspace`：该设备上的绝对路径（各设备可能不同）
   - `plan.steps`：结构化任务程序（见下）——这是你给 RDA 的"剧本"
   - `constraints`：`maxSteps` / `timeoutMs` / `forbidden` 黑名单
3. **派发**：一台设备一个 goal（workspace 不同必须分开派）；
   同一 workspace 相同时可一次 `remote_dispatch` 多台设备并行执行。
4. **监控**：派发后用 `remote_status` 轮询。遇 `ask` 事件：阅读 `context`，
   用 `remote_steer(askId, answer)` 回应（answer 里放新的 plan 或决策）。
   偏航时用 `remote_steer(message)` 纠偏：`skip:s3` 跳过步骤、
   `run:<命令>` 追加一步，或自由文本（会记入 result.steerNotes 供审计）。
5. **聚合**：`remote_collect(goalIds)` 拿 matrix；你负责写**语义层结论**
   （跨设备共性、失败根因、下一步建议），报告文件自动落盘 `runs/report-*.json`。

## 写好 plan 的要点

- 每个 shell 步骤给 `success` 判据（`exitCode` / `stdoutContains` /
  `stderrContains` / `fileExists`），RDA 据此自动重试（`retries`）、
  回退（`fallbacks`）、跳过（`optional: true`）。
- 长任务拆小步：每步一个可验证的里程碑，你才能从 status 流里看清进度。
- 把危险操作写进 `forbidden`（软约束，防误碰；不是沙箱）。
- 预算给足但别无限：`timeoutMs` 是硬上限，到点 Orchestrator 会强杀并保留已收事件。
- RDA 的假设（assumptions）和纠偏记录（steerNotes）在 result 里——**聚合前必审**。

## 结果语义

| status | 含义 |
|---|---|
| success | 全部必需步骤 ok（optional 失败不算） |
| partial | 有 optional 步骤被跳过，或有 steer 收窄了范围 |
| failed | 必需步骤最终失败（含 fallback） |
| timeout | 预算耗尽（步骤数或时间），保留 partial 证据 |
| cancelled | 主动取消 |

## 红线

- 不派发会破坏系统的命令（rm -rf 类）——即使不在 forbidden 里。
- 设备离线时 `remote_dispatch` 会在连接阶段失败并记录；换设备或等恢复，不要盲目重试。
- collect 报告只是材料；跨设备结论必须由你（主 agent）撰写并核对 assumptions。
