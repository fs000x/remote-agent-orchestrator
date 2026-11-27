# rao — 产品方案（跨 Agent 远程编排工具）

- 版本: v1.1（对应实现 v0.5–v0.11）
- 日期: 2026-11-21
- 前置文档: DESIGN.md（技术架构）

## 1. 定位

rao 是一个**独立安装**的跨 Agent 远程编排工具：
任何支持 MCP 的 Agent 宿主（ZCode、Claude Code、Codex…）安装 rao 后，
其主 Agent 即可把目标派发到远程设备上的执行者，并行、可控、可恢复。

**三层架构**：

```
本地 rao CLI（安装/配置层）
  rao setup   检测本机 Agent → 注入 MCP+Skill（备份/合并/可回滚）
  rao devices 独立设备配置（不读不写任何 Agent 的设备配置）
编排层 rao mcp（MCP server，随宿主会话启动）
  6 工具 + DAG + 报告 / DispatchPool / 预算 / RunStore
  派发前自动补部署远端运行时
远端层 rao-node（远端节点服务，RDA daemon 演进）
  executor 模式（内置执行器，非 Agent）
  agent 模式（远端 Claude 整目标托管；Codex 预留）
  能力检测上报（claude CLI / ZCode Server 存在性）
```

## 2. 已锁定决策

| 决策项 | 结论 |
|---|---|
| 宿主范围 | v1: ZCode + Claude Code；Codex 检测留位（注入 v0.8+） |
| 安装形态 | PyPI + pipx（`pipx install rao`） |
| 远端驱动语义 | claude = 整目标托管（headless）；forbidden/预算映射旗标尽力约束；无中途 steer |
| 远端 OS | Linux/macOS |
| 远端凭据 | 用户在远端自行登录 Claude，工具不碰 |
| 注入策略 | 备份 + 合并 + `rao uninstall` 回滚，写前确认 |
| ZCode Server | **不内置、不安装、不修改**；只检测并报告（其安装归 ZCode 桌面端流程所有） |
| 设备配置 | **rao 独立管理，仅用户自行配置**（rao devices / devices.json）；
  不读取任何 Agent 的设备配置或凭据存储 |
| 命名 | rao（PyPI 包名发布时可加后缀） |

## 3. 诚实命名与能力矩阵（硬规则）

**内置执行器不是 Agent**——它是按本地 Agent 下发的步骤程序执行命令行命令的
确定性运行时（无 LLM、无自主推理）。协议级体现：

```jsonc
"execution": {
  "executor": true,          // 所有设备都有：按步骤程序执行
  "is_agent": false,         // 永远 false（executor 模式）
  "agent_claude": "unknown"  // 需远端探测（rao doctor / recon）
}
```

Skill 硬规则（已写入 SKILL.md，约束宿主主 Agent）：
- executor 设备派发前必须产出**命令级步骤程序**（调查/判断由本地 Agent 完成）
- 必须告知用户"该设备由内置执行器按计划步骤执行（非自主 Agent）"
- 禁止暗示远端会"自己想办法"
- 开放性任务应走 `goal.agent: "claude"`（需远端已装 Claude 并登录）

## 4. 执行模式对照

| | executor 模式（builtin） | agent 模式（claude 托管） |
|---|---|---|
| 输入 | objective + 任务程序（命令级） | objective 单独托管 |
| 智能来源 | 本地宿主 Agent | 远端 Claude（真 Agent） |
| forbidden/预算 | 精确强制 | 旗标映射尽力约束 |
| 中途 steer | ✅ stdin 注入 | ❌ 只能 cancel |
| 适用 | 结构化目标 | 开放性目标 |

## 5. 检测矩阵

| 位置 | 检测项 | 方式 |
|---|---|---|
| 本地 | zcode / claude / codex CLI 与配置目录 | PATH + 目录探测（agents.py） |
| 远端 | claude CLI、ZCode Server、RDA 运行时 | rao doctor / recon / 派发前探测 |
| 远端 RDA 缺失 | 自动补部署（SFTP） | provision（v0.5） |

## 6. 分发

- **主通道**：PyPI `pipx install rao` → `rao setup` → 各 Agent 注入
- **ZCode 插件通道**（可选）：`plugin/` 目录（.zcode-plugin/plugin.json，
  mcpServers 指向 `rao mcp`，含 Skill）——需先 pipx 安装
- 构建产物经 twine check；发布命令（人工执行）：`twine upload dist/*`

## 7. 与 ZCode 的关系（重要边界）

rao 与 ZCode **纯插件关系**：不 fork、不改源码、不维护 ZCode Server。
ZCode Server 在远端的存在只被**检测与报告**（"桌面端可接入该设备"）；
对其安装/升级/修复由 ZCode 桌面端自己的流程负责。
未来若 ZCode 上游提供 headless 会话入口，可经 `zcode-remote` backend 纳管
（实验项：借道本地桌面 app-server 协议，未排期）。
