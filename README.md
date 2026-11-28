# rao — Remote Agent Orchestrator

跨 Agent 远程编排工具：安装后，ZCode / Claude Code 的主 Agent 即可把目标
**并行派发**到远程设备上的执行者（内置执行器或远端 Claude），可控、可恢复、
可审计。产品定位与边界见 [docs/PRODUCT.md](docs/PRODUCT.md)；
从零部署到真机跑通见 [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md)。

- 当前版本：**1.0.0**（`src/version.ts` 为唯一版本来源；Python 实现已归档于
  `legacy/python-v0.11.1` 分支/tag，本仓库为 **Node.js / TypeScript 重写版**）
- 测试：**137 个用例**（单元 / 编排 / 运行时进程 / 传输栈 / Noise_IK / 配对 /
  盲转发）+ 真机 T1-T6（`scripts/device-verify.mjs`）
- 许可：MIT
- 仓库不变量（agents & contributors 必读）：[AGENTS.md](AGENTS.md)

## 快速开始

```bash
npm install -g .        # 或: npm install -g rao（发布后）
rao setup               # 检测本机 Agent → 注入 MCP+Skill（备份/合并/可回滚）
rao devices add         # 配置远程设备（SSH 密码认证；设备需 Node >= 18）
rao doctor              # 诊断：Agent / 设备 / 远端能力（RDA、claude、ZCode Server）
```

之后在 ZCode / Claude Code 会话中用自然语言派发任务即可，**9 个 MCP 工具**自动可用。

## MCP 工具（9 个）

| 工具 | 作用 |
|---|---|
| `devices_list` | 设备清单 + 在线探测 + 执行能力矩阵（`is_agent`） |
| `remote_dispatch` | 向多台设备并行派发 goal（**operationId 幂等**） |
| `remote_status` | 按 goal / 设备查进度（跨进程回载持久化记录） |
| `remote_steer` | 中途纠偏（`skip:<stepId>` / `run:<command>`）或回应 ask（幂等） |
| `remote_cancel` | 取消 goal（幂等） |
| `remote_collect` | 等待完成并聚合报告（落盘 `report-*.json`，含回归对比） |
| `remote_dispatch_dag` | 跨设备 DAG 依赖编排（success / lenient 策略；幂等） |
| `remote_dag_status` | DAG 节点状态 |
| `remote_reports` | 历史报告 list / get / diff |

**幂等语义**（P1 borrow，normative）：变更型工具必须带 `operationId`；
同 key 同参数 → 同回执；同 key 异参数 → 指纹冲突拒绝；`accepted` ≠ 完成。

## 执行模式（诚实命名）

| 模式 | 是什么 | 适合 |
|---|---|---|
| **executor**（内置执行器） | 按宿主 Agent 下发的步骤程序执行命令（**非自主 Agent**，`execution.is_agent=false`） | 结构化目标：构建/收集/测试/部署 |
| **agent**（远端 Claude 托管） | 远端真 Agent 整目标自主执行（需远端已装 Claude 并完成登录/API 配置） | 开放性目标 |

规划责任始终在主 Agent（`plan.planner="main-agent"`）；无 plan 的 builtin goal
走 recon → ask 流程；`remote-llm` planner 槽位保留但默认关闭。

## 传输与安全

| 能力 | 说明 |
|---|---|
| SSH（默认） | 每条 goal 一条 SSH 连接：spawn（`node rda.mjs run-goal`）或 daemon（`client-bridge` ↔ 常驻 daemon），`transportMode` 可配 |
| provision | 运行时 `.mjs` 哈希比对自动补部署 + codeHash 变更重启 daemon（单实例保证） |
| 控制面 | steer/cancel 走 inject-one（全新 SSH 连接注入，回执 delivered/pending/dropped） |
| 节点模式 | 设备主动拨入 master:9700（NAT/防火墙场景）；可选 **Noise_IK E2EE**（`rao pair` 配对登记设备公钥，prologue 绑定 nodeId，重放/篡改 fail-closed） |
| 盲转发 relay | `rao-relay`：join 后字节原样转发、永不解密（中继部署在公网时两端仍端到端加密） |

## 配置

**无环境变量配置**。全部集中在 `config/devices.json`（已 gitignore，含明文密码——不要提交、不要外传）：

```jsonc
{
  "settings": {
    "transportMode": "auto",        // auto | spawn | daemon
    "rdaDir": "/home/me/.rao/runtime", // 远端运行时目录（设备需 Node >= 18）
    "runsDir": "runs",
    "masterPort": 9700              // 节点模式主控监听端口
  },
  "devices": [
    {
      "id": "my-box", "host": "192.168.1.10", "port": 22, "user": "me",
      "workspace": "/home/me/project",
      "maxConcurrentGoals": 2,
      "auth": { "type": "password", "password": "..." }
    }
  ]
}
```

凭据链：OS keyring（可选）→ 配置文件。安全须知：`forbidden` 黑名单是软约束
（字符串匹配），不是沙箱；危险操作靠人审 + 预算兜底。

## 组件

```
src/
  cli.ts                     rao CLI（setup/devices/doctor/run/pair/uninstall/mcp）
  mcp/server.ts              9 个 MCP 工具 + operationId 账本 + master 监听
  orchestrator.ts            GoalRun 生命周期 / 槽位 / 预算 / RunStore / DAG / 报告
  master.ts                  节点模式主控（注册/配对端点/Noise_IK/goal 路由）
  transports/                ssh(spawn) / sshDaemon(bridge) / tcpDaemon / lineStream / channel
  provision.ts               远端运行时哈希比对 + 自动补部署
  devices.ts                 设备注册表（devices.json + 节点自动并入）
  agents.ts                  Agent 检测与注入（备份/合并/manifest 回滚）
  ledger.ts                  operationId 幂等账本（P1 borrow）
  protocol.ts                wire schema v1 + 错误码表 + LIMITS
  version.ts                 版本唯一来源
  runtime/                   部署到设备的运行时（Node >= 18，零依赖，原样分发）
    rda.mjs                  goal 执行引擎（executor + claude backend）
    rda_daemon.mjs           常驻 daemon（心跳/缓冲/attach/节点拨出/桥/配对）
    protocol.mjs             部署侧 wire 校验
    noise.mjs                Noise_IK（canonical 实现，主控与设备共用）
  relay/server.ts            盲转发中继（rao-relay）
  data/skills/               cross-device-orchestrator skill（rao setup 时部署）
tests/                       137 个用例（L1-L5，无外部网络）
scripts/device-verify.mjs    真机 T1-T6 验证（发布闸门）
docs/                        PRODUCT / DESIGN / DEPLOYMENT / TEST-POLICY / PROTOCOL-NODE / DEVLOG / HANDOVER / RESEARCH
```

## 开发

```bash
npm install
npm run build          # tsc + 拷贝 runtime .mjs 到 dist
npm test               # 137 用例（无外部网络）
node scripts/device-verify.mjs [--device diablo-100] [--skip-claude]  # 真机 T1-T6
```

文档索引：[docs/PRODUCT.md](docs/PRODUCT.md)（定位/边界）·
[docs/DESIGN.md](docs/DESIGN.md)（架构与协议演进）·
[docs/DEPLOYMENT.md](docs/DEPLOYMENT.md)（部署手册）·
[docs/TEST-POLICY.md](docs/TEST-POLICY.md)（测试政策）·
[docs/PROTOCOL-NODE.md](docs/PROTOCOL-NODE.md)（节点模式协议）·
[docs/RESEARCH-ds-harness-remote.md](docs/RESEARCH-ds-harness-remote.md)
（P1/P2/P3 借鉴来源）· [docs/DEVLOG.md](docs/DEVLOG.md)（开发日志）。
