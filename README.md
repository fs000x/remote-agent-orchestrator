# rao — Remote Agent Orchestrator

跨 Agent 远程编排工具：安装后，ZCode / Claude Code 的主 Agent 即可把目标
**并行派发**到远程设备上的执行者（内置执行器或远端 Claude），可控、可恢复、
可审计。产品定位与边界见 [docs/PRODUCT.md](docs/PRODUCT.md)；
从零部署到真机跑通见 [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md)。

- 当前版本：**0.11.0**（`src/rao/__init__.py` 为唯一版本来源）
- 测试：**90 个用例**（单元 / 本地协议 / daemon / 节点模式 / 真机 T1-T21 / MCP stdio）
- 许可：MIT

## 快速开始

```bash
pipx install rao        # 或: pip install -e .（本仓库开发态）
rao setup               # 检测本机 Agent → 注入 MCP+Skill（备份/合并/可回滚）
rao devices add         # 配置远程设备（SSH 密码认证）
rao doctor              # 诊断：Agent / 设备 / 远端能力（RDA、claude、ZCode Server）
```

之后在 ZCode / Claude Code 会话中用自然语言派发任务即可，**9 个 MCP 工具**自动可用。

## MCP 工具（9 个）

| 工具 | 作用 |
|---|---|
| `devices_list` | 设备清单 + 在线探测 + 执行能力矩阵（`is_agent`） |
| `remote_dispatch` | 向多台设备并行派发 goal |
| `remote_status` | 按 goal / 设备查进度（跨进程回载持久化记录） |
| `remote_steer` | 中途纠偏（`skip:<stepId>` / `run:<command>`）或回应 ask |
| `remote_cancel` | 取消 goal |
| `remote_collect` | 等待完成并聚合报告（含回归对比） |
| `remote_dispatch_dag` | 跨设备 DAG 依赖编排（success / lenient 策略） |
| `remote_dag_status` | DAG 节点状态 |
| `remote_reports` | 历史报告 list / get / diff |

## 执行模式（诚实命名）

| 模式 | 是什么 | 适合 |
|---|---|---|
| **executor**（内置执行器） | 按宿主 Agent 下发的步骤程序执行命令行命令（**非自主 Agent**，`execution.is_agent=false`） | 结构化目标：构建/收集/测试/部署 |
| **agent**（远端 Claude 托管） | 远端真 Agent 整目标自主执行（需远端已装 Claude 并完成登录/API 配置） | 开放性目标 |

## 传输模式

| 模式 | 说明 |
|---|---|
| `ssh`（默认） | 主控经 SSH 连到设备（spawn / daemon 两种子模式，`transportMode` 可配） |
| `node` | 设备侧 daemon 主动拨出注册到主控（`--master`），适配 NAT/防火墙场景；协议见 [docs/PROTOCOL-NODE.md](docs/PROTOCOL-NODE.md) |

## 组件

```
src/rao/                    本地编排器（Python 3.10+，FastMCP + paramiko）
  server.py                 9 个 MCP 工具入口（内嵌 master 监听，默认 :9700）
  orchestrator.py           DispatchPool / 预算强制 / RunStore / 聚合 / DAG
  transport.py              SSH（spawn/daemon）+ Node + MultiTransport
  master.py                 节点模式主控（注册/心跳/goal 转发/steer）
  provision.py              远端运行时哈希比对 + 自动补部署
  agents.py                 Agent 检测与注入（备份/合并/manifest 回滚）
  devices.py                设备注册表（config/devices.json + 节点自动并入）
  protocol.py               wire schema v1（正本）
  cli.py                    rao setup / devices / doctor / run / uninstall / mcp
  runtime/                 随包分发的远端运行时（Python 3.8 stdlib-only）
    rda.py                  goal 执行引擎（executor + claude backend）
    rda_daemon.py           常驻 daemon（心跳/缓冲/attach/节点拨出/控制面命令）
    protocol.py             protocol.py 的部署副本（scripts/sync_protocol.py 同步）
  data/skills/              cross-device-orchestrator skill（rao setup 时部署）
tests/                      90 个用例（含真机 T1-T21、TC-A~D、RU1-RU6）
scripts/                    deploy_rda.py / sync_protocol.py
config/                     devices.example.json（devices.json 已 gitignore）
docs/                       PRODUCT / DESIGN / DEPLOYMENT / DEVLOG / HANDOVER / PROTOCOL-NODE
plugin/                     ZCode 插件分发目录（.zcode-plugin/plugin.json）
```

## 开发态

```bash
python -m venv .venv
./.venv/Scripts/python.exe -m pip install -e ".[dev]"
./.venv/Scripts/python.exe -m pytest tests/ -q          # 90 用例
./.venv/Scripts/python.exe scripts/deploy_rda.py <device_id>   # 手动部署远端运行时
```

## 配置

**无环境变量配置**（v0.8 起）。全部集中在 `config/devices.json`：

```jsonc
{
  "settings": {                     // 全局
    "transportMode": "auto",        // auto | spawn | daemon
    "rdaDir": "$HOME/.rao/runtime", // 远端运行时目录
    "runsDir": "runs",
    "masterPort": 9700              // 节点模式主控监听端口
  },
  "devices": [
    {
      "id": "my-box", "host": "192.168.1.10", "port": 22, "user": "me",
      "workspace": "/home/me/project",
      "rdaDir": "/home/me/.rao/runtime",
      "maxConcurrentGoals": 2,
      "auth": { "type": "password", "password": "..." }
    }
  ]
}
```

凭据链：OS keyring（可选）→ 配置文件。`config/devices.json` 已 gitignore。

## 安全须知

- `config/devices.json` 含明文密码且已 gitignore——不要提交、不要外传。
- `forbidden` 命令黑名单是软约束（字符串匹配），不是沙箱；危险操作靠人审 + 预算兜底。
- 节点模式默认 LAN 明文 + 注册令牌（TLS 计划中）；跨公网部署前请先加隧道/VPN。
