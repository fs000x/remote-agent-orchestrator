# rao — Remote Agent Orchestrator

跨 Agent 远程编排工具：安装后，ZCode / Claude Code 的主 Agent 即可把目标
**并行派发**到远程设备上的执行者（内置执行器或远端 Claude），可控、可恢复、
可审计。产品定位与边界见 [docs/PRODUCT.md](docs/PRODUCT.md)。

## 快速开始

```bash
pipx install rao        # 或: pip install -e .（本仓库开发态）
rao setup               # 检测本机 Agent → 注入 MCP+Skill（备份/合并/可回滚）
rao devices add         # 配置远程设备（独立于任何 Agent 的设备配置）
rao doctor              # 诊断：Agent / 设备 / 远端能力（RDA、claude、ZCode Server）
```

之后在 ZCode / Claude Code 会话中直接用自然语言派发任务即可，
6 个编排工具（devices_list / remote_dispatch / remote_status / remote_steer /
remote_cancel / remote_collect）自动可用。

## 执行模式（诚实命名）

| 模式 | 是什么 | 适合 |
|---|---|---|
| **executor**（内置执行器） | 按宿主 Agent 下发的步骤程序执行命令行命令（**非自主 Agent**，`is_agent:false`） | 结构化目标：构建/收集/测试/部署 |
| **agent**（远端 Claude 托管） | 远端真 Agent 整目标自主执行（需远端已装 Claude 并登录） | 开放性目标 |



## 组件

```
src/rao/        MCP server + 编排器（本地，Python 3.10+，FastMCP + paramiko）
  server.py       6 个 MCP 工具入口
  orchestrator.py DispatchPool / 预算强制 / RunStore / 聚合
  transport.py    SSH 传输（每 goal 一条 SSH 连接）+ 测试用 Fake
  protocol.py     wire schema v1（正本；rda/protocol.py 为同步副本）
  devices.py      设备注册表（config/devices.json + 环境变量覆盖）
rda/            远端 goal 执行引擎（Python 3.8 stdlib-only，随 protocol.py 一起部署）
skills/         cross-device-orchestrator skill（部署到 ~/.zcode/skills/）
tests/          单元 + 本地 RDA 协议 + 真机集成 + MCP stdio 集成（39 个用例）
scripts/        deploy_rda.py / sync_protocol.py
config/         devices.example.json（正本 devices.json 已 gitignore）
```

## 快速开始

```bash
# 1. 环境（Windows Git Bash）
python -m venv .venv
./.venv/Scripts/python.exe -m pip install "mcp<2" "paramiko>=3.4" pytest pytest-asyncio

# 2. 设备配置
cp config/devices.example.json config/devices.json   # 填入 host/user/password
# 或用环境变量: RAO_DEVICE_<ID大写下划线>_PASSWORD

# 3. 部署 RDA 到远端
./.venv/Scripts/python.exe scripts/deploy_rda.py <device_id>

# 4. 测试
./.venv/Scripts/python.exe -m pytest tests/ -q

# 5. 接入 ZCode（已配置则跳过）
# ~/.zcode/cli/config.json -> mcp.servers.remote-agent-orchestrator
#   command: <本项目>/.venv/Scripts/python.exe
#   args:    <本项目>/src/rao/server.py
# 重启 ZCode 会话后，6 个工具自动可用；技能 cross-device-orchestrator 提供工作流。
```

## 环境变量

| 变量 | 作用 |
|---|---|
| `RAO_CONFIG` | devices.json 路径覆盖 |
| `RAO_DEVICE_<ID>_PASSWORD` | 设备密码覆盖（优先于配置文件） |
| `RAO_RDA_DIR` | 远端 RDA 目录（默认 /home/diablo/git500/zcode_prj/rao） |
| `RAO_RUNS_DIR` | 运行记录目录（默认 <项目>/runs） |

## 安全须知

- `config/devices.json` 含明文密码且已 gitignore——不要提交、不要外传。
- `forbidden` 命令黑名单是软约束（字符串匹配），不是沙箱；危险操作靠人审 + 预算兜底。
