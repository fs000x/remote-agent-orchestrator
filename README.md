# Remote Agent Orchestrator (RAO)

ZCode 本地主 agent ←→ 远程设备 RDA agent 的调度系统（MCP plugin，非源码扩展）。

- 设计文档: [docs/DESIGN.md](docs/DESIGN.md)
- 开发文档: [docs/DEVLOG.md](docs/DEVLOG.md)
- 交接文档: [docs/HANDOVER.md](docs/HANDOVER.md)

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
