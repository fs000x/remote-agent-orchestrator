# rao 交接文档（HANDOVER）

> 更新: 2026-11-27（v0.11.0 发布态）

## 1. 当前状态

**v0.1–v0.11 全部实现并验证。** 90 个测试全绿（单元/本地协议/daemon/节点模式/
真机 T1–T21/TC-A~D/RU1–R6/MCP stdio）。真机 Claude 端到端已验证。
main tag 链：v0.1.0 → … → v0.10.0 → **v0.11.0**（各迭代独立分支合并）。

v0.11 变更：版本号收敛为单一来源（`src/rao/__init__.py`，`pyproject` 动态读取，
CLI/master 同源）、README 与真实布局对齐、新增 `docs/DEPLOYMENT.md` 部署手册、
节点模式（master 内嵌 `rao mcp`，设备 `--master` 拨入）并入主线。

| 交付物 | 位置 | 状态 |
|---|---|---|
| 部署手册（从零到真机） | docs/DEPLOYMENT.md | ✅ |
| 产品方案 | docs/PRODUCT.md | ✅ 决策/边界/命名规则 |
| 技术架构 | docs/DESIGN.md | ✅ + roadmap 打勾状态 |
| 节点模式协议 | docs/PROTOCOL-NODE.md + src/rao/master.py | ✅ 本地回环测试 |
| rao CLI（setup/devices/doctor/run/uninstall/mcp） | src/rao/cli.py + agents.py | ✅ 真机注入/回滚演练过 |
| MCP server（9 工具 + DAG/报告） | src/rao/server.py + orchestrator.py | ✅ |
| 远端运行时 | src/rao/runtime/（rda.py/rda_daemon.py） | ✅ executor + claude 双 backend |
| 打包 | pyproject.toml + dist/ + plugin/ + LICENSE(MIT) | ✅ twine PASSED |

## 2. 怎么跑

```bash
cd remote-agent-orchestrator
./.venv/Scripts/python.exe -m pytest tests/ -q      # 90 测试
./.venv/Scripts/rao.exe doctor                      # 诊断
./.venv/Scripts/rao.exe setup --agents zcode,claude --yes
```

用户使用入口 = `rao setup` + 新 Agent 会话自然语言派发（详见 README）。

## 3. 已知限制

1. claude backend 无中途 steer（headless 限制），只能 cancel；
   forbidden 为软约束（--append-system-prompt）。
3. Codex 只有检测留位，注入未实现。
4. `zcode-remote`（借道桌面 app-server 驱动远端会话）为实验项，未排期。
5. devices_list 的 agent_claude 能力默认 unknown，需 rao doctor/recon 探测。

## 4. 测试地图

| 文件 | 覆盖 |
|---|---|
| test_protocol.py / test_protocol_sync.py | wire schema + 双副本防漂移 |
| test_orchestrator.py | 生命周期/取消/并发槽/collect |
| test_rda_local.py | RDA 引擎行为 |
| test_daemon.py | 常驻 daemon/attach 重放/重接预算 |
| test_cli.py | 注入/回滚/设备管理/provision |
| test_dag_and_ws.py | ensureWorkspace/DAG/报告 diff |
| test_v06.py | 设备解密/合并/凭据链/claude backend |
| test_real_device.py | **T1–T21 真机验收** |
| test_mcp_server.py | MCP 层 6 工具 |

## 5. 下一步

1. 用户验收：新 ZCode/Claude 会话派真实任务（MCP 已注入，pipx 化后命令为全局 rao）。
2. 接第二台设备验证真·跨设备并行 + DAG。
3. PyPI 发布（twine upload dist/*——人工执行）+ GitHub 仓库公开 + marketplace。
4. Codex 注入实现；Linux Secret Service 凭据路径；zcode-remote 实验调研。

## 6. 关键文件速查

| 改什么 | 看哪 |
|---|---|
| 协议字段 | src/rao/protocol.py → scripts/sync_protocol.py |
| Agent 检测/注入 | src/rao/agents.py（路径可用 RAO_ZCODE_CONFIG 等覆盖） |
| 远端执行 backend | src/rao/runtime/rda.py（_run_claude_backend / GoalEngine.run） |
| 远端节点服务 | src/rao/runtime/rda_daemon.py |
| 设备/凭据链 | src/rao/devices.py + transport.password_of + zcode_store.py |
| MCP 工具 | src/rao/server.py |
