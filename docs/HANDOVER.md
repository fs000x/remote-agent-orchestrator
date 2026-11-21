# rao 交接文档（HANDOVER）

> 更新: 2026-11-21（v0.7.0 发布就绪态）

## 1. 当前状态

**产品化三阶段（v0.5/v0.6/v0.7）全部实现并验证。** 86 个测试全绿；
真机验收 T1–T21（含断线重接、自动补部署、claude backend 快速失败）。
main 上 tag 链：v0.1.0 → v0.5.0 → v0.6.0 → v0.7.0（各迭代独立分支合并）。

| 交付物 | 位置 | 状态 |
|---|---|---|
| 产品方案 | docs/PRODUCT.md | ✅ 决策/边界/命名规则 |
| 技术架构 | docs/DESIGN.md | ✅ + roadmap 打勾状态 |
| rao CLI（setup/devices/doctor/uninstall/mcp） | src/rao/cli.py + agents.py | ✅ 真机注入/回滚演练过 |
| MCP server（6 工具 + DAG/报告） | src/rao/server.py + orchestrator.py | ✅ |
| 远端 Agent Server（rda-node） | src/rao/runtime/（rda.py/rda_daemon.py） | ✅ executor + claude 双 backend |
| ZCode 设备/凭据复用 | src/rao/zcode_store.py | ✅ 真机数据解密验证 |
| 打包 | pyproject.toml + dist/ + plugin/ | ✅ twine PASSED |

## 2. 怎么跑

```bash
cd remote-agent-orchestrator
./.venv/Scripts/python.exe -m pytest tests/ -q      # 86 测试
./.venv/Scripts/rao.exe doctor                      # 诊断
./.venv/Scripts/rao.exe setup --agents zcode,claude --yes
```

用户使用入口 = `rao setup` + 新 Agent 会话自然语言派发（详见 README）。

## 3. 已知限制

1. claude backend 无中途 steer（headless 限制），只能 cancel；
   forbidden 为软约束（--append-system-prompt）。
2. ZCode 凭据解密仅 Windows 路径实现；Linux Secret Service 待接。
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
