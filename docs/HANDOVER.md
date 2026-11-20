# RAO 交接文档（HANDOVER）

> 给下一个接手的人（人或未来的 agent 会话）：当前状态、怎么跑、已知问题、下一步。

## 1. 当前状态（2026-11-20）

**迭代 v0.2/v0.3/v0.4 全部实现并经测试验证。** 68 个测试用例全绿
（单元 21+2 / RDA 本地 7 / DAG+ensureWorkspace+报告 9 / daemon 4 / 真机 17
[T1-T17] / MCP stdio 1 / 同步守卫 1 / v0.4 凭据+规划器 8）。

| 交付物 | 位置 | 状态 |
|---|---|---|
| 设计文档 | docs/DESIGN.md | ✅ v1.0 定稿 |
| MCP server（6 工具） | src/rao/server.py | ✅ 已注册用户级 MCP 配置 |
| SSH Transport | src/rao/transport.py | ✅ 真机验证 |
| RDA 引擎 | rda/rda.py | ✅ 已部署 diablo-100:/home/diablo/git500/zcode_prj/rao |
| Skill | skills/cross-device-orchestrator/ | ✅ 已部署 ~/.zcode/skills/（ZCode 已发现） |
| 测试 | tests/（68 cases） | ✅ 全绿，含 T1-T17 真机验收表 |
| 运行记录 | runs/（gitignore） | 每次派发自动落盘 |

**Git**：main 上 tag v0.1.0→v0.2.0→v0.3.0→v0.4.0（各迭代独立 feature 分支合并）。
roadmap 已全部消化：§11.2 daemon 化（v0.3）、§11.3 DAG（v0.2）、§11.4 规划器
插槽（v0.4）、§11.5 keyring（v0.4）、§11.6 报告对比（v0.2）。唯一剩余 §11.1
R3 源码 PR 路线（需改 ZCode 源码提上游，超出本插件仓库边界）。

## 2. 怎么跑（30 秒上手）

```bash
cd C:/Users/momo/work/ai_work/git/ai_prj/remote-agent-orchestrator
./.venv/Scripts/python.exe -m pytest tests/ -q          # 全部测试
./.venv/Scripts/python.exe scripts/deploy_rda.py diablo-100   # 更新远端 RDA
```

**在 ZCode 里用**：重启 ZCode 会话（MCP server 随会话启动自动连接），
主 agent 即拥有 6 个工具（remote_dispatch 等）；说"在远程设备上做 X"会触发
cross-device-orchestrator 技能。注意：本交接发生的会话是配置写入前的旧会话，
看不到这些工具属正常。

**协议要点**（详见 DESIGN.md §4）：goal JSON → RDA（stdin 一次性）；
事件 JSON Lines → 主（stdout）；运行中注入 steer/cancel 经 stdin。
半自主 v1 = RDA 自适应执行 + `ask` 升级；规划由主 agent 写 plan
（可先派无 plan goal 触发 recon→ask 拿到设备事实再写）。

## 3. 已知限制（都是设计内妥协，非 bug）

1. ~~RDA 无常驻~~ **已实现（v0.3）**：`RAO_TRANSPORT_MODE=auto` 默认探测 daemon，
断线重接 + attach 重放（seq 去重）；spawn 模式仍作回退。
2. **密码明文**在 config/devices.json（gitignore 挡住版本控制）；env 覆盖
   `RAO_DEVICE_<ID>_PASSWORD` 优先。OS keyring 在 roadmap。
3. **forbidden 黑名单是软约束**（子串匹配），不是沙箱；硬隔离仍在 roadmap。
   凭据链已入 keyring（v0.4），env/config 仍可用。
4. **单 device 并发默认 2**（该机 2 核）；全局默认 5。可在 devices.json 调。
5. **MCP server 进程若退出**，in-flight goal 的内存态丢失；落盘的 runs/*.json 仍在。
6. Windows 本地跑 RDA 仅用于测试（rda.py 兼容 POSIX 为主，如 /proc 采集有 try/except）。
7. **远端 zcode-server daemon 目前是停机状态**（应用户要求关闭）；与 RAO 无依赖，
   ZCode 桌面重连即自动拉起。

## 4. 测试地图（改代码先看这里）

| 文件 | 覆盖 | 依赖 |
|---|---|---|
| tests/test_protocol.py | wire schema 全分支 | 无 |
| tests/test_orchestrator.py | 生命周期/取消竞态/并发槽/collect | FakeTransport |
| tests/test_rda_local.py | RDA 引擎行为（成功/recon+ask/回退/禁令/取消/预算/非法 goal） | 本地子进程 |
| tests/test_real_device.py | **T1-T10 验收表** | diablo-100 在线 + 密码 |
| tests/test_mcp_server.py | MCP 协议层 6 工具 | 设备可达（可退化） |
| tests/test_protocol_sync.py | 双份 protocol.py 防漂移 | 无 |

真机测试自动 skip 条件：设备不可达或无凭据（不会假红）。

## 5. 下一步（按优先级）

1. **用户侧验收**：重启 ZCode → 主 agent 实际派发一个真实任务（非测试命令）
   → 检查 runs/ 报告。这是 v1 的最后一环（自动化测试已覆盖，人工场景未跑）。
2. **接第二台设备**：devices.json 加条目 + deploy_rda.py + keyring 存凭据，即可
   验证真·跨设备并行与 DAG（当前并行测试用同设备双 goal 模拟）。
3. **R3 正式路线**（DESIGN.md §11.1）：session-control 上远端链路的源码 PR；
   Transport 接口已隔离，届时实现 `R3SessionControlTransport` 与 SSH/daemon 垫片
   并存。这是唯一未实现的 roadmap 项。
4. roadmap 其余：RDA daemon 化 / 设备间 DAG / 远端 LLM 规划器 / keyring / 硬沙箱。

## 6. 关键文件速查

| 改什么 | 看哪 |
|---|---|
| 协议字段 | src/rao/protocol.py（正本）→ scripts/sync_protocol.py → 重新 deploy |
| 新 MCP 工具 | src/rao/server.py（薄封装）+ orchestrator 对应能力 |
| 预算/超时策略 | orchestrator.py `_pump`（双端强制，A6 决策） |
| 远端执行行为 | rda/rda.py `GoalEngine`（run_step/run_recon/finish） |
| 设备增删 | config/devices.json（勿提交） |
| 技能文案 | skills/cross-device-orchestrator/SKILL.md → 拷贝到 ~/.zcode/skills/ |
