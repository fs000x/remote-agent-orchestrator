# RAO 开发日志（DEVLOG）

按时间倒序记录开发过程、关键决策与踩坑。交接视角的汇总见 HANDOVER.md。

## 2026-11-20

### 真机集成全绿（38→39 tests）
- 新增 MCP stdio 集成测试：官方 client 拉起 server，6 工具全链路（含真机 goal）。
- 修复 server.py `sys.path` 指向包内导致 `import rao` 失败（server 起不来）。
- MCP server 已注册到用户级配置 `~/.zcode/cli/config.json`（合并写入，保留原 plugins 段）。
- Skill `cross-device-orchestrator` 部署到 `~/.zcode/skills/`，ZCode 已发现。
- 验证配置解析与 cwd 无关（ZCode 以自身 cwd 拉起进程）。

### 真机联调三连修（关键踩坑记录）
1. **goal 双重编码**：Orchestrator 先 `json.dumps` 成串，SshGoalChannel.write_line 又
   dumps 一次 → 远端 `json.loads` 得到 str → RDA 报 `RAO_GOAL_INVALID`。
   修复：transport 接收 goal **对象**，编码只在 write_line 一处。
2. **exit_code 属性/方法冲突**：GoalChannel 子类用实例属性 `self.exit_code` 遮蔽了
   `exit_code()` 方法，pump 收尾时 `'int' object is not callable`。
   修复：pump 用 `getattr` + callable 判断双兼容。
3. **终态竞态（T7）**：编排器判超时→cancel，RDA 的 result(cancelled) 事件随后到达
   并覆盖终态。重构为：**终态粘滞**（先定终态再 force-cancel；RESULT 事件在终态后
   忽略；finalize 按 state 合成 result）。单测 test_per_device_limit、cancel 竞态
   一并借此修复（FakeTransport 补充 cancel 注入语义）。
4. RDA 本地测试抓到：subprocess 输出是 bytes 直接进 JSON 事件 → 序列化崩溃；
   单引号在 cmd.exe 是字面字符导致回退命令失败（测试自身问题，改双引号）。

### 里程碑
- `0040915` feat: RAO v1 core（协议/传输/RDA/编排/MCP + 21 单测）
- `a22c0fc` fix: 真机集成全绿（T1-T10）
- `256d34d` feat: MCP 层集成测试

## 2026-11-19

### 环境与决策
- 本地：Python 3.13.2（VeighNa 发行版）+ pip。为避免污染用户环境：
  mcp<2（FastMCP API）+ paramiko 装进**项目专属 venv**；user-site 里 mcp/h11 已卸载
  （注：用户主环境 h11 0.14 与 httpcore2 的冲突**预先存在**，与本项目无关）。
- 远端 diablo-100（192.168.0.100，diablo，Linux x86_64，py3.8.10，2C/7.6G）：
  RDA 必须 py3.8 兼容 + 零依赖（stdlib only）。
- ZCode server 无 headless 会话入口（session-control 未上远端链路，源码核实）；
  本机无 sshpass/plink/expect → 传输层选 paramiko。
- 密码认证（不装公钥，用户要求）；ZCode 桌面端与 RAO 无交互（daemon 仅 loopback），
  应用户要求已把远端 zcode-server daemon 干净停机（`zcode stop`，可随时重连自启）。

### 设计定稿
- docs/DESIGN.md v1.0：半自主 v1 定义（远端自适应执行+升级提问；规划归主 agent，
  协议留 `planner` 插槽）、RAO wire protocol v1、6 工具契约、预算双端强制、
  边界与 roadmap（R3 源码路线等 6 项明确出界）。

### 工程约定
- 正本协议在 `src/rao/protocol.py`；`rda/protocol.py` 为部署副本，
  `test_protocol_sync` 防漂移，改动后跑 `scripts/sync_protocol.py`。
- 分支：feature/remote-agent-orchestrator（main 为空基线），完成合回 main 打 tag。
