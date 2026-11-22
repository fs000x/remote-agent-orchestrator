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

## 2026-11-20（续）：迭代 v0.2 / v0.3 / v0.4

按用户要求把 roadmap 结构化为三个迭代并全部实现，每迭代独立分支、全量测试、
合并 main、打 tag。

### 迭代一 v0.2.0（feature/v0.2-orchestration-enhancements）
- `ensureWorkspace`：goal 支持 {repoUrl, path, branch?}——RDA 幂等 clone 或
  fetch+checkout+ff-merge（分叉降级为 warning 假设，不强改用户状态）。
  踩坑：shlex 单引号在 cmd.exe 失效 → 平台感知引号助手；协议路径校验放宽为
  双平台绝对路径（POSIX / Windows 盘符）。
- 跨设备 DAG：`remote_dispatch_dag`/`remote_dag_status`——拓扑调度、
  success/lenient 双策略、策略违规传递跳过、环拒绝、dag 记录落盘。
  踩坑：策略违规（partial 依赖）曾卡死 pending——补传播规则。
- 报告历史/对比：`remote_reports(list|get|diff)`，diff 含 regressions 判定。
- 真机验收新增 T11（远端 clone）/T12（DAG 链）/T13（报告 diff）。52 测试。

### 迭代二 v0.3.0（feature/v0.3-rda-daemon）
- rda_daemon.py：常驻 daemon（TCP loopback + token，daemon.json 0600 +
  心跳），goal 事件缓冲 + 落盘 + attach 重放（seq 序号），inject/status/list/
  shutdown；client-bridge（os.read stdin 避免读预缓冲；read1 socket 泵避免
  BufferedReader 凑满 4096 才返回）。
- 传输层双通道：TcpDaemonChannel（本地测试直连）+ SshDaemonChannel（SSH 桥），
  共享 _DaemonLink 行框架（握手与读流共用缓冲——修复旧实现丢弃同 chunk 后续行）。
- GoalRun._pump_daemon：断线 → 重连 + attach + seq 去重重接循环，预算
  RAO_RESUME_MAX；RAO_TRANSPORT_MODE auto|spawn|daemon（auto 探测回退）。
  真机验收 T14（daemon 闭环）/T15（断线重接恢复）/T16（stop）。59 测试。
  踩坑：远端 cmd_start 未建 ~/.rao 目录；ping 的 _local_client 超时异常未捕获
  导致永远"失败"→ ensure_daemon 反复拉孤儿 daemon；bridge BufferedReader 读流
  阻塞——全部修复并记录。

### 迭代三 v0.4.0（feature/v0.4-hardening）
- 凭据链：env > OS keyring（可选依赖，缺失优雅降级）> 配置文件；
  devices 与 transport 统一走一条链。
- 规划器插槽：goal.planner='remote-llm'——RDA 优先跑 RDA_PLANNER_CMD env 或
  ~/.rao/planner.json 下发的命令（模板 {objective}/{recon_file}），产出的
  plan 校验后执行；任何失败回退 recon+ask 并记录假设。真机 T17 端到端。
  68 测试全绿。

### 留给下一棒的话
- 真机偶发（<5%）在 daemon 测试上多为时序竞态，已修三处（见上），如再遇到
  先看 serve-stderr.log 与 RAO_WIRE_DEBUG=1 的 wire 日志。
- MCP 工具面仍是 6 个（DAG/报告/daemon 由现有工具组合覆盖）；如需独立暴露
  dispatch_dag 给主 agent，在 server.py 加薄封装即可。

## 2026-11-21：产品化三个阶段（v0.5 / v0.6 / v0.7）

按用户产品化要求（跨 Agent 独立工具），完整讨论后锁定决策（PRODUCT.md §2），
分三阶段实现。**重要澄清**：编排始终是两层——ZCode/Claude 主 agent 是语义层
"大脑"，Python Orchestrator 是机制层"运行时"（看门狗/并发/预算必须确定性代码）。

### v0.5.0（feature/v0.5-cli-setup）：rao CLI + 注入 + 自动补部署
- rda 运行时 + Skill 收编进包（src/rao/runtime、src/rao/data）→ 可分发
- rao CLI：setup / devices add|list|remove / doctor / uninstall / mcp
- agents.py：检测（PATH+配置目录+桌面安装位置）、备份→合并→幂等写、
  manifest 回滚（uninstall 精确还原）
- provision.py：远端 RDA 哈希比对 + 自动补部署，挂入 GoalRun 在 open_session 之前
  **踩坑**：初次挂到了 open_session 之后 → 远端 cd 失败（远端 shell 是 zsh，
  报"没有那个文件或目录"）→ 调整顺序后 T19 稳定通过
- 真机：rao doctor 检出"远端有 zcode-server、无 claude"；
  rao setup 真实注入 zcode（幂等）+claude，回滚→重装全生命周期演练

### v0.6.0（feature/v0.6-agent-backends）：设备复用 + 能力矩阵 + claude backend
- zcode_store.py：只读导入 ZCode 桌面设备（setting.json）+ 凭据解密
  （AES-256-GCM；Windows 凭据管理器 CredReadW ctypes；候选解码 + GCM 自校验）
  **踩坑**：GCM tag 在密文尾部 16 字节，测试 seal 初版切分反了
- 设备表合并（本地优先，RAO_MERGE_ZCODE_DEVICES=0 关）；凭据链四级
- goal.agent 字段 + RDA claude backend（整目标托管：claude -p stream-json →
  事件映射；maxSteps→max-turns；timeoutMs→击杀看门狗；远端无 claude 快速失败）
- devices_list 能力矩阵（is_agent:false 诚实字段）+ Skill 硬规则
  **踩坑**：测试 shim 的 .cmd 路径反斜杠被转义链搞成 formfeed → 改 %~dp0 方案
- 真机 T20（能力字段）/T21（无 claude 快速失败，报错清晰）

### v0.7.0（feature/v0.7-release）：发布就绪
- pyproject 构建 sdist/wheel，twine check PASSED
- plugin/ 目录：ZCode marketplace 分发形态（mcpServers→`rao mcp`，含 Skill）
- Codex 检测留位（agents.detect_codex，注入未实现，明确标注）
- 文档：PRODUCT.md（产品方案）、README 重写（CLI 优先）、HANDOVER 终稿

## 2026-11-22：真实使用问题修复（v0.9.0）

用户在新 ZCode 会话实测（sess_e05cbf8c"查询远程设备列表"）暴露 4 个真 bug + 2 个
易用性问题，全部修复并以真实场景测试锁定：

### 修复清单
1. **[P1] SSH 桥读泵超时死亡**：`socket.create_connection(timeout=5)` 的超时传导给
   makefile → 桥读泵在 5 秒事件静默后 TimeoutError 死亡（v0.8 前静默死、事件流冻结、
   alive() 恒 True 永不重连）。修复：分离读写文件对象（共享 makefile("rwb") 非线程
   安全）+ `sock.settimeout(None)` + 泵死亡即杀桥进程（EOF → orchestrator attach 恢复）。
   本地桥 5 轮 steer 全部送达。
2. **[P2] steer 送达回执**：daemon inject 回执带 queued 状态（delivered/pending/
   dropped）+ engine 未就绪的 pending 队列；_DaemonLink.write_line 同步等回执。
3. **[P2] durationMs 虚高**：finish 公式硬编码 600000 预算 → 改用引擎真实起始时间。
4. **[P2] collect/status 跨进程回载**：持久化 runs/<goalId>.json 回载兜底。
5. **[P2] dispatch 探针/clone 不一致**：protocol 回填到 prepared 基底再 deepcopy。
6. **[ops] daemon 旧内存代码**：provision 上传后远端 daemon 仍跑旧代码（哈希比对
   上传后恒等，永远检测不到）→ daemon 自报 codeHash（daemon.json）→ 不一致即重启。
7. **[P1] 控制面/数据面分离**：steer/cancel 改走独立短连接（`rda_daemon.py
   inject-one`，fresh on-device connection + 回执确认），不再与事件桥共享通道。
8. **[产品] `rao run` CLI**：一条命令远端执行（无 MCP 依赖，手机端会话经 Bash 可用）。
9. **[产品决策] 移除 ZCode 设备/凭据导入**：设备仅由用户配置（用户明确要求）。

### 验证
- test_real_usage.py：RU1（用户原始 HTTP 场景全链路）/ RU2（门闩 steer ×3 轮）/ 
  RU3（durationMs）/ RU4（跨进程 collect）/ RU5（无 protocol dispatch）/ RU6（rao run）
- 全量 84 测试绿；真机 daemon 时间线核实（sleep 20 真实执行 20s，steer 竞态由
  门闩模式消除）

## 2026-11-22（续）：远端 Claude 落地 + 真实场景修复（v0.10.0）

### 远端 Claude 安装
- Node v10 → v20.20.2（NodeSource，无 npm → sudo 装 nodejs）
- Claude Code 2.1.197 → 自更新升 2.1.330（原生二进制分发，bin/claude.exe 381MB）
- API 模式配置部署：~/.claude/settings.json（env: ANTHROPIC_API_KEY/BASE_URL/MODEL=
  deepseek-flash），chmod 600；用户填 key/URL 后冒烟成功（DeepSeek 网关接受
  deepseek-flash[1m] 模型串）

### 三个修复（本地桥确定性复现 → 真机验证）
1. **桥读泵 5 秒超时死亡（B1 终版根因）**：`socket.create_connection(timeout=5)` 的
   超时传导给 makefile → 读泵在 5 秒事件静默后 TimeoutError 死亡 → os._exit(3) →
   事件流冻结。修复：`sock.settimeout(None)`（事件流阻塞读）。
2. **queue_steps 重算条件 `<` bug**：steer 的 run: 指令追加后恰逢长度相等 → 不重算 →
   steered 步骤被吞。修复为 `!=`。
3. **claude 探测回退缩进错误**：回退循环命中包内二进制后仍无条件报 not found
   （emit/return 缩进在 for 循环后、if 块内）→ 修复缩进 + 回退候选加入包内二进制
   （claude 自更新会删除 /usr/bin/claude 链接并原地更新包内二进制）。
4. **daemon 多实例堆积**：provision 重启失效多次累积 → 不同版本 daemon 并存 →
   goal 桥随机连旧实例 → "claude CLI not found" 取决于桥命中哪个实例。
   修复：单实例保证（pkill 所有 serve → 确认全停 → 启动一个 → ping 验证）。
5. **事件镜像对账（v0.9.1）**：goal 终态后从 daemon status-one 回载权威事件缓冲
   （SSH 流事件丢失的观测兜底），event_seqs 填 1..N。

### 真实场景测试（test_claude_real.py，真机 Claude 全链路）
- TC-A claude backend 冒烟 / TC-B 开放性任务自主执行（创建脚本→运行→写工件，
  真机 artifact 断言）/ TC-C 运行中取消 / TC-D constraints 透传
- T21 改造：回退探测命中包内二进制后，需同时移走链接与包内二进制才能模拟"无 claude"
- 90 测试全绿；tag v0.10.0 推送 GitHub
