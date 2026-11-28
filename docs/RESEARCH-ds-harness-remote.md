# 调研：ds-harness-remote（liguobao）远程方案对比分析

> 克隆位置：`ai_prj/ds-harness-remote`（1846 提交，674 个 TS 文件，249 个测试文件 / 约 9.7 万行测试代码）
> 性质：DeepSeek Harness 的「远程窗口」产品——手机/电脑/浏览器远程访问本机上已运行的 Harness 实例。
> 本文回答两个问题：它的远程方案是怎么实现的；对 rao 有什么可借鉴。

---

## 一、它的远程方案是怎么实现的

### 1.1 三个网络平面（核心设计）

```
REST 平面    HTTPS：账号/设备注册/presence/ICE 凭据下发
Control 平面 双向出站 WSS：presence、signaling、Relay 路由
数据平面     WebRTC DataChannel（LAN/P2P/TURN 直连）或 WebSocket Relay（兜底）
```

- **Host 永不监听公网端口，也不需要端口转发**。Host 与 Client 都主动向 Relay Server 建立出站连接。
- 选路优先级：`LAN → P2P → TURN → Relay`，`AdaptiveTransport`（1010 行）实现竞速与降级：
  仅对 Server 精确 `4001/CONNECTION_FAILED` 重试并重握 Noise；其余错误不重试。
- **Server 盲转发**：`apps/server` 仅 2090 行，零解密——只搬运 opaque Noise 帧 +
  presence/signaling/TURN 凭据下发。安全不变量写死："Remote RPC/Event 不以明文经过或落盘到 Server"。

### 1.2 端到端加密

- `Noise_IK_25519_ChaChaPoly_SHA256`（`packages/crypto/src/noise.ts`，仅 190 行）。
- identity 绑定连接：Noise prologue 混入 `connectionId + hostDeviceId + clientDeviceId`，
  重放/乱序/身份不匹配一律 fail-closed。
- Server membership 与 Host 本机 trusted peer **必须同时成立**。

### 1.3 规范化协议文档（最值得学的工程实践）

`docs/protocol.md` 27 节、2000+ 行，是**规范性（normative）**文档：

- 全部 RPC 方法与请求/响应 schema（§16/§19/§20）；
- 错误码分域清单 + `retryable` 语义（§23）："只表示稍后重试可能成功，
  **不代表应自动重放非幂等请求**"；
- 集中化的默认限制表（§24）：帧大小、pending RPC 上限、心跳间隔、上传限额……
  超限必须返回稳定错误，"不能无限缓存"；
- **安全不变量 15 条**（§25），例如"必须编译期命中 allowlist，禁止反射扩权"、
  "日志禁止记录 token/prompt/工具输出"；
- **Conformance 测试**（§26）+ `fixtures/` golden vectors——客户端/宿主/Android
  各 runtime 用同一套向量做一致性测试。

### 1.4 操作账本（operationId 幂等）——与我们踩过的坑完全同构

```ts
type Receipt = { operationId, status: 'pending' | 'accepted' | 'rejected' | 'unknown', ... }
```

- 所有 mutation（send/interrupt/create/upload）必须带 `operationId`（16-128 字符）；
- **同 key + 同参数 → 返回同一回执；同 key 改参数 → 拒绝**；
- 文档明示语义："`accepted` 表示已排队，**不代表已完成**"；
  "回执在 Remote 重置后丢失；`not_found` 不是未执行过的证明"；
- 实现：内存账本（`LedgerEntry = {fingerprint, receipt, pending}`），只存哈希/目标/回执，
  **从不存消息体**。

这正是我们在 v0.9/v0.10 里踩的 inject 送达竞态的成熟解法。

### 1.5 面向宿主 Agent 的 Remote Tools

插件向 Harness 主 Agent 注册一组工具（Agent 自己决定何时调用）：

| 工具 | 语义要点 |
|---|---|
| `remote_list_hosts` / `remote_list_workspaces` | 设备与工作区发现（无参/带 hostId） |
| `remote_list_presets` | 列出远端可用的 agent preset（`harness` / `codex` / ACP 后端） |
| `remote_send_message` | 排队一个任务（`accepted` = 已排队非完成） |
| `remote_wait_sessions` | **批量观察最多 8 个会话、最长 60 秒**；零请求即快照 |
| `remote_interrupt_session` | 请求中断（"不撤销编辑、不删排队消息"） |
| `remote_upload_file` / `remote_download_file` | 受限文件传输（LAN 1 GiB / Relay 20-100 MiB 分档） |
| `remote_get_operation` | 按 operationId 查回执，不重复执行 |

### 1.6 多后端（ACP）与多客户端

- 通过 ACP（Agent Client Protocol）接入 Claude Code / Cursor / Codex / OpenCode 等，
  每个 backend 独立数据面，UI 仅内存投影，**禁写宿主自己的存储**；
- 客户端矩阵：Android（Expo）/ iOS / Web / HarmonyOS（ArkTS+WebRTC）/ VS Code / TUI；
- 共享远端终端（1,000 行 xterm 快照 + 输入权独占 + 2h 回收）。

### 1.7 规模与成熟度

1846 提交 · 674 TS 文件 · **249 个测试文件 / 约 9.7 万行测试代码** ·
Android/iOS/Web/Harmony/VSCode 五端客户端 · npm 发布 · Docker 自部署 ·
Trendshift 榜单。是成熟产品而非实验项目。

---

## 二、与我们方案（rao）的对比

### 2.1 问题域不同——这是根本差异

| | ds-harness-remote | rao |
|---|---|---|
| 要解决的问题 | **远程访问**本机上已运行的会话（会话在 Host 上跑，远程只是窗口） | **派发任务**到远程设备的执行者（工作发生在设备上） |
| 单位 | 会话（session/message） | 目标（goal/步骤程序/工件） |
| 远端智能 | 远端就是原 Harness（本来就强） | 三档：builtin 执行器 / 远端 Claude / （无） |
| 并行 | 多客户端连同一 Host（会话隔离） | 多设备多 goal 并行 + DAG |
| 审计 | Harness 自身历史 | rao 的 runs/ + collect 报告 |

结论：**不是竞品，是邻近域**。它的传输/安全/协议工程可直接借鉴；我们的
goal/budget/DAG/collect 管道它也没有。

### 2.2 传输层对比

| 维度 | DSHR | rao 现状 | 差距 |
|---|---|---|---|
| 接入方向 | 双向出站（Host 也不监听公网） | SSH（控→设）+ 节点拨出（设→控） | 对齐 ✓（节点模式同思路） |
| 加密 | Noise_IK E2EE，Server 盲转发 | LAN 明文 + 令牌；SSH 路径靠 SSH 加密 | **节点模式明文是最大差距** |
| 选路 | LAN→P2P→TURN→Relay 竞速降级 | 无协商（SSH 直连 / 节点拨出二选一） | 可借鉴选路思想 |
| Server | 盲转发 Relay（2090 行，可自部署 Docker） | 无独立 Server（master 内嵌 rao mcp） | 可借鉴独立 Server 形态 |
| 断线语义 | AdaptiveTransport 仅对 `4001` 重试+重握 Noise；replay buffer 明示不提供，恢复读 baseline | 静默看门狗 + attach 重放（daemon 权威缓冲） | 语义相近，我们的对账回载等价 |

### 2.3 协议工程对比

| 维度 | DSHR | rao 现状 |
|---|---|---|
| 协议文档 | 27 节规范性文档 + conformance 测试 + fixtures | PROTOCOL-NODE.md 约 140 行 |
| 错误码 | 分域稳定错误码 + `retryable` 语义 + 扩展前缀约定 | 少量错误码，无 retryable 语义 |
| 限制 | 集中限制表（帧/并发/心跳/上传），超限稳定报错 | 分散在代码常量 |
| 幂等 | operationId 账本 + 回执语义成文 | inject 回执刚有，无账本 |

---

## 三、rao 可借鉴清单（按优先级）

### P1 — 直接解决我们已踩的坑

1. **operationId 幂等账本**（§1.4）：`remote_dispatch` / `remote_steer` 接受
   `operationId`；同 key 同参 → 同回执；同 key 改参 → 拒绝；回执丢失后可用
   `remote_get_operation` 查询（等价我们的 remote_status，但语义按"操作回执"收敛）。
   配套文档化三句语义：queued≠completed / 回执可丢 / not_found≠未执行。
   这是对我们 v0.9"steer 送达竞态"的体系化收口。

2. **错误码分域 + retryable 语义**（§23）：把现有错误码整理为
   Protocol/Auth/Connection/Harness 四域，给 collect/status 的失败加
   `retryable` 标志，并在 SKILL.md 里写明"retryable 不代表可自动重放非幂等操作"。

3. **集中限制表**（§24）：把散落的预算常量（maxSteps/timeoutMs/事件静默 90s/
   maxConcurrentGoals/帧上限）收敛成文档化的限制表 + 稳定错误码
   （`RATE_LIMITED` / `FRAME_TOO_LARGE`），避免"代码里的魔法数"。

### P2 — 节点模式增强（对齐 DSHR 的传输安全）

4. **Noise_IK E2EE**：节点协议（JSON Lines + 令牌）外套 Noise 帧加密。参考其
   `noise.ts`（190 行）与 fail-closed 规则；LAN 明文作为 v0.11 的已知限制已记录，
   这是升级路径。
5. **独立盲转发 Server**：`apps/server` 形态（2090 行）证明"账号/ presence /
   信令/中继"可以做成一个 2k 行的自部署组件（含 Docker）。rao 若要跨公网节点
   接入，可按此形态拆出独立 server（不内嵌 rao mcp）。
6. **设备配对（二维码 + 一次性码）**：device-pairing-v1 的信任边界设计
   （server 只验密钥持有，验证码不入日志，配对受限设备不能复活）可直接借用。

### P3 — 工具面与工程纪律

7. **`remote_wait_sessions` 式批量观察工具**：一次调用观察多个 goal（≤8 个、
   ≤60s、零请求即快照），比主 Agent 逐个 `remote_status` 轮询省 token。
8. **AGENTS.md 式仓库规约**：把"架构不变量/fail-closed 清单/测试政策/禁写边界"
   写成给编码 Agent 的规范性文档（DSHR 的 AGENTS.md 是最佳范本——含
   "已实现/禁止实现"边界、逐条安全不变量、日志禁令）。我们的 SKILL.md 只约束
   运行时 Agent，缺一份约束"在仓库里干活的 Agent"的文档。
9. **测试政策成文**：DSHR 明确"预算只给协议编解码/加密/鉴权/RPC 关联/传输状态机；
   纯 UI 不写单测；共享实现只在所属包测一次"。值得写进我们的测试约定。

### 明确不借鉴

- 多平台客户端矩阵（Android/iOS/Harmony/VSCode）——超出 rao 的任务编排定位；
- P2P WebRTC 数据面——我们的 goal 是分钟级任务，SSH/节点明文（升级 Noise 后）
  已够；WebRTC 复杂度收益比不划算；
- ACP 多后端接入——我们已有 builtin/claude 双 backend；ACP 是好方向但等有
  第三种 agent 需求再引入。

---

## 四、一句话总结

**DSHR 把"远程访问"做成了一个传输/安全/协议工程问题（Noise E2EE、盲转发、
选路竞速、operationId 账本、规范协议文档），rao 把"任务派发"做成了编排问题
（预算、步骤程序、DAG、聚合报告）。两者互补。**

最值得立刻搬的三件事：
1. operationId 幂等账本（含回执语义文档化）——收口 steer/dispatch 竞态；
2. 错误码分域 + retryable 语义——统一各处散落的错误处理；
3. `rao doctor`/SKILL 之外的第三份文档：给"在仓库里干活的编码 Agent"的
   AGENTS.md 架构规约。
