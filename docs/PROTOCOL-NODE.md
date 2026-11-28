# rao 节点模式协议设计（v0.10 并存架构）

> **v1.0.0 变更**：注册行可携带 `sec:"noise-ik-v1"` + `e/s/pl`（Noise_IK
> ik1 帧）发起加密注册；主控回 `registered` + `e/pl`（ik2）；其后所有消息
> 以 `{"t":"d","n":<计数>,"ct":<base64>}` 密文帧双向传输。配对流程见
> docs/DEPLOYMENT.md §7.4（一次性码 -> 设备公钥登记）。

- 状态: v0.10 实现依据
- 决策记录: 单主控星型拓扑；注册令牌→节点令牌；TCP+JSON Lines+令牌（LAN 明文，TLS 后续）；
  设备清单并存（node 注册自动出现 + devices.json 策略覆盖）；SSH Transport 原样保留

## 1. 角色与部署形态

```
Agent 宿主（ZCode/Claude 会话所在机器）
  rao mcp ── 同进程内 ── rao master 监听服务（TCP :9700，节点控制面）
                              ▲│
              注册/心跳/goal/事件│▼ 出站连接（节点主动拨入）
被控端点（任意 OS，安装 rao）
  rda_daemon --master host:port --token <注册令牌>
  （常驻、心跳、goal 缓冲、attach 重放——v0.3 内功全部复用）
```

- **控制端点** = Agent 宿主上的 `rao mcp`（内嵌 master 监听线程）+ `rao master`（独立运行）
- **被控端点** = 装了 rao 的设备，daemon 以 `--master` 拨出主控
- SSH Transport 原样保留并存（devices.json `transport:"ssh"`）；节点注册设备 `transport:"node"`

## 2. 连接与认证

1. 主控首次 `rao master` 启动：生成注册令牌（`~/.rao/master-token`，0600），打印给用户
2. 节点侧：`rda_daemon.py --master host:port --token <注册令牌>` 拨出
3. 注册握手：`{"cmd":"register","nodeId":...,"token":<注册令牌>,"capabilities":{...}}`
   → 主控校验令牌 → 持久化节点（`~/.rao/nodes.json`）→ 回 `{"type":"registered","nodeToken":...}`
4. 之后该连接的所有消息凭 nodeToken 隐式认证（连接级认证，无需逐条）
5. 断线：节点指数退避重连（0.5→1→2→…→30s 封顶），重连后重新 register（同一 nodeId 覆盖）

## 3. 消息（JSON Lines）

| 方向 | 消息 | 说明 |
|---|---|---|
| 节点→主控 | `{"cmd":"register",...}` | 注册（见上） |
| 节点→主控 | `{"cmd":"heartbeat","ts":N}` | 5s 周期，更新 lastSeen |
| 节点→主控 | `{"type":"event","goalId":G,"event":{...},"seq":N}` | goal 事件（引擎 sink 双写） |
| 节点→主控 | `{"type":"receipt","goalId":G,"ack":A,"queued":...}` | inject 回执 |
| 主控→节点 | `{"cmd":"goal","goal":{...}}` | 派发（引擎线程创建，复用 submit 逻辑） |
| 主控→节点 | `{"cmd":"inject","goalId":G,"payload":{...},"ack":A}` | steer/cancel |
| 主控→节点 | `{"cmd":"ping"}` | 主控侧活性探测 |

## 4. 数据流（goal 生命周期）

```
主控 Agent → MCP 工具 remote_dispatch(--device nid)
  → Orchestrator.dispatch → NodeTransport.open_session(nid)
      → master.get_node_channel(nid, goalId)（inbox deque + 节点 outbox）
      → master 向节点连接发送 {"cmd":"goal"}
  → GoalRun._pump_daemon ← NodeChannel（inbox 逐行 → _pump_main 解析）
节点端: goal → GoalEngine（sink 双写：本地缓冲 + 转发主控）
steer/cancel: master.steer → 节点连接 inject → 节点回执 → 转发 → GoalRun.steer 收到
```

## 5. 与 daemon 现有机制的复用映射

| 现有（v0.3-v0.9） | 节点模式的用途 |
|---|---|
| GoalRecord 缓冲 + seq | 事件转发序号；断线后重放（MVP：转发即走，重放留后续） |
| heartbeat_loop | 节点心跳（同款 5s） |
| inject 回执/pending | 节点连接上的 inject 语义不变 |
| codeHash | register 消息携带，主控比对告警 |

## 6. 设备清单并存

- 节点注册自动并入 DeviceRegistry（`transport:"node"`, `source:"node"`）
- devices.json 手工条目继续生效（SSH 设备 + 对 node 设备的策略覆盖：workspace/并发上限）
- 冲突规则：手工条目 > 注册信息

## 7. 安全（v0.10 范围）

- 注册令牌一次性，防未授权节点接入；节点令牌注册后下发
- 主控绑定 0.0.0.0:9700 可配；LAN 明文 + 令牌（与 SSH 密码方案暴露面相当）
- v0.12: TLS（ssl.SSLContext 包装，协议不变）

## 8. 测试计划

- 本地回环：master + node 同机（tmp home），5 轮 goal/steer/事件连续性（复刻 RU2 门闩模式）
- 注册：令牌错误拒绝、重复注册覆盖、断线重连后重新注册
- 真机：测试设备以节点模式接入主控，goal + steer 端到端
- 既有 84 测试不回归（SSH 路径不动）
