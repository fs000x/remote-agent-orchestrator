# TEST-POLICY — rao 测试政策

（P3 borrow：借鉴 ds-harness-remote 的测试规范文档，结合 rao 真机字段教训。）

## 分层

| 层 | 位置 | 网络 | 覆盖 |
|---|---|---|---|
| L1 单元 | `tests/protocol|ledger|devices` | 无 | schema/账本/注册表纯逻辑 |
| L2 编排 | `tests/orchestrator` / `tests/mcp` (DAG/reports) | Fake 传输 | 生命周期/粘性终态/预算/槽位/DAG/报告/账本 |
| L3 运行时进程 | `tests/runtime_rda` / `tests/runtime_daemon` | 仅 127.0.0.1 | rda.mjs CLI wire、daemon TCP、桥、节点拨出 |
| L4 传输栈 | `tests/transports` | 仅 127.0.0.1（真实 daemon 子进程） | LineStream/通道/ack 注入/MasterServer/TcpDaemon E2E |
| L5 安全 | `tests/crypto-noise` / `tests/noise-link` / `tests/pairing` / `tests/relay` | 仅 127.0.0.1 | Noise_IK 握手/重放/篡改、配对、盲转发 |
| L6 真机 | `scripts/device-verify.mjs` | **真实设备** | T1-T6：provision/echo/steer-skip/cancel/claude/报告 |

规则：

- **L1-L5 不得访问外部网络或真实设备**；一切走 fake 或 127.0.0.1。
- L6 是发布闸门（见 AGENTS.md §1），针对 `config/devices.json` 的真实设备，
  不进 CI。

## 必测语义（改动即必须带测试）

1. 终端状态粘性（迟到 RESULT 不覆盖裁决）。
2. 预算/超时 fail-closed（先裁决后强杀）。
3. operationId 幂等（同 key 同参=同回执；异参=拒绝；并发=单飞）。
4. daemon 重连：seq 去重（重放事件不重复计数）。
5. 并发槽位：全局 + 每设备上限。
6. 注入回执语义（delivered/pending/dropped/timeout）。
7. Noise_IK：prologue/身份绑定 fail-closed、握手重放拒绝、帧计数器、
   篡改断链。
8. 配对：一次性、nodeId 绑定、过期、旧密钥失效。
9. 盲转发：join 后字节原样转发（中继永不解析载荷）。

## 环境卫生（Windows 教训）

- 所有状态目录走 `RAO_STATE_DIR`/HOME 重定向到 `mkdtemp`；用例结束恢复环境。
- `rm(recursive)` 统一带 `maxRetries/retryDelay`（异步落盘竞态 ENOTEMPTY）。
- 跨平台命令用 `node -e "..."` 而非 shell 特有语法；`echo` 语义差异用
  `stdoutContains` 精确断言。
- 子进程 stdin 注入的定时器必须在 close 时清理，避免 unhandled error。
- readline 包装 socket 时必须挂 `rl.on("error")`（readline 会把底层错误
  再发射到接口上，无监听 = uncaught）。

## 当前基线

- 单元/集成：**137 个用例**（`npx vitest run` 全绿）。
- 真机：T1-T6（`node scripts/device-verify.mjs`，diablo-100 验证通过）。
