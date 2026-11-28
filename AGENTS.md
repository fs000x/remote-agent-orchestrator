# AGENTS.md — rao 仓库不变量（agents & contributors 必读）

本文件规定本仓库的**工程不变量**。任何 Agent 或人在修改本仓库时必须遵守；
违反不变量的 PR 一律打回。（借鉴 ds-harness-remote 的 normative discipline，
结合 rao 自身的字段教训。）

## 1. 版本与发布

- **版本唯一来源**：`src/version.ts` 的 `RAO_VERSION`。禁止在任何其他文件
  硬编码版本号（README 引用除外，且必须与之一致）。
- 发布流程：改版本 → 全量测试 → 真机 T1-T6（`scripts/device-verify.mjs`）→
  打 tag（`vX.Y.Z`）→ 推送。

## 2. 运行时（部署到设备的东西）

- `src/runtime/*.mjs` 是**部署到远程设备的运行时**：Node ≥ 18、**零依赖**、
  永不从 TypeScript 源导入。`tsconfig.json` 排除该目录；构建时由
  `scripts/copy-runtime.mjs` 原样拷贝进 `dist/runtime/`。
- `provision.RDA_FILES` 与 daemon `computeCodeHash()` 的文件清单必须保持
  一致（当前：rda.mjs / protocol.mjs / rda_daemon.mjs / noise.mjs）。新增
  运行时文件时两处同步改，否则 provision 比对失效。
- wire 协议（`PROTOCOL_VERSION = "rao/1"`）的任何破坏性变更必须升主版本。

## 3. 密码与凭据

- **绝不安装 SSH 公钥**：设备认证只走密码（`devices.json` 的
  `auth.password` 或可选 keyring），无环境变量凭据。
- `config/devices.json` 含明文密码：已在 `.gitignore`，**永远不许提交**；
  测试一律使用临时目录（`RAO_STATE_DIR` / HOME 重定向）。

## 4. 网络层语义（不许回退的字段教训）

以下语义来自真机字段 bug，回退 = 复发：

1. **终端状态粘性**：首个终态裁决生效；迟到的 RESULT 事件不得覆盖
   （orchestrator `setState` + `applyEvent`）。
2. **先裁决后强杀**：orchestrator 侧超时/预算先 `setState` 再 force-cancel。
3. **spawn 模式 stdin=DEVNULL**（`stdio: ["ignore", ...]`）——继承 stdin 会
   挂住子进程（git clone 等）。
4. **daemon 桥与 master 链路禁止空闲超时**：`connect({timeout})` 安装的是
   空闲超时，安静阶段（wait_file）会杀死桥（字段 bug）。用一次性 connect
   guard，连接后 `setNoDelay`，**不设 idle timeout**。
5. **queue_steps 重算用 `!=` 而非 `<`**（steer `run:` 指令追加的步骤数判断）。
6. **桥死亡要 exit(3)**：client-bridge 读泵死亡必须退出，让 SSH 通道 EOF，
   orchestrator 才能走 attach 恢复而不是吊死。
7. **claude 参数表不含 argv[0]**：`spawn(claudeBin, args)` 的 args 不得再含
   二进制路径（否则 CLI 把它当 prompt）。

## 5. 幂等与错误码（P1 borrow，normative）

- **变更型 MCP 工具（remote_dispatch / remote_steer / remote_cancel /
  remote_dispatch_dag）必须要求 `operationId`**，经 `globalLedger` 去重：
  同 key 同参数 → 同回执；同 key 异参数 → 指纹冲突拒绝。
- `retryable=true` 只代表"稍后重试可能成功"，**绝不自动重放非幂等变更**。
- 稳定错误码（`src/protocol.ts` `ErrorCode`）不许改名；新错误必须入表。

## 6. 安全边界

- **盲转发**（src/relay/server.ts）：只解析 join 行，其后字节原样转发；
  中继永远不解密、不缓存载荷。
- **Noise_IK**（src/runtime/noise.mjs）：prologue 绑定 nodeId；initiator
  静态身份必须与登记公钥一致；数据帧计数器严格单调；重放/篡改 = 断链
  （fail-closed）。
- **配对**（pair）：一次性 6 位码、5 分钟 TTL、nodeId 绑定；服务端只做
  密钥持有登记。密钥丢失后的恢复必须走**重新配对**，不存在"受限设备复活"。

## 7. 测试纪律

见 [docs/TEST-POLICY.md](docs/TEST-POLICY.md)。要点：

- 改动 wire/引擎/orchestrator 语义 → 必须带新测试。
- 测试不得访问真实设备/外部网络（真机验证是独立脚本）。
- 测试不得污染真实 `~/.rao`（`RAO_STATE_DIR` 必须指向临时目录）。
- Windows 上 `rm(recursive)` 可能 ENOTEMPTY（异步落盘竞态）——统一带
  `maxRetries/retryDelay`。
