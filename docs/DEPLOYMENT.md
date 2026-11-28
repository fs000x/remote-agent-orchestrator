# rao 部署手册：从零到真机跑通（Node.js 版）

> **v1.0.0 起本仓库为 Node.js/TypeScript 重写版**。控制机安装从 pipx 改为
> npm；**远程设备运行时改为 `rda.mjs`/`protocol.mjs`/`rda_daemon.mjs`/
> `noise.mjs`（Node >= 18，零依赖，provision 自动上传）**——设备不再需要
> Python。Python 版归档于 `legacy/python-v0.11.1`。
> 仓库不变量见 [../AGENTS.md](../AGENTS.md)，测试政策见
> [TEST-POLICY.md](TEST-POLICY.md)。

## 0. 概念与拓扑

```
控制机（ZCode / Claude Code 所在）
  rao CLI ──安装/配置──▶ ~/.zcode、~/.claude 注入（MCP + Skill）
  rao mcp（MCP server，会话启动时拉起）
        ├── SSH Transport ──▶ 远程设备 A（rda_daemon 或一次性 rda.py）
        └── 节点主控 :9700 ◀── 远程设备 B（daemon 主动拨出注册，transport=node）
```

两种接入方式任选（也可并存）：

| 方式 | 方向 | 适合 |
|---|---|---|
| **SSH**（默认） | 控制机 → 设备 | 设备可被控制机 SSH 直达（局域网/跳板机） |
| **节点模式** | 设备 → 控制机（拨出） | 设备在 NAT/防火墙后、SSH 不可达；或设备主动纳管 |

---

## 1. 控制机准备

### 1.1 依赖（控制机）

- Python **3.10+**
- 想接入的 Agent 宿主：ZCode 桌面端，或 Claude Code
- （SSH 模式）能 `ssh user@device` 登录设备（密码认证即可）

### 1.2 安装 rao

```bash
# 方式 A：发行包（推荐给最终用户）
npm install -g rao

# 方式 B：源码开发态
git clone <repo> && cd remote-agent-orchestrator
npm install
npm run build
npm link          # 全局 rao / rao-relay 命令
```
验证：

```bash
rao --version        # 应输出 rao 0.11.0
```

> 源码开发态下把下文所有 `rao` 替换为 `./.venv/Scripts/rao.exe`（Windows）
> 或 `./.venv/bin/rao`（POSIX）。

---

## 2. 远程设备准备（需要 Node >= 18）

在每台设备上确认：

```bash
node --version    # 需 ≥ 3.8（daemon/rda 为 stdlib-only，无需 pip 安装任何东西）
uname -sm            # Linux/macOS
whoami; hostname     # 记下用户名与主机名（节点模式下主机名 = 节点 id）
```

可选（只有想用「agent 模式」时才需要）：`gcc/make` 等常规工具链。

**不需要预装任何东西**：rao 会在首次派发前自动把运行时（rda.py / rda_daemon.mjs /
protocol.py）经 SFTP 上传到设备的 `rdaDir`（默认 `$HOME/.rao/runtime`）。

---

## 3. 配置设备

### 3.1 交互式添加（推荐）

```bash
rao devices add
```

依次输入：设备 id（如 `dev-a`）、主机、端口、SSH 用户、密码、workspace 绝对路径。
写入 `config/devices.json`（已 gitignore）。

### 3.2 或手工编辑 `config/devices.json`

```jsonc
{
  "settings": {
    "transportMode": "auto",          // auto | spawn | daemon
    "rdaDir": "$HOME/.rao/runtime",   // 可被单设备 rdaDir 覆盖
    "runsDir": "runs",
    "masterPort": 9700                // 节点主控端口
  },
  "devices": [
    {
      "id": "dev-a",
      "host": "192.168.1.50", "port": 22, "user": "alice",
      "workspace": "/home/alice/project",
      "rdaDir": "/home/alice/.rao/runtime",
      "maxConcurrentGoals": 2,
      "auth": { "type": "password", "password": "your-password" }
    }
  ]
}
```

**凭据链**：OS keyring（可选，`pip install "rao[keyring]"`）→ 配置文件。
推荐把密码放进 keyring 后删除配置里的 `auth.password`：

```python
python -c "import keyring; keyring.set_password('rao','dev-a','your-password')"
```

### 3.3 验证设备可用性

```bash
rao devices list     # online=True，creds=True
rao doctor
```

`rao doctor` 应显示：

```
=== 本地 Agent ===
  zcode    installed=True injected=...
  claude   installed=True injected=...
=== 设备 ===
  dev-a    online=True auth=True
    RDA 运行时: 就绪            # 首次可能显示"缺失"，派发时会自动补部署
    远端 Agent: ['zcode-server']  # 或 ['/usr/bin/claude', 'zcode-server']
```

---

## 4. 首次真机跑通（不接 Agent，先用 CLI 验证）

```bash
rao run --device dev-a "uname -a && df -h /home | tail -1"
```

预期输出：

```
[rao] status: success
[rao] summary: all steps ok
[out] Linux ... 
```

这一步会完成：自动上传远端运行时 → 启动 daemon（或 spawn）→ 执行命令 → 回收结果。
**若此步成功，SSH 链路、凭据、运行时部署全部就绪。**

---

## 5. 接入 Agent 宿主（MCP + Skill）

### 5.1 注入

```bash
rao setup
# 或只给指定宿主：rao setup --agents zcode,claude
```

它做三件事（写前自动备份到 `~/.rao/backups/`，可回滚）：

1. 在 `~/.zcode/cli/config.json`（`mcp.servers`）或 `~/.claude.json`
   （`mcpServers`）写入 MCP 条目：`<python> -m rao.server`
2. 部署 Skill 到 `~/.zcode/skills/` 或 `~/.claude/skills/`
3. 写 `~/.rao/manifest.json`（回滚依据）

### 5.2 生效

**重启宿主会话**（新开一个对话即可）。验证：ZCode「设置 → MCP」应显示
`remote-agent-orchestrator` 已连接、工具数 9。

### 5.3 使用

在会话中自然语言派发，例如：

```
在 dev-a 上检查项目构建产物并收集最近 50 行日志
```

主 Agent 会走：`devices_list` → 写计划 → `remote_dispatch` → `remote_status`
→ `remote_collect`，Skill 会强制它按「executor 不是 Agent」的规则拆解命令级步骤。

> **注意**：由协议/手机端创建的会话可能不加载用户级 MCP 配置（宿主行为）。
> 这类会话可直接用 §4 的 `rao run`，它不依赖 MCP。

---

## 6. 升级到 agent 模式（远端 Claude 托管，可选）

executor 模式只执行「你写好的步骤」；若想让远端自主完成开放性目标，需要在
设备上安装 Claude Code。

### 6.1 安装 Node（若无合适的 Node）

设备上检查 `node -v`（需 ≥ 18，建议 20）：

```bash
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt-get install -y nodejs
node -v && npm -v
```

macOS：`brew install node`（或官方安装包）。

### 6.2 安装 Claude Code

```bash
sudo npm install -g @anthropic-ai/claude-code
claude --version
```

> 权限说明：NodeSource 的全局前缀在 `/usr/lib`，需 `sudo`。

### 6.3 认证：二选一

**A. 订阅账号 OAuth（交互一次）**

```bash
ssh user@device
claude        # 按提示在浏览器完成授权；凭据持久保存在设备
```

**B. API 模式（推荐给服务器/无人值守）**

写 `~/.claude/settings.json`：

```json
{
  "env": {
    "ANTHROPIC_API_KEY": "sk-...",
    "ANTHROPIC_BASE_URL": "https://your-anthropic-compatible-endpoint",
    "ANTHROPIC_MODEL": "your-model-id"
  }
}
```

```bash
chmod 600 ~/.claude/settings.json
```

> BASE_URL 必须提供 **Anthropic Messages 协议**（不是 OpenAI 格式；需要网关转换）。
> MODEL 必须与网关侧模型 id 完全一致。

冒烟：

```bash
cd ~ && claude -p "只回复两个字：成功"
```

### 6.4 在 rao 中使用

```python
goal = {
  "agent": "claude",              # 整目标托管
  "objective": "把当前目录的构建跑通并修复失败用例",
  "workspace": "/home/user/project",
  "constraints": {
    "maxSteps": 40,               # 映射为 --max-turns
    "timeoutMs": 600000,          # 硬超时（到点杀进程）
    "claudeAutoApprove": True,    # headless 自主使用工具（--dangerously-skip-permissions）
    "forbidden": ["git push"]     # 软约束（注入 system prompt）
  }
}
```

在会话里最简说法：

```
在 dev-a 上把构建跑通并修复失败用例（用远端 Claude 自主执行）
```

---

## 7. 节点模式（设备主动拨入）

适合设备无法被 SSH 直达的场景。

### 7.1 主控侧

节点主控内嵌在 `rao mcp` 中，随宿主会话启动（或手动 `rao mcp` 前台运行）。
监听 `settings.masterPort`（默认 `9700`，绑 `0.0.0.0`）。

取注册令牌：

```bash
cat ~/.rao/master-token
```

### 7.2 设备侧

先把运行时放上设备（任一方式）：

```bash
# A. 从控制机推送（一次性，需设备已在 devices.json）：直接派发任意 goal，
#    provision 会自动上传 src/runtime/*.mjs
# B. 或手工拷贝 src/runtime/ 的四个文件（rda.mjs / protocol.mjs /
#    rda_daemon.mjs / noise.mjs）到设备 ~/.rao/runtime/
```

在设备上启动节点（拨出）：

```bash
cd ~/.rao/runtime
node rda_daemon.mjs start --master <控制机IP>:9700 --token <注册令牌>
node rda_daemon.mjs ping
```

### 7.3 验证

- 主控侧日志出现 `registered with master ...`；`~/.rao/nodes.json` 出现节点记录
- `rao devices list` 中自动出现该设备（`source=node`，id = 设备主机名）
- 直接派发：`rao run --device <主机名> "hostname"`

### 7.4 配对 + 端到端加密（Noise_IK，推荐）

明文注册令牌只防"陌生人注册"，不加密链路。跨公网部署请先配对：

```bash
# 控制机（master 监听已随 rao mcp 运行）：
rao pair --node <设备主机名>          # 打印一次性 6 位配对码（5 分钟有效）

# 设备：
node rda_daemon.mjs pair --master <控制机IP>:9700     --node-id <设备主机名> --code <配对码>
# 生成/复用设备端 Noise 静态密钥 -> ~/.rao/node-key.json
# （登记的是设备本地生成的公钥；服务端只做密钥持有登记）

# 重启节点（serve 自动读取 node-key.json，走 Noise_IK 加密链路）：
node rda_daemon.mjs stop
node rda_daemon.mjs start --master <控制机IP>:9700
```

安全语义（normative）：

- 配对码一次性、绑定 nodeId、5 分钟过期；旧密钥配对轮换后立即失效。
- Noise_IK 握手 prologue 绑定 nodeId；initiator 静态身份必须与登记公钥一致；
  握手重放（同 ephemeral 复用）与数据帧重放/篡改一律断链（fail-closed）。

### 7.5 盲转发 relay（双方都无法被直连时）

```bash
# 公网机器：
rao-relay --port 9900          # join 后字节原样转发，永不解密
# 主控与设备各自把拨出目标指向 relay 的 linkId（部署层面约定）
```

中继只解析每连接第一行 join JSON；其后所有字节（含 Noise 握手与密文帧）
在两端之间原样转发——中继运营者看不到任何明文。

> 节点模式下 `devices.json` 仍可对该设备做策略覆盖（workspace / 并发上限），
> 手工条目优先于注册信息。

---

## 8. 验收清单

| 检查 | 命令 | 期望 |
|---|---|---|
| CLI 可用 | `rao --version` | `rao 0.11.0` |
| 设备在线 | `rao devices list` | `online=True creds=True` |
| 远端运行时 | `rao doctor` | `RDA 运行时: 就绪` |
| 单命令闭环 | `rao run --device X "uname -a"` | `status: success` |
| Agent 注入 | `rao doctor` | `injected=True` |
| 会话工具 | 宿主「设置 → MCP」 | 已连接，9 工具 |
| （可选）agent 模式 | `claude -p "回复：ok"`（设备上） | 返回文本 |
| 自测 | `pytest tests/ -q` | `90 passed`（真机用例在设备可达时执行） |

---

## 9. 运维与排障

### 9.1 远端 daemon 单实例

rao 在每次派发前会**确保只有一个 daemon 实例**（先停掉全部 `serve`，再启动一个）。
若手工操作导致多实例：

```bash
pkill -f 'rda_daemon.mjs serve'
node ~/.rao/runtime/rda_daemon.mjs start
```

### 9.2 `claude CLI not found`

- 检查：设备上 `claude --version`
- Claude Code 自更新会**删除 `/usr/bin/claude` 链接**（npm 原生二进制分发）。
  rao ≥0.10 会自动回退到包内二进制
  `/usr/lib/node_modules/@anthropic-ai/claude-code/bin/claude.exe`；
  若两者都不存在，重装：

  ```bash
  sudo npm install -g @anthropic-ai/claude-code
  sudo ln -sf /usr/lib/node_modules/@anthropic-ai/claude-code/bin/claude.exe /usr/bin/claude
  ```

- 顺手关闭自更新：`~/.claude/settings.json` 加 `"autoUpdates": false`

### 9.3 改了运行时但远端行为没变

daemon 常驻会保留旧内存代码。rao 通过 `codeHash`（三文件联合哈希）自动重启；
手工触发：

```bash
cd ~/.rao/runtime && node rda_daemon.mjs stop && node rda_daemon.mjs start
```

### 9.4 会话里看不到 MCP 工具

1. 确认 `rao setup` 已注入，重启宿主会话（配置在会话启动时加载）
2. ZCode「设置 → MCP」查看该条目状态与报错
3. 协议/手机端创建的会话不加载用户级 MCP：改用 `rao run`

### 9.5 节点模式连不上

- 设备侧 `nc -vz <master-ip> 9700`
- 主控防火墙放行 9700；跨公网请先建 VPN/隧道（v0.11 为 LAN 明文 + 令牌）
- 令牌不一致会回 `AUTH_FAILED`：重新 `cat ~/.rao/master-token`

### 9.6 日志位置

| 位置 | 内容 |
|---|---|
| 控制机 `runs/<goalId>.json` | 每个 goal 的事件流 + result |
| 控制机 `runs/report-*.json` | collect 聚合报告 |
| 设备 `~/.rao/daemon.log` | 远端 daemon 日志 |
| 设备 `~/.rao/goals/<goalId>.json` | 远端权威事件缓冲 |

### 9.7 超时与预算

- `timeoutMs`：整体硬超时，到点强杀并保留已收事件（`status=timeout`）
- `maxSteps`：步骤预算（含重试）；claude backend 映射为 `--max-turns`
- 事件静默看门狗 90s（可配 `eventSilenceMs`）

---

## 10. 卸载与回滚

```bash
# 回滚 Agent 注入（恢复注入前备份、移除 Skill、删除 manifest 记录）
rao uninstall --agents zcode,claude

# 远端停止 daemon（可选）
ssh user@device "pkill -f 'rda_daemon.mjs serve'"

# 远端删除运行时（可选）
ssh user@device "rm -rf ~/.rao/runtime"
```

`rao uninstall` 只回滚 rao 写入的内容，不触碰你的其他配置。

---

## 附录：端口与目录速查

| 项 | 默认 |
|---|---|
| 远端 daemon 监听 | `127.0.0.1:<随机端口>`（仅回环） |
| 节点主控监听 | `0.0.0.0:9700`（settings.masterPort） |
| 远端运行时目录 | `$HOME/.rao/runtime`（settings.rdaDir / 设备 rdaDir） |
| 远端状态目录 | `$HOME/.rao/`（daemon.json / goals / master-token） |
| 控制机运行记录 | `<项目>/runs/`（settings.runsDir） |
| 注入备份 | `~/.rao/backups/` |
| 注入清单 | `~/.rao/manifest.json` |
