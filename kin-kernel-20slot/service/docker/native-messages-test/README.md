# native_messages 测试虚拟机

用 Docker 模拟一台干净 Linux 测试机：bun 1.3.14、复用仓库里的 `http_to_socks.py`（HTTP CONNECT → SOCKS5），再挂载 `claude-code-kin` 的 CLI 产物。

Claude CLI 不能把 `socks5://` 当 `HTTPS_PROXY`。容器入口会先起桥，再把 `HTTPS_PROXY=http://127.0.0.1:18080` 交给后续命令。

## 凭证

只允许运行时注入，禁止写进 Dockerfile / compose / 本 README：

- 默认读取仓库根目录 `.local/native.env`（已被 `.gitignore` 的 `.local/` 忽略）
- 或 `docker compose run --env-file /path/to/native.env`
- 覆盖路径：`NATIVE_ENV_FILE=/abs/path/native.env`

`.local/native.env` 至少需要：

```text
KIN_CLAUDE_CODE_OAUTH_TOKEN=sk-ant-oat01-...AA
KIN_SOCKS5=socks5h://user:pass@host:port
KIN_SLOT_TZ=America/New_York
```

setup-token 是 inference-only，不能 refresh。过期只能换新 token。
从 CLI 复制时若带上 “Store this token securely” 文案，请求会 401。值必须裁到以 `AA` 结尾。kernel 也会裁，但容器里的 CLI 走原始 `KIN_CLAUDE_CODE_OAUTH_TOKEN`，不会自动裁。

## 时区

SOCKS5 出口 `72.1.181.43` 归属美国弗吉尼亚 Ashburn（ip-api：Charter / GTT Americas）。IANA 用 `America/New_York`，与草稿一致。入口脚本会把 `KIN_SLOT_TZ` 同步到 `TZ` 和 `CLAUDE_CODE_TIMEZONE`。


## 构建与启动

在本目录：

```bash
cd kin-kernel-20slot/service/docker/native-messages-test
docker compose build
docker compose up -d
```

CLI 源码默认挂载 `/mnt/x/project/claude-code-kin`（只挂载，不改仓库外的补丁流程）。覆盖：

```bash
CLAUDE_CODE_KIN=/path/to/claude-code-kin docker compose up -d
```

## 验证连通性

桥健康：

```bash
docker compose exec native-test curl -sf http://127.0.0.1:18080/health
```

经桥访问 Anthropic（应走 SOCKS5 出口，不要直连宿主机 Clash）：

```bash
docker compose exec native-test \
  curl -sI --max-time 20 https://api.anthropic.com
```

期望：HTTP 响应头（常见 `401`/`403`/`200`，只要不是连接失败即可）。

时区：

```bash
docker compose exec native-test sh -c 'echo TZ=$TZ CLAUDE_CODE_TIMEZONE=$CLAUDE_CODE_TIMEZONE; date'
```

## 容器内构建 / 运行 CLI

另一个 agent 仍在给 `claude-code-kin` 打补丁。镜像不烘焙 `dist/`。补丁就绪后：

```bash
docker compose exec native-test bun --version   # 期望 1.3.14
docker compose exec native-test bun install
docker compose exec native-test bun run build
docker compose exec native-test bun dist/cli-node.js --version
```

kernel 侧把 `KIN_CLAUDE_BIN` 指到容器内 `/opt/claude-code-kin/dist/cli-node.js`，并继续用入口注入的 `KIN_HTTPS_PROXY`。

## 停止

```bash
docker compose down
```
