# 连通性验证记录

时间：2026-08-29 15:39–15:41 EDT（容器内 `America/New_York`）
镜像：`kin-native-messages-test:local`（`oven/bun:1.3.14-debian` + python3/curl）
桥：仓库 `service/scripts/http_to_socks.py` → `127.0.0.1:18080`

## 出口 IP / 时区

- SOCKS5 主机 `72.1.181.43`：ip-api 报美国弗吉尼亚 Ashburn（Charter / GTT Americas），IANA `America/New_York`
- 草稿 `KIN_SLOT_TZ=America/New_York` 合适，未改
- 经桥 `curl https://api.ipify.org` → `72.1.181.43`（与 SOCKS 出口一致，不是宿主机 Clash）

## 命令与结果（无凭证）

容器内：

```text
bun --version                 → 1.3.14
python3 --version             → Python 3.13.5
curl -sf http://127.0.0.1:18080/health  → ok
echo $TZ $CLAUDE_CODE_TIMEZONE          → America/New_York America/New_York
date                                    → Sat Aug 29 15:39:48 EDT 2026
```

```text
curl -sI --max-time 20 https://api.anthropic.com
```

摘要：

```text
HTTP/1.1 200 Connection Established   # 本地 CONNECT 桥
HTTP/2 404                            # Cloudflare IAD（Ashburn）
server: cloudflare
cf-ray: ...-IAD
```

`GET /` 对 api.anthropic.com 返回 404 是正常的；说明 TLS 与 SOCKS 出口通。

```text
GET https://api.anthropic.com/v1/models
Authorization: Bearer $KIN_CLAUDE_CODE_OAUTH_TOKEN
anthropic-version: 2023-06-01
→ HTTP 200
```

setup-token 当前有效。未把 token 写入本文件。

## 凭证处理

`.local/native.env` 命中 `.gitignore`（`.local/`）。
最初 token 行粘了 CLI UI 文案后缀，原始值请求 `/v1/models` 为 401；裁到以 `AA` 结尾后为 200。已在 gitignored 的 `.local/native.env` 里去掉该后缀。容器只通过 `--env-file` 读取，Dockerfile 无凭证。
