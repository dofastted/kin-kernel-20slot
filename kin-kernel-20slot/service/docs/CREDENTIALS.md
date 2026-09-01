# Gateway Worker 凭证契约

日期：2026-09-01。本文描述 `kin-kernel --gateway-worker` 的现行实现，不描述未来 secret-manager 架构。

## 1. 所有权与拓扑

- Rust VM 中 `kin-kernel` 是 inference、credential、identity、usage、telemetry 的唯一 owner。
- Rust VM 不挂载或启动 `kin-worker`，没有 `worker.sock`。
- Go VM 继续由 `kin-worker` 独占同一组职责。
- 同一 VM 同一时刻只有当前 `runtime.engine` 可以写凭证。
- Go↔Rust 切换复用磁盘文件，不复制、不迁移 secret。

凭证路径由 `kernel.json.credential_path` 指定，Gateway 容器内固定为：

```text
/home/kincli/.claude/credentials.json
/home/kincli/.claude/credentials.json.lock
```

## 2. 支持的凭证

| 类型 | 落盘主键 | 出站鉴权 | refresh | usage |
|---|---|---|---|---|
| OAuth | `claudeAiOauth` | Bearer | 有 refresh token 时支持 | 支持 |
| setup-token | `claudeAiOauth`，`type=setup-token` | Bearer | 不支持 | 支持 |
| API key | `anthropicApiKey` | `x-api-key` 或配置 scheme | 不支持 | 返回 `usage_unsupported` |

导入端点：

```text
POST /internal/credential/import
```

请求字段：`type`、`access_token`、`refresh_token`、`api_key`、`base_url`、`auth_scheme`、`expires_at` / `expires_in`、`email`、`account_uuid`、`org_uuid`、`scopes`。

切换凭证类型时做 clean cutover：

- 保存 API key 会删除 `claudeAiOauth`。
- 保存 OAuth 或 setup-token 会删除 `anthropicApiKey`。
- setup-token 会删除残留 `refreshToken`。

## 3. 文件兼容与原子写

Rust 读取现有 Claude/Go 形状并兼容以下 generation 字段：

```text
kinGeneration
kin_generation
_token_version
```

写入规则：

1. 打开 `credentials.json.lock` 并取得跨进程独占文件锁。
2. 持锁重读当前文档。
3. generation 取当前值与传入值的较大者，再单调加一。
4. 在同目录创建唯一临时文件。
5. 目录权限设为 `0700`，临时文件与最终文件设为 `0600`。
6. 写入、`sync_all`、原子 `rename` 替换目标文件，再同步父目录。

refresh 响应携带新 `refresh_token` 时，与 access token、expiresAt、scope 和 generation 在同一次原子替换中写入；未携带时保留旧 refresh token。

## 4. Ensure：唯一换票入口

```text
POST /internal/credential/ensure?force=0|1
```

推理与 usage 出站前都调用 `ensure(false)`。没有后台定时刷新；上游 401 不触发强制刷新。

非强制流程：

1. 无锁读取当前凭证并判断是否进入 `refresh_skew_seconds` 窗口；fresh 直接返回，不等待文件锁。
2. 需要刷新时进入进程内 singleflight。
3. 获取 `credentials.json.lock`。
4. 持锁重读文件并再次判断；其他进程已刷新则直接复用。
5. 仍需刷新才请求 OAuth token endpoint。
6. 原子写回完整凭证与 rotation 结果。

`force=1` 仍走 singleflight 和文件锁。并发 force 调用只允许一次 refresh；等待者重读并共享新票。

默认 skew 为 300 秒。状态枚举：

```text
missing | refreshable | expired_refreshable | expired | refresh_window | fresh
```

## 5. Refresh 网络与错误

- 生产 OAuth endpoint 固定 `https://platform.claude.com/v1/oauth/token`。
- 只有 `test_endpoints=true` 才允许非生产 host。
- HTTP client 使用当前槽 `proxy_url`；`proxy_required=true` 且代理为空时拒绝启动。
- 429、5xx、网络错误最多尝试 3 次，退避 300ms、600ms。
- `invalid_grant`、`invalid_refresh_token` 和其他 400 类为 fatal，不覆盖当前文件。
- refresh 失败不写半套凭证，不回退历史 token，也不切换到 Go。

## 6. 内部 API 与脱敏

| 方法 | 路径 | 行为 |
|---|---|---|
| GET | `/internal/credential/status` | 只读状态，不 refresh |
| POST | `/internal/credential/import` | 导入并原子写盘 |
| POST | `/internal/credential/ensure` | 唯一 refresh 入口 |
| GET | `/internal/health` | 返回脱敏 credential 与 `credential_state` |
| GET | `/internal/oauth/usage` | 先 `ensure(false)`，再经槽 SOCKS5 请求 usage |
| POST | `/internal/v1/messages` | 先 `ensure(false)`，再装配上游鉴权 |

所有 `/internal/*` 都要求 `X-Kin-Internal-Token`。公开响应只包含 `has_access`、`has_refresh`、过期时间、generation、账号 metadata、scope、auth scheme 和状态；禁止返回 access token、refresh token 或 API key。

## 7. 验证覆盖

测试覆盖 OAuth/setup-token/API key 导入、类型切换清理、fresh fast path、锁后重读、跨调用 singleflight、refresh rotation、429/5xx 重试、fatal 错误不落盘、重启恢复、generation 单调和公开响应无 secret。
