# 安全模型与凭据边界

## 1. 信任边界

| 边界 | 默认不信任 | 必须控制 |
|---|---|---|
| Client -> kernel | tenant id、session id、tool payload、model 名 | auth、schema、quota、长度、策略 |
| Kernel -> provider | 动态 URL、代理、header、credential | 固定 adapter、allowlist、secret handle |
| Control -> kernel | snapshot、drain 命令 | mTLS、签名、revision、防回滚 |
| Tool runtime | 模型生成的命令/路径/网络目标 | 沙箱、capability、人工批准、审计 |
| Operator -> credential broker | 导入数据、scope、owner | RBAC、双人审批、不可导出 secret |

## 2. 必须禁止的模式

- 把浏览器 session cookie/sessionKey 当成可导入的生产凭据。
- 自动构造未公开 OAuth authorize/token 流或故意选择“与真实 CLI 不可区分”的 scope/UA。
- 把 refresh/access token 写进数据库、请求 JSON、日志、trace、crash dump 或聊天记录。
- 跨租户共享固定 session id、CLI home、工作目录或 tool server。
- 通过源 IP 轮换、TLS/UA 指纹模仿、metadata 伪造、遥测规避绕开供应商风控。
- 为公网请求启用 `bypassPermissions`、任意 Bash 或任意 MCP server。

## 3. 凭据设计

生产只允许：

1. Console API key：存入 Vault/KMS，kernel 通过 workload identity 获取短租约。
2. Workload Identity Federation：优先，避免长期静态密钥。
3. 供应商正式发布且组织批准的 OAuth/服务账号流程。

数据模型只保存：

```text
credential_id, tenant_id, provider, secret_ref, allowed_models,
scope_summary, owner, issued_at, expires_at, rotation_policy, status
```

不保存 secret value。adapter 每次取租约后在内存中使用；rotation 用 fencing token 防止旧 refresher 覆盖新凭据。

## 4. 租户隔离

- 默认一个 isolation domain 只服务一个 tenant。
- 共享 HTTP API 连接池可以跨 tenant，但 auth header 在每次请求局部构造并禁止 header map 复用。
- stateful CLI/process runtime 不跨 tenant 复用。
- session key 由 `tenant_hash + client_session_id` 组成。
- tool workspace 使用一次性目录、只读基础镜像、非 root、seccomp、无宿主机挂载。
- 任何需要网络的工具按目标域名和端口授权；默认 deny。

## 5. SSRF 与 egress

- provider base URL 来自编译/签名配置 allowlist，客户不能传 URL。
- 禁止 link-local、loopback、RFC1918 和 cloud metadata 地址，除非专用 adapter 明确允许。
- proxy 只能引用管理员创建的 egress policy id，不接受客户传入代理 URI。
- DNS 解析后再次校验地址；连接重定向同样重新校验。
- egress gateway 记录目标、tenant surrogate、adapter 和结果，不记录 body/token。

## 6. 数据保护

- prompt/tool payload 默认不落盘；需要审计时采用字段级 opt-in、脱敏与短保留期。
- Redis session transcript 加密或只保存 provider-safe 摘要；完整对话应放在受控会话存储。
- 核心转储默认关闭；崩溃报告过滤环境变量和 header。
- 使用 constant-time token hash 比较；continuation 只存 hash。
- 配置和审计记录有不可变 revision 与操作者身份。

## 7. 安全上线门槛

- threat model 与数据流评审通过；
- secret scanning、SAST、dependency audit、容器镜像扫描通过；
- 负向测试覆盖跨 tenant continuation、重放、tool id 注入、SSRF、header smuggling；
- chaos 测试覆盖 Redis 故障、kernel 重启、半开连接和 provider 429；
- 生产 provider adapter 经过供应商条款与组织法务/安全批准；
- 运行时不得出现真实 sessionKey 换票、指纹伪装或隐藏遥测路径。

