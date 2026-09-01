# Gateway Worker 交付状态

日期：2026-09-01。范围是 Kin Gateway 每槽 Rust 数据面；仓内其他公网 ingress、scheduler、control 原型不在本次生产切换范围。

## 已完成

| 领域 | 当前状态 |
|---|---|
| 运行拓扑 | `kin-kernel --gateway-worker --config /run/kin/kernel.json` 为 Rust VM 容器 PID 1 |
| 互斥引擎 | Rust VM 不挂载/启动 Go、无 `worker.sock`；Go VM 不挂载/启动 Rust、无 `kernel.sock` |
| 推理 | `/internal/v1/messages`，realtime/verified，流式/非流式，usage 与终态 metadata |
| 凭证 | OAuth/setup-token/API key import/status/Ensure；Rust 是 Rust VM 唯一 refresh owner |
| 一致性 | `.lock` 跨进程互斥、持锁重读、generation 单调、rotation 原子写、类型切换清残留 |
| 身份与用量 | `/internal/identity`、`/internal/oauth/usage`，经当前槽 SOCKS5 |
| 遥测 | Rust 进程内 event batch + GrowthBook；官方 machine/user identity 双门槛；reload/touch 热更新 |
| 健康 | `/internal/health` 返回实际 engine、脱敏 credential/state、telemetry 状态 |
| Gateway 路由 | 运行中 `vm.runtime.engine` 为 SSOT；无请求级跨引擎 fallback |
| 控制面切换 | 全局默认延迟到下次启动；单槽立即事务切换；目标失败恢复旧拓扑 |

## 安全与故障语义

- 全部内部端点使用 Unix socket 与 `X-Kin-Internal-Token`。
- production endpoint host 固定；mock endpoint 仅 `test_endpoints=true`。
- `proxy_required=true` 时 fail closed。
- 推理前 `ensure(false)`；上游 401 不 force refresh。
- fatal refresh 错误不覆盖旧凭证；retryable 429/5xx/transport 有界重试。
- 内部响应与错误不返回 access token、refresh token、API key 或代理密码。
- telemetry 失败不影响 inference。
- Rust health 失败时槽位停止调度，不隐式启动或回退 Go。

## 已验证场景

- Rust 单元测试覆盖 credential、hop、SSE、server、identity、telemetry。
- fresh Ensure 不等待文件锁；临期 Ensure 持锁后重读并复用外部 rotation。
- Clippy `-D warnings` 与 release build。
- Gateway unit/e2e、Go race tests、Go worker build。
- 真实 Docker Rust mock VM：PID 1、零 Go、无 `worker.sock`、全部内部端点、rotation、telemetry。
- 真实 Docker Go mock VM：PID 1、零 Rust、无 `kernel.sock`、credential/inference/usage。
- 真实 Docker Go→Rust→Go；Rust health timeout 后恢复 Go。
- 控制台真实浏览器：全局延迟生效、事务切换提示、Rust 不健康不回退/停止调度。

所有外部调用均使用 mock 上游与临时凭证。未使用生产 OAuth 凭证，未部署生产。

## 本次不包含

- 不把仓内公网多租户 Rust ingress/control 原型切入 Gateway 生产路径。
- 不把 Gateway pool、persona、model policy 下沉到 Rust。
- 不新增 `/internal/v1/models`；模型目录继续由 Gateway 管理。
- 不部署、不提交、不推送，除非后续明确要求。
