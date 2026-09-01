use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use reqwest::Client;
use serde::Serialize;
use serde_json::{Map, Value, json};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::sync::{Notify, RwLock};
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use super::config::{TelemetryConfig, TelemetryIdentity, TelemetryProcess, WorkerConfig};
use super::credential::{Credential, now_ms};
use super::hop::{allowed_header, build_client};
use super::oauth::CredentialManager;

const SESSION_TTL_MS: i64 = 10 * 60 * 1000;
const EVENT_BATCH_INTERVAL_MS: i64 = 10 * 1000;
const GROWTHBOOK_INTERVAL_MS: i64 = 6 * 60 * 60 * 1000;
const BATCH_PATH: &str = "/api/event_logging/batch";
const EVAL_PATH: &str = "/api/eval/sdk-zAZezfDKGoZuXXKe";

#[derive(Debug, Clone, Default, Serialize)]
pub struct TelemetryStatus {
    pub configured: bool,
    pub enabled: bool,
    pub active: bool,
    pub last_event_at: Option<i64>,
    pub last_eval_at: Option<i64>,
    pub last_error_class: String,
}

#[derive(Default)]
struct Cycle {
    generation: u64,
    sent_init: bool,
    batch_ok: bool,
    eval_ok: bool,
    stopped: bool,
    last_batch: i64,
    last_eval: i64,
}

#[derive(Debug)]
struct TelemetryFailure {
    class: &'static str,
    status: u16,
}

pub struct TelemetryManager {
    config_path: PathBuf,
    base_url: Url,
    touch_path: PathBuf,
    started_at: i64,
    config: RwLock<TelemetryConfig>,
    credentials: Arc<CredentialManager>,
    http: Client,
    status: RwLock<TelemetryStatus>,
    generation: AtomicU64,
    wake: Notify,
}

impl TelemetryManager {
    pub fn new(
        config: Arc<WorkerConfig>,
        credentials: Arc<CredentialManager>,
    ) -> Result<Self, String> {
        let http = build_client(&config, Some(Duration::from_secs(15)))?;
        let base_url = Url::parse(&config.anthropic_base_url)
            .map_err(|error| format!("parse telemetry base URL: {error}"))?;
        let touch_path = config
            .config_path
            .parent()
            .unwrap_or_else(|| std::path::Path::new("/run/kin"))
            .join("telemetry.touch");
        let telemetry = config.telemetry.clone();
        Ok(Self {
            config_path: config.config_path.clone(),
            base_url,
            touch_path,
            started_at: now_ms(),
            config: RwLock::new(telemetry.clone()),
            credentials,
            http,
            status: RwLock::new(TelemetryStatus {
                configured: telemetry.enabled,
                enabled: telemetry.effective(),
                ..TelemetryStatus::default()
            }),
            generation: AtomicU64::new(1),
            wake: Notify::new(),
        })
    }

    pub fn start(self: Arc<Self>, shutdown: CancellationToken) {
        tokio::spawn(async move {
            self.run(shutdown).await;
        });
    }

    pub async fn reload(&self) -> Result<TelemetryStatus, String> {
        let loaded = WorkerConfig::load(&self.config_path)?;
        let telemetry = loaded.telemetry;
        *self.config.write().await = telemetry.clone();
        self.generation.fetch_add(1, Ordering::SeqCst);
        {
            let mut status = self.status.write().await;
            *status = TelemetryStatus {
                configured: telemetry.enabled,
                enabled: telemetry.effective(),
                active: false,
                ..TelemetryStatus::default()
            };
        }
        self.wake.notify_one();
        Ok(self.status().await)
    }

    pub fn touch(&self) -> Result<(), String> {
        if let Some(parent) = self.touch_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|error| format!("create telemetry runtime directory: {error}"))?;
        }
        std::fs::write(&self.touch_path, format!("{}\n", now_ms()))
            .map_err(|error| format!("write telemetry touch: {error}"))?;
        std::fs::set_permissions(&self.touch_path, std::fs::Permissions::from_mode(0o600))
            .map_err(|error| format!("chmod telemetry touch: {error}"))?;
        self.wake.notify_one();
        Ok(())
    }

    pub async fn status(&self) -> TelemetryStatus {
        self.status.read().await.clone()
    }

    async fn run(&self, shutdown: CancellationToken) {
        let mut cycle = Cycle {
            generation: self.generation.load(Ordering::SeqCst),
            batch_ok: true,
            eval_ok: true,
            ..Cycle::default()
        };
        loop {
            if shutdown.is_cancelled() {
                return;
            }
            let generation = self.generation.load(Ordering::SeqCst);
            if generation != cycle.generation {
                cycle = Cycle {
                    generation,
                    batch_ok: true,
                    eval_ok: true,
                    ..Cycle::default()
                };
            }
            self.tick(&mut cycle).await;
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = self.wake.notified() => {},
                _ = tokio::time::sleep(Duration::from_secs(1)) => {},
            }
        }
    }

    async fn tick(&self, cycle: &mut Cycle) {
        let config = self.config.read().await.clone();
        let active =
            config.effective() && now_ms().saturating_sub(self.last_activity()) <= SESSION_TTL_MS;
        {
            let mut status = self.status.write().await;
            status.configured = config.enabled;
            status.enabled = config.effective();
            status.active = active && !cycle.stopped;
        }
        if !active || cycle.stopped {
            return;
        }
        let Ok(credential) = self.credentials.status() else {
            return;
        };
        if credential.is_api_key()
            || !credential.valid()
            || credential.needs_refresh(now_ms(), 60_000)
        {
            return;
        }
        let identity = overlay_identity(config.identity, &credential);
        let now = now_ms();
        if !cycle.sent_init {
            match self
                .post_json(
                    BATCH_PATH,
                    credential.token(),
                    &config.headers,
                    &json!({"events": [named_event(&identity, "tengu_init", now, 0.0, "")]}),
                )
                .await
            {
                Ok(()) => cycle.sent_init = true,
                Err(error) => {
                    self.record_error(&error).await;
                    if is_4xx(&error) {
                        cycle.stopped = true;
                        self.status.write().await.active = false;
                        return;
                    }
                }
            }
        }
        if cycle.batch_ok
            && (cycle.last_batch == 0
                || now.saturating_sub(cycle.last_batch) >= EVENT_BATCH_INTERVAL_MS)
        {
            let uptime = now.saturating_sub(self.started_at) as f64 / 1000.0;
            match self
                .post_json(
                    BATCH_PATH,
                    credential.token(),
                    &config.headers,
                    &json!({"events": [named_event(&identity, "tengu_api_success", now, uptime, "")]}),
                )
                .await
            {
                Ok(()) => {
                    cycle.last_batch = now;
                    self.status.write().await.last_event_at = Some(now);
                }
                Err(error) => {
                    self.record_error(&error).await;
                    if is_4xx(&error) {
                        cycle.batch_ok = false;
                    }
                }
            }
        }
        if cycle.eval_ok
            && (cycle.last_eval == 0
                || now.saturating_sub(cycle.last_eval) >= GROWTHBOOK_INTERVAL_MS)
        {
            match self
                .post_json(
                    EVAL_PATH,
                    credential.token(),
                    &config.headers,
                    &growthbook_eval(&identity),
                )
                .await
            {
                Ok(()) => {
                    cycle.last_eval = now;
                    self.status.write().await.last_eval_at = Some(now);
                }
                Err(error) => {
                    self.record_error(&error).await;
                    if is_4xx(&error) {
                        cycle.eval_ok = false;
                    }
                }
            }
        }
    }

    async fn post_json(
        &self,
        path: &str,
        access_token: &str,
        headers: &BTreeMap<String, String>,
        body: &Value,
    ) -> Result<(), TelemetryFailure> {
        let endpoint = self.base_url.join(path).map_err(|_| TelemetryFailure {
            class: "config",
            status: 0,
        })?;
        let mut request = self
            .http
            .post(endpoint)
            .header("Content-Type", "application/json")
            .bearer_auth(access_token)
            .json(body);
        for (name, value) in headers {
            let name = name.trim().to_ascii_lowercase();
            if allowed_header(&name) && !value.trim().is_empty() {
                request = request.header(name, value.trim());
            }
        }
        let response = request.send().await.map_err(|error| TelemetryFailure {
            class: if error.is_timeout() { "timeout" } else { "net" },
            status: 0,
        })?;
        if response.status().is_success() {
            return Ok(());
        }
        Err(TelemetryFailure {
            class: "status",
            status: response.status().as_u16(),
        })
    }

    async fn record_error(&self, error: &TelemetryFailure) {
        let mut status = self.status.write().await;
        status.last_error_class = if error.status == 0 {
            error.class.to_string()
        } else {
            format!("{}:{}", error.class, error.status)
        };
    }

    fn last_activity(&self) -> i64 {
        std::fs::metadata(&self.touch_path)
            .ok()
            .and_then(|metadata| metadata.modified().ok())
            .and_then(system_time_ms)
            .unwrap_or(self.started_at)
    }
}

fn system_time_ms(value: SystemTime) -> Option<i64> {
    value
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_millis() as i64)
}

fn is_4xx(error: &TelemetryFailure) -> bool {
    (400..500).contains(&error.status)
}

fn overlay_identity(mut identity: TelemetryIdentity, credential: &Credential) -> TelemetryIdentity {
    if !credential.email.trim().is_empty() {
        identity.email = credential.email.clone();
    }
    if !credential.account_uuid.trim().is_empty() {
        identity.account_uuid = credential.account_uuid.clone();
    }
    if !credential.org_uuid.trim().is_empty() {
        identity.org_uuid = credential.org_uuid.clone();
    }
    if identity.platform.trim().is_empty() {
        identity.platform = "linux".into();
    }
    if identity.entrypoint.trim().is_empty() {
        identity.entrypoint = "cli".into();
    }
    identity
}

fn named_event(
    identity: &TelemetryIdentity,
    name: &str,
    now: i64,
    uptime_seconds: f64,
    model: &str,
) -> Value {
    let mut data = Map::new();
    data.insert("event_name".into(), Value::String(name.into()));
    data.insert("event_id".into(), Value::String(Uuid::new_v4().to_string()));
    data.insert("client_timestamp".into(), Value::String(timestamp(now)));
    data.insert(
        "entrypoint".into(),
        Value::String(value_or(&identity.entrypoint, "cli")),
    );
    data.insert("is_interactive".into(), Value::Bool(name != "tengu_init"));
    data.insert("client_type".into(), Value::String("cli".into()));
    data.insert("user_type".into(), Value::String("external".into()));
    data.insert("betas".into(), Value::String(String::new()));
    data.insert("agent_sdk_version".into(), Value::String(String::new()));
    data.insert("additional_metadata".into(), Value::String(String::new()));
    insert_nonempty(&mut data, "device_id", &identity.device_id);
    insert_nonempty(&mut data, "session_id", &identity.session_id);
    insert_nonempty(&mut data, "email", &identity.email);
    insert_nonempty(&mut data, "model", model);
    let mut auth = Map::new();
    insert_nonempty(&mut auth, "account_uuid", &identity.account_uuid);
    insert_nonempty(&mut auth, "organization_uuid", &identity.org_uuid);
    if !auth.is_empty() {
        data.insert("auth".into(), Value::Object(auth));
    }
    data.insert("env".into(), env_block(identity));
    if name != "tengu_init" {
        let process = serde_json::to_vec(&process_json(&identity.process, uptime_seconds))
            .unwrap_or_default();
        data.insert("process".into(), Value::String(BASE64.encode(process)));
    }
    json!({
        "event_type": "ClaudeCodeInternalEvent",
        "event_data": data,
    })
}

fn env_block(identity: &TelemetryIdentity) -> Value {
    let platform = value_or(&identity.platform, "linux");
    let raw = value_or(&identity.platform_raw, &platform);
    let version = identity.cli_version.trim();
    json!({
        "platform": platform,
        "platform_raw": raw,
        "arch": identity.arch.trim(),
        "node_version": identity.node_version.trim(),
        "terminal": value_or(&identity.terminal, "unknown"),
        "package_managers": identity.package_managers.trim(),
        "runtimes": "node",
        "is_running_with_bun": false,
        "is_ci": false,
        "is_claubbit": false,
        "is_claude_code_remote": false,
        "is_local_agent_mode": false,
        "is_conductor": false,
        "is_github_action": false,
        "is_claude_code_action": false,
        "is_claude_ai_auth": !identity.account_uuid.trim().is_empty(),
        "version": version,
        "version_base": version_base(version),
        "build_time": "",
        "deployment_environment": format!("unknown-{platform}"),
        "vcs": "git",
        "github_event_name": "",
        "github_actions_runner_environment": "",
        "github_actions_runner_os": "",
        "github_action_ref": "",
        "wsl_version": "",
        "remote_environment_type": "",
        "claude_code_container_id": "",
        "claude_code_remote_session_id": "",
        "tags": [],
        "coworker_type": "",
        "linux_distro_id": identity.linux_distro_id.trim(),
        "linux_distro_version": identity.linux_distro_version.trim(),
        "linux_kernel": identity.linux_kernel.trim(),
    })
}

fn process_json(process: &TelemetryProcess, uptime_seconds: f64) -> Value {
    json!({
        "uptime": uptime_seconds,
        "rss": midpoint(&process.rss_range, 300_000_000, 500_000_000),
        "heapTotal": midpoint(&process.heap_total_range, 40_000_000, 80_000_000),
        "heapUsed": midpoint(&process.heap_used_range, 100_000_000, 200_000_000),
        "external": midpoint(&process.external_range, 1_000_000, 3_000_000),
        "arrayBuffers": midpoint(&process.array_buffers_range, 10_000, 50_000),
        "constrainedMemory": process.constrained_memory,
        "cpuUsage": {"user": 275_000, "system": 82_500},
        "cpuPercent": 2.5,
    })
}

fn growthbook_eval(identity: &TelemetryIdentity) -> Value {
    let device = if identity.user_id.trim().is_empty() {
        identity.device_id.trim()
    } else {
        identity.user_id.trim()
    };
    let mut attrs = Map::new();
    attrs.insert("id".into(), Value::String(device.into()));
    attrs.insert(
        "sessionId".into(),
        Value::String(if identity.session_id.trim().is_empty() {
            Uuid::new_v4().to_string()
        } else {
            identity.session_id.trim().to_string()
        }),
    );
    attrs.insert("deviceID".into(), Value::String(device.into()));
    attrs.insert(
        "platform".into(),
        Value::String(value_or(&identity.platform, "linux")),
    );
    attrs.insert(
        "appVersion".into(),
        Value::String(value_or(&identity.cli_version, "2.1.241")),
    );
    insert_nonempty(&mut attrs, "email", &identity.email);
    insert_nonempty(&mut attrs, "accountUUID", &identity.account_uuid);
    insert_nonempty(&mut attrs, "organizationUUID", &identity.org_uuid);
    insert_nonempty(&mut attrs, "subscriptionType", &identity.subscription_type);
    json!({"attributes": attrs, "forcedFeatures": {}})
}

fn insert_nonempty(target: &mut Map<String, Value>, key: &str, value: &str) {
    if !value.trim().is_empty() {
        target.insert(key.into(), Value::String(value.trim().to_string()));
    }
}

fn midpoint(bounds: &[i64], fallback_min: i64, fallback_max: i64) -> i64 {
    let (min, max) = match bounds {
        [min, max, ..] if max > min => (*min, *max),
        _ => (fallback_min, fallback_max),
    };
    min.saturating_add(max.saturating_sub(min) / 2)
}

fn version_base(version: &str) -> String {
    let parts: Vec<&str> = version.trim().split('.').collect();
    if parts.len() > 3 {
        parts[..3].join(".")
    } else {
        version.trim().to_string()
    }
}

fn value_or(value: &str, fallback: &str) -> String {
    if value.trim().is_empty() {
        fallback.to_string()
    } else {
        value.trim().to_string()
    }
}

fn timestamp(ms: i64) -> String {
    OffsetDateTime::from_unix_timestamp_nanos(ms as i128 * 1_000_000)
        .ok()
        .and_then(|value| value.format(&Rfc3339).ok())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::AtomicUsize;

    use axum::extract::State;
    use axum::http::HeaderMap;
    use axum::routing::post;
    use axum::{Json, Router};
    use tempfile::tempdir;

    use super::super::credential::CredentialImport;

    #[derive(Clone)]
    struct Capture {
        hits: Arc<AtomicUsize>,
        authorized: Arc<AtomicUsize>,
    }

    async fn capture(
        State(state): State<Capture>,
        headers: HeaderMap,
        Json(_body): Json<Value>,
    ) -> Json<Value> {
        state.hits.fetch_add(1, Ordering::SeqCst);
        if headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            == Some("Bearer access")
        {
            state.authorized.fetch_add(1, Ordering::SeqCst);
        }
        Json(json!({}))
    }

    #[test]
    fn requires_official_machine_and_user_identity() {
        let mut config = TelemetryConfig {
            enabled: true,
            ..TelemetryConfig::default()
        };
        assert!(!config.effective());
        config.identity.device_id = "device".into();
        assert!(!config.effective());
        config.identity.user_id = "user".into();
        assert!(config.effective());
    }

    #[test]
    fn event_matches_safe_claude_shape() {
        let identity = TelemetryIdentity {
            device_id: "device".into(),
            user_id: "user".into(),
            session_id: "session".into(),
            account_uuid: "account".into(),
            platform: "linux".into(),
            cli_version: "2.1.241".into(),
            ..TelemetryIdentity::default()
        };
        let event = named_event(&identity, "tengu_api_success", 1_800_000_000_000, 4.0, "");
        assert_eq!(event["event_type"], "ClaudeCodeInternalEvent");
        assert_eq!(event["event_data"]["device_id"], "device");
        assert!(event["event_data"]["process"].as_str().is_some());
        let raw = event.to_string().to_ascii_lowercase();
        for forbidden in [
            "hostname",
            "machine-id",
            "runtime_kind",
            "git_remote",
            "\"cwd\"",
        ] {
            assert!(!raw.contains(forbidden));
        }
    }

    #[test]
    fn growthbook_prefers_official_user_id() {
        let identity = TelemetryIdentity {
            device_id: "device".into(),
            user_id: "user".into(),
            session_id: "session".into(),
            ..TelemetryIdentity::default()
        };
        let value = growthbook_eval(&identity);
        assert_eq!(value["attributes"]["id"], "user");
        assert_eq!(value["attributes"]["deviceID"], "user");
    }

    #[tokio::test]
    async fn loop_sends_init_success_and_growthbook_without_blocking() {
        let capture_state = Capture {
            hits: Arc::new(AtomicUsize::new(0)),
            authorized: Arc::new(AtomicUsize::new(0)),
        };
        let app = Router::new()
            .route(BATCH_PATH, post(capture))
            .route(EVAL_PATH, post(capture))
            .with_state(capture_state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let root = tempdir().unwrap();
        let config_path = root.path().join("kernel.json");
        let credential_path = root.path().join("credentials.json");
        std::fs::write(
            &config_path,
            serde_json::to_vec(&json!({
                "vm_id": "vm-test",
                "socket_path": root.path().join("kernel.sock"),
                "credential_path": credential_path,
                "proxy_required": false,
                "internal_token": "internal",
                "oauth_token_url": format!("http://{address}/token"),
                "anthropic_base_url": format!("http://{address}"),
                "test_endpoints": true,
                "telemetry": {
                    "enabled": true,
                    "identity": {
                        "device_id": "device",
                        "user_id": "user",
                        "session_id": "session",
                        "platform": "linux",
                        "cli_version": "2.1.241"
                    },
                    "headers": {"user-agent": "claude-cli/2.1.241"}
                }
            }))
            .unwrap(),
        )
        .unwrap();
        let config = Arc::new(WorkerConfig::load(&config_path).unwrap());
        let credentials = Arc::new(CredentialManager::new(config.clone()).unwrap());
        credentials
            .import(CredentialImport {
                access_token: "access".into(),
                refresh_token: "refresh".into(),
                expires_in: 3600,
                ..CredentialImport::default()
            })
            .await
            .unwrap();
        let telemetry = Arc::new(TelemetryManager::new(config, credentials).unwrap());
        telemetry.touch().unwrap();
        let shutdown = CancellationToken::new();
        telemetry.clone().start(shutdown.clone());
        tokio::time::timeout(Duration::from_secs(2), async {
            while capture_state.hits.load(Ordering::SeqCst) < 3 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        shutdown.cancel();
        assert_eq!(capture_state.hits.load(Ordering::SeqCst), 3);
        assert_eq!(capture_state.authorized.load(Ordering::SeqCst), 3);
        let status = telemetry.status().await;
        assert!(status.enabled);
        assert!(status.active);
        assert!(status.last_event_at.is_some());
        assert!(status.last_eval_at.is_some());
    }
}
