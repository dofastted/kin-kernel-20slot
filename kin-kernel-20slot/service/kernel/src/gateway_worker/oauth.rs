use std::sync::Arc;
use std::time::Duration;

use axum::http::StatusCode;
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::{Mutex, MutexGuard};

use super::config::WorkerConfig;
use super::credential::{Credential, CredentialImport, CredentialStore, now_ms};
use super::hop::build_client;

const DEFAULT_CLIENT_ID: &str = "9d1c250a-e61b-44d9-88ed-5944d1962f5e";

#[derive(Debug, Clone)]
pub struct CredentialFailure {
    pub status: StatusCode,
    pub code: &'static str,
    pub message: String,
    pub retryable: bool,
}

impl CredentialFailure {
    fn internal(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code,
            message: truncate(message.into()),
            retryable: false,
        }
    }

    fn bad_request(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code,
            message: truncate(message.into()),
            retryable: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct EnsureResult {
    pub credential: Credential,
    pub refreshed: bool,
    pub shared: bool,
}

#[derive(Debug, Deserialize)]
struct TokenResponse {
    #[serde(default)]
    access_token: String,
    #[serde(default)]
    refresh_token: String,
    #[serde(default)]
    expires_in: i64,
    #[serde(default)]
    scope: String,
    #[serde(default)]
    error: String,
    #[serde(default)]
    error_description: String,
}

pub struct CredentialManager {
    config: Arc<WorkerConfig>,
    store: CredentialStore,
    http: Client,
    serial: Mutex<()>,
}

impl CredentialManager {
    pub fn new(config: Arc<WorkerConfig>) -> Result<Self, String> {
        let store = CredentialStore::new(&config.credential_path);
        let http = build_client(&config, Some(Duration::from_secs(60)))?;
        Ok(Self {
            config,
            store,
            http,
            serial: Mutex::new(()),
        })
    }

    pub fn status(&self) -> Result<Credential, CredentialFailure> {
        self.store
            .load()
            .map(|loaded| loaded.credential)
            .map_err(|error| CredentialFailure {
                status: StatusCode::SERVICE_UNAVAILABLE,
                code: "credential_unavailable",
                message: truncate(error),
                retryable: true,
            })
    }

    pub async fn import(&self, payload: CredentialImport) -> Result<Credential, CredentialFailure> {
        let _serial = self.serial.lock().await;
        let _file = self
            .store
            .lock(Duration::from_secs(30))
            .await
            .map_err(|error| CredentialFailure::internal("credential_import_failed", error))?;
        let credential = payload
            .into_credential(now_ms())
            .map_err(|error| CredentialFailure::bad_request("credential_import_invalid", error))?;
        let document = self
            .store
            .load_or_empty()
            .map(|loaded| loaded.document)
            .map_err(|error| CredentialFailure::internal("credential_import_failed", error))?;
        self.store
            .save(credential, document)
            .map_err(|error| CredentialFailure::internal("credential_import_failed", error))
    }

    pub async fn ensure(&self, force: bool) -> Result<EnsureResult, CredentialFailure> {
        let current = self.status()?;
        if current.is_api_key() {
            if !current.valid() {
                return Err(CredentialFailure::bad_request(
                    "api_key_missing",
                    "console API key is missing",
                ));
            }
            return Ok(EnsureResult {
                credential: current,
                refreshed: false,
                shared: false,
            });
        }
        if !force && !current.needs_refresh(now_ms(), self.config.refresh_skew_ms()) {
            return Ok(EnsureResult {
                credential: current,
                refreshed: false,
                shared: false,
            });
        }

        let (guard, shared) = self.serial_guard().await;
        let result = self.ensure_serial(force, shared).await;
        drop(guard);
        result
    }

    async fn serial_guard(&self) -> (MutexGuard<'_, ()>, bool) {
        match self.serial.try_lock() {
            Ok(guard) => (guard, false),
            Err(_) => (self.serial.lock().await, true),
        }
    }

    async fn ensure_serial(
        &self,
        force: bool,
        shared: bool,
    ) -> Result<EnsureResult, CredentialFailure> {
        let _file = self
            .store
            .lock(Duration::from_secs(30))
            .await
            .map_err(|error| CredentialFailure::internal("credential_refresh_failed", error))?;
        let loaded = self.store.load().map_err(|error| CredentialFailure {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "credential_unavailable",
            message: truncate(error),
            retryable: true,
        })?;
        let current = loaded.credential;
        if current.is_api_key() {
            if !current.valid() {
                return Err(CredentialFailure::bad_request(
                    "api_key_missing",
                    "console API key is missing",
                ));
            }
            return Ok(EnsureResult {
                credential: current,
                refreshed: false,
                shared,
            });
        }
        if (!force || shared) && !current.needs_refresh(now_ms(), self.config.refresh_skew_ms()) {
            return Ok(EnsureResult {
                credential: current,
                refreshed: false,
                shared,
            });
        }
        if current.refresh_token.trim().is_empty() {
            return Err(CredentialFailure::bad_request(
                "refresh_token_missing",
                "credential has no refresh token",
            ));
        }
        let refreshed = self.request_with_retry(&current).await?;
        let saved = self
            .store
            .save(refreshed, loaded.document)
            .map_err(|error| CredentialFailure::internal("credential_refresh_failed", error))?;
        Ok(EnsureResult {
            credential: saved,
            refreshed: true,
            shared,
        })
    }

    async fn request_with_retry(
        &self,
        current: &Credential,
    ) -> Result<Credential, CredentialFailure> {
        let mut last = None;
        for attempt in 0..3 {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_millis(300 * (1_u64 << (attempt - 1)))).await;
            }
            match self.request_refresh(current).await {
                Ok(credential) => return Ok(credential),
                Err(error) if error.retryable => last = Some(error),
                Err(error) => return Err(error),
            }
        }
        Err(last.unwrap_or_else(|| {
            CredentialFailure::internal("credential_refresh_failed", "OAuth refresh failed")
        }))
    }

    async fn request_refresh(&self, current: &Credential) -> Result<Credential, CredentialFailure> {
        let response = self
            .http
            .post(&self.config.oauth_token_url)
            .header("Accept", "application/json, text/plain, */*")
            .header("Content-Type", "application/json")
            .header("User-Agent", "axios/1.13.6")
            .json(&json!({
                "grant_type": "refresh_token",
                "refresh_token": current.refresh_token,
                "client_id": DEFAULT_CLIENT_ID,
            }))
            .send()
            .await
            .map_err(|error| CredentialFailure {
                status: StatusCode::BAD_GATEWAY,
                code: "credential_refresh_transport",
                message: sanitize_transport(&error),
                retryable: true,
            })?;
        let status = response.status();
        let data = response.bytes().await.map_err(|error| CredentialFailure {
            status: StatusCode::BAD_GATEWAY,
            code: "credential_refresh_transport",
            message: truncate(format!("read OAuth refresh response: {error}")),
            retryable: true,
        })?;
        let decoded: TokenResponse = serde_json::from_slice(&data).unwrap_or(TokenResponse {
            access_token: String::new(),
            refresh_token: String::new(),
            expires_in: 0,
            scope: String::new(),
            error: String::new(),
            error_description: String::new(),
        });
        if !status.is_success() {
            let retryable =
                status == reqwest::StatusCode::TOO_MANY_REQUESTS || status.is_server_error();
            let fatal = decoded.error == "invalid_grant"
                || decoded.error == "invalid_refresh_token"
                || status == reqwest::StatusCode::BAD_REQUEST;
            return Err(CredentialFailure {
                status: StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY),
                code: "credential_refresh_failed",
                message: truncate(if decoded.error_description.trim().is_empty() {
                    decoded.error
                } else {
                    decoded.error_description
                }),
                retryable: retryable && !fatal,
            });
        }
        if decoded.access_token.trim().is_empty() {
            return Err(CredentialFailure::bad_request(
                "missing_access_token",
                "refresh response has no access_token",
            ));
        }
        let expires_at = now_ms().saturating_add(if decoded.expires_in > 0 {
            decoded.expires_in.saturating_mul(1000)
        } else {
            8 * 60 * 60 * 1000
        });
        Ok(Credential {
            cred_type: current.cred_type.clone(),
            access_token: decoded.access_token.trim().to_string(),
            refresh_token: if decoded.refresh_token.trim().is_empty() {
                current.refresh_token.clone()
            } else {
                decoded.refresh_token.trim().to_string()
            },
            expires_at,
            generation: current.generation,
            email: current.email.clone(),
            account_uuid: current.account_uuid.clone(),
            org_uuid: current.org_uuid.clone(),
            scopes: if decoded.scope.trim().is_empty() {
                current.scopes.clone()
            } else {
                decoded
                    .scope
                    .split_whitespace()
                    .map(str::to_string)
                    .collect()
            },
            auth_scheme: current.auth_scheme.clone(),
            base_url: current.base_url.clone(),
            ..Credential::default()
        })
    }
}

fn sanitize_transport(error: &reqwest::Error) -> String {
    let mut message = error.to_string();
    if let Some(url) = error.url() {
        message = message.replace(url.as_str(), "");
    }
    if message.trim().is_empty() {
        "OAuth refresh transport failed".into()
    } else {
        truncate(message)
    }
}

fn truncate(message: impl Into<String>) -> String {
    message.into().chars().take(300).collect()
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use axum::extract::State;

    use axum::Router;
    use axum::routing::post;
    use serde_json::Value;
    use tempfile::tempdir;

    use super::*;

    async fn config(root: &Path, token_url: &str) -> Arc<WorkerConfig> {
        let config_path = root.join("kernel.json");
        let credential_path = root.join("credentials.json");
        std::fs::write(
            &config_path,
            serde_json::to_vec(&json!({
                "vm_id": "vm-test",
                "socket_path": root.join("kernel.sock"),
                "credential_path": credential_path,
                "proxy_required": false,
                "internal_token": "internal",
                "oauth_token_url": token_url,
                "anthropic_base_url": "http://127.0.0.1:1",
                "test_endpoints": true
            }))
            .unwrap(),
        )
        .unwrap();
        Arc::new(WorkerConfig::load(&config_path).unwrap())
    }

    #[tokio::test]
    async fn fresh_credential_is_not_refreshed() {
        let root = tempdir().unwrap();
        let cfg = config(root.path(), "http://127.0.0.1:1/token").await;
        let manager = CredentialManager::new(cfg.clone()).unwrap();
        manager
            .import(CredentialImport {
                access_token: "access".into(),
                refresh_token: "refresh".into(),
                expires_in: 3600,
                ..CredentialImport::default()
            })
            .await
            .unwrap();
        let result = manager.ensure(false).await.unwrap();
        assert!(!result.refreshed);
        assert_eq!(result.credential.access_token, "access");
    }

    #[tokio::test]
    async fn fresh_credential_does_not_wait_for_file_lock() {
        let root = tempdir().unwrap();
        let cfg = config(root.path(), "http://127.0.0.1:1/token").await;
        let manager = CredentialManager::new(cfg).unwrap();
        manager
            .import(CredentialImport {
                access_token: "access".into(),
                refresh_token: "refresh".into(),
                expires_in: 3600,
                ..CredentialImport::default()
            })
            .await
            .unwrap();
        let lock = manager.store.lock(Duration::from_secs(1)).await.unwrap();
        let result = tokio::time::timeout(Duration::from_millis(100), manager.ensure(false))
            .await
            .unwrap()
            .unwrap();
        drop(lock);
        assert!(!result.refreshed);
    }

    #[tokio::test]
    async fn refresh_reloads_credential_after_file_lock() {
        let root = tempdir().unwrap();
        let cfg = config(root.path(), "http://127.0.0.1:1/token").await;
        let manager = Arc::new(CredentialManager::new(cfg).unwrap());
        manager
            .import(CredentialImport {
                access_token: "expired".into(),
                refresh_token: "refresh".into(),
                expires_at: 1,
                ..CredentialImport::default()
            })
            .await
            .unwrap();

        let lock = manager.store.lock(Duration::from_secs(1)).await.unwrap();
        let ensure = {
            let manager = manager.clone();
            tokio::spawn(async move { manager.ensure(false).await.unwrap() })
        };
        tokio::time::sleep(Duration::from_millis(25)).await;
        let mut loaded = manager.store.load().unwrap();
        loaded.credential.access_token = "external-refresh".into();
        loaded.credential.expires_at = now_ms() + 3_600_000;
        manager
            .store
            .save(loaded.credential, loaded.document)
            .unwrap();
        drop(lock);

        let result = ensure.await.unwrap();
        assert!(!result.refreshed);
        assert_eq!(result.credential.access_token, "external-refresh");
    }

    #[tokio::test]
    async fn forced_refresh_rotates_and_persists() {
        let app = Router::new().route(
            "/token",
            post(|| async {
                axum::Json(json!({
                    "access_token": "next-access",
                    "refresh_token": "next-refresh",
                    "expires_in": 7200,
                    "scope": "user:inference user:profile"
                }))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let root = tempdir().unwrap();
        let cfg = config(root.path(), &format!("http://{address}/token")).await;
        let manager = CredentialManager::new(cfg.clone()).unwrap();
        let imported = manager
            .import(CredentialImport {
                access_token: "access".into(),
                refresh_token: "refresh".into(),
                expires_in: 3600,
                ..CredentialImport::default()
            })
            .await
            .unwrap();
        let result = manager.ensure(true).await.unwrap();
        assert!(result.refreshed);
        assert_eq!(result.credential.access_token, "next-access");
        assert_eq!(result.credential.refresh_token, "next-refresh");
        assert!(result.credential.generation > imported.generation);
        assert_eq!(manager.status().unwrap().access_token, "next-access");
    }

    #[tokio::test]
    async fn invalid_grant_is_fatal_and_preserves_file() {
        let app = Router::new().route(
            "/token",
            post(|| async {
                (
                    StatusCode::BAD_REQUEST,
                    axum::Json(json!({
                        "error": "invalid_grant",
                        "error_description": "refresh rejected"
                    })),
                )
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let root = tempdir().unwrap();
        let cfg = config(root.path(), &format!("http://{address}/token")).await;
        let manager = CredentialManager::new(cfg).unwrap();
        manager
            .import(CredentialImport {
                access_token: "access".into(),
                refresh_token: "refresh".into(),
                expires_in: 3600,
                ..CredentialImport::default()
            })
            .await
            .unwrap();
        let error = manager.ensure(true).await.unwrap_err();
        assert!(!error.retryable);
        assert_eq!(manager.status().unwrap().access_token, "access");
    }

    #[tokio::test]
    async fn imports_setup_token_and_api_key_and_recovers_after_restart() {
        let root = tempdir().unwrap();
        let cfg = config(root.path(), "http://127.0.0.1:1/token").await;
        let manager = CredentialManager::new(cfg.clone()).unwrap();
        let setup = manager
            .import(CredentialImport {
                cred_type: "setup-token".into(),
                access_token: "setup-access".into(),
                ..CredentialImport::default()
            })
            .await
            .unwrap();
        assert!(setup.is_setup_token());
        assert!(!setup.needs_refresh(now_ms(), cfg.refresh_skew_ms()));
        let api = manager
            .import(CredentialImport {
                cred_type: "apikey".into(),
                api_key: "sk-ant-api03-console".into(),
                base_url: "https://console.example".into(),
                ..CredentialImport::default()
            })
            .await
            .unwrap();
        assert!(api.is_api_key());
        assert!(api.generation > setup.generation);
        let restarted = CredentialManager::new(cfg).unwrap();
        let loaded = restarted.status().unwrap();
        assert!(loaded.is_api_key());
        assert_eq!(loaded.token(), "sk-ant-api03-console");
        assert_eq!(loaded.base_url, "https://console.example");
    }

    #[tokio::test]
    async fn retries_5xx_then_persists_success() {
        async fn flaky(State(hits): State<Arc<AtomicUsize>>) -> (StatusCode, axum::Json<Value>) {
            let attempt = hits.fetch_add(1, Ordering::SeqCst);
            if attempt < 2 {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    axum::Json(json!({"error": "temporary"})),
                );
            }
            (
                StatusCode::OK,
                axum::Json(json!({
                    "access_token": "retried-access",
                    "refresh_token": "retried-refresh",
                    "expires_in": 3600
                })),
            )
        }
        let hits = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route("/token", post(flaky))
            .with_state(hits.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let root = tempdir().unwrap();
        let cfg = config(root.path(), &format!("http://{address}/token")).await;
        let manager = CredentialManager::new(cfg).unwrap();
        manager
            .import(CredentialImport {
                access_token: "old".into(),
                refresh_token: "refresh".into(),
                expires_at: 1,
                ..CredentialImport::default()
            })
            .await
            .unwrap();
        let result = manager.ensure(false).await.unwrap();
        assert!(result.refreshed);
        assert_eq!(result.credential.access_token, "retried-access");
        assert_eq!(hits.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn concurrent_force_ensure_shares_one_refresh() {
        async fn slow(State(hits): State<Arc<AtomicUsize>>) -> axum::Json<Value> {
            hits.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(50)).await;
            axum::Json(json!({
                "access_token": "shared-access",
                "refresh_token": "shared-refresh",
                "expires_in": 3600
            }))
        }
        let hits = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route("/token", post(slow))
            .with_state(hits.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let root = tempdir().unwrap();
        let cfg = config(root.path(), &format!("http://{address}/token")).await;
        let manager = Arc::new(CredentialManager::new(cfg).unwrap());
        manager
            .import(CredentialImport {
                access_token: "old".into(),
                refresh_token: "refresh".into(),
                expires_in: 3600,
                ..CredentialImport::default()
            })
            .await
            .unwrap();
        let first = {
            let manager = manager.clone();
            tokio::spawn(async move { manager.ensure(true).await.unwrap() })
        };
        tokio::time::sleep(Duration::from_millis(5)).await;
        let second = {
            let manager = manager.clone();
            tokio::spawn(async move { manager.ensure(true).await.unwrap() })
        };
        let first = first.await.unwrap();
        let second = second.await.unwrap();
        assert_eq!(hits.load(Ordering::SeqCst), 1);
        assert_eq!(
            usize::from(first.refreshed) + usize::from(second.refreshed),
            1
        );
        assert!(first.shared || second.shared);
        assert_eq!(first.credential.access_token, "shared-access");
        assert_eq!(second.credential.access_token, "shared-access");
    }
    #[test]
    fn public_contract_contains_no_tokens() {
        let credential = Credential {
            access_token: "secret-access".into(),
            refresh_token: "secret-refresh".into(),
            ..Credential::default()
        };
        let serialized = credential.public_value(now_ms(), 300_000).to_string();
        assert!(!serialized.contains("secret-access"));
        assert!(!serialized.contains("secret-refresh"));
    }
}
