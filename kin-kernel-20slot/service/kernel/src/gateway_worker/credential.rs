use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use uuid::Uuid;

pub const TYPE_OAUTH: &str = "oauth";
pub const TYPE_SETUP_TOKEN: &str = "setup-token";
pub const TYPE_API_KEY: &str = "apikey";
pub const AUTH_X_API_KEY: &str = "x_api_key";
pub const AUTH_BEARER: &str = "authorization_bearer";

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Credential {
    pub cred_type: String,
    pub access_token: String,
    pub refresh_token: String,
    pub expires_at: i64,
    pub generation: i64,
    pub email: String,
    pub account_uuid: String,
    pub org_uuid: String,
    pub scopes: Vec<String>,
    pub api_key: String,
    pub base_url: String,
    pub auth_scheme: String,
}

impl Credential {
    pub fn token(&self) -> &str {
        if self.is_api_key() && !self.api_key.trim().is_empty() {
            return self.api_key.trim();
        }
        self.access_token.trim()
    }

    pub fn valid(&self) -> bool {
        !self.token().is_empty()
    }

    pub fn is_api_key(&self) -> bool {
        normalize_type(&self.cred_type) == TYPE_API_KEY
    }

    pub fn is_setup_token(&self) -> bool {
        normalize_type(&self.cred_type) == TYPE_SETUP_TOKEN
    }

    pub fn auth_is_bearer(&self) -> bool {
        normalize_auth_scheme(&self.auth_scheme, &self.cred_type) == AUTH_BEARER
    }

    pub fn needs_refresh(&self, now_ms: i64, skew_ms: i64) -> bool {
        if self.is_api_key() {
            return false;
        }
        if !self.valid() {
            return true;
        }
        if self.is_setup_token() && self.refresh_token.trim().is_empty() && self.expires_at <= 0 {
            return false;
        }
        if self.expires_at <= 0 {
            return true;
        }
        now_ms >= self.expires_at.saturating_sub(skew_ms)
    }

    pub fn state(&self, now_ms: i64, skew_ms: i64) -> &'static str {
        if !self.valid() {
            return if self.refresh_token.trim().is_empty() {
                "missing"
            } else {
                "refreshable"
            };
        }
        if self.expires_at > 0 && self.expires_at <= now_ms {
            return if self.refresh_token.trim().is_empty() {
                "expired"
            } else {
                "expired_refreshable"
            };
        }
        if self.needs_refresh(now_ms, skew_ms) {
            return "refresh_window";
        }
        "fresh"
    }

    pub fn public_value(&self, now: i64, skew_ms: i64) -> Value {
        let ttl = if self.expires_at > 0 {
            Some((self.expires_at.saturating_sub(now)) / 1000)
        } else {
            None
        };
        json!({
            "type": normalize_type(&self.cred_type),
            "has_access": self.valid(),
            "has_refresh": !self.refresh_token.trim().is_empty(),
            "expires_at": if self.expires_at > 0 { Some(self.expires_at) } else { None },
            "ttl_seconds": ttl,
            "generation": self.generation,
            "email": self.email,
            "account_uuid": self.account_uuid,
            "org_uuid": self.org_uuid,
            "scopes": self.scopes,
            "base_url": self.base_url,
            "auth_scheme": normalize_auth_scheme(&self.auth_scheme, &self.cred_type),
            "needs_refresh": self.needs_refresh(now, skew_ms),
            "credential_state": self.state(now, skew_ms),
        })
    }
}

#[derive(Debug, Clone)]
pub struct LoadedCredential {
    pub credential: Credential,
    pub document: Map<String, Value>,
}

#[derive(Debug, Clone)]
pub struct CredentialStore {
    path: PathBuf,
    lock_path: PathBuf,
}

#[derive(Debug)]
pub struct CredentialLock {
    file: File,
}

impl Drop for CredentialLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

impl CredentialStore {
    pub fn new(path: impl AsRef<Path>) -> Self {
        let path = path.as_ref().to_path_buf();
        let lock_path = PathBuf::from(format!("{}.lock", path.display()));
        Self { path, lock_path }
    }

    pub fn load(&self) -> Result<LoadedCredential, String> {
        let data = std::fs::read(&self.path).map_err(|err| format!("read credentials: {err}"))?;
        let value: Value =
            serde_json::from_slice(&data).map_err(|err| format!("decode credentials: {err}"))?;
        let document = value
            .as_object()
            .cloned()
            .ok_or_else(|| "decode credentials: root must be an object".to_string())?;
        let credential = decode(&document);
        Ok(LoadedCredential {
            credential,
            document,
        })
    }

    pub fn load_or_empty(&self) -> Result<LoadedCredential, String> {
        match self.load() {
            Ok(loaded) => Ok(loaded),
            Err(_) if !self.path.exists() => Ok(LoadedCredential {
                credential: Credential::default(),
                document: Map::new(),
            }),
            Err(error) => Err(error),
        }
    }

    pub async fn lock(&self, timeout: Duration) -> Result<CredentialLock, String> {
        if let Some(parent) = self.lock_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|err| format!("create credential lock directory: {err}"))?;
            std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))
                .map_err(|err| format!("chmod credential lock directory: {err}"))?;
        }
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&self.lock_path)
            .map_err(|err| format!("open credential lock: {err}"))?;
        let deadline = Instant::now() + timeout;
        loop {
            match file.try_lock() {
                Ok(()) => return Ok(CredentialLock { file }),
                Err(std::fs::TryLockError::WouldBlock) if Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                Err(std::fs::TryLockError::WouldBlock) => {
                    return Err("lock credentials: timeout".to_string());
                }
                Err(std::fs::TryLockError::Error(error)) => {
                    return Err(format!("lock credentials: {error}"));
                }
            }
        }
    }

    pub fn save(
        &self,
        mut credential: Credential,
        mut document: Map<String, Value>,
    ) -> Result<Credential, String> {
        if !credential.valid() {
            return Err("access token is required".to_string());
        }
        credential.cred_type = normalize_type(&credential.cred_type);
        credential.auth_scheme =
            normalize_auth_scheme(&credential.auth_scheme, &credential.cred_type);
        document.insert("type".into(), Value::String(credential.cred_type.clone()));
        document.insert(
            "authScheme".into(),
            Value::String(credential.auth_scheme.clone()),
        );
        if credential.is_api_key() {
            let mut api = object_at(&document, "anthropicApiKey").unwrap_or_default();
            api.insert(
                "apiKey".into(),
                Value::String(credential.token().to_string()),
            );
            if !credential.base_url.trim().is_empty() {
                api.insert("baseUrl".into(), Value::String(credential.base_url.clone()));
            }
            api.insert(
                "authScheme".into(),
                Value::String(credential.auth_scheme.clone()),
            );
            document.insert("anthropicApiKey".into(), Value::Object(api));
            document.remove("claudeAiOauth");
            let current = first_i64(
                &document,
                &["kinGeneration", "kin_generation", "_token_version"],
            );
            credential.generation = next_generation(credential.generation, current);
            document.insert("kinGeneration".into(), Value::from(credential.generation));
            self.atomic_write(&Value::Object(document))?;
            return Ok(credential);
        }

        let mut oauth = object_at(&document, "claudeAiOauth").unwrap_or_default();
        let current = first_i64(
            &oauth,
            &["kinGeneration", "kin_generation", "_token_version"],
        );
        credential.generation = next_generation(credential.generation, current);
        oauth.insert(
            "accessToken".into(),
            Value::String(credential.access_token.clone()),
        );
        if !credential.refresh_token.trim().is_empty() {
            oauth.insert(
                "refreshToken".into(),
                Value::String(credential.refresh_token.clone()),
            );
        } else if credential.is_setup_token() {
            oauth.remove("refreshToken");
        }
        if credential.expires_at > 0 {
            oauth.insert("expiresAt".into(), Value::from(credential.expires_at));
        }
        oauth.insert("kinGeneration".into(), Value::from(credential.generation));
        insert_nonempty(&mut oauth, "email", &credential.email);
        insert_nonempty(&mut oauth, "accountUuid", &credential.account_uuid);
        insert_nonempty(&mut oauth, "orgUuid", &credential.org_uuid);
        if !credential.scopes.is_empty() {
            oauth.insert(
                "scopes".into(),
                Value::Array(
                    credential
                        .scopes
                        .iter()
                        .cloned()
                        .map(Value::String)
                        .collect(),
                ),
            );
        }
        if credential.is_setup_token() {
            oauth.insert("type".into(), Value::String(TYPE_SETUP_TOKEN.into()));
        }
        document.insert("claudeAiOauth".into(), Value::Object(oauth));
        document.insert("kinGeneration".into(), Value::from(credential.generation));
        self.atomic_write(&Value::Object(document))?;
        Ok(credential)
    }

    fn atomic_write(&self, document: &Value) -> Result<(), String> {
        let parent = self
            .path
            .parent()
            .ok_or_else(|| "credential path has no parent".to_string())?;
        std::fs::create_dir_all(parent)
            .map_err(|err| format!("create credential directory: {err}"))?;
        std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))
            .map_err(|err| format!("chmod credential directory: {err}"))?;
        let temp_path = parent.join(format!(".credentials-{}.tmp", Uuid::new_v4()));
        let mut temp = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp_path)
            .map_err(|err| format!("create credential temp file: {err}"))?;
        temp.set_permissions(std::fs::Permissions::from_mode(0o600))
            .map_err(|err| format!("chmod credential temp file: {err}"))?;
        let mut data = serde_json::to_vec_pretty(document)
            .map_err(|err| format!("encode credentials: {err}"))?;
        data.push(b'\n');
        if let Err(error) = temp.write_all(&data).and_then(|_| temp.sync_all()) {
            let _ = std::fs::remove_file(&temp_path);
            return Err(format!("write credential temp file: {error}"));
        }
        drop(temp);
        if let Err(error) = std::fs::rename(&temp_path, &self.path) {
            let _ = std::fs::remove_file(&temp_path);
            return Err(format!("replace credential file: {error}"));
        }
        std::fs::set_permissions(&self.path, std::fs::Permissions::from_mode(0o600))
            .map_err(|err| format!("chmod credential file: {err}"))?;
        if let Ok(directory) = File::open(parent) {
            let _ = directory.sync_all();
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct CredentialImport {
    #[serde(default, rename = "type")]
    pub cred_type: String,
    #[serde(default)]
    pub access_token: String,
    #[serde(default)]
    pub refresh_token: String,
    #[serde(default)]
    pub api_key: String,
    #[serde(default)]
    pub base_url: String,
    #[serde(default)]
    pub auth_scheme: String,
    #[serde(default)]
    pub expires_at: i64,
    #[serde(default)]
    pub expires_in: i64,
    #[serde(default)]
    pub email: String,
    #[serde(default)]
    pub account_uuid: String,
    #[serde(default)]
    pub org_uuid: String,
    #[serde(default)]
    pub scopes: Vec<String>,
}

impl CredentialImport {
    pub fn into_credential(self, now: i64) -> Result<Credential, String> {
        let mut expires_at = normalize_expiry(self.expires_at);
        if expires_at == 0 && self.expires_in > 0 {
            expires_at = now.saturating_add(self.expires_in.saturating_mul(1000));
        }
        let cred_type = normalize_type(&self.cred_type);
        let access_token = self.access_token.trim().to_string();
        let api_key = if !self.api_key.trim().is_empty() {
            self.api_key.trim().to_string()
        } else if cred_type == TYPE_API_KEY || access_token.starts_with("sk-ant-api03-") {
            access_token.clone()
        } else {
            String::new()
        };
        if !api_key.is_empty()
            && (cred_type == TYPE_API_KEY || api_key.starts_with("sk-ant-api03-"))
        {
            return Ok(Credential {
                cred_type: TYPE_API_KEY.into(),
                access_token: api_key.clone(),
                api_key,
                base_url: self.base_url.trim().to_string(),
                email: self.email.trim().to_string(),
                account_uuid: self.account_uuid.trim().to_string(),
                org_uuid: self.org_uuid.trim().to_string(),
                auth_scheme: normalize_auth_scheme(&self.auth_scheme, TYPE_API_KEY),
                ..Credential::default()
            });
        }
        let credential = Credential {
            cred_type: cred_type.clone(),
            access_token: self.access_token.trim().to_string(),
            refresh_token: self.refresh_token.trim().to_string(),
            expires_at,
            email: self.email.trim().to_string(),
            account_uuid: self.account_uuid.trim().to_string(),
            org_uuid: self.org_uuid.trim().to_string(),
            scopes: self.scopes,
            auth_scheme: normalize_auth_scheme(&self.auth_scheme, &cred_type),
            ..Credential::default()
        };
        if !credential.valid() {
            return Err("access token is required".to_string());
        }
        Ok(credential)
    }
}

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

pub fn normalize_type(raw: &str) -> String {
    match raw.trim().to_ascii_lowercase().replace('_', "-").as_str() {
        "setup-token" | "inference" => TYPE_SETUP_TOKEN.into(),
        "apikey" | "api-key" | "console" => TYPE_API_KEY.into(),
        _ => TYPE_OAUTH.into(),
    }
}

pub fn normalize_auth_scheme(raw: &str, cred_type: &str) -> String {
    match raw.trim().to_ascii_lowercase().replace('-', "_").as_str() {
        "authorization_bearer" | "bearer" | "authorization" | "oauth" => AUTH_BEARER.into(),
        "x_api_key" | "apikey" | "api_key" => AUTH_X_API_KEY.into(),
        _ if normalize_type(cred_type) == TYPE_API_KEY => AUTH_X_API_KEY.into(),
        _ => AUTH_BEARER.into(),
    }
}

fn decode(document: &Map<String, Value>) -> Credential {
    let oauth = object_at(document, "claudeAiOauth").unwrap_or_else(|| document.clone());
    let mut credential = Credential {
        cred_type: normalize_type(&first_string(document, &["type"])),
        access_token: first_string(&oauth, &["accessToken", "access_token"]),
        refresh_token: first_string(&oauth, &["refreshToken", "refresh_token"]),
        expires_at: normalize_expiry(first_i64(&oauth, &["expiresAt", "expires_at"])),
        generation: first_i64(
            &oauth,
            &["kinGeneration", "kin_generation", "_token_version"],
        ),
        email: first_string(&oauth, &["email", "emailAddress", "email_address"]),
        account_uuid: first_string(&oauth, &["accountUuid", "account_uuid"]),
        org_uuid: first_string(&oauth, &["orgUuid", "org_uuid", "organization_uuid"]),
        scopes: string_slice(oauth.get("scopes")),
        auth_scheme: first_string(document, &["authScheme", "auth_scheme"]),
        ..Credential::default()
    };
    let nested_type = first_string(&oauth, &["type"]);
    if !nested_type.is_empty() && credential.cred_type == TYPE_OAUTH {
        credential.cred_type = normalize_type(&nested_type);
    }
    if let Some(api) = object_at(document, "anthropicApiKey") {
        let key = first_string(&api, &["apiKey", "api_key"]);
        if !key.is_empty() {
            credential.cred_type = TYPE_API_KEY.into();
            credential.api_key = key.clone();
            credential.access_token = key;
            credential.base_url = first_string(&api, &["baseUrl", "base_url"]);
            if credential.email.is_empty() {
                credential.email = first_string(&api, &["email"]);
            }
            if credential.auth_scheme.is_empty() {
                credential.auth_scheme = first_string(&api, &["authScheme", "auth_scheme"]);
            }
        }
    }
    if credential.auth_scheme.is_empty() {
        credential.auth_scheme = first_string(&oauth, &["authScheme", "auth_scheme"]);
    }
    if credential.generation == 0 {
        credential.generation = first_i64(
            document,
            &["kinGeneration", "kin_generation", "_token_version"],
        );
    }
    credential.auth_scheme = normalize_auth_scheme(&credential.auth_scheme, &credential.cred_type);
    credential
}

fn object_at(document: &Map<String, Value>, key: &str) -> Option<Map<String, Value>> {
    document.get(key)?.as_object().cloned()
}

fn first_string(document: &Map<String, Value>, keys: &[&str]) -> String {
    keys.iter()
        .find_map(|key| document.get(*key).and_then(Value::as_str))
        .unwrap_or("")
        .trim()
        .to_string()
}

fn first_i64(document: &Map<String, Value>, keys: &[&str]) -> i64 {
    keys.iter()
        .find_map(|key| document.get(*key).and_then(as_i64))
        .unwrap_or(0)
}

fn as_i64(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_u64().map(|number| number as i64))
        .or_else(|| value.as_f64().map(|number| number as i64))
        .or_else(|| value.as_str()?.trim().parse::<i64>().ok())
}

fn normalize_expiry(mut expiry: i64) -> i64 {
    if expiry > 0 && expiry < 10_000_000_000 {
        expiry = expiry.saturating_mul(1000);
    }
    expiry
}

fn string_slice(value: Option<&Value>) -> Vec<String> {
    match value {
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect(),
        Some(Value::String(text)) => text.split_whitespace().map(str::to_string).collect(),
        _ => Vec::new(),
    }
}

fn insert_nonempty(document: &mut Map<String, Value>, key: &str, value: &str) {
    if !value.trim().is_empty() {
        document.insert(key.into(), Value::String(value.trim().to_string()));
    }
}

fn next_generation(requested: i64, current: i64) -> i64 {
    requested.max(current.saturating_add(1)).max(now_ms())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn decodes_seconds_expiry_and_metadata() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("credentials.json");
        std::fs::write(
            &path,
            r#"{"claudeAiOauth":{"accessToken":"a","refreshToken":"r","expiresAt":2000000000,"accountUuid":"acc","scopes":["user:inference"]}}"#,
        )
        .unwrap();
        let cred = CredentialStore::new(&path).load().unwrap().credential;
        assert_eq!(cred.expires_at, 2_000_000_000_000);
        assert_eq!(cred.account_uuid, "acc");
        assert_eq!(cred.scopes, vec!["user:inference"]);
    }

    #[test]
    fn setup_token_without_expiry_is_fresh() {
        let cred = Credential {
            cred_type: TYPE_SETUP_TOKEN.into(),
            access_token: "oat".into(),
            ..Credential::default()
        };
        assert!(!cred.needs_refresh(1_000, 300_000));
        assert_eq!(cred.state(1_000, 300_000), "fresh");
    }

    #[test]
    fn import_detects_console_api_key() {
        let imported = CredentialImport {
            access_token: "sk-ant-api03-test".into(),
            ..CredentialImport::default()
        }
        .into_credential(1)
        .unwrap();
        assert!(imported.is_api_key());
        assert_eq!(imported.auth_scheme, AUTH_X_API_KEY);
    }

    #[test]
    fn save_is_atomic_and_increments_generation() {
        let dir = tempdir().unwrap();
        let path = dir.path().join(".claude/credentials.json");
        let store = CredentialStore::new(&path);
        let first = store
            .save(
                Credential {
                    access_token: "one".into(),
                    refresh_token: "refresh".into(),
                    expires_at: now_ms() + 60_000,
                    ..Credential::default()
                },
                Map::new(),
            )
            .unwrap();
        let loaded = store.load().unwrap();
        assert_eq!(loaded.credential.access_token, "one");
        assert_eq!(loaded.credential.generation, first.generation);
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert!(dir.path().read_dir().unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".credentials-")
        }));
    }

    #[tokio::test]
    async fn lock_is_compatible_and_times_out() {
        let dir = tempdir().unwrap();
        let store = CredentialStore::new(dir.path().join("credentials.json"));
        let first = store.lock(Duration::from_secs(1)).await.unwrap();
        let error = store.lock(Duration::from_millis(40)).await.unwrap_err();
        assert!(error.contains("timeout"));
        drop(first);
        store.lock(Duration::from_secs(1)).await.unwrap();
    }
}
