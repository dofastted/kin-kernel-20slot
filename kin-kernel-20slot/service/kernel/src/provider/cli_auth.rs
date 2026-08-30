//! Claude Code auth for Kin-spawned CLI.
//!
//! Two supported modes:
//! - **subscriber oauth**: `.credentials.json` `claudeAiOauth` with refresh.
//!   `CLAUDE_CODE_OAUTH_TOKEN` is stripped so the CLI uses the file and refreshes.
//! - **setup-token**: `claude setup-token` / inference-only oat. Injected as
//!   `CLAUDE_CODE_OAUTH_TOKEN`. No refresh. Local MCP still works; Remote Control
//!   and `user:sessions:claude_code` do not.

use std::{env, fs, os::unix::fs::PermissionsExt, path::Path};

use serde_json::{Value, json};

use crate::error::KernelError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CliAuthMode {
    Subscriber,
    SetupToken,
}

#[derive(Debug, Clone)]
pub struct ResolvedCliAuth {
    pub mode: CliAuthMode,
    pub blob: Value,
    pub setup_token: Option<String>,
}

impl ResolvedCliAuth {
    pub fn apply_std(&self, cmd: &mut std::process::Command) {
        apply_std(cmd, self.setup_token.as_deref());
    }

    pub fn apply_tokio(&self, cmd: &mut tokio::process::Command) {
        apply_tokio(cmd, self.setup_token.as_deref());
    }
}

fn apply_std(cmd: &mut std::process::Command, setup: Option<&str>) {
    cmd.env_remove("ANTHROPIC_API_KEY");
    match setup {
        Some(token) if !token.is_empty() => {
            cmd.env("CLAUDE_CODE_OAUTH_TOKEN", token);
        }
        _ => {
            cmd.env_remove("CLAUDE_CODE_OAUTH_TOKEN");
        }
    }
}

fn apply_tokio(cmd: &mut tokio::process::Command, setup: Option<&str>) {
    cmd.env_remove("ANTHROPIC_API_KEY");
    match setup {
        Some(token) if !token.is_empty() => {
            cmd.env("CLAUDE_CODE_OAUTH_TOKEN", token);
        }
        _ => {
            cmd.env_remove("CLAUDE_CODE_OAUTH_TOKEN");
        }
    }
}

/// Official oat tokens end with `AA`. Export UIs sometimes concatenate
/// "Store this token securely…" onto the same string.
pub fn sanitize_setup_token(raw: &str) -> String {
    let s = raw.trim();
    let Some(start) = s.find("sk-ant-oat01-") else {
        return s.to_string();
    };
    let body = &s[start..];
    if let Some(end) = body.rfind("AA") {
        return body[..=end + 1].to_string();
    }
    body.to_string()
}

pub fn is_setup_token_blob(blob: &Value) -> bool {
    let kind = blob
        .get("kind")
        .or_else(|| blob.get("type"))
        .or_else(|| blob.get("auth_mode"))
        .and_then(Value::as_str)
        .unwrap_or("");
    if matches!(
        kind,
        "setup-token" | "setup_token" | "claude_code_oauth_token"
    ) {
        return true;
    }
    let refresh = blob
        .get("refreshToken")
        .or_else(|| blob.get("refresh_token"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim();
    if !refresh.is_empty() {
        return false;
    }
    let scopes = blob.get("scopes").and_then(Value::as_array);
    match scopes {
        None => blob
            .get("accessToken")
            .or_else(|| blob.get("access_token"))
            .is_some(),
        Some(items) => {
            let names: Vec<&str> = items.iter().filter_map(Value::as_str).collect();
            !names.is_empty() && names.iter().all(|s| *s == "user:inference")
        }
    }
}

fn unwrap_oauth_json(v: Value) -> Value {
    if let Some(inner) = v.get("claudeAiOauth") {
        return inner.clone();
    }
    if v.get("accessToken").is_some() || v.get("access_token").is_some() {
        return v;
    }
    if let Some(accounts) = v.get("accounts").and_then(Value::as_array)
        && let Some(first) = accounts.first() {
            let typ = first
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let creds = first
                .get("credentials")
                .cloned()
                .unwrap_or_else(|| first.clone());
            return flatten_credentials(creds, &typ);
        }
    v
}

fn flatten_credentials(creds: Value, typ: &str) -> Value {
    let access = creds
        .get("accessToken")
        .or_else(|| creds.get("access_token"))
        .and_then(Value::as_str)
        .unwrap_or("");
    let refresh = creds
        .get("refreshToken")
        .or_else(|| creds.get("refresh_token"))
        .and_then(Value::as_str)
        .unwrap_or("");
    let mut expires = creds
        .get("expiresAt")
        .or_else(|| creds.get("expires_at"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    if expires > 0 && expires < 1_000_000_000_000 {
        expires *= 1000;
    }
    let scopes = creds.get("scopes").cloned().unwrap_or_else(|| {
        creds
            .get("scope")
            .and_then(Value::as_str)
            .map(|s| json!(s.split_whitespace().collect::<Vec<_>>()))
            .unwrap_or_else(|| json!(["user:inference"]))
    });
    let kind = if typ == "setup-token" || typ == "setup_token" {
        "setup-token"
    } else if refresh.is_empty() {
        "setup-token"
    } else {
        "claude_ai_oauth"
    };
    json!({
        "accessToken": sanitize_setup_token(access),
        "refreshToken": if refresh.is_empty() { Value::Null } else { json!(refresh) },
        "expiresAt": expires,
        "scopes": scopes,
        "kind": kind,
    })
}

fn default_subscriber_blob() -> Value {
    json!({
        "accessToken": "sk-ant-oat01-demo-local-only",
        "refreshToken": "sk-ant-ort01-demo-local-only",
        "expiresAt": 1_893_456_000_000u64,
        "scopes": [
            "user:profile",
            "user:inference",
            "user:sessions:claude_code",
            "user:mcp_servers",
            "user:file_upload"
        ],
        "subscriptionType": "max",
        "rateLimitTier": "default"
    })
}

fn inference_blob(token: &str, expires_at: u64) -> Value {
    json!({
        "accessToken": token,
        "refreshToken": Value::Null,
        "expiresAt": expires_at,
        "scopes": ["user:inference"],
        "kind": "setup-token",
        "subscriptionType": Value::Null,
        "rateLimitTier": Value::Null
    })
}

pub fn resolve() -> Result<ResolvedCliAuth, KernelError> {
    if let Ok(raw) = env::var("KIN_CLAUDE_CODE_OAUTH_TOKEN") {
        let token = sanitize_setup_token(&raw);
        if !token.is_empty() {
            return Ok(ResolvedCliAuth {
                mode: CliAuthMode::SetupToken,
                blob: inference_blob(&token, 0),
                setup_token: Some(token),
            });
        }
    }
    let raw = env::var("KIN_CLAUDE_AI_OAUTH_JSON").ok();
    let blob = match raw {
        Some(text) if !text.trim().is_empty() => {
            let parsed: Value = serde_json::from_str(&text)
                .map_err(|err| KernelError::Provider(format!("oauth json: {err}")))?;
            let mut blob = unwrap_oauth_json(parsed);
            if let Some(access) = blob.get("accessToken").and_then(Value::as_str) {
                let cleaned = sanitize_setup_token(access);
                if let Some(obj) = blob.as_object_mut() {
                    obj.insert("accessToken".into(), json!(cleaned));
                }
            }
            blob
        }
        _ => default_subscriber_blob(),
    };
    if is_setup_token_blob(&blob) {
        let token = blob
            .get("accessToken")
            .or_else(|| blob.get("access_token"))
            .and_then(Value::as_str)
            .map(sanitize_setup_token)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| KernelError::Provider("setup-token missing accessToken".into()))?;
        let expires = blob.get("expiresAt").and_then(Value::as_u64).unwrap_or(0);
        return Ok(ResolvedCliAuth {
            mode: CliAuthMode::SetupToken,
            blob: inference_blob(&token, expires),
            setup_token: Some(token),
        });
    }
    Ok(ResolvedCliAuth {
        mode: CliAuthMode::Subscriber,
        blob,
        setup_token: None,
    })
}

pub fn write_credentials(dir: &Path, auth: &ResolvedCliAuth) -> Result<(), KernelError> {
    fs::create_dir_all(dir).map_err(|err| KernelError::Provider(err.to_string()))?;
    let path = dir.join(".credentials.json");
    fs::write(
        &path,
        serde_json::to_vec(&json!({ "claudeAiOauth": auth.blob })).unwrap(),
    )
    .map_err(|err| KernelError::Provider(err.to_string()))?;
    let mut perms = fs::metadata(&path)
        .map_err(|err| KernelError::Provider(err.to_string()))?
        .permissions();
    perms.set_mode(0o600);
    fs::set_permissions(&path, perms).map_err(|err| KernelError::Provider(err.to_string()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn with_env<F: FnOnce()>(pairs: &[(&str, Option<&str>)], f: F) {
        let _g = ENV_LOCK.lock().unwrap();
        let mut prev = Vec::new();
        for (k, _) in pairs {
            prev.push((*k, env::var(k).ok()));
        }
        for (k, v) in pairs {
            match v {
                Some(val) => unsafe { env::set_var(k, val) },
                None => unsafe { env::remove_var(k) },
            }
        }
        f();
        for (k, v) in prev {
            match v {
                Some(val) => unsafe { env::set_var(k, val) },
                None => unsafe { env::remove_var(k) },
            }
        }
    }

    #[test]
    fn sanitizes_concatenated_store_this_suffix() {
        let raw = "sk-ant-oat01-abc_def-wucZOwAAStorethistokensecurely.Youwon";
        assert_eq!(sanitize_setup_token(raw), "sk-ant-oat01-abc_def-wucZOwAA");
    }

    #[test]
    fn sanitizes_plain_token() {
        let raw = "sk-ant-oat01-abc_def-wucZOwAA";
        assert_eq!(sanitize_setup_token(raw), raw);
    }

    #[test]
    fn detects_explicit_env() {
        with_env(
            &[
                (
                    "KIN_CLAUDE_CODE_OAUTH_TOKEN",
                    Some("sk-ant-oat01-x-AAStorethis.no"),
                ),
                ("KIN_CLAUDE_AI_OAUTH_JSON", None),
            ],
            || {
                let auth = resolve().unwrap();
                assert_eq!(auth.mode, CliAuthMode::SetupToken);
                assert_eq!(auth.setup_token.as_deref(), Some("sk-ant-oat01-x-AA"));
            },
        );
    }

    #[test]
    fn detects_kind_in_oauth_json() {
        let body = json!({
            "kind": "setup-token",
            "accessToken": "sk-ant-oat01-y-AA",
            "refreshToken": null,
            "scopes": ["user:inference"]
        })
        .to_string();
        with_env(
            &[
                ("KIN_CLAUDE_CODE_OAUTH_TOKEN", None),
                ("KIN_CLAUDE_AI_OAUTH_JSON", Some(&body)),
            ],
            || {
                let auth = resolve().unwrap();
                assert_eq!(auth.mode, CliAuthMode::SetupToken);
                assert_eq!(auth.setup_token.as_deref(), Some("sk-ant-oat01-y-AA"));
            },
        );
    }

    #[test]
    fn detects_sub2api_export() {
        let body = json!({
            "type": "sub2api-data",
            "accounts": [{
                "type": "setup-token",
                "credentials": {
                    "access_token": "sk-ant-oat01-z-AAStorethistokensecurely.Youwon",
                    "refresh_token": "",
                    "expires_at": 1819548605,
                    "scopes": ["user:inference"]
                }
            }]
        })
        .to_string();
        with_env(
            &[
                ("KIN_CLAUDE_CODE_OAUTH_TOKEN", None),
                ("KIN_CLAUDE_AI_OAUTH_JSON", Some(&body)),
            ],
            || {
                let auth = resolve().unwrap();
                assert_eq!(auth.mode, CliAuthMode::SetupToken);
                assert_eq!(auth.setup_token.as_deref(), Some("sk-ant-oat01-z-AA"));
                assert_eq!(auth.blob["expiresAt"], 1_819_548_605_000u64);
            },
        );
    }

    #[test]
    fn subscriber_blob_does_not_inject_env() {
        let body = json!({
            "accessToken": "sk-ant-oat01-full-AA",
            "refreshToken": "sk-ant-ort01-full-AA",
            "expiresAt": 1_893_456_000_000u64,
            "scopes": [
                "user:profile",
                "user:inference",
                "user:sessions:claude_code",
                "user:mcp_servers",
                "user:file_upload"
            ]
        })
        .to_string();
        with_env(
            &[
                ("KIN_CLAUDE_CODE_OAUTH_TOKEN", None),
                ("KIN_CLAUDE_AI_OAUTH_JSON", Some(&body)),
            ],
            || {
                let auth = resolve().unwrap();
                assert_eq!(auth.mode, CliAuthMode::Subscriber);
                assert!(auth.setup_token.is_none());
            },
        );
    }

    #[test]
    fn writes_credentials_0600() {
        let dir = std::env::temp_dir().join(format!("kin-cli-auth-{}", uuid::Uuid::new_v4()));
        let auth = ResolvedCliAuth {
            mode: CliAuthMode::SetupToken,
            blob: inference_blob("sk-ant-oat01-w-AA", 0),
            setup_token: Some("sk-ant-oat01-w-AA".into()),
        };
        write_credentials(&dir, &auth).unwrap();
        let meta = fs::metadata(dir.join(".credentials.json")).unwrap();
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(meta.permissions().mode() & 0o777, 0o600);
        let parsed: Value =
            serde_json::from_slice(&fs::read(dir.join(".credentials.json")).unwrap()).unwrap();
        assert_eq!(parsed["claudeAiOauth"]["kind"], "setup-token");
        let _ = fs::remove_dir_all(dir);
    }
}
