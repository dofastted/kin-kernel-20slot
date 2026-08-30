use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::Value;

#[derive(Debug, Clone, Default)]
pub struct Credential {
    pub cred_type: String,
    pub access_token: String,
    pub refresh_token: String,
    pub expires_at: i64,
    pub api_key: String,
    pub auth_scheme: String,
}

impl Credential {
    pub fn load(path: &Path) -> Result<Self, String> {
        let data = std::fs::read(path).map_err(|err| format!("read credentials: {err}"))?;
        let document: Value =
            serde_json::from_slice(&data).map_err(|err| format!("decode credentials: {err}"))?;
        Ok(decode(&document))
    }

    pub fn token(&self) -> &str {
        if self.is_api_key() && !self.api_key.is_empty() {
            return &self.api_key;
        }
        self.access_token.trim()
    }

    pub fn is_api_key(&self) -> bool {
        normalize_type(&self.cred_type) == "apikey"
    }

    pub fn auth_is_bearer(&self) -> bool {
        match normalize_scheme(&self.auth_scheme).as_str() {
            "bearer" => true,
            "x-api-key" => false,
            _ => !self.is_api_key(),
        }
    }

    pub fn needs_refresh(&self, now_ms: i64, skew_ms: i64) -> bool {
        if self.is_api_key() {
            return false;
        }
        if self.token().is_empty() {
            return true;
        }
        let setup_token = normalize_type(&self.cred_type) == "setup-token"
            && self.refresh_token.trim().is_empty();
        if setup_token && self.expires_at <= 0 {
            return false;
        }
        if self.expires_at <= 0 {
            return true;
        }
        now_ms >= self.expires_at.saturating_sub(skew_ms)
    }

    pub fn state(&self, now_ms: i64, skew_ms: i64) -> &'static str {
        if self.token().is_empty() {
            if self.refresh_token.trim().is_empty() {
                return "missing";
            }
            return "refreshable";
        }
        if self.expires_at > 0 && self.expires_at <= now_ms {
            if self.refresh_token.trim().is_empty() {
                return "expired";
            }
            return "expired_refreshable";
        }
        if self.needs_refresh(now_ms, skew_ms) {
            return "refresh_window";
        }
        "fresh"
    }
}

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn decode(document: &Value) -> Credential {
    let oauth = document.get("claudeAiOauth").unwrap_or(document);
    let mut cred = Credential {
        cred_type: normalize_type(&first_string(document, &["type"])),
        access_token: first_string(oauth, &["accessToken", "access_token"]),
        refresh_token: first_string(oauth, &["refreshToken", "refresh_token"]),
        expires_at: normalize_expiry(first_value(oauth, &["expiresAt", "expires_at"])),
        api_key: String::new(),
        auth_scheme: first_string(document, &["authScheme", "auth_scheme"]),
    };
    if let Some(typed) = oauth.get("type").and_then(Value::as_str)
        && cred.cred_type == "oauth"
    {
        cred.cred_type = normalize_type(typed);
    }
    if let Some(api) = document.get("anthropicApiKey") {
        let key = first_string(api, &["apiKey", "api_key"]);
        if !key.is_empty() {
            cred.cred_type = "apikey".to_string();
            cred.api_key = key.clone();
            cred.access_token = key;
            if cred.auth_scheme.is_empty() {
                cred.auth_scheme = first_string(api, &["authScheme", "auth_scheme"]);
            }
        }
    }
    if cred.auth_scheme.is_empty() {
        cred.auth_scheme = first_string(oauth, &["authScheme", "auth_scheme"]);
    }
    cred
}

fn normalize_type(raw: &str) -> String {
    match raw.trim().to_ascii_lowercase().replace('_', "-").as_str() {
        "setup-token" | "inference" => "setup-token".to_string(),
        "apikey" | "api-key" | "console" => "apikey".to_string(),
        _ => "oauth".to_string(),
    }
}

fn normalize_scheme(raw: &str) -> String {
    match raw.trim().to_ascii_lowercase().replace('-', "_").as_str() {
        "bearer" | "authorization" | "oauth" => "bearer".to_string(),
        "x_api_key" | "apikey" | "api_key" => "x-api-key".to_string(),
        other => other.to_string(),
    }
}

fn first_string(value: &Value, keys: &[&str]) -> String {
    match first_value(value, keys) {
        Some(Value::String(text)) => text.trim().to_string(),
        Some(other) => other.as_str().unwrap_or("").trim().to_string(),
        None => String::new(),
    }
}

fn first_value<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a Value> {
    let object = value.as_object()?;
    keys.iter().find_map(|key| object.get(*key))
}

fn normalize_expiry(value: Option<&Value>) -> i64 {
    let mut expiry = as_i64(value);
    if expiry > 0 && expiry < 10_000_000_000 {
        expiry *= 1000;
    }
    expiry
}

fn as_i64(value: Option<&Value>) -> i64 {
    let Some(value) = value else {
        return 0;
    };
    if let Some(n) = value.as_i64() {
        return n;
    }
    if let Some(n) = value.as_u64() {
        return n as i64;
    }
    if let Some(n) = value.as_f64() {
        return n as i64;
    }
    value
        .as_str()
        .and_then(|s| s.trim().parse::<i64>().ok())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn seconds_expiry_is_scaled_to_millis() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("credentials.json");
        std::fs::write(
            &path,
            r#"{"claudeAiOauth":{"accessToken":"a","refreshToken":"r","expiresAt":2000000000}}"#,
        )
        .unwrap();
        let cred = Credential::load(&path).unwrap();
        assert_eq!(cred.expires_at, 2_000_000_000_000);
    }

    #[test]
    fn missing_token_needs_refresh() {
        let cred = Credential::default();
        assert!(cred.needs_refresh(1_000, 300_000));
        assert_eq!(cred.state(1_000, 300_000), "missing");
    }

    #[test]
    fn skew_window_needs_refresh() {
        let cred = Credential {
            access_token: "tok".into(),
            refresh_token: "ref".into(),
            expires_at: 10_000,
            ..Credential::default()
        };
        assert!(cred.needs_refresh(9_800, 300));
        assert!(!cred.needs_refresh(9_600, 300));
    }
}
