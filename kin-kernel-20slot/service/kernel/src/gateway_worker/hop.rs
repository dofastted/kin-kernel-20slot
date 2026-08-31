use std::collections::HashMap;

use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use reqwest::{Client, Proxy, redirect::Policy};
use url::Url;

use super::config::WorkerConfig;
use super::credential::Credential;
use super::error::WorkerError;

pub struct HopClient {
    http: Client,
    anthropic_base: Url,
    first_byte: std::time::Duration,
}

pub struct HopResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: reqwest::Response,
}

impl HopClient {
    pub fn new(config: &WorkerConfig) -> Result<Self, String> {
        let mut builder = Client::builder()
            .redirect(Policy::none())
            .no_proxy()
            .tcp_nodelay(true);
        if let Some(timeout) = config.request_timeout() {
            builder = builder.timeout(timeout);
        }
        if config.proxy_url.trim().is_empty() && !config.test_endpoints {
            return Err("slot proxy is required".into());
        }
        if !config.proxy_url.trim().is_empty() {
            builder = builder.proxy(
                Proxy::all(config.proxy_url.trim())
                    .map_err(|err| format!("invalid proxy_url: {err}"))?,
            );
        }
        let http = builder
            .build()
            .map_err(|err| format!("build hop client: {err}"))?;
        let anthropic_base = Url::parse(&config.anthropic_base_url)
            .map_err(|err| format!("anthropic_base_url: {err}"))?;
        Ok(Self {
            http,
            anthropic_base,
            first_byte: config.first_byte(),
        })
    }

    pub async fn messages(
        &self,
        payload: &[u8],
        headers: &HashMap<String, String>,
        credential: &Credential,
    ) -> Result<HopResponse, WorkerError> {
        let url = resolve(&self.anthropic_base, "/v1/messages")?;
        let outbound = outbound_headers(headers, credential)?;
        let request = self.http.post(url).headers(outbound).body(payload.to_vec());
        let response = match tokio::time::timeout(self.first_byte, request.send()).await {
            Ok(Ok(response)) => response,
            Ok(Err(err)) => {
                return Err(WorkerError::new(
                    StatusCode::BAD_GATEWAY,
                    "upstream_transport",
                    sanitize_reqwest(&err),
                ));
            }
            Err(_) => {
                return Err(WorkerError::new(
                    StatusCode::BAD_GATEWAY,
                    "upstream_transport",
                    "stream first-byte timeout",
                ));
            }
        };
        let status =
            StatusCode::from_u16(response.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
        let headers = strip_sensitive(response.headers());
        Ok(HopResponse {
            status,
            headers,
            body: response,
        })
    }
}

pub fn allowed_header(key: &str) -> bool {
    matches!(
        key.trim().to_ascii_lowercase().as_str(),
        "accept"
            | "accept-language"
            | "anthropic-beta"
            | "anthropic-dangerous-direct-browser-access"
            | "anthropic-version"
            | "content-type"
            | "user-agent"
            | "x-app"
            | "x-claude-code-session-id"
            | "x-client-request-id"
            | "x-stainless-arch"
            | "x-stainless-helper-method"
            | "x-stainless-lang"
            | "x-stainless-os"
            | "x-stainless-package-version"
            | "x-stainless-retry-count"
            | "x-stainless-runtime"
            | "x-stainless-runtime-version"
            | "x-stainless-timeout"
    )
}

pub fn copyable_response_header(key: &str) -> bool {
    !matches!(
        key.trim().to_ascii_lowercase().as_str(),
        "content-length"
            | "connection"
            | "transfer-encoding"
            | "set-cookie"
            | "authorization"
            | "x-api-key"
    )
}

fn outbound_headers(
    from: &HashMap<String, String>,
    credential: &Credential,
) -> Result<HeaderMap, WorkerError> {
    let mut headers = HeaderMap::new();
    for (key, value) in from {
        if !allowed_header(key) || value.trim().is_empty() {
            continue;
        }
        insert_header(&mut headers, key, value)?;
    }
    apply_auth(&mut headers, credential)?;
    if !headers.contains_key(header::CONTENT_TYPE) {
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
    }
    if !headers.contains_key(header::ACCEPT) {
        headers.insert(header::ACCEPT, HeaderValue::from_static("application/json"));
    }
    if headers.get("anthropic-version").is_none() {
        headers.insert(
            HeaderName::from_static("anthropic-version"),
            HeaderValue::from_static("2023-06-01"),
        );
    }
    Ok(headers)
}

fn apply_auth(headers: &mut HeaderMap, credential: &Credential) -> Result<(), WorkerError> {
    let token = credential.token();
    if token.is_empty() {
        return Err(WorkerError::new(
            StatusCode::UNAUTHORIZED,
            "needs_refresh",
            "credential needs refresh",
        ));
    }
    if credential.auth_is_bearer() {
        headers.remove("x-api-key");
        insert_header(headers, "authorization", &format!("Bearer {token}"))?;
        return Ok(());
    }
    headers.remove(header::AUTHORIZATION);
    insert_header(headers, "x-api-key", token)
}

fn insert_header(headers: &mut HeaderMap, key: &str, value: &str) -> Result<(), WorkerError> {
    let name = HeaderName::try_from(key).map_err(|_| {
        WorkerError::new(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "invalid header name",
        )
    })?;
    let value = HeaderValue::from_str(value).map_err(|_| {
        WorkerError::new(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "invalid header value",
        )
    })?;
    headers.insert(name, value);
    Ok(())
}

fn resolve(base: &Url, path: &str) -> Result<Url, WorkerError> {
    let target = base.join(path).map_err(|_| {
        WorkerError::new(
            StatusCode::BAD_GATEWAY,
            "upstream_transport",
            "invalid Anthropic base URL",
        )
    })?;
    if !same_origin(base, &target) {
        return Err(WorkerError::new(
            StatusCode::BAD_GATEWAY,
            "upstream_transport",
            "upstream path escaped Anthropic origin",
        ));
    }
    Ok(target)
}

fn same_origin(left: &Url, right: &Url) -> bool {
    left.scheme() == right.scheme()
        && left
            .host_str()
            .unwrap_or("")
            .eq_ignore_ascii_case(right.host_str().unwrap_or(""))
        && normalized_port(left) == normalized_port(right)
}

fn normalized_port(value: &Url) -> u16 {
    if let Some(port) = value.port() {
        return port;
    }
    if value.scheme() == "https" { 443 } else { 80 }
}

fn strip_sensitive(source: &reqwest::header::HeaderMap) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (key, value) in source.iter() {
        if !copyable_response_header(key.as_str()) {
            continue;
        }
        if let Ok(name) = HeaderName::try_from(key.as_str())
            && let Ok(copied) = HeaderValue::from_bytes(value.as_bytes())
        {
            headers.append(name, copied);
        }
    }
    headers
}

fn sanitize_reqwest(err: &reqwest::Error) -> String {
    let mut message = err.to_string();
    if let Some(url) = err.url() {
        message = message.replace(url.as_str(), "");
    }
    if message.trim().is_empty() {
        "upstream transport failed".into()
    } else {
        message
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_cookie_and_api_key() {
        assert!(!allowed_header("Cookie"));
        assert!(!allowed_header("X-Api-Key"));
        assert!(!allowed_header("Authorization"));
        assert!(allowed_header("anthropic-beta"));
        assert!(allowed_header("X-Stainless-Lang"));
        assert!(allowed_header("User-Agent"));
    }

    #[test]
    fn rejects_missing_proxy_outside_test_mode() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("worker.json");
        std::fs::write(&path, r#"{"vm_id":"vm-test","internal_token":"test"}"#).unwrap();
        let config = WorkerConfig::load(&path).unwrap();
        let error = HopClient::new(&config).err().unwrap();
        assert!(error.contains("slot proxy is required"), "{error}");
    }

    #[test]
    fn strips_sensitive_response_headers() {
        assert!(!copyable_response_header("Set-Cookie"));
        assert!(!copyable_response_header("Authorization"));
        assert!(!copyable_response_header("X-Api-Key"));
        assert!(copyable_response_header("Content-Type"));
    }
}
