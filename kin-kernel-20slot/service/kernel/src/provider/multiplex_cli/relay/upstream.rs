use std::env;

use axum::http::{HeaderMap, Method, Uri};
use reqwest::{Client, Proxy};

use crate::error::KernelError;

#[derive(Clone)]
pub struct UpstreamClient {
    base: String,
    client: Client,
}

impl UpstreamClient {
    pub fn new(base: &str) -> Result<Self, KernelError> {
        let mut builder = Client::builder();
        if let Ok(proxy) = env::var("KIN_SOCKS5")
            && !proxy.trim().is_empty()
        {
            builder = builder.proxy(Proxy::all(socks5h_url(&proxy)).map_err(proxy_error)?);
        } else if let Ok(proxy) = env::var("KIN_HTTPS_PROXY")
            && !proxy.trim().is_empty()
        {
            builder = builder.proxy(Proxy::https(proxy).map_err(proxy_error)?);
        }
        let client = builder
            .build()
            .map_err(|err| KernelError::Provider(format!("relay upstream client: {err}")))?;
        Ok(Self {
            base: base.trim_end_matches('/').to_string(),
            client,
        })
    }

    pub async fn send(
        &self,
        method: Method,
        uri: &Uri,
        headers: HeaderMap,
        body: reqwest::Body,
    ) -> Result<reqwest::Response, KernelError> {
        let path = uri.path_and_query().map(|pq| pq.as_str()).unwrap_or("/");
        let url = format!("{}{}", self.base, path);
        let method = reqwest::Method::from_bytes(method.as_str().as_bytes())
            .map_err(|err| KernelError::Provider(format!("relay method: {err}")))?;
        self.client
            .request(method, url)
            .headers(headers)
            .body(body)
            .send()
            .await
            .map_err(|err| KernelError::Provider(format!("relay upstream send: {err}")))
    }
}

fn socks5h_url(value: &str) -> String {
    let trimmed = value.trim();
    if let Some(rest) = trimmed.strip_prefix("socks5://") {
        format!("socks5h://{rest}")
    } else if trimmed.contains("://") {
        trimmed.to_string()
    } else {
        format!("socks5h://{trimmed}")
    }
}

fn proxy_error(err: reqwest::Error) -> KernelError {
    KernelError::Provider(format!("relay proxy config: {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn socks5h_url_adds_scheme_only_when_missing() {
        assert_eq!(socks5h_url("127.0.0.1:10808"), "socks5h://127.0.0.1:10808");
        assert_eq!(
            socks5h_url("socks5h://127.0.0.1:10808"),
            "socks5h://127.0.0.1:10808"
        );
        assert_eq!(
            socks5h_url("socks5://127.0.0.1:10808"),
            "socks5h://127.0.0.1:10808"
        );
    }
}
