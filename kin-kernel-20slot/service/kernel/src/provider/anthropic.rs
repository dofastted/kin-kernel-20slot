use std::{env, time::Duration};

use async_trait::async_trait;
use reqwest::{
    Client, StatusCode,
    header::{CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue, USER_AGENT},
};
use serde_json::json;

use crate::{
    error::KernelError,
    model::MessageRequest,
    provider::{
        ExecutionContext, Provider, ProviderCapabilities, StreamRx, StreamTx, stream_channel,
    },
    stream::{StreamAssembler, StreamItem, parse_sse_block},
};

const MESSAGES_URL: &str = "https://api.anthropic.com/v1/messages";

pub struct AnthropicProvider {
    client: Client,
}

impl AnthropicProvider {
    pub fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        let api_key = env::var("ANTHROPIC_API_KEY")
            .map_err(|_| "ANTHROPIC_API_KEY is required for KIN_PROVIDER=anthropic_api")?;
        let mut api_key_header = HeaderValue::from_str(&api_key)?;
        api_key_header.set_sensitive(true);

        let mut headers = HeaderMap::new();
        headers.insert(HeaderName::from_static("x-api-key"), api_key_header);
        headers.insert(
            HeaderName::from_static("anthropic-version"),
            HeaderValue::from_static("2023-06-01"),
        );
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.insert(
            USER_AGENT,
            HeaderValue::from_static(concat!("kin-kernel/", env!("CARGO_PKG_VERSION"))),
        );

        let client = Client::builder()
            .default_headers(headers)
            .timeout(Duration::from_secs(300))
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        Ok(Self { client })
    }
}

#[async_trait]
impl Provider for AnthropicProvider {
    fn name(&self) -> &'static str {
        "anthropic_api"
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            streaming: true,
            resume: false,
            multiplex_slots: true,
            native_tool_wait: false,
            cancel_receipt: false,
        }
    }

    async fn execute_stream(
        &self,
        request: &MessageRequest,
        _context: &ExecutionContext,
    ) -> Result<StreamRx, KernelError> {
        let mut body =
            serde_json::to_value(request).map_err(|err| KernelError::Provider(err.to_string()))?;
        body["stream"] = json!(true);

        let response = self
            .client
            .post(MESSAGES_URL)
            .json(&body)
            .send()
            .await
            .map_err(|error| KernelError::Provider(error.to_string()))?;

        let status = response.status();
        let request_id = response
            .headers()
            .get("request-id")
            .or_else(|| response.headers().get("x-request-id"))
            .and_then(|value| value.to_str().ok())
            .unwrap_or("unknown")
            .to_string();

        if status == StatusCode::TOO_MANY_REQUESTS {
            let retry_after = response
                .headers()
                .get("retry-after")
                .and_then(|value| value.to_str().ok())
                .map(ToOwned::to_owned);
            return Err(KernelError::ProviderRateLimited { retry_after });
        }
        if !status.is_success() {
            return Err(KernelError::Provider(format!(
                "upstream status {status}; request-id={request_id}"
            )));
        }

        let (tx, rx) = stream_channel();
        let model = request.model.clone();
        let request = request.clone();
        tokio::spawn(async move {
            if let Err(err) = pump_anthropic_sse(response, &model, &request, &tx).await {
                let _ = tx.send(Err(err)).await;
            }
        });
        Ok(rx)
    }
}

async fn pump_anthropic_sse(
    response: reqwest::Response,
    model: &str,
    request: &MessageRequest,
    tx: &StreamTx,
) -> Result<(), KernelError> {
    let mut assembler = StreamAssembler::new(model);
    let mut response = response;
    let mut buf = String::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|err| KernelError::Provider(err.to_string()))?
    {
        buf.push_str(&String::from_utf8_lossy(&chunk));
        while let Some(pos) = split_sse(&buf) {
            let block = buf[..pos].to_string();
            buf.replace_range(..pos, "");
            let Some(event) = parse_sse_block(&block) else {
                continue;
            };
            assembler.apply_event(&event);
            if tx.send(Ok(StreamItem::Event(event))).await.is_err() {
                return Ok(());
            }
        }
    }
    if !buf.trim().is_empty() {
        if let Some(event) = parse_sse_block(&buf) {
            assembler.apply_event(&event);
            let _ = tx.send(Ok(StreamItem::Event(event))).await;
        }
    }
    let finished = assembler.finish(request);
    let _ = tx.send(Ok(StreamItem::Finished(finished))).await;
    Ok(())
}

fn split_sse(buf: &str) -> Option<usize> {
    if let Some(pos) = buf.find("\n\n") {
        return Some(pos + 2);
    }
    buf.find("\r\n\r\n").map(|pos| pos + 4)
}
