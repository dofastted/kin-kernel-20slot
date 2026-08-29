use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};

use axum::{
    body::{Body, Bytes, to_bytes},
    extract::State,
    http::{HeaderMap, HeaderName, Method, Request, Response, StatusCode, Uri},
    response::IntoResponse,
};
use futures_util::StreamExt;

use crate::error::KernelError;

use super::{
    RelayState,
    correlate::{ContextScanner, CorrelationOutcome},
    sse_tap::{TapBinding, TapQueue},
    system_rewrite,
};

pub async fn proxy(State(state): State<RelayState>, req: Request<Body>) -> Response<Body> {
    state.metrics.inc_relay_requests();
    let (parts, body) = req.into_parts();
    let collected = match to_bytes(body, 32 * 1024 * 1024).await {
        Ok(bytes) => bytes,
        Err(err) => {
            return KernelError::Provider(format!("relay read body: {err}")).into_response();
        }
    };
    let scanner = Arc::new(Mutex::new(ContextScanner::new(state.runtime.secret())));
    {
        if let Ok(mut scanner) = scanner.lock() {
            scanner.push(&collected);
            scanner.finish();
        }
    }
    let outbound_bytes = system_rewrite::rewrite_messages_body(&collected);
    capture_outbound(&parts.method, &parts.uri, &parts.headers, &outbound_bytes);
    let outbound = reqwest::Body::from(outbound_bytes);
    let upstream = state
        .upstream
        .send(
            parts.method,
            &parts.uri,
            filtered_headers(&parts.headers),
            outbound,
        )
        .await;
    let outcome = {
        // Take the scanner out of the mutex so the async liveness checks in
        // last_valid() do not run under a std::sync lock.
        let taken = scanner
            .lock()
            .map(|mut guard| std::mem::replace(&mut *guard, ContextScanner::new(&[])))
            .ok();
        match taken {
            Some(scanner) => scanner.last_valid(&state.runtime).await,
            None => CorrelationOutcome::Miss,
        }
    };
    let correlated = match outcome {
        CorrelationOutcome::Matched(job) => Some(job),
        CorrelationOutcome::Miss => {
            state.metrics.inc_correlate_miss();
            None
        }
        CorrelationOutcome::Ambiguous => {
            // Root-supervisor traffic (with --forward-subagent-text) carries
            // several live jobs' tokens at once; tapping it would tee the
            // root response into an unrelated user stream.
            state.metrics.inc_correlate_ambiguous();
            tracing::debug!(
                "relay request ambiguous: multiple live job tokens; forwarding untapped"
            );
            None
        }
    };
    let tap = match &correlated {
        Some(job) => state.runtime.tap_binding(&job.job_id).await,
        None => None,
    };
    if let Some(job) = &correlated {
        state.metrics.inc_correlate_hit();
        let turn_id = tap.as_ref().map(|binding| binding.turn_id).unwrap_or(0);
        tracing::debug!(
            job_id = %job.job_id,
            slot_id = %job.slot_id,
            turn_id,
            "relay correlated request"
        );
    }
    let response = match upstream {
        Ok(response) => response,
        Err(err) => return err.into_response(),
    };
    let tap = tap.or_else(|| {
        state.tap_events.clone().map(|events| TapBinding {
            events,
            poisoned: None,
            index_allocator: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            turn_id: 0,
        })
    });
    upstream_response(response, correlated, state, tap).await
}

async fn upstream_response(
    response: reqwest::Response,
    correlated: Option<super::correlate::CorrelatedJob>,
    state: RelayState,
    tap_binding: Option<TapBinding>,
) -> Response<Body> {
    let status = response.status();
    let headers = filtered_headers(response.headers());
    let tap = if status.is_success() {
        match (correlated, tap_binding) {
            (Some(job), Some(binding)) => {
                state.runtime.register_tap_response(&job.job_id).await;
                state.metrics.inc_tap_response_started();
                Some(TapQueue::spawn(
                    job.job_id,
                    binding.events,
                    Arc::clone(&state.metrics),
                    binding.poisoned,
                    binding.index_allocator,
                    binding.turn_id,
                ))
            }
            _ => None,
        }
    } else {
        None
    };
    let stream = response.bytes_stream().map(move |chunk| match chunk {
        Ok(bytes) => {
            if let Some(tap) = &tap {
                tap.offer(bytes.clone());
            }
            Ok::<Bytes, reqwest::Error>(bytes)
        }
        Err(err) => {
            if let Some(tap) = &tap {
                tap.poison();
            }
            Err(err)
        }
    });
    build_response(status, headers, Body::from_stream(stream))
}

fn build_response(status: StatusCode, headers: HeaderMap, body: Body) -> Response<Body> {
    let mut builder = Response::builder().status(status);
    for (name, value) in headers {
        if let Some(name) = name {
            builder = builder.header(name, value);
        }
    }
    builder.body(body).unwrap_or_else(|_| {
        KernelError::Provider("relay response build failed".into()).into_response()
    })
}

fn filtered_headers(headers: &HeaderMap) -> HeaderMap {
    let mut out = HeaderMap::new();
    for (name, value) in headers {
        if is_hop_by_hop(name) {
            continue;
        }
        // The CLI offers gzip/br/zstd and decompresses transparently, but the
        // tap cannot: a compressed upstream body reaches SseDecoder as opaque
        // bytes that parse to zero events (silently — no poison under the
        // 1 MiB frame cap). Force identity by never forwarding the offer.
        // Harmless on the response path (responses don't carry this header).
        if name == "accept-encoding" {
            continue;
        }
        out.append(name.clone(), value.clone());
    }
    out
}

fn is_hop_by_hop(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "host"
            | "connection"
            | "content-length"
            | "transfer-encoding"
            | "proxy-connection"
            | "keep-alive"
            | "te"
            | "trailer"
            | "upgrade"
    )
}

fn capture_outbound(method: &Method, uri: &Uri, headers: &HeaderMap, body: &[u8]) {
    let Ok(dir) = std::env::var("KIN_RELAY_CAPTURE_DIR") else {
        return;
    };
    if dir.trim().is_empty() {
        return;
    }
    static SEQ: AtomicU64 = AtomicU64::new(1);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    let path_label = uri
        .path()
        .trim_start_matches('/')
        .replace('/', "-")
        .replace('?', "-");
    let folder = format!(
        "{dir}/{seq:03}-{method}-{path_label}",
        method = method.as_str()
    );
    let _ = std::fs::create_dir_all(&folder);
    let pretty = serde_json::from_slice::<serde_json::Value>(body)
        .ok()
        .and_then(|v| serde_json::to_vec_pretty(&v).ok())
        .unwrap_or_else(|| body.to_vec());
    let _ = std::fs::write(format!("{folder}/request-body.json"), pretty);
    let system = serde_json::from_slice::<serde_json::Value>(body)
        .ok()
        .and_then(|v| v.get("system").cloned());
    let heads: Vec<String> = match system {
        Some(serde_json::Value::Array(blocks)) => blocks
            .iter()
            .filter_map(|b| b.get("text").and_then(serde_json::Value::as_str))
            .map(|t| t.chars().take(160).collect())
            .collect(),
        Some(serde_json::Value::String(s)) => vec![s.chars().take(160).collect()],
        _ => Vec::new(),
    };
    let meta = serde_json::json!({
        "seq": seq,
        "method": method.as_str(),
        "path": uri.to_string(),
        "system_heads": heads,
        "has_authorization": headers.get("authorization").is_some(),
    });
    let _ = std::fs::write(
        format!("{folder}/meta.json"),
        serde_json::to_vec_pretty(&meta).unwrap_or_default(),
    );
}

#[cfg(test)]
mod tests {
    use axum::http::header::{AUTHORIZATION, CONTENT_LENGTH, HOST, USER_AGENT};

    use super::*;

    #[test]
    fn headers_strip_hop_by_hop_and_keep_sensitive_without_logging() {
        let mut headers = HeaderMap::new();
        headers.insert(HOST, "example.com".parse().unwrap());
        headers.insert(CONTENT_LENGTH, "123".parse().unwrap());
        headers.insert(AUTHORIZATION, "Bearer secret".parse().unwrap());
        headers.insert(USER_AGENT, "claude-cli".parse().unwrap());
        headers.insert("anthropic-beta", "beta".parse().unwrap());
        headers.insert(
            "accept-encoding",
            "gzip, deflate, br, zstd".parse().unwrap(),
        );
        let out = filtered_headers(&headers);
        assert!(!out.contains_key(HOST));
        assert!(!out.contains_key(CONTENT_LENGTH));
        // A forwarded compression offer would make the upstream body opaque
        // to the tap decoder — identity only.
        assert!(!out.contains_key("accept-encoding"));
        assert_eq!(out[AUTHORIZATION], "Bearer secret");
        assert_eq!(out[USER_AGENT], "claude-cli");
        assert_eq!(out["anthropic-beta"], "beta");
    }
}
