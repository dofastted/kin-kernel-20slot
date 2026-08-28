use std::sync::{Arc, Mutex};

use axum::{
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, HeaderName, Request, Response, StatusCode},
    response::IntoResponse,
};
use futures_util::{StreamExt, TryStreamExt};

use crate::error::KernelError;

use super::{
    RelayState,
    correlate::{ContextScanner, last_valid_candidate},
    sse_tap::TapQueue,
};

pub async fn proxy(State(state): State<RelayState>, req: Request<Body>) -> Response<Body> {
    state.metrics.inc_relay_requests();
    let (parts, body) = req.into_parts();
    let scanner = Arc::new(Mutex::new(ContextScanner::default()));
    let scan_ref = Arc::clone(&scanner);
    let body = body.into_data_stream().inspect_ok(move |bytes| {
        if let Ok(mut scanner) = scan_ref.lock() {
            scanner.push(bytes);
        }
    });
    let outbound = reqwest::Body::wrap_stream(body);
    let upstream = state
        .upstream
        .send(
            parts.method,
            &parts.uri,
            filtered_headers(&parts.headers),
            outbound,
        )
        .await;
    if let Ok(mut scanner) = scanner.lock() {
        scanner.finish();
    }
    let candidates = match scanner.lock() {
        Ok(scanner) => scanner.candidates().to_vec(),
        Err(_) => Vec::new(),
    };
    let correlated = match candidates.is_empty() {
        true => None,
        false => last_valid_candidate(&candidates, &state.runtime).await,
    };
    let response = match upstream {
        Ok(response) => response,
        Err(err) => return err.into_response(),
    };
    let tap = match &correlated {
        Some(job) => state.runtime.tap_binding(&job.job_id).await,
        None => None,
    };
    let tap = tap.or_else(|| state.tap_events.clone().map(|events| (events, None)));
    upstream_response(response, correlated, state, tap)
}

fn upstream_response(
    response: reqwest::Response,
    correlated: Option<super::correlate::CorrelatedJob>,
    state: RelayState,
    tap_binding: Option<(
        tokio::sync::mpsc::Sender<super::sse_tap::TapEvent>,
        Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    )>,
) -> Response<Body> {
    let status = response.status();
    let headers = filtered_headers(response.headers());
    let tap = if status.is_success() {
        correlated.and_then(|job| {
            tap_binding.map(|(events, poisoned)| {
                TapQueue::spawn(job.job_id, events, Arc::clone(&state.metrics), poisoned)
            })
        })
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
        Err(err) => Err(err),
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
        let out = filtered_headers(&headers);
        assert!(!out.contains_key(HOST));
        assert!(!out.contains_key(CONTENT_LENGTH));
        assert_eq!(out[AUTHORIZATION], "Bearer secret");
        assert_eq!(out[USER_AGENT], "claude-cli");
        assert_eq!(out["anthropic-beta"], "beta");
    }
}
