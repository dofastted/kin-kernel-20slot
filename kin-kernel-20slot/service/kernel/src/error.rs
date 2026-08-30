use axum::{
    Json,
    http::{HeaderValue, StatusCode, header::RETRY_AFTER},
    response::{IntoResponse, Response},
};
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum KernelError {
    #[error("{0}")]
    InvalidRequest(String),
    /// Reserved 501 surface: constructed by callers that reject a
    /// request feature this build does not implement. Kept even while
    /// unconstructed so the HTTP contract stays stable.
    #[allow(dead_code)]
    #[error("{0}")]
    UnsupportedFeature(String),
    #[error("no compatible capacity is currently available")]
    NoCapacity,
    #[error("runtime overloaded")]
    Overloaded { retry_after: Option<String> },
    #[error("{0}")]
    ContinuationMismatch(String),
    #[error("the bound runtime is no longer available")]
    ContinuationLost,
    #[error("provider error: {0}")]
    Provider(String),
    #[error("provider rate limit reached")]
    ProviderRateLimited { retry_after: Option<String> },
    #[error("internal error")]
    Internal,
}

#[derive(Serialize)]
struct ErrorEnvelope {
    r#type: &'static str,
    error: ErrorBody,
}

#[derive(Serialize)]
struct ErrorBody {
    code: &'static str,
    message: String,
    retryable: bool,
}

impl IntoResponse for KernelError {
    fn into_response(self) -> Response {
        let (status, code, retryable) = match &self {
            Self::InvalidRequest(_) => (StatusCode::BAD_REQUEST, "invalid_request", false),
            Self::UnsupportedFeature(_) => {
                (StatusCode::NOT_IMPLEMENTED, "unsupported_feature", false)
            }
            Self::NoCapacity => (StatusCode::SERVICE_UNAVAILABLE, "no_capacity", true),
            Self::Overloaded { .. } => (StatusCode::SERVICE_UNAVAILABLE, "overloaded", true),
            Self::ContinuationMismatch(_) => (StatusCode::CONFLICT, "continuation_mismatch", false),
            Self::ContinuationLost => (StatusCode::CONFLICT, "continuation_lost", false),
            Self::Provider(_) => (StatusCode::BAD_GATEWAY, "provider_error", true),
            Self::ProviderRateLimited { .. } => {
                (StatusCode::TOO_MANY_REQUESTS, "provider_rate_limited", true)
            }
            Self::Internal => (StatusCode::INTERNAL_SERVER_ERROR, "internal_error", false),
        };

        let retry_after = match &self {
            Self::ProviderRateLimited { retry_after } => retry_after.clone(),
            Self::Overloaded { retry_after } => retry_after.clone(),
            _ => None,
        };
        let mut response = (
            status,
            Json(ErrorEnvelope {
                r#type: "error",
                error: ErrorBody {
                    code,
                    message: self.to_string(),
                    retryable,
                },
            }),
        )
            .into_response();
        if let Some(value) = retry_after.and_then(|value| HeaderValue::from_str(&value).ok()) {
            response.headers_mut().insert(RETRY_AFTER, value);
        }
        response
    }
}
