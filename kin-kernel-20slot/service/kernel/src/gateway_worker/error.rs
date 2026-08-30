use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;

#[derive(Debug)]
pub struct WorkerError {
    pub status: StatusCode,
    pub code: &'static str,
    pub message: String,
}

impl WorkerError {
    pub fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: truncate(&message.into()),
        }
    }
}

#[derive(Serialize)]
struct Envelope {
    ok: bool,
    error: ErrorBody,
}

#[derive(Serialize)]
struct ErrorBody {
    #[serde(rename = "type")]
    kind: &'static str,
    code: &'static str,
    message: String,
}

impl IntoResponse for WorkerError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(Envelope {
                ok: false,
                error: ErrorBody {
                    kind: "worker_error",
                    code: self.code,
                    message: self.message,
                },
            }),
        )
            .into_response()
    }
}

pub fn truncate(message: &str) -> String {
    if message.len() <= 300 {
        return message.to_string();
    }
    let mut end = 300;
    while end > 0 && !message.is_char_boundary(end) {
        end -= 1;
    }
    message[..end].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncates_to_300_bytes() {
        let long = "a".repeat(400);
        assert_eq!(truncate(&long).len(), 300);
    }
}
