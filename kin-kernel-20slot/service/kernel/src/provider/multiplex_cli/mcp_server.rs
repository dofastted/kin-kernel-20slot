use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode, header::ACCEPT},
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{get, post},
};
use futures_util::Stream;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::mpsc::Receiver;

use super::Runtime;
use crate::error::KernelError;

pub async fn spawn(runtime: Arc<Runtime>, bind: SocketAddr) -> Result<SocketAddr, KernelError> {
    let listener = TcpListener::bind(bind)
        .await
        .map_err(|err| KernelError::Provider(format!("mcp bind: {err}")))?;
    let addr = listener
        .local_addr()
        .map_err(|err| KernelError::Provider(err.to_string()))?;
    let app = Router::new()
        .route("/mcp", post(mcp_post).get(mcp_get))
        .route("/healthz", get(|| async { "ok" }))
        .with_state(runtime);
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Ok(addr)
}

async fn mcp_get() -> impl IntoResponse {
    Sse::new(futures_util::stream::empty::<
        Result<Event, std::convert::Infallible>,
    >())
    .keep_alive(KeepAlive::new().interval(Duration::from_secs(20)))
}

async fn mcp_post(
    State(runtime): State<Arc<Runtime>>,
    headers: HeaderMap,
    Json(msg): Json<Value>,
) -> Response {
    let sse = headers
        .get(ACCEPT)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.contains("text/event-stream"))
        .unwrap_or(false);
    let id = msg.get("id").cloned();
    let method = msg.get("method").and_then(Value::as_str).unwrap_or("");
    match method {
        "initialize" => json_ok(
            id,
            json!({
                "protocolVersion": "2024-11-05",
                "capabilities": { "tools": { "listChanged": false } },
                "serverInfo": { "name": "kin_runtime", "version": "0.1.0" }
            }),
        ),
        "notifications/initialized" | "notifications/cancelled" => {
            StatusCode::ACCEPTED.into_response()
        }
        "ping" => json_ok(id, json!({})),
        "tools/list" => json_ok(id, json!({ "tools": tools() })),
        "tools/call" => {
            let name = msg
                .pointer("/params/name")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let args = msg
                .pointer("/params/arguments")
                .cloned()
                .unwrap_or_else(|| json!({}));
            let progress_token = msg
                .pointer("/params/_meta/progressToken")
                .cloned()
                .or_else(|| msg.pointer("/params/progressToken").cloned());
            if sse {
                return tool_sse(runtime, id, name, args, progress_token).await;
            }
            match dispatch(&runtime, &name, args).await {
                Ok(result) => json_ok(id, tool_result(result)),
                Err(err) => json_err(id, err.to_string()),
            }
        }
        _ => json_err(id, format!("unknown method {method}")),
    }
}

async fn dispatch(runtime: &Runtime, name: &str, args: Value) -> Result<Value, KernelError> {
    let short = name.rsplit("__").next().unwrap_or(name);
    match short {
        "slot_wait" => runtime.mcp_slot_wait(args).await,
        "client_tool" => runtime.mcp_client_tool(args).await,
        "kin_done" => runtime.mcp_kin_done(args).await,
        "kin_fail" => runtime.mcp_kin_fail(args).await,
        other => Err(KernelError::Provider(format!("unknown tool {other}"))),
    }
}

/// Claude Code 2.1.x Zod-validates `notifications/progress.params.progressToken`
/// as string|number. A missing token throws in the notification handler and
/// drops the MCP HTTP connection, so idle `slot_wait` never receives the job.
fn progress_notification(token: &Value, progress: u64) -> Value {
    json!({
        "jsonrpc": "2.0",
        "method": "notifications/progress",
        "params": {
            "progressToken": token,
            "progress": progress,
            "total": progress.saturating_add(1),
            "message": "waiting"
        }
    })
}

async fn tool_sse(
    runtime: Arc<Runtime>,
    id: Option<Value>,
    name: String,
    args: Value,
    progress_token: Option<Value>,
) -> Response {
    let (tx, rx) = tokio::sync::mpsc::channel(128);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(15));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval.tick().await;
        let work = dispatch(&runtime, &name, args);
        tokio::pin!(work);
        let mut progress_n = 0u64;
        loop {
            tokio::select! {
                result = &mut work => {
                    let body = match result {
                        Ok(value) => jsonrpc_ok(id, tool_result(value)),
                        Err(err) => jsonrpc_err(id, err.to_string()),
                    };
                    let _ = tx.send(Ok(Event::default().event("message").data(body.to_string()))).await;
                    break;
                }
                _ = interval.tick() => {
                    progress_n += 1;
                    let event = if let Some(token) = &progress_token {
                        Event::default()
                            .event("message")
                            .data(progress_notification(token, progress_n).to_string())
                    } else {
                        Event::default().comment("waiting")
                    };
                    if tx.send(Ok(event)).await.is_err() {
                        break;
                    }
                }
            }
        }
    });
    Sse::new(ReceiverStream(rx))
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
        .into_response()
}

struct ReceiverStream(Receiver<Result<Event, std::convert::Infallible>>);

impl Stream for ReceiverStream {
    type Item = Result<Event, std::convert::Infallible>;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.0.poll_recv(cx)
    }
}

fn tools() -> Vec<Value> {
    vec![
        tool(
            "slot_wait",
            "Block until Kin assigns the next job",
            json!({
                "type": "object",
                "properties": { "slot_id": { "type": "string" } }
            }),
        ),
        tool(
            "client_tool",
            "Block this agent loop until the HTTP client returns tool_result",
            json!({
                "type": "object",
                "properties": {
                    "job_id": { "type": "string" },
                    "name": { "type": "string" },
                    "input": { "type": "object" },
                    "client_tool_use_id": { "type": "string" }
                },
                "required": ["job_id", "name"]
            }),
        ),
        tool(
            "kin_done",
            "Finish a job after its client-visible answer has already been emitted as ordinary assistant text. Do not repeat the answer in tool arguments. text and fallback_content are legacy fallback fields only.",
            json!({
                "type": "object",
                "properties": {
                    "job_id": { "type": "string" },
                    "stop_reason": { "type": "string" },
                    "usage": { "type": "object" },
                    "final_digest": { "type": "string" },
                    "fallback_content": { "type": "string" },
                    "text": { "type": "string" }
                },
                "required": ["job_id"]
            }),
        ),
        tool(
            "kin_fail",
            "Mark the assigned job failed",
            json!({
                "type": "object",
                "properties": {
                    "job_id": { "type": "string" },
                    "error": { "type": "string" },
                    "retire": { "type": "boolean" }
                },
                "required": ["job_id"]
            }),
        ),
    ]
}

fn tool(name: &str, description: &str, schema: Value) -> Value {
    json!({ "name": name, "description": description, "inputSchema": schema })
}

fn tool_result(value: Value) -> Value {
    json!({
        "content": [{ "type": "text", "text": value.to_string() }],
        "structuredContent": value
    })
}

fn jsonrpc_ok(id: Option<Value>, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

fn jsonrpc_err(id: Option<Value>, message: String) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": -32000, "message": message } })
}

fn json_ok(id: Option<Value>, result: Value) -> Response {
    Json(jsonrpc_ok(id, result)).into_response()
}

fn json_err(id: Option<Value>, message: String) -> Response {
    Json(jsonrpc_err(id, message)).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kin_done_description_does_not_ask_for_answer_text() {
        let kin_done = tools()
            .into_iter()
            .find(|tool| tool["name"] == "kin_done")
            .expect("kin_done tool");
        let desc = kin_done["description"].as_str().unwrap();
        assert!(desc.contains("ordinary assistant text"));
        assert!(!desc.contains("Put the complete final answer in `text`"));
        assert_eq!(kin_done["inputSchema"]["required"], json!(["job_id"]));
        assert!(kin_done["inputSchema"]["properties"].get("text").is_some());
    }

    #[test]
    fn progress_includes_token() {
        let note = progress_notification(&json!("tok-1"), 3);
        assert_eq!(note["method"], "notifications/progress");
        assert_eq!(note["params"]["progressToken"], "tok-1");
        assert_eq!(note["params"]["progress"], 3);
        let note_n = progress_notification(&json!(7), 1);
        assert_eq!(note_n["params"]["progressToken"], 7);
    }
}
