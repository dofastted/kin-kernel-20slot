//! Local replay of a recorded Claude stdout trace.
//!
//! Does not talk to Claude. 20 virtual sessions share the same bytes
//! (`PayloadMode::Shared`) or independently parse (`Independent`) to bound
//! gateway memory without spending tokens.

use std::sync::{
    Arc,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};
use std::time::Duration;

use serde_json::Value;
use tokio::sync::mpsc;

use super::job_stream::JobStream;
use crate::config::EVENT_CHANNEL_SIZE;
use crate::error::KernelError;
use crate::stream::StreamItem;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PayloadMode {
    Shared,
    Independent,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientKind {
    Normal,
    Slow,
    Stalled,
    Disconnected,
}

#[derive(Clone)]
pub struct Trace {
    pub lines: Arc<[Arc<[u8]>]>,
    pub frames: Arc<[Value]>,
}

impl Trace {
    pub fn from_ndjson(text: &str) -> Result<Self, KernelError> {
        let mut lines = Vec::new();
        let mut frames = Vec::new();
        for raw in text.lines() {
            let raw = raw.trim();
            if raw.is_empty() {
                continue;
            }
            let value: Value = serde_json::from_str(raw)
                .map_err(|err| KernelError::Provider(format!("trace parse: {err}")))?;
            let frame = value.get("frame").cloned().unwrap_or(value);
            lines.push(Arc::from(raw.as_bytes().to_vec()));
            frames.push(frame);
        }
        Ok(Self {
            lines: lines.into(),
            frames: frames.into(),
        })
    }

    pub fn synthetic() -> Self {
        let frames = vec![
            serde_json::json!({
                "type": "assistant",
                "parent_tool_use_id": "parent_rec",
                "message": {"content": [
                    {"type":"tool_use","id":"toolu_ws","name":"WebSearch","input":{"query":"SPCX ETF"}},
                ]}
            }),
            serde_json::json!({
                "type": "user",
                "parent_tool_use_id": "parent_rec",
                "message": {"content": [
                    {"type":"tool_result","tool_use_id":"toolu_ws","content":"Web search results for SPCX"}
                ]}
            }),
            serde_json::json!({
                "type": "assistant",
                "parent_tool_use_id": "parent_rec",
                "message": {"content": [
                    {"type":"text","text":"SPCX is a SPAC ETF. ".repeat(80)}
                ]}
            }),
        ];
        let lines: Vec<Arc<[u8]>> = frames
            .iter()
            .map(|frame| Arc::from(frame.to_string().into_bytes()))
            .collect();
        Self {
            lines: lines.into(),
            frames: frames.into(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct VirtualIds {
    pub session_id: String,
    pub job_id: String,
    pub parent_tool_use_id: String,
}

impl VirtualIds {
    pub fn new(index: usize) -> Self {
        Self {
            session_id: format!("vsess-{index:02}"),
            job_id: format!("job-{index:02}"),
            parent_tool_use_id: format!("parent-{index:02}"),
        }
    }
}

#[derive(Default)]
pub struct ReplayStats {
    pub events_emitted: AtomicU64,
    pub stage_dropped: AtomicU64,
    pub jobs_aborted_slow_client: AtomicU64,
    pub successful_text_delta_lost: AtomicU64,
    pub finished: AtomicUsize,
    pub peak_inflight: AtomicUsize,
    inflight: AtomicUsize,
}

impl ReplayStats {
    fn begin(&self) {
        let n = self.inflight.fetch_add(1, Ordering::Relaxed) + 1;
        self.peak_inflight.fetch_max(n, Ordering::Relaxed);
    }
    fn end(&self) {
        self.inflight.fetch_sub(1, Ordering::Relaxed);
        self.finished.fetch_add(1, Ordering::Relaxed);
    }
}

pub fn client_kind(index: usize) -> ClientKind {
    match index {
        0 => ClientKind::Disconnected,
        1 => ClientKind::Stalled,
        2..=4 => ClientKind::Slow,
        _ => ClientKind::Normal,
    }
}

pub async fn replay_one(
    trace: Trace,
    ids: VirtualIds,
    mode: PayloadMode,
    kind: ClientKind,
    stats: Arc<ReplayStats>,
    time_scale: f64,
) {
    stats.begin();
    let cap = match kind {
        ClientKind::Stalled | ClientKind::Slow => 2,
        _ => EVENT_CHANNEL_SIZE,
    };
    let (tx, mut rx) = mpsc::channel::<Result<StreamItem, KernelError>>(cap);
    let consumer = tokio::spawn(async move {
        match kind {
            ClientKind::Disconnected => {}
            ClientKind::Stalled => {
                tokio::time::sleep(Duration::from_millis(80)).await;
            }
            ClientKind::Slow => {
                while let Some(item) = rx.recv().await {
                    let _ = item;
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            }
            ClientKind::Normal => while rx.recv().await.is_some() {},
        }
    });
    tokio::task::yield_now().await;
    tokio::time::sleep(Duration::from_millis(8)).await;

    let mut stream = JobStream::new();
    let mut aborted = false;
    let mut text_delta_lost = 0u64;
    let start = JobStream::message_start("claude-sonnet-5", &ids.job_id);
    match emit(&tx, stats.as_ref(), StreamItem::Event(start)) {
        EmitResult::Sent | EmitResult::StageDropped | EmitResult::Closed => {}
        EmitResult::TextDeltaLost => {
            aborted = true;
            text_delta_lost += 1;
        }
    }
    if time_scale > 0.0 {
        tokio::time::sleep(Duration::from_micros((500.0 * time_scale) as u64)).await;
    }

    match mode {
        PayloadMode::Shared => {
            for frame in trace.frames.iter() {
                if aborted {
                    break;
                }
                let events = stream.ingest(frame);
                for event in events {
                    match emit(&tx, stats.as_ref(), StreamItem::Event(event)) {
                        EmitResult::Sent | EmitResult::StageDropped | EmitResult::Closed => {}
                        EmitResult::TextDeltaLost => {
                            aborted = true;
                            text_delta_lost += 1;
                            break;
                        }
                    }
                }
            }
        }
        PayloadMode::Independent => {
            for line in trace.lines.iter() {
                if aborted {
                    break;
                }
                let parsed: Value = match serde_json::from_slice(line) {
                    Ok(value) => value,
                    Err(_) => continue,
                };
                let frame = parsed.get("frame").cloned().unwrap_or(parsed);
                let events = stream.ingest(&frame);
                for event in events {
                    match emit(&tx, stats.as_ref(), StreamItem::Event(event)) {
                        EmitResult::Sent | EmitResult::StageDropped | EmitResult::Closed => {}
                        EmitResult::TextDeltaLost => {
                            aborted = true;
                            text_delta_lost += 1;
                            break;
                        }
                    }
                }
            }
        }
    }
    if aborted {
        stats
            .jobs_aborted_slow_client
            .fetch_add(1, Ordering::Relaxed);
    } else {
        stats
            .successful_text_delta_lost
            .fetch_add(text_delta_lost, Ordering::Relaxed);
        for event in stream.finish("end_turn", serde_json::json!({})) {
            match emit(&tx, stats.as_ref(), StreamItem::Event(event)) {
                EmitResult::Sent | EmitResult::StageDropped | EmitResult::Closed => {}
                EmitResult::TextDeltaLost => {
                    stats
                        .successful_text_delta_lost
                        .fetch_add(1, Ordering::Relaxed);
                    break;
                }
            }
        }
    }
    drop(tx);
    let _ = consumer.await;
    stats.end();
}

fn emit(
    tx: &mpsc::Sender<Result<StreamItem, KernelError>>,
    stats: &ReplayStats,
    item: StreamItem,
) -> EmitResult {
    stats.events_emitted.fetch_add(1, Ordering::Relaxed);
    match tx.try_send(Ok(item)) {
        Ok(()) => EmitResult::Sent,
        Err(mpsc::error::TrySendError::Full(Ok(item))) => {
            if is_lossless_delta(&item) {
                EmitResult::TextDeltaLost
            } else {
                stats.stage_dropped.fetch_add(1, Ordering::Relaxed);
                EmitResult::StageDropped
            }
        }
        Err(mpsc::error::TrySendError::Full(Err(_))) => {
            stats.stage_dropped.fetch_add(1, Ordering::Relaxed);
            EmitResult::StageDropped
        }
        Err(mpsc::error::TrySendError::Closed(_)) => EmitResult::Closed,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EmitResult {
    Sent,
    StageDropped,
    TextDeltaLost,
    Closed,
}

fn is_lossless_delta(item: &StreamItem) -> bool {
    let StreamItem::Event(event) = item else {
        return true;
    };
    matches!(
        event.get("type").and_then(Value::as_str),
        Some(
            "message_start"
                | "content_block_start"
                | "content_block_delta"
                | "content_block_stop"
                | "message_delta"
                | "message_stop"
        )
    )
}

pub async fn replay_pool(
    trace: Trace,
    concurrency: usize,
    mode: PayloadMode,
    repeats: usize,
) -> Arc<ReplayStats> {
    let stats = Arc::new(ReplayStats::default());
    for _ in 0..repeats {
        let mut joins = Vec::with_capacity(concurrency);
        for index in 0..concurrency {
            let trace = trace.clone();
            let stats = Arc::clone(&stats);
            joins.push(tokio::spawn(async move {
                replay_one(
                    trace,
                    VirtualIds::new(index),
                    mode,
                    client_kind(index),
                    stats,
                    0.0,
                )
                .await;
            }));
        }
        for join in joins {
            let _ = join.await;
        }
    }
    stats
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{MessageResponse, StopReason, Usage};

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn twenty_shared_replays_do_not_mix_sessions() {
        let stats = replay_pool(Trace::synthetic(), 20, PayloadMode::Shared, 2).await;
        assert_eq!(stats.finished.load(Ordering::Relaxed), 40);
        assert!(stats.events_emitted.load(Ordering::Relaxed) >= 40);
        assert!(stats.peak_inflight.load(Ordering::Relaxed) >= 10);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn independent_parse_survives_fifty_rounds() {
        let stats = replay_pool(Trace::synthetic(), 20, PayloadMode::Independent, 50).await;
        assert_eq!(stats.finished.load(Ordering::Relaxed), 1000);
        assert!(
            stats.jobs_aborted_slow_client.load(Ordering::Relaxed) > 0,
            "stalled/slow clients must abort before losing text deltas"
        );
        assert_eq!(stats.successful_text_delta_lost.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn virtual_ids_are_unique() {
        let a = VirtualIds::new(0);
        let b = VirtualIds::new(1);
        assert_ne!(a.parent_tool_use_id, b.parent_tool_use_id);
        assert_ne!(a.session_id, b.session_id);
    }

    #[test]
    fn structural_events_are_lossless_but_ping_can_drop() {
        assert!(is_lossless_delta(&StreamItem::Event(serde_json::json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "text", "text": ""}
        }))));
        assert!(is_lossless_delta(&StreamItem::Event(serde_json::json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "input_json_delta", "partial_json": "{}"}
        }))));
        assert!(is_lossless_delta(&StreamItem::Finished(MessageResponse {
            id: "msg".into(),
            r#type: "message",
            role: "assistant",
            model: "claude-sonnet-5".into(),
            content: Vec::new(),
            stop_reason: StopReason::EndTurn,
            usage: Usage::default(),
        })));
        assert!(!is_lossless_delta(&StreamItem::Event(serde_json::json!({
            "type": "ping"
        }))));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn recorded_spcx_trace_replays_if_present() {
        let path = "/workspace/artifacts/spcx-long-trace/response.ndjson";
        let Ok(text) = std::fs::read_to_string(path) else {
            return;
        };
        if text.len() < 200 {
            return;
        }
        let trace = Trace::from_ndjson(&text).expect("recorded ndjson");
        assert!(!trace.frames.is_empty());
        let frames = trace.frames.len();
        let stats = replay_pool(trace, 20, PayloadMode::Independent, 50).await;
        assert_eq!(stats.finished.load(Ordering::Relaxed), 1000);
        let report = serde_json::json!({
            "concurrency": 20,
            "repeats": 50,
            "mode": "independent",
            "finished": stats.finished.load(Ordering::Relaxed),
            "events_emitted": stats.events_emitted.load(Ordering::Relaxed),
            "stage_dropped": stats.stage_dropped.load(Ordering::Relaxed),
            "jobs_aborted_slow_client": stats.jobs_aborted_slow_client.load(Ordering::Relaxed),
            "successful_text_delta_lost": stats.successful_text_delta_lost.load(Ordering::Relaxed),
            "peak_inflight": stats.peak_inflight.load(Ordering::Relaxed),
            "frames": frames,
        });
        let _ = std::fs::write(
            "/workspace/artifacts/spcx-long-trace/replay-stats.json",
            serde_json::to_string_pretty(&report).unwrap_or_default(),
        );
    }
}
