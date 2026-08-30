pub mod anthropic;
pub mod cli_auth;
pub mod mock;
pub mod multiplex_cli;

use async_trait::async_trait;
use tokio::sync::mpsc::Receiver;

use crate::{
    config::{CLIENT_CHANNEL_SIZE, EVENT_CHANNEL_SIZE},
    error::KernelError,
    model::{MessageRequest, MessageResponse},
    stream::StreamItem,
};

pub type StreamTx = tokio::sync::mpsc::Sender<Result<StreamItem, KernelError>>;
pub type StreamRx = Receiver<Result<StreamItem, KernelError>>;

pub fn stream_channel() -> (StreamTx, StreamRx) {
    tokio::sync::mpsc::channel(CLIENT_CHANNEL_SIZE)
}

pub fn job_event_channel() -> (StreamTx, StreamRx) {
    tokio::sync::mpsc::channel(EVENT_CHANNEL_SIZE)
}

#[derive(Clone, Debug)]
pub struct ExecutionContext {
    pub tenant_id: String,
    pub session_id: String,
    pub worker_id: String,
    pub worker_generation: u64,
    pub resumed: bool,
}

#[derive(Clone, Debug)]
pub struct ProviderCapabilities {
    pub streaming: bool,
    pub resume: bool,
    pub multiplex_slots: bool,
    pub native_tool_wait: bool,
    pub cancel_receipt: bool,
}

#[async_trait]
pub trait Provider: Send + Sync {
    fn name(&self) -> &'static str;
    fn capabilities(&self) -> ProviderCapabilities;

    /// Convenience default on the Provider contract. Current call sites
    /// drive `execute_stream` + `collect_stream` themselves.
    #[allow(dead_code)]
    async fn execute(
        &self,
        request: &MessageRequest,
        context: &ExecutionContext,
    ) -> Result<MessageResponse, KernelError> {
        collect_stream(self.execute_stream(request, context).await?).await
    }

    async fn execute_stream(
        &self,
        request: &MessageRequest,
        context: &ExecutionContext,
    ) -> Result<StreamRx, KernelError>;

    /// Start long-lived workers (Claude PID + slots). Default is a no-op.
    /// Multiplexed CLI boots here so HTTP cancel cannot abort slot spawn.
    async fn boot(&self) -> Result<(), KernelError> {
        Ok(())
    }

    fn session_pid(&self, _session_id: &str) -> Option<u32> {
        None
    }

    /// Diagnostic only: current slot_id bound to this session, if the
    /// provider tracks per-session slot state (native execution modes).
    fn session_slot(&self, _session_id: &str) -> Option<String> {
        None
    }

    fn memory_snapshot(&self) -> Option<serde_json::Value> {
        None
    }

    /// True if the native host's config_hash handshake was rejected
    /// (design.md §6, AC14). Default: no such check exists for this provider.
    fn config_hash_mismatch(&self) -> bool {
        false
    }
}

pub async fn collect_stream(mut rx: StreamRx) -> Result<MessageResponse, KernelError> {
    let mut finished = None;
    while let Some(item) = rx.recv().await {
        match item? {
            StreamItem::Event(_) => {}
            StreamItem::Finished(response) => finished = Some(response),
        }
    }
    finished.ok_or_else(|| KernelError::Provider("stream ended without a result".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ContentBlock, StopReason};
    use crate::stream::StreamAssembler;
    use serde_json::json;

    /// OQ1: the non-streaming path (`stream:false`) returns whatever the
    /// provider put in `StreamItem::Finished`, discarding the individual
    /// events. Since that response is built by the same `StreamAssembler`
    /// the streaming path feeds, a `tool_use` block assembled from
    /// `input_json_delta` fragments must survive into the aggregated
    /// response with its input intact — there is no separate accumulation
    /// path that could drop it.
    #[tokio::test]
    async fn collect_stream_preserves_assembled_tool_use_input() {
        let mut assembler = StreamAssembler::new("claude-sonnet-5");
        for event in [
            json!({"type":"content_block_start","index":0,
                   "content_block":{"type":"tool_use","id":"toolu_1","name":"get_weather","input":{}}}),
            json!({"type":"content_block_delta","index":0,
                   "delta":{"type":"input_json_delta","partial_json":"{\"city\":"}}),
            json!({"type":"content_block_delta","index":0,
                   "delta":{"type":"input_json_delta","partial_json":"\"Tokyo\"}"}}),
            json!({"type":"content_block_stop","index":0}),
            json!({"type":"message_delta","delta":{"stop_reason":"tool_use"}}),
        ] {
            assembler.apply_event(&event);
        }

        let (tx, rx) = job_event_channel();
        // Events are sent too, to prove collect_stream ignores them rather
        // than trying to re-derive the response from them.
        tx.try_send(Ok(StreamItem::Event(json!({"type":"message_stop"}))))
            .expect("send event");
        tx.try_send(Ok(StreamItem::Finished(
            assembler.finish(&MessageRequest::default()),
        )))
        .expect("send finished");
        drop(tx);

        let response = collect_stream(rx).await.expect("aggregate");
        assert!(matches!(response.stop_reason, StopReason::ToolUse));
        match response
            .content
            .iter()
            .find(|b| matches!(b, ContentBlock::ToolUse { .. }))
            .expect("tool_use block must survive aggregation")
        {
            ContentBlock::ToolUse {
                id, name, input, ..
            } => {
                assert_eq!(id, "toolu_1");
                assert_eq!(name, "get_weather");
                assert_eq!(input, &json!({"city": "Tokyo"}));
            }
            other => panic!("expected tool_use, got {other:?}"),
        }
    }
}
