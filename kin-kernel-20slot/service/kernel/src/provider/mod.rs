pub mod anthropic;
pub mod cli_auth;
pub mod local_cli;
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

    fn relay_snapshot(&self) -> Option<serde_json::Value> {
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
