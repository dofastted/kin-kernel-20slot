//! One Claude OS process, N background kin-slot subagents, Rust MCP rendezvous.
//!
//! HTTP requests never share stdin. Jobs wake a ReadyBlocked slot via MCP
//! `slot_wait`; stream-json is demuxed by `parent_tool_use_id`.

pub mod bootstrap;
pub mod continuation;
pub mod job_stream;
pub mod mcp_server;
pub mod memory_guard;
pub mod pending_call;
pub mod relay;
pub mod replay;
pub mod scheduler;
pub mod signing;
pub mod slot;
pub mod stream_decoder;
pub mod supervisor;

use std::{
    collections::{HashMap, HashSet, VecDeque},
    env,
    path::PathBuf,
    sync::{
        Arc, OnceLock, Weak,
        atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::sync::{Mutex, Notify, OnceCell, mpsc};
use uuid::Uuid;

use crate::{
    config::{DEFAULT_CLIENT_STALL_SECS, RelayMode},
    error::KernelError,
    model::{ContentBlock, MessageContent, MessageRequest, MessageResponse, StopReason, Usage},
    provider::{ExecutionContext, Provider, ProviderCapabilities, StreamTx, job_event_channel},
    stream::StreamItem,
};

use self::{
    continuation::ContinuationToken,
    job_stream::JobStream,
    memory_guard::MemoryGuard,
    pending_call::{Job, JobOutcome, PendingCalls, SlotWaitPayload, new_id},
    relay::{
        arbiter::{ArbiterEffect, SourceArbiter},
        correlate::{CorrelatedJob, RelayContextToken},
        sse_tap::TapEvent,
    },
    scheduler::SlotScheduler,
    slot::{Slot, SlotPhase, SlotSnapshot},
};

#[derive(Clone)]
pub struct MultiplexConfig {
    pub slot_count: usize,
    pub simulate: bool,
    pub bin: PathBuf,
    pub mock_bin: bool,
    pub model: String,
    pub retire_after_turn: bool,
    pub max_jobs_per_slot: u32,
    pub slot_max_lifetime: Duration,
    pub session_idle_ttl: Duration,
    pub simulate_latency: Duration,
    pub continuation_ttl_secs: i64,
    pub client_stall_timeout: Duration,
    /// Bounded wait for a slot to re-enter slot_wait before returning 503.
    pub submit_wait: Duration,
    pub relay_mode: RelayMode,
    pub relay_addr: std::net::SocketAddr,
    pub relay_upstream: String,
}

impl MultiplexConfig {
    pub fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        let slot_count = env::var("KIN_SLOTS_PER_WORKER")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(20)
            .clamp(1, 20);
        let bin = env::var("KIN_CLAUDE_BIN")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into()))
                    .join("../../scripts/kin-node-kernel/mock-claude.mjs")
            });
        let mock_bin = bin.to_string_lossy().contains("mock-claude");
        let simulate = env::var("KIN_MULTIPLEX_SIMULATE")
            .map(|value| value != "0")
            .unwrap_or(mock_bin);
        let relay_addr = env::var("KIN_RELAY_ADDR")
            .unwrap_or_else(|_| "127.0.0.1:0".to_string())
            .parse()?;
        Ok(Self {
            slot_count,
            simulate,
            mock_bin,
            bin,
            model: env::var("KIN_CLI_MODEL").unwrap_or_else(|_| "claude-sonnet-5".into()),
            retire_after_turn: env::var("KIN_ISOLATION")
                .map(|value| value == "session-reset")
                .unwrap_or(false),
            max_jobs_per_slot: env::var("KIN_SLOT_MAX_JOBS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(50),
            slot_max_lifetime: Duration::from_secs(
                env::var("KIN_SLOT_MAX_LIFETIME_SECS")
                    .ok()
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(1800),
            ),
            session_idle_ttl: Duration::from_secs(
                env::var("KIN_SESSION_IDLE_TTL_SECS")
                    .ok()
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(600),
            ),
            simulate_latency: Duration::from_millis(
                env::var("KIN_SIMULATE_LATENCY_MS")
                    .ok()
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(40),
            ),
            continuation_ttl_secs: 600,
            client_stall_timeout: crate::config::client_stall_timeout_from_env()?,
            submit_wait: Duration::from_millis(
                env::var("KIN_SUBMIT_WAIT_MS")
                    .ok()
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(2000),
            ),
            relay_mode: RelayMode::from_env()?,
            relay_addr,
            relay_upstream: env::var("KIN_RELAY_UPSTREAM")
                .unwrap_or_else(|_| "https://api.anthropic.com".into()),
        })
    }
}

pub struct Runtime {
    pub process_generation: AtomicU64,
    pub pid: AtomicU32,
    secret: Vec<u8>,
    cfg: MultiplexConfig,
    slots: Mutex<Vec<Slot>>,
    sched: Mutex<SlotScheduler>,
    pending: Mutex<PendingCalls>,
    jobs: Mutex<HashMap<String, Job>>,
    sinks: Mutex<HashMap<String, JobSink>>,
    parents: Mutex<HashMap<String, String>>,
    unassigned_parents: Mutex<VecDeque<String>>,
    issued: Mutex<HashMap<String, ContinuationToken>>,
    streamed: Mutex<HashSet<String>>,
    job_streams: Mutex<HashMap<String, JobStream>>,
    job_sizes: Mutex<HashMap<String, usize>>,
    arbiters: Mutex<HashMap<String, SourceArbiter>>,
    tap_senders: Mutex<HashMap<String, mpsc::Sender<TapEvent>>>,
    tap_poisoned: Mutex<HashMap<String, TapPoisonState>>,
    tap_index_allocators: Mutex<HashMap<String, Arc<AtomicUsize>>>,
    tap_drains: Mutex<HashMap<String, TapDrainState>>,
    tap_turns: Mutex<HashMap<String, u64>>,
    relay: OnceLock<relay::RelayHandle>,
    pub memory: MemoryGuard,
    running: AtomicUsize,
    peak_running: AtomicUsize,
    ready: AtomicUsize,
    stage_dropped: AtomicU64,
}

const JOB_SINK_ITEMS: usize = 256;
const JOB_SINK_BYTES: usize = 2 * 1024 * 1024;

#[derive(Clone)]
struct JobSink {
    data_tx: mpsc::Sender<SinkEnvelope>,
    terminal: Arc<OnceLock<Terminal>>,
    terminal_notify: Arc<Notify>,
    budget: Arc<ByteBudget>,
    client_tx: Arc<Mutex<Option<StreamTx>>>,
}

struct SinkEnvelope {
    item: StreamItem,
    bytes: usize,
}

struct ByteBudget {
    used: AtomicUsize,
    max: usize,
}

struct TapDrainState {
    active: usize,
    notify: Arc<Notify>,
}

struct TapPoisonState {
    turn_id: u64,
    poisoned: Arc<AtomicBool>,
}

#[derive(Debug)]
enum Terminal {
    Overflow,
    ClientTooSlow,
    ClientGone,
    Done,
    Failed(KernelError),
}

#[derive(Debug, PartialEq, Eq)]
enum EmitResult {
    Sent,
    StageDropped,
    Failed,
    Closed,
    Missing,
}

enum SinkPushError {
    Full,
    Closed,
}

impl JobSink {
    fn new(client_tx: StreamTx) -> (Self, mpsc::Receiver<SinkEnvelope>) {
        let (data_tx, data_rx) = mpsc::channel(JOB_SINK_ITEMS);
        let sink = Self {
            data_tx,
            terminal: Arc::new(OnceLock::new()),
            terminal_notify: Arc::new(Notify::new()),
            budget: Arc::new(ByteBudget::new(JOB_SINK_BYTES)),
            client_tx: Arc::new(Mutex::new(Some(client_tx))),
        };
        (sink, data_rx)
    }

    async fn replace_client(&self, tx: StreamTx) {
        *self.client_tx.lock().await = Some(tx);
    }

    fn try_push(&self, item: StreamItem) -> Result<(), SinkPushError> {
        if self.terminal_failed() {
            return Err(SinkPushError::Closed);
        }
        let bytes = stream_item_bytes(&item);
        if !self.budget.try_reserve(bytes) {
            return Err(SinkPushError::Full);
        }
        let envelope = SinkEnvelope { item, bytes };
        match self.data_tx.try_send(envelope) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(envelope)) => {
                self.budget.release(envelope.bytes);
                Err(SinkPushError::Full)
            }
            Err(mpsc::error::TrySendError::Closed(envelope)) => {
                self.budget.release(envelope.bytes);
                Err(SinkPushError::Closed)
            }
        }
    }

    fn set_terminal(&self, terminal: Terminal) -> bool {
        let set = self.terminal.set(terminal).is_ok();
        if set {
            self.terminal_notify.notify_waiters();
        }
        set
    }

    fn terminal_failed(&self) -> bool {
        self.terminal.get().is_some_and(Terminal::is_failure)
    }
}

impl ByteBudget {
    fn new(max: usize) -> Self {
        Self {
            used: AtomicUsize::new(0),
            max,
        }
    }

    fn try_reserve(&self, bytes: usize) -> bool {
        let bytes = bytes.max(1);
        let mut current = self.used.load(Ordering::Relaxed);
        loop {
            let Some(next) = current.checked_add(bytes) else {
                return false;
            };
            if next > self.max {
                return false;
            }
            match self.used.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }

    fn release(&self, bytes: usize) {
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(bytes.max(1)))
            })
            .ok();
    }
}

impl Terminal {
    fn is_failure(&self) -> bool {
        matches!(
            self,
            Self::Overflow | Self::ClientTooSlow | Self::ClientGone | Self::Failed(_)
        )
    }

    fn error(&self) -> KernelError {
        match self {
            Self::Overflow => KernelError::Provider("job stream overflow".into()),
            Self::ClientTooSlow => KernelError::Provider("client read timed out".into()),
            Self::ClientGone => KernelError::Provider("client disconnected".into()),
            Self::Done => KernelError::Provider("job already completed".into()),
            Self::Failed(err) => KernelError::Provider(err.to_string()),
        }
    }
}

impl Runtime {
    pub fn new(cfg: MultiplexConfig) -> Arc<Self> {
        let mut secret = vec![0u8; 32];
        for byte in &mut secret {
            *byte = rand_byte();
        }
        Arc::new(Self {
            process_generation: AtomicU64::new(1),
            pid: AtomicU32::new(0),
            secret,
            cfg,
            slots: Mutex::new(Vec::new()),
            sched: Mutex::new(SlotScheduler::new()),
            pending: Mutex::new(PendingCalls::new()),
            jobs: Mutex::new(HashMap::new()),
            sinks: Mutex::new(HashMap::new()),
            parents: Mutex::new(HashMap::new()),
            unassigned_parents: Mutex::new(VecDeque::new()),
            issued: Mutex::new(HashMap::new()),
            streamed: Mutex::new(HashSet::new()),
            job_streams: Mutex::new(HashMap::new()),
            job_sizes: Mutex::new(HashMap::new()),
            arbiters: Mutex::new(HashMap::new()),
            tap_senders: Mutex::new(HashMap::new()),
            tap_poisoned: Mutex::new(HashMap::new()),
            tap_index_allocators: Mutex::new(HashMap::new()),
            tap_drains: Mutex::new(HashMap::new()),
            tap_turns: Mutex::new(HashMap::new()),
            relay: OnceLock::new(),
            memory: MemoryGuard::from_env(),
            running: AtomicUsize::new(0),
            peak_running: AtomicUsize::new(0),
            ready: AtomicUsize::new(0),
            stage_dropped: AtomicU64::new(0),
        })
    }

    pub async fn start(self: &Arc<Self>) -> Result<(), KernelError> {
        if self.cfg.simulate {
            self.start_simulated().await
        } else {
            self.start_claude().await
        }
    }

    async fn start_simulated(self: &Arc<Self>) -> Result<(), KernelError> {
        self.pid.store(std::process::id(), Ordering::Relaxed);
        for index in 0..self.cfg.slot_count {
            let parent = format!("parent_{index}");
            let runtime = Arc::clone(self);
            tokio::spawn(async move {
                simulate_worker(runtime, parent).await;
            });
        }
        bootstrap::wait_ready(self, self.cfg.slot_count, Duration::from_secs(5)).await
    }

    async fn start_claude(self: &Arc<Self>) -> Result<(), KernelError> {
        let mcp_addr = mcp_server::spawn(
            Arc::clone(self),
            "127.0.0.1:0"
                .parse()
                .map_err(|err| KernelError::Provider(format!("{err}")))?,
        )
        .await?;
        tracing::info!(mcp = %mcp_addr, "kin mcp listening");
        let mut anthropic_base_url = None;
        if self.cfg.relay_mode != RelayMode::Off {
            let handle = relay::spawn(Arc::clone(self), &self.cfg).await?;
            relay::confirm_healthy(handle.addr).await?;
            relay::upstream::UpstreamClient::new(&self.cfg.relay_upstream)?
                .preflight()
                .await?;
            anthropic_base_url = Some(format!("http://{}", handle.addr));
            let _ = self.relay.set(handle);
        }
        let dir = PathBuf::from("/tmp/kin-cli/multiplex").join(Uuid::new_v4().to_string());
        let spec = supervisor::SpawnSpec {
            bin: self.cfg.bin.clone(),
            mock: self.cfg.mock_bin,
            model: self.cfg.model.clone(),
            slot_count: self.cfg.slot_count,
            mcp_url: format!("http://{mcp_addr}/mcp"),
            session_dir: dir,
            anthropic_base_url,
        };
        let mut supervised = supervisor::spawn(&spec).await?;
        self.pid.store(supervised.pid, Ordering::Relaxed);
        self.memory.set_claude_pid(supervised.pid);
        let stdout = supervised
            .child
            .stdout
            .take()
            .ok_or_else(|| KernelError::Provider("claude stdout missing".into()))?;
        let mut stdin = supervised
            .child
            .stdin
            .take()
            .ok_or_else(|| KernelError::Provider("claude stdin missing".into()))?;
        let runtime = Arc::clone(self);
        tokio::spawn(async move {
            decode_stdout(runtime, stdout).await;
        });
        if let Some(stderr) = supervised.child.stderr.take() {
            tokio::spawn(async move {
                use tokio::io::{AsyncBufReadExt, BufReader};
                let mut lines = BufReader::new(stderr).lines();
                let path = "/tmp/kin-live/claude.multiplex.stderr.log";
                let mut file = tokio::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path)
                    .await
                    .ok();
                while let Ok(Some(line)) = lines.next_line().await {
                    tracing::info!(target: "kin_kernel::claude", "{line}");
                    if let Some(file) = file.as_mut() {
                        use tokio::io::AsyncWriteExt;
                        let _ = file.write_all(line.as_bytes()).await;
                        let _ = file.write_all(b"\n").await;
                    }
                }
            });
        }
        bootstrap::write_root_prompt(&mut stdin, self.cfg.slot_count).await?;
        tokio::spawn(async move {
            // Keep stdin open (no more writes) and wait so kill_on_drop cannot
            // reap the process when this task would otherwise fall off the end.
            let _stdin = stdin;
            match supervised.child.wait().await {
                Ok(status) => tracing::warn!(%status, "claude process exited"),
                Err(err) => tracing::warn!(%err, "claude wait failed"),
            }
        });
        tracing::info!(pid = supervised.pid, "claude supervisor alive");
        let wait = Duration::from_secs((45 + 8 * self.cfg.slot_count as u64).min(240));
        bootstrap::wait_ready(self, self.cfg.slot_count, wait).await
    }

    pub fn ready_slots(&self) -> usize {
        self.ready.load(Ordering::Relaxed)
    }

    pub async fn snapshots(&self) -> Vec<SlotSnapshot> {
        self.slots
            .lock()
            .await
            .iter()
            .map(|slot| SlotSnapshot {
                id: slot.id.clone(),
                phase: slot.phase,
                parent_tool_use_id: slot.parent_tool_use_id.clone(),
                session_id: slot.session_id.clone(),
                tenant_id: slot.tenant_id.clone(),
                jobs_completed: slot.jobs_completed,
            })
            .collect()
    }

    pub fn peak_running(&self) -> usize {
        self.peak_running.load(Ordering::Relaxed)
    }

    pub fn bump_generation(&self) -> u64 {
        self.process_generation.fetch_add(1, Ordering::AcqRel) + 1
    }

    pub async fn mcp_slot_wait(&self, args: Value) -> Result<Value, KernelError> {
        let hinted = args
            .get("slot_id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        let slot_id = self.register_or_get_slot(hinted).await;
        let rx = {
            let mut pending = self.pending.lock().await;
            pending.register_slot_wait(&slot_id)
        };
        {
            let mut slots = self.slots.lock().await;
            if let Some(slot) = slots.iter_mut().find(|slot| slot.id == slot_id) {
                if slot.phase != SlotPhase::Dead
                    && slot.phase != SlotPhase::Draining
                    && slot.phase != SlotPhase::ReadyBlocked
                {
                    slot.phase = SlotPhase::ReadyBlocked;
                    slot.job_id = None;
                }
            }
            if self.sched.lock().await.enqueue_ready(slot_id.clone()) {
                self.ready.fetch_add(1, Ordering::Relaxed);
            }
        }
        match rx.await {
            Ok(SlotWaitPayload::Job(job)) => {
                let relay_context = self.issue_relay_context(&job)?;
                let mut payload = json!({
                "type": "job",
                "job_id": job.job_id,
                "slot_id": job.slot_id,
                "session_id": job.session_id,
                "model": job.request.model,
                "max_tokens": job.request.max_tokens,
                "stream": job.request.stream,
                "system": job.request.system,
                "thinking": job.request.thinking,
                "tool_choice": job.request.tool_choice,
                "metadata": job.request.metadata,
                "temperature": job.request.temperature,
                "top_p": job.request.top_p,
                "top_k": job.request.top_k,
                "stop_sequences": job.request.stop_sequences,
                "betas": job.request.betas,
                "messages": job.request.messages,
                "tools": job.request.tools,
                "extra": job.request.extra,
                "request": job.request
                });
                if let Some(token) = relay_context {
                    payload["relay_context"] = json!(token);
                }
                Ok(payload)
            }
            Ok(SlotWaitPayload::Retire) => Ok(json!({ "type": "retire" })),
            Err(_) => Err(KernelError::Provider("slot_wait cancelled".into())),
        }
    }

    fn issue_relay_context(&self, job: &Job) -> Result<Option<String>, KernelError> {
        if self.cfg.relay_mode == RelayMode::Off {
            return Ok(None);
        }
        RelayContextToken::issue(
            job.job_id.clone(),
            job.slot_id.clone(),
            self.process_generation.load(Ordering::Relaxed),
            &self.secret,
        )
        .map(Some)
    }

    pub(crate) fn secret(&self) -> &[u8] {
        &self.secret
    }

    pub(crate) async fn correlate_lookup(
        &self,
        token: &RelayContextToken,
    ) -> Option<CorrelatedJob> {
        let current_generation = self.process_generation.load(Ordering::Relaxed);
        if token.generation != current_generation {
            return None;
        }
        let job = self.jobs.lock().await.get(&token.job_id).cloned()?;
        if job.slot_id != token.slot_id {
            return None;
        }
        let slot_matches =
            self.slots.lock().await.iter().any(|slot| {
                slot.id == token.slot_id && slot.job_id.as_deref() == Some(&token.job_id)
            });
        if !slot_matches {
            return None;
        }
        Some(CorrelatedJob {
            job_id: token.job_id.clone(),
            slot_id: token.slot_id.clone(),
            generation: token.generation,
        })
    }

    pub async fn mcp_client_tool(&self, args: Value) -> Result<Value, KernelError> {
        let job_id = args
            .get("job_id")
            .and_then(Value::as_str)
            .ok_or_else(|| KernelError::InvalidRequest("client_tool needs job_id".into()))?
            .to_string();
        let name = args
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("tool")
            .to_string();
        let input = args.get("input").cloned().unwrap_or_else(|| json!({}));
        let tool_id = args
            .get("client_tool_use_id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| new_id("toolu"));
        let job = self
            .jobs
            .lock()
            .await
            .get(&job_id)
            .cloned()
            .ok_or(KernelError::ContinuationLost)?;
        {
            let mut slots = self.slots.lock().await;
            let slot = slots
                .iter_mut()
                .find(|slot| slot.id == job.slot_id)
                .ok_or(KernelError::ContinuationLost)?;
            if !slot.cas(SlotPhase::Running, SlotPhase::WaitingTool) {
                return Err(KernelError::ContinuationMismatch("slot not running".into()));
            }
        }
        let generation = self.process_generation.load(Ordering::Relaxed);
        let (token, encoded) = ContinuationToken::issue(
            generation,
            &job.slot_id,
            &job.job_id,
            &job.session_id,
            &tool_id,
            self.cfg.continuation_ttl_secs,
            &self.secret,
        )?;
        self.issued.lock().await.insert(token.nonce.clone(), token);
        let tool_block = json!({
            "type": "tool_use",
            "id": tool_id.clone(),
            "name": name.clone(),
            "input": input.clone()
        });
        let events = {
            let mut streams = self.job_streams.lock().await;
            match streams.get_mut(&job_id) {
                Some(stream) => {
                    let mut events = stream.emit_complete_block(&tool_block);
                    events.extend(stream.finish("tool_use", json!({})));
                    events
                }
                None => vec![json!({
                    "type": "content_block_start",
                    "index": 0,
                    "content_block": {
                        "type": "tool_use",
                        "id": &tool_id,
                        "name": &name,
                        "input": &input
                    }
                })],
            }
        };
        for event in events {
            self.emit(&job_id, StreamItem::Event(event)).await;
        }
        let response = MessageResponse {
            id: format!("msg_{}", self.pid.load(Ordering::Relaxed)),
            r#type: "message",
            role: "assistant",
            model: job.request.model.clone(),
            content: vec![ContentBlock::ToolUse {
                id: tool_id.clone(),
                name,
                input,
            }],
            stop_reason: StopReason::ToolUse,
            usage: Usage::default(),
        };
        self.emit(&job_id, StreamItem::Finished(response)).await;
        let rx = {
            let mut pending = self.pending.lock().await;
            pending.register_client_tool(&job_id, Some(tool_id.as_str()))
        };
        let result = rx.await.map_err(|_| KernelError::ContinuationLost)?;
        {
            let mut slots = self.slots.lock().await;
            if let Some(slot) = slots.iter_mut().find(|slot| slot.id == job.slot_id) {
                if slot.phase != SlotPhase::Dead {
                    let _ = slot.cas(SlotPhase::WaitingTool, SlotPhase::Running);
                }
            }
        }
        let _ = encoded;
        Ok(result)
    }

    pub async fn mcp_kin_done(&self, args: Value) -> Result<Value, KernelError> {
        let job_id = args
            .get("job_id")
            .and_then(Value::as_str)
            .ok_or_else(|| KernelError::InvalidRequest("kin_done needs job_id".into()))?;
        let fallback = args
            .get("fallback_content")
            .or_else(|| args.get("text"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let stop = args
            .get("stop_reason")
            .and_then(Value::as_str)
            .unwrap_or("end_turn")
            .to_string();
        let usage = args.get("usage").cloned().unwrap_or_else(|| json!({}));
        self.complete_job(job_id, fallback, false, &stop, usage)
            .await?;
        Ok(json!({ "ok": true }))
    }

    pub async fn mcp_kin_fail(&self, args: Value) -> Result<Value, KernelError> {
        let job_id = args
            .get("job_id")
            .and_then(Value::as_str)
            .ok_or_else(|| KernelError::InvalidRequest("kin_fail needs job_id".into()))?;
        let error = args
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or("slot failed")
            .to_string();
        let retire = args.get("retire").and_then(Value::as_bool).unwrap_or(true);
        self.complete_job(job_id, error, true, "refusal", json!({}))
            .await?;
        if retire {
            if let Some(job) = self.jobs.lock().await.get(job_id).cloned() {
                self.retire_slot(&job.slot_id).await;
            }
        }
        Ok(json!({ "ok": true }))
    }

    async fn complete_job(
        &self,
        job_id: &str,
        fallback: String,
        is_error: bool,
        stop_reason: &str,
        usage: Value,
    ) -> Result<(), KernelError> {
        let job = self
            .jobs
            .lock()
            .await
            .get(job_id)
            .cloned()
            .ok_or(KernelError::ContinuationLost)?;
        if is_error {
            if let Some(sink) = self.sinks.lock().await.get(job_id).cloned() {
                sink.set_terminal(Terminal::Failed(KernelError::Provider(fallback)));
            }
            self.abort_terminal_job(job_id).await;
            return Ok(());
        }
        if !self.wait_tap_drain(job_id, Duration::from_secs(2)).await {
            self.note_tap_incomplete(job_id).await;
            if fail_if_upstream_poisoned(self, job_id).await {
                self.abort_terminal_job(job_id).await;
                return Ok(());
            }
        }
        // Deferred arbitration: if the tap never produced a body this turn,
        // the stdout frames held back in the arbiter are the user's body —
        // release them before finishing.
        let deferred = {
            let mut arbiters = self.arbiters.lock().await;
            match arbiters.get_mut(job_id) {
                Some(arbiter) => arbiter.take_deferred(),
                None => Vec::new(),
            }
        };
        for event in deferred {
            if self.emit(job_id, StreamItem::Event(event)).await == EmitResult::Failed {
                self.abort_terminal_job(job_id).await;
                return Ok(());
            }
        }
        let (fallback_events, finish_events, response) = {
            let mut streams = self.job_streams.lock().await;
            let stream = streams
                .entry(job_id.to_string())
                .or_insert_with(JobStream::new);
            let mut arbiters = self.arbiters.lock().await;
            let arbiter = arbiters.get_mut(job_id);
            let finish = finish_body(arbiter, stream, &fallback, usage);
            self.record_digest_mismatch(job_id, arbiters.get(job_id), &stream.text, &fallback);
            let fallback_events = stream.fallback_text(&fallback);
            let finish_events = stream.finish(stop_reason, finish.usage.clone());
            let response = MessageResponse {
                id: format!("msg_{}", self.pid.load(Ordering::Relaxed)),
                r#type: "message",
                role: "assistant",
                model: job.request.model.clone(),
                content: vec![ContentBlock::Text {
                    text: finish.text.clone(),
                    cache_control: None,
                }],
                stop_reason: StopReason::EndTurn,
                usage: usage_from_value(&finish.usage),
            };
            (fallback_events, finish_events, response)
        };
        for event in fallback_events {
            if self.emit(job_id, StreamItem::Event(event)).await == EmitResult::Failed {
                self.abort_terminal_job(job_id).await;
                return Ok(());
            }
        }
        for event in finish_events {
            if self.emit(job_id, StreamItem::Event(event)).await == EmitResult::Failed {
                self.abort_terminal_job(job_id).await;
                return Ok(());
            }
        }
        if self.emit(job_id, StreamItem::Finished(response)).await != EmitResult::Sent {
            self.abort_terminal_job(job_id).await;
            return Ok(());
        }
        Ok(())
    }

    async fn finish_sent_job(&self, job_id: &str, response: MessageResponse) {
        if let Some(sink) = self.sinks.lock().await.get(job_id).cloned()
            && !sink.set_terminal(Terminal::Done)
        {
            self.abort_terminal_job(job_id).await;
            return;
        }
        let text = response_text(&response);
        let is_error = matches!(response.stop_reason, StopReason::Refusal);
        let job = self.jobs.lock().await.remove(job_id);
        let slot_id = job.as_ref().map(|job| job.slot_id.clone());
        self.sinks.lock().await.remove(job_id);
        self.job_streams.lock().await.remove(job_id);
        self.pending
            .lock()
            .await
            .finish_job(job_id, JobOutcome { text, is_error })
            .ok();
        self.running
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_sub(1))
            })
            .ok();
        if let Some(size) = self.job_sizes.lock().await.remove(job_id) {
            self.memory.end(size);
        }
        self.streamed.lock().await.remove(job_id);
        self.drop_job_tap(job_id).await;
        let retire = if let Some(job) = job {
            let mut slots = self.slots.lock().await;
            if let Some(slot) = slots.iter_mut().find(|slot| slot.id == job.slot_id) {
                slot.jobs_completed = slot.jobs_completed.saturating_add(1);
                self.cfg.retire_after_turn
                    || slot.should_retire(
                        self.cfg.max_jobs_per_slot,
                        self.cfg.slot_max_lifetime,
                        self.cfg.session_idle_ttl,
                    )
            } else {
                false
            }
        } else {
            false
        };
        if retire {
            if let Some(slot_id) = slot_id {
                self.retire_slot(&slot_id).await;
            }
        }
    }

    async fn retire_idle(&self) {
        let ids: Vec<String> = {
            let slots = self.slots.lock().await;
            slots
                .iter()
                .filter(|slot| {
                    slot.phase == SlotPhase::ReadyBlocked
                        && slot.should_retire(
                            self.cfg.max_jobs_per_slot,
                            self.cfg.slot_max_lifetime,
                            self.cfg.session_idle_ttl,
                        )
                })
                .map(|slot| slot.id.clone())
                .collect()
        };
        for id in ids {
            self.retire_slot(&id).await;
        }
    }

    async fn retire_slot(&self, slot_id: &str) {
        {
            let mut slots = self.slots.lock().await;
            if let Some(slot) = slots.iter_mut().find(|slot| slot.id == slot_id) {
                slot.retire();
            }
        }
        self.sched.lock().await.forget(slot_id);
        let mut pending = self.pending.lock().await;
        let _ = pending.wake_slot(slot_id, SlotWaitPayload::Retire);
    }

    async fn register_or_get_slot(&self, hinted: Option<String>) -> String {
        let mut slots = self.slots.lock().await;
        if let Some(id) = hinted.as_ref()
            && let Some(slot) = slots.iter().find(|slot| slot.id == *id)
            && slot.phase != SlotPhase::Dead
            && slot.phase != SlotPhase::Draining
        {
            return id.clone();
        }
        let id = hinted.unwrap_or_else(|| new_id("slot"));
        let mut slot = Slot::new(&id);
        if let Some(parent) = self.unassigned_parents.lock().await.pop_front() {
            slot.parent_tool_use_id = Some(parent.clone());
            self.parents.lock().await.insert(parent, id.clone());
        }
        slots.push(slot);
        id
    }

    pub async fn note_agent_spawn(&self, tool_use_id: String) {
        let mut slots = self.slots.lock().await;
        if let Some(slot) = slots
            .iter_mut()
            .find(|slot| slot.parent_tool_use_id.is_none() && slot.phase != SlotPhase::Dead)
        {
            slot.parent_tool_use_id = Some(tool_use_id.clone());
            self.parents
                .lock()
                .await
                .insert(tool_use_id, slot.id.clone());
            return;
        }
        drop(slots);
        self.unassigned_parents.lock().await.push_back(tool_use_id);
    }

    pub async fn handle_cli_frame(&self, frame: Value) {
        match stream_decoder::decode(&frame) {
            stream_decoder::Decoded::AgentSpawn { tool_use_id } => {
                self.note_agent_spawn(tool_use_id).await;
            }
            stream_decoder::Decoded::Routed {
                parent_tool_use_id, ..
            } => {
                let slot_id = self.parents.lock().await.get(&parent_tool_use_id).cloned();
                let Some(slot_id) = slot_id else {
                    return;
                };
                let job_id = {
                    let slots = self.slots.lock().await;
                    slots
                        .iter()
                        .find(|slot| slot.id == slot_id)
                        .and_then(|slot| slot.job_id.clone())
                };
                let Some(job_id) = job_id else {
                    return;
                };
                let events = {
                    let mut streams = self.job_streams.lock().await;
                    match streams.get_mut(&job_id) {
                        Some(stream) => stream.ingest(&frame),
                        None => Vec::new(),
                    }
                };
                let events = {
                    let mut arbiters = self.arbiters.lock().await;
                    match arbiters.get_mut(&job_id) {
                        Some(arbiter) => arbiter.filter_stdout(events),
                        None => events,
                    }
                };
                for event in events {
                    self.emit(&job_id, StreamItem::Event(event)).await;
                }
            }
            stream_decoder::Decoded::Root => {}
        }
    }

    async fn emit(&self, job_id: &str, item: StreamItem) -> EmitResult {
        let lossless_delta = is_lossless_delta(&item);
        let sink = self.sinks.lock().await.get(job_id).cloned();
        let Some(sink) = sink else {
            return EmitResult::Missing;
        };
        match sink.try_push(item) {
            Ok(()) => {
                if lossless_delta {
                    self.streamed.lock().await.insert(job_id.to_string());
                }
                EmitResult::Sent
            }
            Err(SinkPushError::Full) => {
                if lossless_delta {
                    sink.set_terminal(Terminal::Overflow);
                    EmitResult::Failed
                } else if matches!(sink.terminal.get(), Some(Terminal::Done)) {
                    EmitResult::Closed
                } else {
                    self.stage_dropped.fetch_add(1, Ordering::Relaxed);
                    EmitResult::StageDropped
                }
            }
            Err(SinkPushError::Closed) => EmitResult::Closed,
        }
    }

    async fn start_job_sink(self: &Arc<Self>, job_id: String, tx: StreamTx) {
        let (sink, data_rx) = JobSink::new(tx);
        let index_allocator = Arc::new(AtomicUsize::new(0));
        self.tap_index_allocators
            .lock()
            .await
            .insert(job_id.clone(), Arc::clone(&index_allocator));
        self.job_streams.lock().await.insert(
            job_id.clone(),
            JobStream::with_index_allocator(Arc::clone(&index_allocator)),
        );
        self.sinks.lock().await.insert(job_id.clone(), sink.clone());
        tokio::spawn(job_egress(
            Arc::downgrade(self),
            job_id.clone(),
            data_rx,
            sink,
            self.cfg.client_stall_timeout,
        ));
        self.start_tap_forwarder(job_id).await;
    }

    async fn start_tap_forwarder(self: &Arc<Self>, job_id: String) {
        if self.cfg.relay_mode == RelayMode::Off {
            return;
        }
        let (tap_tx, tap_rx) = mpsc::channel(256);
        let poisoned = Arc::new(AtomicBool::new(false));
        self.tap_senders.lock().await.insert(job_id.clone(), tap_tx);
        self.tap_poisoned.lock().await.insert(
            job_id.clone(),
            TapPoisonState {
                turn_id: 0,
                poisoned: Arc::clone(&poisoned),
            },
        );
        self.tap_drains.lock().await.insert(
            job_id.clone(),
            TapDrainState {
                active: 0,
                notify: Arc::new(Notify::new()),
            },
        );
        self.tap_turns.lock().await.insert(job_id.clone(), 0);
        self.arbiters
            .lock()
            .await
            .insert(job_id.clone(), SourceArbiter::new(self.cfg.relay_mode));
        let runtime = Arc::downgrade(self);
        tokio::spawn(async move {
            job_tap_forwarder(runtime, job_id, tap_rx).await;
        });
    }

    pub(crate) async fn tap_binding(&self, job_id: &str) -> Option<relay::sse_tap::TapBinding> {
        let events = self.tap_senders.lock().await.get(job_id).cloned()?;
        let index_allocator = self
            .tap_index_allocators
            .lock()
            .await
            .get(job_id)
            .cloned()
            .unwrap_or_else(|| Arc::new(AtomicUsize::new(0)));
        let turn_id = self
            .tap_turns
            .lock()
            .await
            .get(job_id)
            .copied()
            .unwrap_or(0);
        let poisoned = self
            .tap_poisoned
            .lock()
            .await
            .get(job_id)
            .filter(|state| state.turn_id == turn_id)
            .map(|state| Arc::clone(&state.poisoned));
        Some(relay::sse_tap::TapBinding {
            events,
            poisoned,
            index_allocator,
            turn_id,
        })
    }

    pub fn relay_snapshot(&self) -> Value {
        let (healthy, dropped, mismatch, hit, miss, ambiguous, started) = match self.relay.get() {
            Some(handle) => (
                handle.healthy(),
                handle.metrics.tap_dropped.load(Ordering::Relaxed),
                handle.metrics.digest_mismatch.load(Ordering::Relaxed),
                handle.metrics.correlate_hit.load(Ordering::Relaxed),
                handle.metrics.correlate_miss.load(Ordering::Relaxed),
                handle.metrics.correlate_ambiguous.load(Ordering::Relaxed),
                handle.metrics.tap_response_started.load(Ordering::Relaxed),
            ),
            None => (false, 0, 0, 0, 0, 0, 0),
        };
        json!({
            "relay_mode": self.cfg.relay_mode.as_str(),
            "relay_healthy": healthy,
            "tap_dropped": dropped,
            "digest_mismatch": mismatch,
            "correlate_hit": hit,
            "correlate_miss": miss,
            "correlate_ambiguous": ambiguous,
            "tap_response_started": started,
        })
    }

    fn record_digest_mismatch(
        &self,
        job_id: &str,
        arbiter: Option<&SourceArbiter>,
        stdout: &str,
        fallback: &str,
    ) {
        let Some(arbiter) = arbiter else {
            return;
        };
        let stdout = if stdout.is_empty() { fallback } else { stdout };
        let Some((upstream, stdout)) = arbiter.mismatch_digests(stdout) else {
            return;
        };
        if let Some(handle) = self.relay.get() {
            handle.metrics.inc_digest_mismatch();
        }
        tracing::warn!(
            job_id,
            upstream = %upstream,
            stdout = %stdout,
            "relay digest mismatch"
        );
    }

    async fn drop_job_tap(&self, job_id: &str) {
        self.arbiters.lock().await.remove(job_id);
        self.tap_senders.lock().await.remove(job_id);
        self.tap_poisoned.lock().await.remove(job_id);
        self.tap_index_allocators.lock().await.remove(job_id);
        self.tap_drains.lock().await.remove(job_id);
        self.tap_turns.lock().await.remove(job_id);
    }

    pub(crate) async fn register_tap_response(&self, job_id: &str) {
        if let Some(arbiter) = self.arbiters.lock().await.get_mut(job_id) {
            arbiter.set_tap_attached();
        }
        let mut drains = self.tap_drains.lock().await;
        let state = drains
            .entry(job_id.to_string())
            .or_insert_with(|| TapDrainState {
                active: 0,
                notify: Arc::new(Notify::new()),
            });
        state.active = state.active.saturating_add(1);
    }

    async fn note_tap_drained(&self, job_id: &str) {
        let notify = {
            let mut drains = self.tap_drains.lock().await;
            let Some(state) = drains.get_mut(job_id) else {
                return;
            };
            state.active = state.active.saturating_sub(1);
            if state.active == 0 {
                Some(Arc::clone(&state.notify))
            } else {
                None
            }
        };
        if let Some(notify) = notify {
            notify.notify_waiters();
        }
    }

    async fn wait_tap_drain(&self, job_id: &str, timeout: Duration) -> bool {
        loop {
            let notify = {
                let drains = self.tap_drains.lock().await;
                let Some(state) = drains.get(job_id) else {
                    return true;
                };
                if state.active == 0 {
                    return true;
                }
                Arc::clone(&state.notify)
            };
            let notified = notify.notified();
            if tokio::time::timeout(timeout, notified).await.is_err() {
                return false;
            }
        }
    }

    async fn note_tap_incomplete(&self, job_id: &str) {
        if let Some(poisoned) = self
            .tap_poisoned
            .lock()
            .await
            .get(job_id)
            .map(|state| Arc::clone(&state.poisoned))
            && !poisoned.swap(true, Ordering::Relaxed)
            && let Some(handle) = self.relay.get()
        {
            handle.metrics.inc_tap_dropped();
        }
    }

    async fn abort_terminal_job(&self, job_id: &str) {
        let sink = self.sinks.lock().await.get(job_id).cloned();
        let reason = sink
            .as_ref()
            .and_then(|sink| sink.terminal.get())
            .map(Terminal::error)
            .unwrap_or_else(|| KernelError::Provider("job stream aborted".into()));
        let message = reason.to_string();
        if let Some(sink) = &sink
            && let Some(terminal) = sink.terminal.get()
            && terminal.is_failure()
        {
            fail_client_stream(sink, terminal).await;
        }
        let job = self.jobs.lock().await.remove(job_id);
        self.sinks.lock().await.remove(job_id);
        self.job_streams.lock().await.remove(job_id);
        self.streamed.lock().await.remove(job_id);
        self.drop_job_tap(job_id).await;
        if let Some(size) = self.job_sizes.lock().await.remove(job_id) {
            self.memory.end(size);
        }
        self.pending
            .lock()
            .await
            .abort_client_tools(job_id, &message);
        self.pending.lock().await.drop_job(job_id);
        if let Some(job) = job {
            self.running
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                    Some(n.saturating_sub(1))
                })
                .ok();
            self.retire_slot(&job.slot_id).await;
        }
    }

    pub async fn submit(
        self: &Arc<Self>,
        request: MessageRequest,
        context: ExecutionContext,
        tx: StreamTx,
    ) -> Result<(), KernelError> {
        if context.resumed {
            return self.resume(request, context, tx).await;
        }
        let request_bytes = serde_json::to_vec(&request).map(|v| v.len()).unwrap_or(0);
        self.memory.admit(request_bytes)?;
        self.retire_idle().await;
        let generation = self.process_generation.load(Ordering::Relaxed);
        // A slot that just finished a job is briefly neither Running nor back
        // in slot_wait (ReadyBlocked). A burst of submissions can hit that
        // re-entry gap and see NoCapacity even though the pool is not full,
        // so retry with a bounded wait instead of failing fast.
        let deadline = tokio::time::Instant::now() + self.cfg.submit_wait;
        let (slot_id, job_id) = loop {
            let mut slots = self.slots.lock().await;
            let mut sched = self.sched.lock().await;
            match sched.pick(&mut slots, &context.tenant_id, &context.session_id) {
                Ok(slot_id) => {
                    self.ready
                        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                            Some(n.saturating_sub(1))
                        })
                        .ok();
                    let job_id = new_id("job");
                    let slot = slots
                        .iter_mut()
                        .find(|slot| slot.id == slot_id)
                        .ok_or(KernelError::NoCapacity)?;
                    if !slot.bind_job(&context.tenant_id, &context.session_id, &job_id) {
                        return Err(KernelError::NoCapacity);
                    }
                    break (slot_id, job_id);
                }
                Err(err) => {
                    drop(sched);
                    drop(slots);
                    // Busy-now is not full-forever: keep retrying until the
                    // deadline. A genuinely saturated pool still 503s, just
                    // submit_wait later.
                    if tokio::time::Instant::now() >= deadline {
                        return Err(err);
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        };
        let job = Job {
            job_id: job_id.clone(),
            tenant_id: context.tenant_id,
            session_id: context.session_id,
            slot_id: slot_id.clone(),
            generation,
            request,
        };
        self.jobs.lock().await.insert(job_id.clone(), job.clone());
        self.start_job_sink(job_id.clone(), tx).await;
        self.job_sizes
            .lock()
            .await
            .insert(job_id.clone(), request_bytes);
        self.memory.begin(request_bytes);
        let n = self.running.fetch_add(1, Ordering::Relaxed) + 1;
        self.peak_running.fetch_max(n, Ordering::Relaxed);
        let msg_id = format!("msg_{job_id}");
        self.emit(
            &job_id,
            StreamItem::Event(JobStream::message_start(&job.request.model, &msg_id)),
        )
        .await;
        self.pending
            .lock()
            .await
            .wake_slot(&slot_id, SlotWaitPayload::Job(job))?;
        Ok(())
    }

    async fn resume(
        self: &Arc<Self>,
        request: MessageRequest,
        context: ExecutionContext,
        tx: StreamTx,
    ) -> Result<(), KernelError> {
        let generation = self.process_generation.load(Ordering::Relaxed);
        let slots = self.slots.lock().await;
        let slot = slots
            .iter()
            .find(|slot| slot.session_id.as_deref() == Some(context.session_id.as_str()))
            .ok_or(KernelError::ContinuationLost)?;
        if slot.phase != SlotPhase::WaitingTool {
            return Err(KernelError::ContinuationLost);
        }
        let job_id = slot.job_id.clone().ok_or(KernelError::ContinuationLost)?;
        drop(slots);
        let job = self
            .jobs
            .lock()
            .await
            .get(&job_id)
            .cloned()
            .ok_or(KernelError::ContinuationLost)?;
        if job.generation != generation || job.tenant_id != context.tenant_id {
            return Err(KernelError::ContinuationLost);
        }
        let sink = self
            .sinks
            .lock()
            .await
            .get(&job_id)
            .cloned()
            .ok_or(KernelError::ContinuationLost)?;
        if sink.terminal_failed() {
            return Err(KernelError::ContinuationLost);
        }
        sink.replace_client(tx).await;
        let index_allocator = self
            .tap_index_allocators
            .lock()
            .await
            .get(&job_id)
            .cloned()
            .unwrap_or_else(|| Arc::new(AtomicUsize::new(0)));
        self.job_streams.lock().await.insert(
            job_id.clone(),
            JobStream::with_index_allocator(index_allocator),
        );
        let turn_id = {
            let mut turns = self.tap_turns.lock().await;
            let turn = turns.entry(job_id.clone()).or_insert(0);
            *turn = turn.saturating_add(1);
            *turn
        };
        self.tap_poisoned.lock().await.insert(
            job_id.clone(),
            TapPoisonState {
                turn_id,
                poisoned: Arc::new(AtomicBool::new(false)),
            },
        );
        if let Some(drain) = self.tap_drains.lock().await.get_mut(&job_id) {
            drain.active = 0;
            drain.notify.notify_waiters();
        }
        if self.cfg.relay_mode != RelayMode::Off {
            self.arbiters
                .lock()
                .await
                .insert(job_id.clone(), SourceArbiter::new(self.cfg.relay_mode));
        }
        let msg_id = format!("msg_{job_id}");
        self.emit(
            &job_id,
            StreamItem::Event(JobStream::message_start(&job.request.model, &msg_id)),
        )
        .await;
        let results = tool_results(&request);
        self.pending
            .lock()
            .await
            .complete_client_tools(&job_id, results)?;
        Ok(())
    }

    pub fn pid(&self) -> u32 {
        self.pid.load(Ordering::Relaxed)
    }
}

async fn job_egress(
    runtime: Weak<Runtime>,
    job_id: String,
    mut data_rx: mpsc::Receiver<SinkEnvelope>,
    sink: JobSink,
    stall_timeout: Duration,
) {
    loop {
        if let Some(terminal) = sink.terminal.get()
            && terminal.is_failure()
        {
            fail_client_stream(&sink, terminal).await;
            if let Some(runtime) = runtime.upgrade() {
                runtime.abort_terminal_job(&job_id).await;
            }
            break;
        }
        let envelope = tokio::select! {
            biased;
            envelope = data_rx.recv() => envelope,
            _ = sink.terminal_notify.notified() => continue,
        };
        let Some(envelope) = envelope else {
            break;
        };
        sink.budget.release(envelope.bytes);
        let final_response = match &envelope.item {
            StreamItem::Finished(response)
                if !matches!(response.stop_reason, StopReason::ToolUse) =>
            {
                Some(response.clone())
            }
            _ => None,
        };
        let tool_response = match &envelope.item {
            StreamItem::Finished(response) => matches!(response.stop_reason, StopReason::ToolUse),
            StreamItem::Event(_) => false,
        };
        let Some(tx) = sink.client_tx.lock().await.clone() else {
            sink.set_terminal(Terminal::ClientGone);
            continue;
        };
        let send = tx.send(Ok(envelope.item));
        tokio::pin!(send);
        let deadline = tokio::time::sleep(stall_timeout);
        tokio::pin!(deadline);
        // A `Done` notification must not abandon the envelope mid-send (it may
        // be the Finished frame); only failure terminals abort the delivery.
        let sent = loop {
            tokio::select! {
                biased;
                result = &mut send => break result.map_err(|_| Terminal::ClientGone),
                _ = sink.terminal_notify.notified() => {
                    if sink.terminal_failed() {
                        break Err(Terminal::ClientGone);
                    }
                }
                _ = &mut deadline => break Err(Terminal::ClientTooSlow),
            }
        };
        match sent {
            Ok(()) if final_response.is_some() || tool_response => {
                sink.client_tx.lock().await.take();
                if let Some(response) = final_response {
                    if let Some(runtime) = runtime.upgrade() {
                        runtime.finish_sent_job(&job_id, response).await;
                    }
                    break;
                }
            }
            Ok(()) => {}
            Err(terminal) => {
                sink.set_terminal(terminal);
            }
        }
    }
}

async fn fail_client_stream(sink: &JobSink, terminal: &Terminal) {
    if let Some(tx) = sink.client_tx.lock().await.take() {
        let _ = tx.try_send(Err(terminal.error()));
    }
}

fn stream_item_bytes(item: &StreamItem) -> usize {
    match item {
        StreamItem::Event(event) => event.to_string().len(),
        StreamItem::Finished(response) => {
            serde_json::to_vec(response).map_or(1, |bytes| bytes.len())
        }
    }
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

fn response_text(response: &MessageResponse) -> String {
    let mut out = String::new();
    for block in &response.content {
        if let ContentBlock::Text { text, .. } = block {
            out.push_str(text);
        }
    }
    out
}

struct JobFinish {
    text: String,
    usage: Value,
}

fn finish_body(
    arbiter: Option<&mut SourceArbiter>,
    stream: &mut JobStream,
    fallback: &str,
    usage: Value,
) -> JobFinish {
    if let Some(arbiter) = arbiter {
        if arbiter.upstream_authoritative() {
            stream.streamed_text = true;
        }
        arbiter.on_kin_done();
        let usage = arbiter.usage().unwrap_or(usage);
        return JobFinish {
            text: arbiter.final_text(&stream.text, fallback),
            usage,
        };
    }
    let text = if stream.text.is_empty() {
        fallback.to_string()
    } else {
        stream.text.clone()
    };
    JobFinish { text, usage }
}

fn usage_from_value(value: &Value) -> Usage {
    Usage {
        input_tokens: value
            .get("input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        output_tokens: value
            .get("output_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
    }
}

async fn job_tap_forwarder(
    runtime: Weak<Runtime>,
    job_id: String,
    mut rx: mpsc::Receiver<TapEvent>,
) {
    while let Some(event) = rx.recv().await {
        let Some(runtime) = runtime.upgrade() else {
            return;
        };
        if apply_tap_event(&runtime, &job_id, event).await {
            return;
        }
    }
}

async fn apply_tap_event(runtime: &Runtime, job_id: &str, event: TapEvent) -> bool {
    let current_turn = runtime
        .tap_turns
        .lock()
        .await
        .get(job_id)
        .copied()
        .unwrap_or(0);
    if event.turn_id != current_turn {
        return false;
    }
    if event.is_drained() {
        runtime.note_tap_drained(job_id).await;
        return false;
    }
    if event.is_poisoned() {
        return fail_if_upstream_poisoned(runtime, job_id).await;
    }
    if let Some(usage) = event.usage_value() {
        if let Some(arbiter) = runtime.arbiters.lock().await.get_mut(job_id) {
            arbiter.note_usage(usage);
        }
        return false;
    }
    let effect = {
        let mut arbiters = runtime.arbiters.lock().await;
        match arbiters.get_mut(job_id) {
            Some(arbiter) => arbiter.on_upstream(&event.event),
            None => ArbiterEffect::Ignore,
        }
    };
    match effect {
        ArbiterEffect::Forward => {
            let mapped = {
                let mut streams = runtime.job_streams.lock().await;
                streams
                    .get_mut(job_id)
                    .map(|stream| stream.adopt_tap_event(event.event))
            };
            if let Some(event) = mapped {
                runtime.emit(job_id, StreamItem::Event(event)).await;
            }
            false
        }
        ArbiterEffect::FailJob => fail_tap_job(runtime, job_id).await,
        ArbiterEffect::Suppress | ArbiterEffect::Ignore => false,
    }
}

async fn fail_if_upstream_poisoned(runtime: &Runtime, job_id: &str) -> bool {
    let effect = {
        let mut arbiters = runtime.arbiters.lock().await;
        match arbiters.get_mut(job_id) {
            Some(arbiter) => arbiter.on_tap_poisoned(),
            None => ArbiterEffect::Ignore,
        }
    };
    if effect == ArbiterEffect::FailJob {
        return fail_tap_job(runtime, job_id).await;
    }
    false
}

async fn fail_tap_job(runtime: &Runtime, job_id: &str) -> bool {
    if let Some(sink) = runtime.sinks.lock().await.get(job_id).cloned() {
        sink.set_terminal(Terminal::Failed(KernelError::Provider(
            "relay tap overflow".into(),
        )));
    }
    true
}

fn tool_results(request: &MessageRequest) -> Vec<(String, Value)> {
    let mut out = Vec::new();
    for message in &request.messages {
        if let MessageContent::Blocks(blocks) = &message.content {
            for block in blocks {
                if let ContentBlock::ToolResult {
                    tool_use_id,
                    content,
                    is_error,
                } = block
                {
                    out.push((
                        tool_use_id.clone(),
                        json!({
                            "tool_use_id": tool_use_id,
                            "content": content,
                            "is_error": is_error
                        }),
                    ));
                }
            }
        }
    }
    out
}

fn latest_text(request: &MessageRequest) -> String {
    for message in request.messages.iter().rev() {
        if message.role != "user" {
            continue;
        }
        match &message.content {
            MessageContent::Text(text) => return text.clone(),
            MessageContent::Blocks(blocks) => {
                let mut out = String::new();
                for block in blocks {
                    if let ContentBlock::Text { text, .. } = block {
                        out.push_str(text);
                    }
                }
                if !out.is_empty() {
                    return out;
                }
            }
        }
    }
    String::new()
}

fn rand_byte() -> u8 {
    Uuid::new_v4().as_bytes()[0]
}

async fn simulate_worker(runtime: Arc<Runtime>, parent: String) {
    runtime.note_agent_spawn(parent.clone()).await;
    let slot_id = runtime.register_or_get_slot(None).await;
    runtime
        .parents
        .lock()
        .await
        .insert(parent.clone(), slot_id.clone());
    {
        let mut slots = runtime.slots.lock().await;
        if let Some(slot) = slots.iter_mut().find(|slot| slot.id == slot_id) {
            slot.parent_tool_use_id = Some(parent.clone());
        }
    }
    loop {
        let payload = match runtime.mcp_slot_wait(json!({ "slot_id": slot_id })).await {
            Ok(value) => value,
            Err(_) => break,
        };
        if payload.get("type").and_then(Value::as_str) == Some("retire") {
            break;
        }
        let job_id = payload
            .get("job_id")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let job = {
            let jobs = runtime.jobs.lock().await;
            jobs.get(&job_id).cloned()
        };
        let Some(job) = job else {
            continue;
        };
        let text = latest_text(&job.request);
        tokio::time::sleep(runtime.cfg.simulate_latency).await;
        if text.contains("[web_search]") {
            let ws = json!({
                "type": "assistant",
                "parent_tool_use_id": parent,
                "message": { "content": [{
                    "type": "tool_use",
                    "id": "toolu_ws_sim",
                    "name": "WebSearch",
                    "input": { "query": text }
                }]}
            });
            runtime.handle_cli_frame(ws).await;
            let result = json!({
                "type": "user",
                "parent_tool_use_id": parent,
                "message": { "content": [{
                    "type": "tool_result",
                    "tool_use_id": "toolu_ws_sim",
                    "content": "Web search results for query"
                }]}
            });
            runtime.handle_cli_frame(result).await;
            let answer = json!({
                "type": "assistant",
                "parent_tool_use_id": parent,
                "message": { "content": [{ "type": "text", "text": "search-complete" }] }
            });
            runtime.handle_cli_frame(answer).await;
            let _ = runtime
                .mcp_kin_done(json!({
                    "job_id": job.job_id,
                    "stop_reason": "end_turn",
                    "usage": { "web_search_requests": 1 },
                    "fallback_content": "DUPLICATE_FROM_KIN_DONE"
                }))
                .await;
        } else if let Some(tool) = text
            .split("[use_tool:")
            .nth(1)
            .and_then(|part| part.split(']').next())
        {
            let _ = runtime
                .mcp_client_tool(json!({
                    "job_id": job.job_id,
                    "name": tool,
                    "input": { "echo": true }
                }))
                .await;
            let _ = runtime
                .mcp_kin_done(json!({
                    "job_id": job.job_id,
                    "text": format!("tool finished for {tool}")
                }))
                .await;
        } else {
            let reply = format!("slot {} :: {text}", &slot_id[..8.min(slot_id.len())]);
            let frame = json!({
                "type": "assistant",
                "parent_tool_use_id": parent,
                "message": { "role": "assistant", "content": [{ "type": "text", "text": reply }] }
            });
            runtime.handle_cli_frame(frame).await;
            let _ = runtime
                .mcp_kin_done(json!({
                    "job_id": job.job_id,
                    "stop_reason": "end_turn",
                    "usage": { "output_tokens": 8 },
                    "fallback_content": reply,
                    "text": reply
                }))
                .await;
        }
    }
}

async fn decode_stdout(runtime: Arc<Runtime>, stdout: impl tokio::io::AsyncRead + Unpin) {
    use tokio::io::{AsyncBufReadExt, BufReader};
    let mut lines = BufReader::new(stdout).lines();
    let mut job_bytes: HashMap<String, usize> = HashMap::new();
    let mut dump = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open("/tmp/kin-live/claude.multiplex.stdout.log")
        .await
        .ok();
    while let Ok(Some(line)) = lines.next_line().await {
        if let Some(file) = dump.as_mut() {
            use tokio::io::AsyncWriteExt;
            let _ = file.write_all(line.as_bytes()).await;
            let _ = file.write_all(b"\n").await;
        }
        if line.len() > stream_decoder::MAX_LINE_BYTES {
            continue;
        }
        let Ok(frame) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        if let Some(parent) = stream_decoder::parent_id(&frame) {
            let used = job_bytes.entry(parent.to_string()).or_insert(0);
            *used = used.saturating_add(line.len());
            if *used > stream_decoder::MAX_JOB_BYTES {
                continue;
            }
        }
        runtime.handle_cli_frame(frame).await;
    }
}

pub struct MultiplexCliProvider {
    cfg: MultiplexConfig,
    runtime: OnceCell<Arc<Runtime>>,
}

impl MultiplexCliProvider {
    pub fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            cfg: MultiplexConfig::from_env()?,
            runtime: OnceCell::new(),
        })
    }

    pub fn simulated(slot_count: usize) -> Self {
        Self {
            cfg: MultiplexConfig {
                slot_count,
                simulate: true,
                bin: PathBuf::from("simulated"),
                mock_bin: true,
                model: "claude-sonnet-5".into(),
                retire_after_turn: false,
                max_jobs_per_slot: 32,
                slot_max_lifetime: Duration::from_secs(1800),
                session_idle_ttl: Duration::from_secs(600),
                simulate_latency: Duration::from_millis(60),
                continuation_ttl_secs: 600,
                client_stall_timeout: Duration::from_secs(DEFAULT_CLIENT_STALL_SECS),
                submit_wait: Duration::from_millis(200),
                relay_mode: RelayMode::Off,
                relay_addr: "127.0.0.1:0".parse().unwrap(),
                relay_upstream: "https://api.anthropic.com".into(),
            },
            runtime: OnceCell::new(),
        }
    }

    async fn runtime(&self) -> Result<&Arc<Runtime>, KernelError> {
        self.runtime
            .get_or_try_init(|| async {
                let runtime = Runtime::new(MultiplexConfig {
                    slot_count: self.cfg.slot_count,
                    simulate: self.cfg.simulate,
                    bin: self.cfg.bin.clone(),
                    mock_bin: self.cfg.mock_bin,
                    model: self.cfg.model.clone(),
                    retire_after_turn: self.cfg.retire_after_turn,
                    max_jobs_per_slot: self.cfg.max_jobs_per_slot,
                    slot_max_lifetime: self.cfg.slot_max_lifetime,
                    session_idle_ttl: self.cfg.session_idle_ttl,
                    simulate_latency: self.cfg.simulate_latency,
                    continuation_ttl_secs: self.cfg.continuation_ttl_secs,
                    client_stall_timeout: self.cfg.client_stall_timeout,
                    submit_wait: self.cfg.submit_wait,
                    relay_mode: self.cfg.relay_mode,
                    relay_addr: self.cfg.relay_addr,
                    relay_upstream: self.cfg.relay_upstream.clone(),
                });
                runtime.start().await?;
                Ok(runtime)
            })
            .await
    }
}

#[async_trait]
impl Provider for MultiplexCliProvider {
    fn name(&self) -> &'static str {
        "local_cli"
    }

    async fn boot(&self) -> Result<(), KernelError> {
        self.runtime().await.map(|_| ())
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            streaming: true,
            resume: true,
            multiplex_slots: true,
            native_tool_wait: true,
            cancel_receipt: true,
        }
    }

    fn session_pid(&self, _session_id: &str) -> Option<u32> {
        self.runtime.get().map(|runtime| runtime.pid())
    }

    fn memory_snapshot(&self) -> Option<serde_json::Value> {
        self.runtime.get().map(|runtime| {
            serde_json::to_value(runtime.memory.snapshot()).unwrap_or(serde_json::Value::Null)
        })
    }

    fn relay_snapshot(&self) -> Option<serde_json::Value> {
        Some(match self.runtime.get() {
            Some(runtime) => runtime.relay_snapshot(),
            None => json!({
                "relay_mode": self.cfg.relay_mode.as_str(),
                "relay_healthy": false,
                "tap_dropped": 0,
                "digest_mismatch": 0,
                "correlate_hit": 0,
                "correlate_miss": 0,
                "correlate_ambiguous": 0,
                "tap_response_started": 0,
            }),
        })
    }

    async fn execute_stream(
        &self,
        request: &MessageRequest,
        context: &ExecutionContext,
    ) -> Result<crate::provider::StreamRx, KernelError> {
        let runtime = self.runtime().await?;
        let (tx, rx) = job_event_channel();
        runtime.submit(request.clone(), context.clone(), tx).await?;
        Ok(rx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Message, ToolDefinition};
    use crate::provider::stream_channel;
    use tokio::{net::TcpListener, time::timeout};

    fn ctx(session: &str, resumed: bool) -> ExecutionContext {
        ExecutionContext {
            tenant_id: "demo".into(),
            session_id: session.into(),
            worker_id: "runtime-00".into(),
            worker_generation: 1,
            resumed,
        }
    }

    fn text_request(text: &str) -> MessageRequest {
        MessageRequest {
            model: "claude-sonnet-5".into(),
            messages: vec![Message {
                role: "user".into(),
                content: MessageContent::Text(text.into()),
                tool_call_id: None,
                tool_calls: Vec::new(),
            }],
            tools: vec![ToolDefinition {
                name: "echo".into(),
                description: "echo".into(),
                input_schema: json!({"type":"object"}),
                cache_control: None,
                tool_type: None,
                extra: Default::default(),
            }],
            max_tokens: 128,
            stream: false,
            ..MessageRequest::default()
        }
    }

    fn test_cfg(stall: Duration) -> MultiplexConfig {
        MultiplexConfig {
            slot_count: 1,
            simulate: true,
            bin: PathBuf::from("simulated"),
            mock_bin: true,
            model: "claude-sonnet-5".into(),
            retire_after_turn: false,
            max_jobs_per_slot: 32,
            slot_max_lifetime: Duration::from_secs(1800),
            session_idle_ttl: Duration::from_secs(600),
            simulate_latency: Duration::from_millis(1),
            continuation_ttl_secs: 600,
            client_stall_timeout: stall,
            submit_wait: Duration::from_millis(200),
            relay_mode: RelayMode::Off,
            relay_addr: "127.0.0.1:0".parse().unwrap(),
            relay_upstream: "https://api.anthropic.com".into(),
        }
    }

    fn relay_cfg(stall: Duration) -> MultiplexConfig {
        let mut cfg = test_cfg(stall);
        cfg.relay_mode = RelayMode::Authoritative;
        cfg
    }

    fn delta(text: &str) -> StreamItem {
        StreamItem::Event(json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": { "type": "text_delta", "text": text }
        }))
    }

    fn stage_event(name: &str) -> StreamItem {
        StreamItem::Event(json!({ "type": name }))
    }

    fn structural_start() -> StreamItem {
        StreamItem::Event(json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": { "type": "text", "text": "" }
        }))
    }

    fn finished_response() -> StreamItem {
        StreamItem::Finished(MessageResponse {
            id: "msg_test".into(),
            r#type: "message",
            role: "assistant",
            model: "claude-sonnet-5".into(),
            content: vec![ContentBlock::Text {
                text: "done".into(),
                cache_control: None,
            }],
            stop_reason: StopReason::EndTurn,
            usage: Usage::default(),
        })
    }

    async fn wait_for_overflow(sink: &JobSink) {
        timeout(Duration::from_secs(1), async {
            loop {
                if matches!(sink.terminal.get(), Some(Terminal::Overflow)) {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("overflow terminal");
    }

    async fn collect(
        provider: &MultiplexCliProvider,
        request: MessageRequest,
        context: ExecutionContext,
    ) -> Result<MessageResponse, KernelError> {
        let rx = provider.execute_stream(&request, &context).await?;
        crate::provider::collect_stream(rx).await
    }

    async fn insert_test_job(runtime: &Arc<Runtime>, job_id: &str, slot_id: &str, session: &str) {
        runtime.jobs.lock().await.insert(
            job_id.to_string(),
            Job {
                job_id: job_id.to_string(),
                tenant_id: "demo".into(),
                session_id: session.to_string(),
                slot_id: slot_id.to_string(),
                generation: runtime.process_generation.load(Ordering::Relaxed),
                request: text_request("hello"),
            },
        );
        let mut slot = Slot::new(slot_id);
        slot.phase = SlotPhase::WaitingTool;
        slot.job_id = Some(job_id.to_string());
        slot.session_id = Some(session.to_string());
        slot.tenant_id = Some("demo".into());
        runtime.slots.lock().await.push(slot);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fast_consumer_receives_all_deltas() {
        let runtime = Runtime::new(test_cfg(Duration::from_secs(1)));
        let (tx, mut rx) = mpsc::channel(64);
        runtime.start_job_sink("job-fast".into(), tx).await;

        for index in 0..24 {
            assert_eq!(
                runtime
                    .emit("job-fast", delta(&format!("chunk-{index};")))
                    .await,
                EmitResult::Sent
            );
        }
        assert_eq!(
            runtime.emit("job-fast", finished_response()).await,
            EmitResult::Sent
        );

        let mut deltas = Vec::new();
        let mut finished = false;
        while let Some(item) = timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("stream item")
        {
            match item.unwrap() {
                StreamItem::Event(event) => {
                    if event.pointer("/delta/type").and_then(Value::as_str) == Some("text_delta") {
                        deltas.push(event["delta"]["text"].as_str().unwrap_or("").to_string());
                    }
                }
                StreamItem::Finished(_) => {
                    finished = true;
                    break;
                }
            }
        }
        assert!(finished);
        assert_eq!(deltas.len(), 24);
        assert_eq!(deltas[0], "chunk-0;");
        assert_eq!(deltas[23], "chunk-23;");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn final_finished_sets_done_only_after_client_delivery() {
        let runtime = Runtime::new(test_cfg(Duration::from_secs(1)));
        let (tx, mut rx) = mpsc::channel(8);
        runtime.start_job_sink("job-done".into(), tx).await;
        let sink = runtime
            .sinks
            .lock()
            .await
            .get("job-done")
            .cloned()
            .expect("sink");

        assert_eq!(
            runtime.emit("job-done", finished_response()).await,
            EmitResult::Sent
        );
        assert!(matches!(
            timeout(Duration::from_secs(1), rx.recv())
                .await
                .expect("finished")
                .expect("finished")
                .unwrap(),
            StreamItem::Finished(_)
        ));
        timeout(Duration::from_secs(1), async {
            loop {
                if matches!(sink.terminal.get(), Some(Terminal::Done)) {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("done terminal");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn oversized_delta_overflow_sends_explicit_error() {
        let runtime = Runtime::new(test_cfg(Duration::from_secs(1)));
        let (tx, mut rx) = mpsc::channel(8);
        runtime.start_job_sink("job-big".into(), tx).await;

        let big = "x".repeat(JOB_SINK_BYTES + 1);
        assert_eq!(
            runtime.emit("job-big", delta(&big)).await,
            EmitResult::Failed
        );

        let item = timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("error item")
            .expect("error item");
        assert!(item.is_err(), "{item:?}");
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn queue_full_text_delta_sets_overflow_terminal() {
        let runtime = Runtime::new(test_cfg(Duration::from_secs(1)));
        let (tx, _rx) = mpsc::channel(1);
        runtime.start_job_sink("job-full".into(), tx).await;
        let sink = runtime
            .sinks
            .lock()
            .await
            .get("job-full")
            .cloned()
            .expect("sink");

        for index in 0..(JOB_SINK_ITEMS + 32) {
            let _ = runtime
                .emit("job-full", delta(&format!("chunk-{index}")))
                .await;
        }

        wait_for_overflow(&sink).await;
        assert!(sink.terminal_failed());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn queue_full_structural_event_sets_overflow_terminal() {
        let runtime = Runtime::new(test_cfg(Duration::from_secs(1)));
        let (tx, _rx) = mpsc::channel(1);
        runtime.start_job_sink("job-struct-full".into(), tx).await;
        let sink = runtime
            .sinks
            .lock()
            .await
            .get("job-struct-full")
            .cloned()
            .expect("sink");

        for _ in 0..(JOB_SINK_ITEMS + 8) {
            let _ = runtime.emit("job-struct-full", structural_start()).await;
        }

        wait_for_overflow(&sink).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stalled_client_never_receives_success_finish() {
        let runtime = Runtime::new(test_cfg(Duration::from_millis(20)));
        let (tx, mut rx) = mpsc::channel(1);
        runtime.start_job_sink("job-stalled".into(), tx).await;
        let sink = runtime
            .sinks
            .lock()
            .await
            .get("job-stalled")
            .cloned()
            .expect("sink");

        assert_eq!(
            runtime
                .emit("job-stalled", stage_event("message_start"))
                .await,
            EmitResult::Sent
        );
        assert_eq!(
            runtime.emit("job-stalled", finished_response()).await,
            EmitResult::Sent
        );

        tokio::time::sleep(Duration::from_millis(80)).await;
        let mut saw_finished = false;
        while let Some(item) = rx.recv().await {
            if matches!(item.unwrap(), StreamItem::Finished(_)) {
                saw_finished = true;
            }
        }
        assert!(!saw_finished);
        assert!(matches!(sink.terminal.get(), Some(Terminal::ClientTooSlow)));
        assert!(!matches!(sink.terminal.get(), Some(Terminal::Done)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn kin_done_waits_for_tap_drain_before_finishing() {
        let runtime = Runtime::new(relay_cfg(Duration::from_secs(1)));
        let (tx, mut rx) = mpsc::channel(64);
        runtime.start_job_sink("job-drain".into(), tx).await;
        insert_test_job(&runtime, "job-drain", "slot-drain", "sess-drain").await;
        runtime.register_tap_response("job-drain").await;
        let binding = runtime.tap_binding("job-drain").await.unwrap();
        let tap = relay::sse_tap::TapQueue::spawn(
            "job-drain".into(),
            binding.events,
            Arc::new(relay::metrics::RelayMetrics::default()),
            binding.poisoned,
            binding.index_allocator,
            binding.turn_id,
        );
        tap.offer(axum::body::Bytes::from(
            [
                "event: message\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":7}}}\n\n",
                "event: message\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
                "event: message\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"last token\"}}\n\n",
                "event: message\ndata: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":3}}\n\n",
            ]
            .concat(),
        ));
        drop(tap);

        runtime
            .mcp_kin_done(json!({
                "job_id": "job-drain",
                "stop_reason": "end_turn",
                "usage": {},
                "fallback_content": ""
            }))
            .await
            .unwrap();

        let mut text = String::new();
        let mut usage = Usage::default();
        let mut finished = false;
        while let Some(item) = timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("stream")
        {
            match item.unwrap() {
                StreamItem::Event(event) => {
                    if event.pointer("/delta/type").and_then(Value::as_str) == Some("text_delta") {
                        text.push_str(event["delta"]["text"].as_str().unwrap_or(""));
                    }
                    if event.get("type").and_then(Value::as_str) == Some("message_delta") {
                        usage = usage_from_value(&event["usage"]);
                    }
                }
                StreamItem::Finished(response) => {
                    assert_eq!(response.usage.input_tokens, 7);
                    assert_eq!(response.usage.output_tokens, 3);
                    finished = true;
                    break;
                }
            }
        }
        assert_eq!(text, "last token");
        assert_eq!(usage.input_tokens, 7);
        assert_eq!(usage.output_tokens, 3);
        assert!(finished);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn resume_resets_turn_poison_without_resetting_index_base() {
        let runtime = Runtime::new(relay_cfg(Duration::from_secs(1)));
        let (tx1, _rx1) = mpsc::channel(8);
        runtime.start_job_sink("job-resume".into(), tx1).await;
        insert_test_job(&runtime, "job-resume", "slot-resume", "sess-resume").await;
        let _tool_rx = runtime
            .pending
            .lock()
            .await
            .register_client_tool("job-resume", None);
        if let Some(state) = runtime.tap_poisoned.lock().await.get("job-resume") {
            state.poisoned.store(true, Ordering::Relaxed);
        }
        if let Some(arbiter) = runtime.arbiters.lock().await.get_mut("job-resume") {
            let _ = arbiter.on_upstream(&json!({
                "type": "content_block_delta",
                "index": 0,
                "delta": { "type": "text_delta", "text": "bad" }
            }));
            assert_eq!(arbiter.on_tap_poisoned(), ArbiterEffect::FailJob);
        }
        runtime
            .tap_index_allocators
            .lock()
            .await
            .get("job-resume")
            .unwrap()
            .store(4, Ordering::Relaxed);

        let (tx2, _rx2) = mpsc::channel(8);
        let mut request = text_request("resume");
        request.messages[0].content = MessageContent::Blocks(vec![ContentBlock::ToolResult {
            tool_use_id: "toolu_resume".into(),
            content: json!("ok"),
            is_error: false,
        }]);
        runtime
            .submit(request, ctx("sess-resume", true), tx2)
            .await
            .unwrap();

        assert!(
            !runtime
                .tap_poisoned
                .lock()
                .await
                .get("job-resume")
                .unwrap()
                .poisoned
                .load(Ordering::Relaxed)
        );
        let state = runtime
            .arbiters
            .lock()
            .await
            .get("job-resume")
            .unwrap()
            .state();
        assert_eq!(state, relay::arbiter::BodyState::NoBody);
        assert_eq!(
            runtime.tap_turns.lock().await.get("job-resume").copied(),
            Some(1)
        );
        let stale = TapEvent {
            job_id: "job-resume".into(),
            turn_id: 0,
            event: json!({
                "type": "content_block_delta",
                "index": 0,
                "delta": { "type": "text_delta", "text": "stale" }
            }),
        };
        assert!(!apply_tap_event(&runtime, "job-resume", stale).await);
        assert_eq!(
            runtime
                .arbiters
                .lock()
                .await
                .get("job-resume")
                .unwrap()
                .state(),
            relay::arbiter::BodyState::NoBody
        );
        let mut stream = runtime.job_streams.lock().await;
        let event = stream
            .get_mut("job-resume")
            .unwrap()
            .adopt_tap_event(json!({
                "type": "content_block_start",
                "index": 4,
                "content_block": { "type": "text", "text": "" }
            }));
        assert_eq!(event["index"], 4);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stale_tap_queue_poison_after_resume_does_not_fail_new_turn() {
        let runtime = Runtime::new(relay_cfg(Duration::from_secs(1)));
        let (tx1, _rx1) = mpsc::channel(8);
        runtime.start_job_sink("job-stale-poison".into(), tx1).await;
        insert_test_job(
            &runtime,
            "job-stale-poison",
            "slot-stale-poison",
            "sess-stale-poison",
        )
        .await;
        let _tool_rx = runtime
            .pending
            .lock()
            .await
            .register_client_tool("job-stale-poison", None);
        runtime.register_tap_response("job-stale-poison").await;
        let old_binding = runtime.tap_binding("job-stale-poison").await.unwrap();
        let old_tap = relay::sse_tap::TapQueue::spawn(
            "job-stale-poison".into(),
            old_binding.events,
            Arc::new(relay::metrics::RelayMetrics::default()),
            old_binding.poisoned,
            old_binding.index_allocator,
            old_binding.turn_id,
        );

        let (tx2, mut rx2) = mpsc::channel(8);
        let mut request = text_request("resume");
        request.messages[0].content = MessageContent::Blocks(vec![ContentBlock::ToolResult {
            tool_use_id: "toolu_resume".into(),
            content: json!("ok"),
            is_error: false,
        }]);
        runtime
            .submit(request, ctx("sess-stale-poison", true), tx2)
            .await
            .unwrap();
        assert_eq!(
            runtime
                .tap_turns
                .lock()
                .await
                .get("job-stale-poison")
                .copied(),
            Some(1)
        );

        let current = TapEvent {
            job_id: "job-stale-poison".into(),
            turn_id: 1,
            event: json!({
                "type": "content_block_delta",
                "index": 0,
                "delta": { "type": "text_delta", "text": "current" }
            }),
        };
        assert!(!apply_tap_event(&runtime, "job-stale-poison", current).await);
        old_tap.poison();
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        let sink = runtime
            .sinks
            .lock()
            .await
            .get("job-stale-poison")
            .cloned()
            .unwrap();
        assert!(!sink.terminal_failed());
        assert!(
            !runtime
                .tap_poisoned
                .lock()
                .await
                .get("job-stale-poison")
                .unwrap()
                .poisoned
                .load(Ordering::Relaxed)
        );
        assert!(
            !runtime
                .arbiters
                .lock()
                .await
                .get("job-stale-poison")
                .unwrap()
                .failed()
        );

        runtime
            .mcp_kin_done(json!({
                "job_id": "job-stale-poison",
                "stop_reason": "end_turn",
                "usage": {},
                "fallback_content": "fallback"
            }))
            .await
            .unwrap();

        let mut finished = None;
        while let Some(item) = timeout(Duration::from_secs(1), rx2.recv())
            .await
            .expect("stream item")
        {
            if let StreamItem::Finished(response) = item.unwrap() {
                finished = Some(response);
                break;
            }
        }
        let response = finished.expect("finished response");
        assert_eq!(response_text(&response), "current");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn start_claude_preflight_failure_returns_before_cli_spawn() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = listener.local_addr().unwrap();
        drop(listener);
        let mut cfg = relay_cfg(Duration::from_secs(1));
        cfg.simulate = false;
        cfg.bin = PathBuf::from("must-not-be-spawned");
        cfg.mock_bin = false;
        cfg.relay_upstream = format!("http://{upstream_addr}");
        let runtime = Runtime::new(cfg);

        let err = runtime.start_claude().await.unwrap_err();
        assert!(
            err.to_string().contains("relay upstream preflight"),
            "{err}"
        );
        assert_eq!(runtime.pid(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn submit_retries_through_slot_reentry_gap() {
        // FAIL-3 in the 20-way live test: a burst submission can land in the
        // window where a slot has finished its job but has not yet re-entered
        // slot_wait, seeing a spurious NoCapacity. submit must wait it out.
        let provider = MultiplexCliProvider::simulated(1);
        provider.runtime().await.expect("start");
        let runtime = Arc::clone(provider.runtime.get().unwrap());
        // Occupy the only slot, then submit again while it is busy: the retry
        // loop should pick it up as soon as the first job completes.
        let (tx1, rx1) = stream_channel();
        runtime
            .submit(text_request("first"), ctx("sess-gap-1", false), tx1)
            .await
            .unwrap();
        let second = {
            let runtime = Arc::clone(&runtime);
            tokio::spawn(async move {
                let (tx2, rx2) = stream_channel();
                runtime
                    .submit(text_request("second"), ctx("sess-gap-2", false), tx2)
                    .await?;
                crate::provider::collect_stream(rx2).await
            })
        };
        let first = crate::provider::collect_stream(rx1).await.unwrap();
        assert!(matches!(first.stop_reason, StopReason::EndTurn));
        let second = timeout(Duration::from_secs(5), second)
            .await
            .expect("second submit within retry window")
            .unwrap()
            .unwrap();
        assert!(matches!(second.stop_reason, StopReason::EndTurn));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn one_pid_five_parallel_slots() {
        let shared = MultiplexCliProvider::simulated(5);
        shared.runtime().await.expect("start");
        let pid = shared.session_pid("any").expect("pid");
        let mut joins = Vec::new();
        for index in 0..5 {
            let request = text_request(&format!("hello {index}"));
            let context = ctx(&format!("sess-{index}"), false);
            let runtime = Arc::clone(shared.runtime.get().unwrap());
            joins.push(tokio::spawn(async move {
                let (tx, rx) = stream_channel();
                runtime.submit(request, context, tx).await.unwrap();
                crate::provider::collect_stream(rx).await
            }));
        }
        let mut texts = Vec::new();
        for join in joins {
            let response = timeout(Duration::from_secs(5), join)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert!(matches!(response.stop_reason, StopReason::EndTurn));
            assert_eq!(shared.session_pid("x").unwrap(), pid);
            if let ContentBlock::Text { text, .. } = &response.content[0] {
                texts.push(text.clone());
            }
        }
        assert_eq!(texts.len(), 5);
        assert!(shared.runtime.get().unwrap().peak_running() >= 2);
        let parents: Vec<_> = shared
            .runtime
            .get()
            .unwrap()
            .snapshots()
            .await
            .into_iter()
            .filter_map(|slot| slot.parent_tool_use_id)
            .collect();
        let unique: std::collections::HashSet<_> = parents.iter().cloned().collect();
        assert_eq!(unique.len(), 5);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn two_tool_calls_resume_independently() {
        let provider = MultiplexCliProvider::simulated(2);
        let first = collect(
            &provider,
            text_request("please [use_tool:echo] now"),
            ctx("sess-tool-a", false),
        )
        .await
        .unwrap();
        let second = collect(
            &provider,
            text_request("please [use_tool:echo] now"),
            ctx("sess-tool-b", false),
        )
        .await
        .unwrap();
        assert!(matches!(first.stop_reason, StopReason::ToolUse));
        assert!(matches!(second.stop_reason, StopReason::ToolUse));
        let id_a = match &first.content[0] {
            ContentBlock::ToolUse { id, .. } => id.clone(),
            _ => panic!("tool a"),
        };
        let id_b = match &second.content[0] {
            ContentBlock::ToolUse { id, .. } => id.clone(),
            _ => panic!("tool b"),
        };
        assert_ne!(id_a, id_b);
        let resume = |id: String| MessageRequest {
            model: "claude-sonnet-5".into(),
            messages: vec![Message {
                role: "user".into(),
                content: MessageContent::Blocks(vec![ContentBlock::ToolResult {
                    tool_use_id: id,
                    content: json!("ok"),
                    is_error: false,
                }]),
                tool_call_id: None,
                tool_calls: Vec::new(),
            }],
            max_tokens: 32,
            stream: false,
            ..MessageRequest::default()
        };
        let a = collect(&provider, resume(id_a), ctx("sess-tool-a", true))
            .await
            .unwrap();
        let b = collect(&provider, resume(id_b), ctx("sess-tool-b", true))
            .await
            .unwrap();
        assert!(matches!(a.stop_reason, StopReason::EndTurn));
        assert!(matches!(b.stop_reason, StopReason::EndTurn));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stale_generation_is_continuation_lost() {
        let provider = MultiplexCliProvider::simulated(1);
        let _ = collect(
            &provider,
            text_request("please [use_tool:echo] now"),
            ctx("sess-gen", false),
        )
        .await
        .unwrap();
        provider.runtime.get().unwrap().bump_generation();
        let err = collect(&provider, text_request("ignored"), ctx("sess-gen", true))
            .await
            .unwrap_err();
        assert!(matches!(err, KernelError::ContinuationLost));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn one_pid_twenty_parallel_slots() {
        let shared = MultiplexCliProvider::simulated(20);
        shared.runtime().await.expect("start");
        let pid = shared.session_pid("any").expect("pid");
        let started = std::time::Instant::now();
        let mut joins = Vec::new();
        for index in 0..20 {
            let request = text_request(&format!("hello {index}"));
            let context = ctx(&format!("sess-20-{index}"), false);
            let runtime = Arc::clone(shared.runtime.get().unwrap());
            joins.push(tokio::spawn(async move {
                let (tx, rx) = stream_channel();
                runtime.submit(request, context, tx).await.unwrap();
                crate::provider::collect_stream(rx).await
            }));
        }
        let mut texts = Vec::new();
        for join in joins {
            let response = timeout(Duration::from_secs(5), join)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(shared.session_pid("x").unwrap(), pid);
            if let ContentBlock::Text { text, .. } = &response.content[0] {
                texts.push(text.clone());
            }
        }
        let elapsed = started.elapsed();
        assert_eq!(texts.len(), 20);
        let unique: std::collections::HashSet<_> = texts.into_iter().collect();
        assert_eq!(unique.len(), 20, "outputs must not mix across slots");
        assert!(
            elapsed < Duration::from_millis(800),
            "20 slots should overlap, elapsed={elapsed:?}"
        );
        assert!(shared.runtime.get().unwrap().peak_running() >= 10);
        let parents: Vec<_> = shared
            .runtime
            .get()
            .unwrap()
            .snapshots()
            .await
            .into_iter()
            .filter_map(|slot| slot.parent_tool_use_id)
            .collect();
        let unique_parents: std::collections::HashSet<_> = parents.iter().cloned().collect();
        assert_eq!(unique_parents.len(), 20);
    }

    #[tokio::test]
    async fn memory_guard_rejects_when_rss_is_over_limit() {
        let provider = MultiplexCliProvider::simulated(2);
        provider.runtime().await.expect("start");
        provider
            .runtime
            .get()
            .unwrap()
            .memory
            .set_rss_override(4 * crate::provider::multiplex_cli::memory_guard::GIB);
        let err = collect(&provider, text_request("hello"), ctx("sess-oom", false))
            .await
            .unwrap_err();
        assert!(matches!(err, KernelError::Overloaded { .. }));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn message_start_arrives_before_slot_finishes() {
        let provider = MultiplexCliProvider::simulated(1);
        let rx = provider
            .execute_stream(&text_request("hello stream"), &ctx("sess-stream", false))
            .await
            .unwrap();
        let mut rx = rx;
        let first = timeout(Duration::from_millis(40), rx.recv())
            .await
            .expect("message_start should not wait for the slot")
            .unwrap()
            .unwrap();
        match first {
            StreamItem::Event(event) => {
                assert_eq!(
                    event.get("type").and_then(Value::as_str),
                    Some("message_start")
                );
            }
            other => panic!("expected message_start, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stdout_text_is_one_block_not_fake_tokens() {
        let provider = MultiplexCliProvider::simulated(1);
        let rx = provider
            .execute_stream(&text_request("hello stream"), &ctx("sess-block", false))
            .await
            .unwrap();
        let mut deltas = Vec::new();
        let mut types = Vec::new();
        let mut rx = rx;
        while let Some(item) = rx.recv().await {
            match item.unwrap() {
                StreamItem::Event(event) => {
                    types.push(
                        event
                            .get("type")
                            .and_then(Value::as_str)
                            .unwrap_or("")
                            .to_string(),
                    );
                    if event.pointer("/delta/type").and_then(Value::as_str) == Some("text_delta") {
                        deltas.push(event["delta"]["text"].as_str().unwrap_or("").to_string());
                    }
                }
                StreamItem::Finished(_) => {}
            }
        }
        assert_eq!(types.first().map(String::as_str), Some("message_start"));
        assert_eq!(deltas.len(), 1, "do not fake-chunk: {deltas:?}");
        assert!(deltas[0].contains("hello stream"), "{}", deltas[0]);
        assert!(types.iter().any(|t| t == "message_delta"));
        assert!(types.iter().any(|t| t == "message_stop"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn web_search_frames_are_forwarded() {
        let provider = MultiplexCliProvider::simulated(1);
        let rx = provider
            .execute_stream(
                &text_request("please [web_search] now"),
                &ctx("sess-ws", false),
            )
            .await
            .unwrap();
        let mut block_types = Vec::new();
        let mut deltas = Vec::new();
        let mut rx = rx;
        while let Some(item) = rx.recv().await {
            if let StreamItem::Event(event) = item.unwrap() {
                if event.get("type").and_then(Value::as_str) == Some("content_block_start") {
                    block_types.push(
                        event["content_block"]["type"]
                            .as_str()
                            .unwrap_or("")
                            .to_string(),
                    );
                }
                if event.pointer("/delta/type").and_then(Value::as_str) == Some("text_delta") {
                    deltas.push(event["delta"]["text"].as_str().unwrap_or("").to_string());
                }
            }
        }
        assert!(
            block_types.contains(&"server_tool_use".to_string()),
            "{block_types:?}"
        );
        assert!(
            block_types.contains(&"web_search_tool_result".to_string()),
            "{block_types:?}"
        );
        assert!(block_types.contains(&"text".to_string()), "{block_types:?}");
        assert_eq!(deltas, vec!["search-complete".to_string()]);
        assert!(
            !deltas.iter().any(|d| d.contains("DUPLICATE_FROM_KIN_DONE")),
            "{deltas:?}"
        );
    }

    #[test]
    fn relay_context_generation_respects_mode() {
        let off = Runtime::new(test_cfg(Duration::from_secs(1)));
        let job = Job {
            job_id: "job-ctx".into(),
            tenant_id: "demo".into(),
            session_id: "session".into(),
            slot_id: "slot-ctx".into(),
            generation: 1,
            request: MessageRequest::default(),
        };
        assert!(off.issue_relay_context(&job).unwrap().is_none());

        let mut cfg = test_cfg(Duration::from_secs(1));
        cfg.relay_mode = RelayMode::Observe;
        let observe = Runtime::new(cfg);
        let encoded = observe.issue_relay_context(&job).unwrap().unwrap();
        let decoded = relay::correlate::RelayContextToken::decode(&encoded, observe.secret())
            .expect("relay context");
        assert_eq!(decoded.job_id, "job-ctx");
        assert_eq!(decoded.slot_id, "slot-ctx");
        assert_eq!(decoded.generation, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn simulated_off_mode_does_not_start_relay() {
        let provider = MultiplexCliProvider::simulated(1);
        provider.runtime().await.expect("start");
        let snap = provider.relay_snapshot().expect("snapshot");
        assert_eq!(snap["relay_mode"], "off");
        assert_eq!(snap["relay_healthy"], false);
        assert_eq!(snap["tap_dropped"], 0);
        assert_eq!(snap["digest_mismatch"], 0);
        assert!(provider.runtime.get().unwrap().relay.get().is_none());
    }
}
