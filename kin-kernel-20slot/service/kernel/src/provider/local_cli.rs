//! Drive a local Claude Code process with a leased `claudeAiOauth` blob.
//!
//! Isolation (KIN_ISOLATION):
//! - `process`: each turn owns a Claude child; retire on end_turn.
//! - `session-reset`: one child, `/clear` between non-resume turns.
//! - `subagent-pool` (default): up to N live children, **one `--session-id`
//!   per inbound session**. Same session reuses the pid; different sessions
//!   never share a conversation.

use std::{
    collections::{HashMap, HashSet},
    env, fs,
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::task::spawn_blocking;
use uuid::Uuid;

use crate::{
    config::IsolationMode,
    error::KernelError,
    model::{
        ContentBlock, Message, MessageContent, MessageRequest, MessageResponse, StopReason, Usage,
    },
    provider::{
        ExecutionContext, Provider, ProviderCapabilities, StreamRx, StreamTx, cli_auth,
        stream_channel,
    },
    stream::{StreamAssembler, StreamItem},
};

struct Parked {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    generation: u64,
    pid: u32,
    last_used: Instant,
}

struct SessionTable {
    parked: HashMap<String, Parked>,
    busy: HashSet<String>,
}

pub struct LocalCliProvider {
    bin: PathBuf,
    mock: bool,
    isolation: IsolationMode,
    max_slots: usize,
    table: Arc<Mutex<SessionTable>>,
}

impl LocalCliProvider {
    pub fn from_env(isolation: IsolationMode) -> Result<Self, Box<dyn std::error::Error>> {
        let max_slots = env::var("KIN_SLOTS_PER_WORKER")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(20)
            .max(1);
        let table = Arc::new(Mutex::new(SessionTable {
            parked: HashMap::new(),
            busy: HashSet::new(),
        }));
        if let Ok(bin) = env::var("KIN_CLAUDE_BIN") {
            return Ok(Self {
                mock: bin.contains("mock-claude"),
                bin: PathBuf::from(bin),
                isolation,
                max_slots,
                table,
            });
        }
        let mock = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into()))
            .join("../../scripts/kin-node-kernel/mock-claude.mjs");
        Ok(Self {
            mock: true,
            bin: mock.canonicalize().unwrap_or(mock),
            isolation,
            max_slots,
            table,
        })
    }
}

#[async_trait]
impl Provider for LocalCliProvider {
    fn name(&self) -> &'static str {
        "local_cli"
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            streaming: true,
            resume: true,
            multiplex_slots: self.isolation == IsolationMode::Multiplexed,
            native_tool_wait: true,
            cancel_receipt: true,
        }
    }

    fn session_pid(&self, session_id: &str) -> Option<u32> {
        self.table
            .lock()
            .ok()?
            .parked
            .get(session_id)
            .map(|parked| parked.pid)
    }

    async fn execute_stream(
        &self,
        request: &MessageRequest,
        context: &ExecutionContext,
    ) -> Result<StreamRx, KernelError> {
        let bin = self.bin.clone();
        let mock = self.mock;
        let isolation = self.isolation;
        let max_slots = self.max_slots;
        let request = request.clone();
        let context = context.clone();
        let table = Arc::clone(&self.table);
        let (tx, rx) = stream_channel();
        spawn_blocking(move || {
            let result = run_turn(
                &bin,
                mock,
                isolation,
                max_slots,
                table.as_ref(),
                &request,
                &context,
                Some(tx.clone()),
            );
            match result {
                Ok(response) => {
                    let _ = tx.try_send(Ok(StreamItem::Finished(response)));
                }
                Err(err) => {
                    let _ = tx.try_send(Err(err));
                }
            }
        });
        Ok(rx)
    }
}

fn run_turn(
    bin: &Path,
    mock: bool,
    isolation: IsolationMode,
    max_slots: usize,
    table: &Mutex<SessionTable>,
    request: &MessageRequest,
    context: &ExecutionContext,
    events: Option<StreamTx>,
) -> Result<MessageResponse, KernelError> {
    let cli_session = cli_session_uuid(&context.session_id);
    let session_dir = PathBuf::from("/tmp/kin-cli")
        .join(&context.tenant_id)
        .join(&cli_session);
    fs::create_dir_all(&session_dir).map_err(|err| KernelError::Provider(err.to_string()))?;
    let auth = cli_auth::resolve()?;
    cli_auth::write_credentials(&session_dir, &auth)?;

    let keep_alive = isolation != IsolationMode::ProcessPerTurn;
    let mut parked = take_session(
        table,
        &context.session_id,
        context.resumed,
        context.worker_generation,
        max_slots,
        keep_alive,
        || {
            spawn_parked(
                bin,
                mock,
                isolation,
                &session_dir,
                &cli_session,
                &request.model,
                context.worker_generation,
                &auth,
            )
        },
    )?;

    if isolation == IsolationMode::ResetAndReuse && !context.resumed {
        let _ = write_text(&mut parked, "/clear", &cli_session);
        let _ = read_until_boundary(&mut parked, &request.model, None);
    }

    write_turn(&mut parked, request, &cli_session)?;
    let (content, stop, usage) = read_until_boundary(&mut parked, &request.model, events.as_ref())?;
    let pid = parked.pid;
    parked.last_used = Instant::now();

    if matches!(stop, StopReason::ToolUse) || keep_alive {
        put_session(table, &context.session_id, parked);
        let mut response = response(request, content, stop, usage);
        response.id = format!("msg_{pid}");
        return Ok(response);
    }
    retire(&mut parked);
    release_busy(table, &context.session_id);
    Ok(response(request, content, stop, usage))
}

fn take_session(
    table: &Mutex<SessionTable>,
    session_id: &str,
    resumed: bool,
    generation: u64,
    max_slots: usize,
    keep_alive: bool,
    spawn: impl Fn() -> Result<Parked, KernelError>,
) -> Result<Parked, KernelError> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let mut guard = table.lock().expect("cli table poisoned");
        if guard.busy.contains(session_id) {
            drop(guard);
            if Instant::now() > deadline {
                return Err(KernelError::Provider("session busy".into()));
            }
            thread::sleep(Duration::from_millis(15));
            continue;
        }
        if let Some(mut existing) = guard.parked.remove(session_id) {
            let dead = existing.child.try_wait().ok().flatten().is_some();
            if !dead && existing.generation == generation {
                guard.busy.insert(session_id.to_string());
                return Ok(existing);
            }
            retire(&mut existing);
            if resumed {
                guard.busy.remove(session_id);
                return Err(KernelError::ContinuationLost);
            }
        } else if resumed {
            return Err(KernelError::ContinuationLost);
        }
        if keep_alive {
            evict_idle(&mut guard, max_slots.saturating_sub(1));
        }
        guard.busy.insert(session_id.to_string());
        drop(guard);
        return spawn();
    }
}

fn put_session(table: &Mutex<SessionTable>, session_id: &str, parked: Parked) {
    let mut guard = table.lock().expect("cli table poisoned");
    guard.busy.remove(session_id);
    guard.parked.insert(session_id.to_string(), parked);
}

fn release_busy(table: &Mutex<SessionTable>, session_id: &str) {
    table
        .lock()
        .expect("cli table poisoned")
        .busy
        .remove(session_id);
}

fn evict_idle(table: &mut SessionTable, keep: usize) {
    while table.parked.len() > keep {
        let oldest = table
            .parked
            .iter()
            .min_by_key(|(_, parked)| parked.last_used)
            .map(|(key, _)| key.clone());
        let Some(key) = oldest else {
            break;
        };
        if let Some(mut old) = table.parked.remove(&key) {
            retire(&mut old);
        }
    }
}

fn retire(parked: &mut Parked) {
    match parked.child.try_wait() {
        Ok(Some(_)) => {}
        _ => {
            let _ = parked.child.kill();
            let _ = parked.child.wait();
        }
    }
}

fn write_turn(
    parked: &mut Parked,
    request: &MessageRequest,
    session_id: &str,
) -> Result<(), KernelError> {
    let frame = latest_user(request, session_id);
    writeln!(parked.stdin, "{}", serde_json::to_string(&frame).unwrap())
        .map_err(|err| KernelError::Provider(err.to_string()))?;
    parked
        .stdin
        .flush()
        .map_err(|err| KernelError::Provider(err.to_string()))
}

fn write_text(parked: &mut Parked, text: &str, session_id: &str) -> Result<(), KernelError> {
    let frame = json!({
        "type": "user",
        "session_id": session_id,
        "message": { "role": "user", "content": [{ "type": "text", "text": text }] }
    });
    writeln!(parked.stdin, "{}", serde_json::to_string(&frame).unwrap())
        .map_err(|err| KernelError::Provider(err.to_string()))?;
    parked
        .stdin
        .flush()
        .map_err(|err| KernelError::Provider(err.to_string()))
}

fn cli_session_uuid(session_id: &str) -> String {
    if let Ok(value) = Uuid::parse_str(session_id) {
        return value.to_string();
    }
    let mut bytes = [0u8; 16];
    for (index, byte) in session_id.as_bytes().iter().enumerate() {
        bytes[index % 16] ^= byte.wrapping_mul(31).wrapping_add(index as u8);
    }
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes).to_string()
}

fn apply_proxy_env(cmd: &mut Command) -> Result<(), KernelError> {
    if let Ok(http) = env::var("KIN_HTTPS_PROXY") {
        cmd.env("HTTPS_PROXY", &http)
            .env("HTTP_PROXY", &http)
            .env("https_proxy", &http)
            .env("http_proxy", &http)
            .env("ALL_PROXY", &http)
            .env("NO_PROXY", "127.0.0.1,localhost")
            .env("no_proxy", "127.0.0.1,localhost");
        return Ok(());
    }
    if let Ok(socks5) = env::var("KIN_SOCKS5") {
        if socks5.starts_with("socks") {
            return Err(KernelError::Provider(
                "Claude CLI cannot use SOCKS5 as HTTPS_PROXY; set KIN_HTTPS_PROXY to an HTTP CONNECT bridge that egresses via that SOCKS5".into(),
            ));
        }
        cmd.env("HTTPS_PROXY", &socks5)
            .env("HTTP_PROXY", &socks5)
            .env("ALL_PROXY", &socks5)
            .env("NO_PROXY", "127.0.0.1,localhost");
    }
    Ok(())
}

fn spawn_parked(
    bin: &Path,
    mock: bool,
    isolation: IsolationMode,
    session_dir: &Path,
    session_id: &str,
    model: &str,
    generation: u64,
    auth: &cli_auth::ResolvedCliAuth,
) -> Result<Parked, KernelError> {
    let mut cmd = if mock {
        let mut cmd = Command::new("node");
        cmd.arg(bin);
        cmd
    } else {
        Command::new(bin)
    };
    apply_proxy_env(&mut cmd)?;
    let agents = r#"{"kin-slot":{"description":"persistent request slot","prompt":"You are a persistent request slot."}}"#;
    let mut args = vec![
        "-p",
        "--output-format",
        "stream-json",
        "--input-format",
        "stream-json",
        "--verbose",
        "--include-partial-messages",
        "--replay-user-messages",
        "--permission-mode",
        "acceptEdits",
        "--no-session-persistence",
        "--session-id",
        session_id,
        "--model",
        model,
    ];
    if isolation == IsolationMode::Multiplexed {
        args.extend(["--agents", agents]);
    }
    auth.apply_std(&mut cmd);
    let mut child = cmd
        .args(args)
        .current_dir(session_dir)
        .env("CLAUDE_CONFIG_DIR", session_dir)
        .env("CLAUDE_CODE_ENTRYPOINT", "cli")
        .env("CLAUDE_CODE_DISABLE_TELEMETRY", "1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|err| KernelError::Provider(format!("spawn claude: {err}")))?;
    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| KernelError::Provider("cli stdin missing".into()))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| KernelError::Provider("cli stdout missing".into()))?;
    let stderr_buf = Arc::new(Mutex::new(String::new()));
    if let Some(stderr) = child.stderr.take() {
        let buf = Arc::clone(&stderr_buf);
        std::thread::spawn(move || {
            let mut reader = BufReader::new(stderr);
            let mut line = String::new();
            while reader
                .read_line(&mut line)
                .ok()
                .filter(|n| *n > 0)
                .is_some()
            {
                if let Ok(mut slot) = buf.lock()
                    && slot.len() < 8_192 {
                        slot.push_str(&line);
                    }
                line.clear();
            }
        });
    }
    let _ = stderr_buf;
    Ok(Parked {
        pid: child.id(),
        child,
        stdin,
        stdout: BufReader::new(stdout),
        generation,
        last_used: Instant::now(),
    })
}

fn read_until_boundary(
    parked: &mut Parked,
    model: &str,
    events: Option<&StreamTx>,
) -> Result<(Vec<ContentBlock>, StopReason, Usage), KernelError> {
    let mut assembler = StreamAssembler::new(model);
    loop {
        let Some(frame) = read_line(&mut parked.stdout)? else {
            break;
        };
        match frame.get("type").and_then(Value::as_str) {
            Some("stream_event") => {
                if let Some(event) = frame.get("event") {
                    if let Some(tx) = events {
                        let _ = tx.try_send(Ok(StreamItem::Event(event.clone())));
                    }
                    assembler.apply_event(event);
                    if assembler.has_tool_use()
                        && event.get("type").and_then(Value::as_str) == Some("message_stop")
                    {
                        return Ok(assembler.parts());
                    }
                }
            }
            Some("assistant") => {
                assembler.apply_assistant(&frame);
                if assembler.has_tool_use() {
                    return Ok(assembler.parts());
                }
            }
            Some("result") => {
                assembler.apply_result(&frame);
                break;
            }
            _ => {}
        }
    }
    Ok(assembler.parts())
}

fn read_line(stdout: &mut BufReader<ChildStdout>) -> Result<Option<Value>, KernelError> {
    loop {
        let mut line = String::new();
        let n = stdout
            .read_line(&mut line)
            .map_err(|err| KernelError::Provider(err.to_string()))?;
        if n == 0 {
            return Ok(None);
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        return Ok(Some(serde_json::from_str(trimmed).map_err(|err| {
            KernelError::Provider(format!("bad stream-json: {err}"))
        })?));
    }
}

fn latest_user(request: &MessageRequest, session_id: &str) -> Value {
    let message = request
        .messages
        .iter()
        .rev()
        .find(|item| item.role == "user")
        .cloned()
        .unwrap_or_else(|| Message {
            role: "user".into(),
            content: MessageContent::Text(String::new()),
            tool_call_id: None,
            tool_calls: Vec::new(),
        });
    let content = match &message.content {
        MessageContent::Text(text) => json!([{ "type": "text", "text": text }]),
        MessageContent::Blocks(blocks) => {
            serde_json::to_value(blocks).unwrap_or_else(|_| json!([]))
        }
    };
    json!({
        "type": "user",
        "session_id": session_id,
        "message": { "role": "user", "content": content }
    })
}

fn response(
    request: &MessageRequest,
    content: Vec<ContentBlock>,
    stop: StopReason,
    usage: Usage,
) -> MessageResponse {
    MessageResponse {
        id: format!("msg_{}", Uuid::new_v4().simple()),
        r#type: "message",
        role: "assistant",
        model: request.model.clone(),
        content,
        stop_reason: stop,
        usage,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::ToolDefinition;

    fn ctx(session: &str, resumed: bool, generation: u64) -> ExecutionContext {
        ExecutionContext {
            tenant_id: "demo".into(),
            session_id: session.into(),
            worker_id: "w0".into(),
            worker_generation: generation,
            resumed,
        }
    }

    fn text_request(text: &str) -> MessageRequest {
        MessageRequest {
            model: "claude-opus-4-1".into(),
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

    fn provider(mode: IsolationMode) -> LocalCliProvider {
        LocalCliProvider::from_env(mode).expect("mock bin")
    }

    fn run(
        provider: &LocalCliProvider,
        request: &MessageRequest,
        context: &ExecutionContext,
    ) -> Result<MessageResponse, KernelError> {
        run_turn(
            &provider.bin,
            true,
            provider.isolation,
            provider.max_slots,
            provider.table.as_ref(),
            request,
            context,
            None,
        )
    }

    #[test]
    fn parks_and_binds_same_pid() {
        let provider = provider(IsolationMode::ProcessPerTurn);
        let session = format!("sess-{}", Uuid::new_v4().simple());
        let first = run(
            &provider,
            &text_request("please [use_tool:echo] now"),
            &ctx(&session, false, 1),
        )
        .expect("first turn");
        assert!(matches!(first.stop_reason, StopReason::ToolUse));
        let pid = provider.session_pid(&session).expect("parked pid");
        let tool_id = first
            .content
            .iter()
            .find_map(|block| match block {
                ContentBlock::ToolUse { id, .. } => Some(id.clone()),
                _ => None,
            })
            .expect("tool id");
        let continued = MessageRequest {
            model: "claude-opus-4-1".into(),
            messages: vec![Message {
                role: "user".into(),
                content: MessageContent::Blocks(vec![ContentBlock::ToolResult {
                    tool_use_id: tool_id,
                    content: json!("ok"),
                    is_error: false,
                }]),
                tool_call_id: None,
                tool_calls: Vec::new(),
            }],
            max_tokens: 128,
            stream: false,
            ..MessageRequest::default()
        };
        let second = run(&provider, &continued, &ctx(&session, true, 1)).expect("bind");
        assert!(matches!(second.stop_reason, StopReason::EndTurn));
        assert!(provider.session_pid(&session).is_none());
        assert!(pid > 0);
    }

    #[test]
    fn multiplex_isolates_by_session_id() {
        let provider = provider(IsolationMode::Multiplexed);
        let first = run(
            &provider,
            &text_request("hello one"),
            &ctx("sess-a", false, 1),
        )
        .expect("first");
        let pid_a = provider.session_pid("sess-a").expect("a");
        let second = run(
            &provider,
            &text_request("hello two"),
            &ctx("sess-b", false, 1),
        )
        .expect("second");
        let pid_b = provider.session_pid("sess-b").expect("b");
        assert!(matches!(first.stop_reason, StopReason::EndTurn));
        assert!(matches!(second.stop_reason, StopReason::EndTurn));
        assert_ne!(pid_a, pid_b);
        assert!(first.id.contains(&pid_a.to_string()));
        assert!(second.id.contains(&pid_b.to_string()));
    }

    #[test]
    fn multiplex_same_session_reuses_pid() {
        let provider = provider(IsolationMode::Multiplexed);
        let session = "sess-sticky";
        let first = run(
            &provider,
            &text_request("hello one"),
            &ctx(session, false, 1),
        )
        .expect("first");
        let pid = provider.session_pid(session).expect("pid");
        let second = run(
            &provider,
            &text_request("hello two"),
            &ctx(session, false, 1),
        )
        .expect("second");
        assert_eq!(provider.session_pid(session).expect("still"), pid);
        assert!(first.id.contains(&pid.to_string()));
        assert!(second.id.contains(&pid.to_string()));
    }

    #[test]
    fn generation_mismatch_is_lost() {
        let provider = provider(IsolationMode::ProcessPerTurn);
        let session = format!("sess-{}", Uuid::new_v4().simple());
        let _ = run(
            &provider,
            &text_request("please [use_tool:echo] now"),
            &ctx(&session, false, 1),
        )
        .unwrap();
        let err = run(
            &provider,
            &text_request("tool done"),
            &ctx(&session, true, 2),
        )
        .unwrap_err();
        assert!(matches!(err, KernelError::ContinuationLost));
    }
}
