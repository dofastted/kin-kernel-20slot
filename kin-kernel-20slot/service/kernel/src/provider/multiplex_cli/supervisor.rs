use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Stdio,
};

use serde_json::json;
use tokio::process::{Child, Command};

use crate::error::KernelError;
use crate::provider::cli_auth;

pub struct Supervised {
    pub child: Child,
    pub pid: u32,
}

pub struct SpawnSpec {
    pub bin: PathBuf,
    pub mock: bool,
    pub model: String,
    pub slot_count: usize,
    pub mcp_url: String,
    pub session_dir: PathBuf,
    pub native_slots: Option<usize>,
    pub desired_config_hash: Option<String>,
}

pub fn write_mcp_config(dir: &Path, url: &str) -> Result<PathBuf, KernelError> {
    let path = dir.join("mcp.json");
    let body = json!({
        "mcpServers": {
            "kin_runtime": {
                "type": "http",
                "url": url
            }
        }
    });
    fs::write(&path, serde_json::to_vec_pretty(&body).unwrap())
        .map_err(|err| KernelError::Provider(err.to_string()))?;
    let _ = fs::write(
        dir.join(".mcp.json"),
        serde_json::to_vec_pretty(&body).unwrap(),
    );
    Ok(path)
}

pub fn native_cli_args(model: &str) -> Vec<String> {
    [
        "-p",
        "--output-format",
        "stream-json",
        "--verbose",
        "--include-partial-messages",
        "--no-session-persistence",
        "--permission-mode",
        "acceptEdits",
        "--strict-mcp-config",
        "--model",
        model,
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

pub fn kin_slot_agents() -> String {
    json!({
        "kin-slot": {
            "description": "Persistent Kin request execution slot",
            "prompt": "You are a persistent Kin request slot. Repeatedly call mcp__kin_runtime__slot_wait. The job payload is a full Anthropic Messages request: system, thinking, tool_choice, tools, messages (text/image/document/tool_result), betas, sampling. Honor system and thinking. If the job includes web_search / type web_search_20250305, you MUST use WebSearch to look up live information before answering. If job.tools lists other client tools, call mcp__kin_runtime__client_tool with job_id, name, input — never invent tool output. After client_tool returns, continue the same job. The runtime streams your assistant/user frames to the HTTP client. For every completed job: (1) emit the complete client-visible answer as ordinary assistant text; (2) in the same assistant response, after the text block, call mcp__kin_runtime__kin_done; (3) kin_done is mandatory and must contain only job_id, stop_reason, usage, final_digest; (4) do not repeat the answer in kin_done.text or fallback_content — those are legacy emergency fallback fields only; (5) never finish a job with assistant text alone; always call kin_done so the persistent slot loop continues. Use kin_fail on errors. Never mix jobs.",
            "tools": [
                "WebSearch",
                "mcp__kin_runtime__slot_wait",
                "mcp__kin_runtime__client_tool",
                "mcp__kin_runtime__kin_done",
                "mcp__kin_runtime__kin_fail"
            ],
            "model": "inherit",
            "background": true
        }
    })
    .to_string()
}

pub fn bootstrap_prompt(n: usize) -> String {
    format!(
        "You are the Kin runtime supervisor. Spawn exactly {n} background kin-slot agents with the Agent tool. Do not answer user questions. Keep {n} slots alive: when a kin-slot returns, spawn a replacement. Never call tools other than Agent."
    )
}

pub async fn spawn(spec: &SpawnSpec) -> Result<Supervised, KernelError> {
    let auth = cli_auth::resolve()?;
    cli_auth::write_credentials(&spec.session_dir, &auth)?;
    let n = spec.slot_count.to_string();
    let mut cmd = if spec.mock {
        let mut cmd = Command::new("node");
        cmd.arg(&spec.bin);
        cmd
    } else {
        Command::new(&spec.bin)
    };
    apply_proxy_env(&mut cmd)?;
    if spec.native_slots.is_some() {
        cmd.args(native_cli_args(&spec.model));
        let layout = crate::provider::multiplex_cli::envelope::load();
        cmd.env("CLAUDE_CODE_KIN_NATIVE_SLOTS", &n)
            .env("CLAUDE_CODE_SYSTEM_LAYOUT", layout.mode.as_str())
            .env("CLAUDE_CODE_TIMEZONE", &layout.timezone);
        if let Some(hash) = &spec.desired_config_hash {
            cmd.env("CLAUDE_CODE_KIN_CONFIG_HASH", hash);
        }
    } else {
        let mcp_path = write_mcp_config(&spec.session_dir, &spec.mcp_url)?;
        let mcp_path_str = mcp_path.to_string_lossy().into_owned();
        let agents = kin_slot_agents();
        cmd.args([
            "-p",
            "--input-format",
            "stream-json",
            "--output-format",
            "stream-json",
            "--verbose",
            "--include-partial-messages",
            "--forward-subagent-text",
            "--replay-user-messages",
            "--no-session-persistence",
            "--permission-mode",
            "acceptEdits",
            "--agents",
            &agents,
            "--mcp-config",
            &mcp_path_str,
            "--strict-mcp-config",
            "--allowedTools",
            "Agent,WebSearch,mcp__kin_runtime__slot_wait,mcp__kin_runtime__client_tool,mcp__kin_runtime__kin_done,mcp__kin_runtime__kin_fail",
            "--model",
            &spec.model,
        ])
        .env("CLAUDE_CODE_MAX_CONCURRENT_SUBAGENTS", &n)
        .env("CLAUDE_CODE_MAX_TOOL_USE_CONCURRENCY", &n)
        .env("CLAUDE_CODE_MAX_SUBAGENT_SPAWN_DEPTH", "1")
        .env("CLAUDE_CODE_FORWARD_SUBAGENT_TEXT", "1");
        if env::var("KIN_FORWARD_SUBAGENT_PARTIALS")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
        {
            cmd.arg("--forward-subagent-partials")
                .env("CLAUDE_CODE_FORWARD_SUBAGENT_PARTIALS", "1");
        }
    }
    cmd.current_dir(&spec.session_dir)
        .env("CLAUDE_CONFIG_DIR", &spec.session_dir)
        .env("CLAUDE_CODE_ENTRYPOINT", "cli")
        .env("CLAUDE_CODE_DISABLE_TELEMETRY", "1");
    apply_envelope_env(&mut cmd);
    auth.apply_tokio(&mut cmd);
    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if let Ok(debug) = env::var("KIN_CLAUDE_DEBUG_FILE") {
        cmd.arg("--debug-file").arg(debug);
    }
    let child = cmd
        .spawn()
        .map_err(|err| KernelError::Provider(format!("spawn claude: {err}")))?;
    let pid = child.id().unwrap_or(0);
    Ok(Supervised { child, pid })
}

fn apply_envelope_env(cmd: &mut Command) {
    let cfg = crate::provider::multiplex_cli::envelope::load();
    let path = crate::provider::multiplex_cli::envelope::config_path();
    cmd.env("KIN_SYSTEM_MODE", cfg.mode.as_str())
        .env("KIN_SLOT_TZ", &cfg.timezone)
        .env("TZ", &cfg.timezone)
        .env("KIN_ENVELOPE_PATH", path.as_os_str())
        .env("CLAUDE_CODE_KIN_ENVELOPE", cfg.mode.as_str());
}

fn apply_proxy_env(cmd: &mut Command) -> Result<(), KernelError> {
    match proxy_env_plan(
        env::var("KIN_HTTPS_PROXY").ok().as_deref(),
        env::var("KIN_SOCKS5").ok().as_deref(),
    )? {
        ProxyEnvPlan::Http(http) => {
            cmd.env("HTTPS_PROXY", &http)
                .env("HTTP_PROXY", &http)
                .env("https_proxy", &http)
                .env("http_proxy", &http)
                .env("ALL_PROXY", &http)
                .env("NO_PROXY", "127.0.0.1,localhost")
                .env("no_proxy", "127.0.0.1,localhost");
        }
        ProxyEnvPlan::None => {}
    }
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
enum ProxyEnvPlan {
    Http(String),
    None,
}

fn proxy_env_plan(
    https_proxy: Option<&str>,
    socks5: Option<&str>,
) -> Result<ProxyEnvPlan, KernelError> {
    if let Some(http) = https_proxy
        && !http.trim().is_empty()
    {
        return Ok(ProxyEnvPlan::Http(http.to_string()));
    }
    if let Some(socks5) = socks5
        && socks5.trim().starts_with("socks")
    {
        return Err(KernelError::Provider(
            "Claude CLI cannot use SOCKS5 as HTTPS_PROXY; set KIN_HTTPS_PROXY to an HTTP CONNECT bridge".into(),
        ));
    }
    Ok(ProxyEnvPlan::None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_argv_skips_mcp_and_keeps_print_mode() {
        let args = native_cli_args("claude-sonnet-5");
        assert!(args.contains(&"--strict-mcp-config".into()));
        assert!(!args.iter().any(|a| a == "--mcp-config"));
        assert!(args.contains(&"-p".into()));
        assert_eq!(args.last().map(String::as_str), Some("claude-sonnet-5"));
    }

    #[test]
    fn kin_slot_prompt_requires_assistant_text_then_kin_done() {
        let agents = kin_slot_agents();
        assert!(
            agents.contains("ordinary assistant text"),
            "slot must emit native text"
        );
        assert!(
            !agents.contains("do not also send the full answer as message text"),
            "old prompt forbade native text"
        );
        assert!(agents.contains("always call kin_done"));
    }

    #[test]
    fn socks5_without_http_bridge_is_rejected() {
        let err = proxy_env_plan(None, Some("socks5h://127.0.0.1:10808")).unwrap_err();
        assert!(err.to_string().contains("cannot use SOCKS5"));
    }

    #[test]
    fn https_proxy_still_wins_over_socks5() {
        assert_eq!(
            proxy_env_plan(
                Some("http://127.0.0.1:7890"),
                Some("socks5h://127.0.0.1:10808")
            )
            .unwrap(),
            ProxyEnvPlan::Http("http://127.0.0.1:7890".into())
        );
    }
}
