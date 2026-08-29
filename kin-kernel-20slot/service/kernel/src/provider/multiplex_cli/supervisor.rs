use std::{
    env, fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Stdio,
};

use serde_json::json;
use tokio::process::{Child, Command};

use crate::error::KernelError;

pub struct Supervised {
    pub child: Child,
    pub pid: u32,
    pub session_dir: PathBuf,
}

pub struct SpawnSpec {
    pub bin: PathBuf,
    pub mock: bool,
    pub model: String,
    pub slot_count: usize,
    pub mcp_url: String,
    pub session_dir: PathBuf,
    pub anthropic_base_url: Option<String>,
}

pub fn write_oauth_file(dir: &Path) -> Result<(), KernelError> {
    fs::create_dir_all(dir).map_err(|err| KernelError::Provider(err.to_string()))?;
    let raw = env::var("KIN_CLAUDE_AI_OAUTH_JSON").unwrap_or_else(|_| {
        json!({
            "accessToken": "sk-ant-oat01-demo-local-only",
            "refreshToken": "sk-ant-ort01-demo-local-only",
            "expiresAt": 1_893_456_000_000u64,
            "scopes": [
                "user:profile",
                "user:inference",
                "user:sessions:claude_code",
                "user:mcp_servers",
                "user:file_upload"
            ],
            "subscriptionType": "max",
            "rateLimitTier": "default"
        })
        .to_string()
    });
    let oauth: serde_json::Value =
        serde_json::from_str(&raw).map_err(|err| KernelError::Provider(err.to_string()))?;
    let path = dir.join(".credentials.json");
    fs::write(
        &path,
        serde_json::to_vec(&json!({ "claudeAiOauth": oauth })).unwrap(),
    )
    .map_err(|err| KernelError::Provider(err.to_string()))?;
    let mut perms = fs::metadata(&path)
        .map_err(|err| KernelError::Provider(err.to_string()))?
        .permissions();
    perms.set_mode(0o600);
    fs::set_permissions(&path, perms).map_err(|err| KernelError::Provider(err.to_string()))?;
    Ok(())
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

pub fn kin_slot_agents() -> String {
    json!({
        "kin-slot": {
            "description": "Persistent Kin request execution slot",
            "prompt": "You are a persistent Kin request slot. Repeatedly call mcp__kin_runtime__slot_wait. The job payload is a full Anthropic Messages request: system, thinking, tool_choice, tools, messages (text/image/document/tool_result), betas, sampling. Honor system and thinking. If the job includes web_search / type web_search_20250305, you MUST use WebSearch to look up live information before answering. If job.tools lists other client tools, call mcp__kin_runtime__client_tool with job_id, name, input — never invent tool output. After client_tool returns, continue the same job. The runtime streams your assistant/user frames to the HTTP client. Finish with kin_done {job_id, stop_reason, usage, final_digest, text} where text is your complete final answer — do not also send the full answer as message text. Use kin_fail on errors. Never mix jobs.",
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
    write_oauth_file(&spec.session_dir)?;
    let mcp_path = write_mcp_config(&spec.session_dir, &spec.mcp_url)?;
    let agents = kin_slot_agents();
    let n = spec.slot_count.to_string();
    let mut cmd = if spec.mock {
        let mut cmd = Command::new("node");
        cmd.arg(&spec.bin);
        cmd
    } else {
        Command::new(&spec.bin)
    };
    apply_proxy_env(&mut cmd, spec.anthropic_base_url.is_some())?;
    let mcp_path_str = mcp_path.to_string_lossy().into_owned();
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
    .current_dir(&spec.session_dir)
    .env("CLAUDE_CONFIG_DIR", &spec.session_dir)
    .env("CLAUDE_CODE_ENTRYPOINT", "cli")
    .env("CLAUDE_CODE_DISABLE_TELEMETRY", "1")
    .env("CLAUDE_CODE_MAX_CONCURRENT_SUBAGENTS", &n)
    .env("CLAUDE_CODE_MAX_TOOL_USE_CONCURRENCY", &n)
    .env("CLAUDE_CODE_MAX_SUBAGENT_SPAWN_DEPTH", "1")
    .env("CLAUDE_CODE_FORWARD_SUBAGENT_TEXT", "1")
    .env_remove("CLAUDE_CODE_OAUTH_TOKEN")
    .env_remove("ANTHROPIC_API_KEY")
    .stdin(Stdio::piped())
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .kill_on_drop(true);
    if let Some(url) = &spec.anthropic_base_url {
        cmd.env("ANTHROPIC_BASE_URL", url);
    }

    if let Ok(debug) = env::var("KIN_CLAUDE_DEBUG_FILE") {
        cmd.arg("--debug-file").arg(debug);
    }
    let child = cmd
        .spawn()
        .map_err(|err| KernelError::Provider(format!("spawn claude: {err}")))?;
    let pid = child.id().unwrap_or(0);
    Ok(Supervised {
        child,
        pid,
        session_dir: spec.session_dir.clone(),
    })
}

fn apply_proxy_env(cmd: &mut Command, relay_enabled: bool) -> Result<(), KernelError> {
    match proxy_env_plan(
        env::var("KIN_HTTPS_PROXY").ok().as_deref(),
        env::var("KIN_SOCKS5").ok().as_deref(),
        relay_enabled,
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
        ProxyEnvPlan::RelayLoopbackOnly => {
            cmd.env_remove("HTTPS_PROXY")
                .env_remove("HTTP_PROXY")
                .env_remove("https_proxy")
                .env_remove("http_proxy")
                .env_remove("ALL_PROXY")
                .env_remove("all_proxy")
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
    RelayLoopbackOnly,
    None,
}

fn proxy_env_plan(
    https_proxy: Option<&str>,
    socks5: Option<&str>,
    relay_enabled: bool,
) -> Result<ProxyEnvPlan, KernelError> {
    if let Some(http) = https_proxy
        && !http.trim().is_empty()
    {
        return Ok(ProxyEnvPlan::Http(http.to_string()));
    }
    if let Some(socks5) = socks5
        && socks5.trim().starts_with("socks")
    {
        if relay_enabled {
            return Ok(ProxyEnvPlan::RelayLoopbackOnly);
        }
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
    fn relay_mode_allows_socks5_without_cli_https_proxy() {
        assert_eq!(
            proxy_env_plan(None, Some("socks5h://127.0.0.1:10808"), true).unwrap(),
            ProxyEnvPlan::RelayLoopbackOnly
        );
    }

    #[test]
    fn relay_off_rejects_socks5_without_http_bridge() {
        let err = proxy_env_plan(None, Some("socks5h://127.0.0.1:10808"), false).unwrap_err();
        assert!(err.to_string().contains("cannot use SOCKS5"));
    }

    #[test]
    fn https_proxy_still_wins_over_socks5() {
        assert_eq!(
            proxy_env_plan(
                Some("http://127.0.0.1:7890"),
                Some("socks5h://127.0.0.1:10808"),
                true
            )
            .unwrap(),
            ProxyEnvPlan::Http("http://127.0.0.1:7890".into())
        );
    }
}
