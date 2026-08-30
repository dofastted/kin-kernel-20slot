use std::{env, path::PathBuf, process::Stdio};

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
    pub session_dir: PathBuf,
    pub desired_config_hash: Option<String>,
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
    cmd.args(native_cli_args(&spec.model));
    let layout = crate::provider::multiplex_cli::envelope::load();
    cmd.env("CLAUDE_CODE_KIN_NATIVE_SLOTS", &n)
        .env("CLAUDE_CODE_SYSTEM_LAYOUT", layout.mode.as_str())
        .env("CLAUDE_CODE_TIMEZONE", &layout.timezone);
    if let Some(hash) = &spec.desired_config_hash {
        cmd.env("CLAUDE_CODE_KIN_CONFIG_HASH", hash);
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
