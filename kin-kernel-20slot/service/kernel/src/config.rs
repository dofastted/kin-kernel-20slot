use std::{env, fmt, net::SocketAddr, str::FromStr, time::Duration};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RelayMode {
    Off,
    Observe,
    Authoritative,
}

impl RelayMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Observe => "observe",
            Self::Authoritative => "authoritative",
        }
    }

    pub fn from_env_value(value: Option<&str>) -> Result<Self, String> {
        match value {
            None => Ok(Self::Off),
            Some(value) => value.parse(),
        }
    }

    pub fn from_env() -> Result<Self, String> {
        match env::var("KIN_RELAY_MODE") {
            Ok(value) => Self::from_env_value(Some(&value)),
            Err(env::VarError::NotPresent) => Ok(Self::Off),
            Err(env::VarError::NotUnicode(_)) => Err("KIN_RELAY_MODE must be valid unicode".into()),
        }
    }
}

impl fmt::Display for RelayMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for RelayMode {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "off" => Ok(Self::Off),
            "observe" => Ok(Self::Observe),
            "authoritative" => Ok(Self::Authoritative),
            other => Err(format!(
                "KIN_RELAY_MODE must be off, observe, or authoritative (got {other})"
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IsolationMode {
    /// `--one-shot --one-shot-strategy process`: one Claude child per turn.
    ProcessPerTurn,
    /// `--one-shot-strategy session-reset`: reuse one child, /clear between turns.
    ResetAndReuse,
    /// `subagent-pool`: one Claude OS process, N MCP-blocked background slots.
    Multiplexed,
}

impl IsolationMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ProcessPerTurn => "process",
            Self::ResetAndReuse => "session-reset",
            Self::Multiplexed => "subagent-pool",
        }
    }
}

impl fmt::Display for IsolationMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for IsolationMode {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "process" | "one-shot" | "process-per-turn" => Ok(Self::ProcessPerTurn),
            "session-reset" | "reset" => Ok(Self::ResetAndReuse),
            "subagent-pool" | "multiplexed" | "pool" => Ok(Self::Multiplexed),
            other => Err(format!(
                "KIN_ISOLATION must be process, session-reset, or subagent-pool (got {other})"
            )),
        }
    }
}

pub const CLIENT_CHANNEL_SIZE: usize = 32;
pub const EVENT_CHANNEL_SIZE: usize = 128;
pub const DEFAULT_CLIENT_STALL_SECS: u64 = 30;
pub const PER_CONNECTION_BUFFER: usize = 512 * 1024;
pub const MAX_TOOL_RESULT_BYTES: usize = 4 * 1024 * 1024;
pub const DEFAULT_SLOTS: usize = 20;

#[derive(Clone, Debug)]
pub struct Config {
    pub listen_addr: SocketAddr,
    pub worker_count: usize,
    pub slots_per_worker: usize,
    pub isolation: IsolationMode,
    pub max_body_bytes: usize,
    pub max_tool_result_bytes: usize,
    pub max_session_bytes: usize,
    pub session_ttl: Duration,
    pub continuation_ttl: Duration,
    pub slot_max_jobs: u32,
    pub slot_max_lifetime: Duration,
    pub default_tenant: String,
    pub expose_slot_header: bool,
    pub provider: String,
    /// Go control-plane's computed `RuntimeProfile` hash (design.md §6).
    /// Read once at process startup only — config changes require a drain +
    /// restart cycle, never a runtime re-fetch. `None` means the Go
    /// integration isn't wired up yet; three-way validation is skipped.
    pub desired_config_hash: Option<String>,
}

impl Config {
    pub fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        let listen_addr = env::var("KIN_KERNEL_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:8080".to_string())
            .parse()?;
        let isolation = env::var("KIN_ISOLATION")
            .ok()
            .map(|value| value.parse())
            .transpose()?
            .unwrap_or(IsolationMode::Multiplexed);
        // One kernel runtime == one Claude OS process. Slot count is --max-procs.
        let worker_count = parse_env(
            "KIN_WORKER_COUNT",
            if isolation == IsolationMode::ProcessPerTurn {
                4
            } else {
                1
            },
        )?;
        let slots_per_worker = parse_env(
            "KIN_SLOTS_PER_WORKER",
            if isolation == IsolationMode::ProcessPerTurn {
                5
            } else {
                DEFAULT_SLOTS
            },
        )?;
        let max_body_bytes = parse_env("KIN_MAX_BODY_BYTES", 8 * 1024 * 1024)?;
        let max_tool_result_bytes = parse_env("KIN_MAX_TOOL_RESULT_BYTES", MAX_TOOL_RESULT_BYTES)?;
        let max_session_bytes = parse_env("KIN_MAX_SESSION_BYTES", 16 * 1024 * 1024)?;
        let session_ttl_seconds = parse_env("KIN_SESSION_TTL_SECONDS", 600)?;
        let continuation_ttl_seconds = parse_env("KIN_CONTINUATION_TTL_SECONDS", 600)?;
        let slot_max_jobs = parse_env("KIN_SLOT_MAX_JOBS", 50u32)?;
        let slot_max_lifetime = Duration::from_secs(parse_env("KIN_SLOT_MAX_LIFETIME_SECS", 1800)?);
        if worker_count == 0
            || slots_per_worker == 0
            || max_body_bytes == 0
            || max_session_bytes == 0
            || session_ttl_seconds == 0
            || continuation_ttl_seconds == 0
            || slot_max_jobs == 0
        {
            return Err("worker, slot, body, and TTL settings must be positive".into());
        }
        if slots_per_worker > 20 {
            return Err(
                "Claude official subagent cap is 20; KIN_SLOTS_PER_WORKER must be <= 20".into(),
            );
        }

        Ok(Self {
            listen_addr,
            worker_count,
            slots_per_worker,
            isolation,
            max_body_bytes,
            max_tool_result_bytes,
            max_session_bytes,
            session_ttl: Duration::from_secs(session_ttl_seconds),
            continuation_ttl: Duration::from_secs(continuation_ttl_seconds),
            slot_max_jobs,
            slot_max_lifetime,
            default_tenant: env::var("KIN_DEFAULT_TENANT").unwrap_or_else(|_| "demo".to_string()),
            expose_slot_header: parse_bool_env("KIN_EXPOSE_SLOT_HEADER", true)?,
            provider: env::var("KIN_PROVIDER").unwrap_or_else(|_| "mock".to_string()),
            desired_config_hash: env::var("KIN_DESIRED_CONFIG_HASH").ok(),
        })
    }
}

fn parse_env<T>(name: &str, default: T) -> Result<T, Box<dyn std::error::Error>>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + 'static,
{
    match env::var(name) {
        Ok(value) => Ok(value.parse()?),
        Err(_) => Ok(default),
    }
}

fn parse_bool_env(name: &str, default: bool) -> Result<bool, Box<dyn std::error::Error>> {
    match env::var(name) {
        Ok(value) => match value.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" => Ok(true),
            "0" | "false" | "no" => Ok(false),
            _ => Err(format!("{name} must be true or false").into()),
        },
        Err(_) => Ok(default),
    }
}

pub fn client_stall_timeout_from_env() -> Result<Duration, Box<dyn std::error::Error>> {
    let seconds = parse_env("KIN_CLIENT_STALL_SECS", DEFAULT_CLIENT_STALL_SECS)?;
    if seconds == 0 {
        return Err("KIN_CLIENT_STALL_SECS must be positive".into());
    }
    Ok(Duration::from_secs(seconds))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relay_mode_parses_valid_values_and_default() {
        assert_eq!(RelayMode::from_env_value(None).unwrap(), RelayMode::Off);
        assert_eq!(
            RelayMode::from_env_value(Some("off")).unwrap(),
            RelayMode::Off
        );
        assert_eq!(
            RelayMode::from_env_value(Some("observe")).unwrap(),
            RelayMode::Observe
        );
        assert_eq!(
            RelayMode::from_env_value(Some("authoritative")).unwrap(),
            RelayMode::Authoritative
        );
        assert_eq!(
            RelayMode::from_env_value(Some(" OBSERVE ")).unwrap(),
            RelayMode::Observe
        );
    }

    #[test]
    fn relay_mode_rejects_invalid_values() {
        let err = RelayMode::from_env_value(Some("bogus")).unwrap_err();
        assert!(err.contains("KIN_RELAY_MODE"));
    }
}
