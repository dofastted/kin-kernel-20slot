//! Console-managed Claude Code outbound envelope.
//!
//! Mode `zero` (default): official identity lives inside billing `prompt_version`.
//! Mode `identity`: official identity is its own system block.
//! Environment is timezone-only and must match the SOCKS egress TZ.

use std::{env, fs, path::PathBuf};

use serde::{Deserialize, Serialize};

pub const IDENTITY: &str = "You are a Claude agent, built on Anthropic's Claude Agent SDK.";
const DEFAULT_TZ: &str = "America/New_York";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SystemMode {
    #[default]
    Zero,
    Identity,
}

impl SystemMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Zero => "zero",
            Self::Identity => "identity",
        }
    }

    pub fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "identity" | "id" | "block" => Self::Identity,
            _ => Self::Zero,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvelopeConfig {
    pub mode: SystemMode,
    pub timezone: String,
}

impl Default for EnvelopeConfig {
    fn default() -> Self {
        Self {
            mode: SystemMode::Zero,
            timezone: default_timezone(),
        }
    }
}

pub fn config_path() -> PathBuf {
    env::var("KIN_ENVELOPE_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/tmp/kin-cli/envelope.json"))
}

pub fn default_timezone() -> String {
    env::var("KIN_SLOT_TZ")
        .or_else(|_| env::var("TZ"))
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_TZ.to_string())
}

pub fn load() -> EnvelopeConfig {
    let path = config_path();
    if let Ok(raw) = fs::read_to_string(&path)
        && let Ok(mut cfg) = serde_json::from_str::<EnvelopeConfig>(&raw)
    {
        if cfg.timezone.trim().is_empty() {
            cfg.timezone = default_timezone();
        }
        return cfg;
    }
    let mut cfg = EnvelopeConfig::default();
    if let Ok(mode) = env::var("KIN_SYSTEM_MODE") {
        cfg.mode = SystemMode::parse(&mode);
    }
    cfg
}

pub fn save(cfg: &EnvelopeConfig) -> Result<EnvelopeConfig, String> {
    let path = config_path();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| err.to_string())?;
    }
    let body = serde_json::to_vec_pretty(cfg).map_err(|err| err.to_string())?;
    fs::write(&path, body).map_err(|err| err.to_string())?;
    Ok(cfg.clone())
}
