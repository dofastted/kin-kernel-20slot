//! Console-managed Claude Code outbound envelope.
//!
//! Mode `zero` (default): official identity lives inside billing `prompt_version`.
//! Mode `identity`: official identity is its own system block.
//! Environment is timezone-only and must match the SOCKS egress TZ.

use std::{env, fs, path::PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

const SALT: &str = "59cf53e54c78";
const CLI_VER: &str = "2.1.241";
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

pub fn build_system(
    cfg: &EnvelopeConfig,
    first_user: &str,
    session_id: &str,
    leftover: Option<&str>,
) -> serde_json::Value {
    let billing = billing_line(cfg.mode, first_user, session_id);
    let env_block = format!("# Environment\nTime zone: {}", cfg.timezone.trim());
    let mut blocks = vec![text_block(&billing)];
    if cfg.mode == SystemMode::Identity {
        blocks.push(text_block(IDENTITY));
    }
    blocks.push(text_block(&env_block));
    if let Some(text) = leftover.map(str::trim).filter(|s| !s.is_empty()) {
        blocks.push(text_block(text));
    }
    serde_json::Value::Array(blocks)
}

pub fn billing_line(mode: SystemMode, first_user: &str, session_id: &str) -> String {
    let fp = compute_fp(first_user, CLI_VER);
    let cch = compute_cch(first_user, CLI_VER);
    let prompt_id = prompt_id(session_id, &fp);
    match mode {
        SystemMode::Zero => format!(
            "x-anthropic-billing-header: cc_version={CLI_VER}.{fp}; cc_entrypoint=sdk-cli; cch={cch}; cc_prompt_id={prompt_id}; prompt_version={IDENTITY}"
        ),
        SystemMode::Identity => format!(
            "x-anthropic-billing-header: cc_version={CLI_VER}.{fp}; cc_entrypoint=sdk-cli; cch={cch}; cc_prompt_id={prompt_id}"
        ),
    }
}

fn text_block(text: &str) -> serde_json::Value {
    serde_json::json!({ "type": "text", "text": text })
}

fn prompt_id(session_id: &str, fp: &str) -> String {
    let raw = session_id.trim();
    if Uuid::parse_str(raw).is_ok() {
        return raw.to_string();
    }
    uuid_from_seed(if raw.is_empty() {
        format!("prompt:{CLI_VER}:{fp}")
    } else {
        raw.to_string()
    })
}

fn uuid_from_seed(seed: String) -> String {
    let sum = Sha256::digest(seed.as_bytes());
    let hx = hex(sum.as_slice());
    let variant = (u8::from_str_radix(&hx[16..18], 16).unwrap_or(0) & 0x3f) | 0x80;
    format!(
        "{}-{}-4{}-{:02x}{}-{}",
        &hx[0..8],
        &hx[8..12],
        &hx[13..16],
        variant,
        &hx[18..20],
        &hx[20..32]
    )
}

fn compute_fp(first_user: &str, ver: &str) -> String {
    let buf = first_user.as_bytes();
    let mut chars = [b'0'; 3];
    for (slot, idx) in [4usize, 7, 20].into_iter().enumerate() {
        if idx < buf.len() {
            chars[slot] = buf[idx];
        }
    }
    let mut hasher = Sha256::new();
    hasher.update(SALT.as_bytes());
    hasher.update(chars);
    hasher.update(ver.as_bytes());
    hex(&hasher.finalize())[..3].to_string()
}

fn compute_cch(first_user: &str, ver: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(format!("{SALT}:cch:{first_user}:{ver}").as_bytes());
    hex(&hasher.finalize())[..5].to_string()
}

fn hex(data: &[u8]) -> String {
    const H: &[u8] = b"0123456789abcdef";
    let mut out = String::with_capacity(data.len() * 2);
    for byte in data {
        out.push(H[(byte >> 4) as usize] as char);
        out.push(H[(byte & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_puts_identity_in_billing_not_as_block() {
        let cfg = EnvelopeConfig {
            mode: SystemMode::Zero,
            timezone: "America/New_York".into(),
        };
        let system = build_system(&cfg, "hello-user", "11111111-1111-4111-8111-111111111111", None);
        let blocks = system.as_array().unwrap();
        assert_eq!(blocks.len(), 2);
        let billing = blocks[0]["text"].as_str().unwrap();
        assert!(billing.contains("prompt_version=You are a Claude agent"));
        assert_eq!(
            blocks[1]["text"].as_str().unwrap(),
            "# Environment\nTime zone: America/New_York"
        );
    }

    #[test]
    fn identity_mode_adds_official_sentence_block() {
        let cfg = EnvelopeConfig {
            mode: SystemMode::Identity,
            timezone: "Asia/Shanghai".into(),
        };
        let system = build_system(&cfg, "hello-user", "s", Some("你是一个高速收费员。"));
        let texts: Vec<&str> = system
            .as_array()
            .unwrap()
            .iter()
            .map(|b| b["text"].as_str().unwrap())
            .collect();
        assert!(!texts[0].contains("prompt_version="));
        assert_eq!(texts[1], IDENTITY);
        assert!(texts[2].contains("Asia/Shanghai"));
        assert_eq!(texts[3], "你是一个高速收费员。");
    }
}
