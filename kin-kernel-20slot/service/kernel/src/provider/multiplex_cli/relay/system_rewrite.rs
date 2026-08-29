//! Rewrite CLI outbound `system` to the 0-inject default header.
//!
//! Default blocks:
//!   0. billing (`prompt_version=<You are a Claude agent, built on Anthropic's Claude Agent SDK.>`)
//!   1. `# Environment` + slot timezone
//!   2. caller `--system` leftover, only if the inbound job carried one
//!
//! Identity is never emitted as its own block unless the caller sent it.

use sha2::{Digest, Sha256};
use uuid::Uuid;

const SALT: &str = "59cf53e54c78";
const CLI_VER: &str = "2.1.241";
const IDENTITY: &str = "You are a Claude agent, built on Anthropic's Claude Agent SDK.";

pub fn rewrite_messages_body(raw: &[u8]) -> Vec<u8> {
    let Ok(mut body) = serde_json::from_slice::<serde_json::Value>(raw) else {
        return raw.to_vec();
    };
    let Some(obj) = body.as_object_mut() else {
        return raw.to_vec();
    };
    if !is_kin_slot_request(obj) {
        return raw.to_vec();
    }
    let leftover = leftover_from_job(obj);
    let first_user = job_first_user(obj).unwrap_or_else(|| first_user_text(obj));
    let session_id = session_id_of(obj);
    let tz = std::env::var("TZ").unwrap_or_else(|_| "America/New_York".into());
    obj.insert(
        "system".into(),
        build_zero_system(&first_user, &tz, &session_id, leftover.as_deref()),
    );
    serde_json::to_vec(&body).unwrap_or_else(|_| raw.to_vec())
}

pub fn build_zero_system(
    first_user: &str,
    timezone: &str,
    session_id: &str,
    leftover: Option<&str>,
) -> serde_json::Value {
    let mut blocks = vec![
        serde_json::json!({
            "type": "text",
            "text": billing_line(first_user, session_id),
        }),
        serde_json::json!({
            "type": "text",
            "text": format!("# Environment\n - Timezone: {timezone}"),
        }),
    ];
    if let Some(text) = leftover.map(str::trim).filter(|s| !s.is_empty()) {
        blocks.push(serde_json::json!({ "type": "text", "text": text }));
    }
    serde_json::Value::Array(blocks)
}

pub fn billing_line(first_user: &str, session_id: &str) -> String {
    let fp = compute_fp(first_user, CLI_VER);
    let cch = compute_cch(first_user, CLI_VER);
    let prompt_id = prompt_id(session_id, &fp);
    format!(
        "x-anthropic-billing-header: cc_version={CLI_VER}.{fp}; cc_entrypoint=sdk-cli; cch={cch}; cc_prompt_id={prompt_id}; prompt_version=<{IDENTITY}>"
    )
}

fn is_kin_slot_request(obj: &serde_json::Map<String, serde_json::Value>) -> bool {
    obj.get("tools")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|tool| tool.get("name").and_then(serde_json::Value::as_str))
        .any(|name| name.starts_with("mcp__kin_runtime__"))
}

fn leftover_from_job(obj: &serde_json::Map<String, serde_json::Value>) -> Option<String> {
    let job = latest_job_payload(obj)?;
    let system = job.get("system").or_else(|| {
        job.get("request")
            .and_then(serde_json::Value::as_object)
            .and_then(|req| req.get("system"))
    })?;
    leftover_text(system)
}

fn leftover_text(system: &serde_json::Value) -> Option<String> {
    if system.is_null() {
        return None;
    }
    if let Some(text) = system.as_str() {
        return sanitize_leftover(text);
    }
    let mut parts = Vec::new();
    for block in system.as_array()? {
        let text = if let Some(s) = block.as_str() {
            s
        } else if let Some(t) = block.get("text").and_then(serde_json::Value::as_str) {
            t
        } else {
            continue;
        };
        if let Some(kept) = sanitize_leftover(text) {
            parts.push(kept);
        }
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("\n\n"))
    }
}

fn sanitize_leftover(text: &str) -> Option<String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    if trimmed.starts_with("x-anthropic-billing-header:") {
        return None;
    }
    if trimmed.starts_with("# Environment") {
        return None;
    }
    if trimmed == IDENTITY {
        return Some(trimmed.to_string());
    }
    if trimmed.contains("mcp__kin_runtime__") || trimmed.contains("persistent Kin") {
        return None;
    }
    Some(trimmed.to_string())
}

fn latest_job_payload(obj: &serde_json::Map<String, serde_json::Value>) -> Option<serde_json::Value> {
    let messages = obj.get("messages")?.as_array()?;
    for message in messages.iter().rev() {
        if message.get("role").and_then(serde_json::Value::as_str) != Some("user") {
            continue;
        }
        let content = message.get("content")?;
        let blocks = match content {
            serde_json::Value::Array(blocks) => blocks.clone(),
            serde_json::Value::String(s) => {
                if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(s) {
                    if parsed.get("type").and_then(serde_json::Value::as_str) == Some("job") {
                        return Some(parsed);
                    }
                }
                continue;
            }
            _ => continue,
        };
        for block in blocks.iter().rev() {
            if block.get("type").and_then(serde_json::Value::as_str) != Some("tool_result") {
                continue;
            }
            let raw = match block.get("content") {
                Some(serde_json::Value::String(s)) => s.clone(),
                Some(other) => other.to_string(),
                None => continue,
            };
            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&raw)
                && parsed.get("type").and_then(serde_json::Value::as_str) == Some("job")
            {
                return Some(parsed);
            }
        }
    }
    None
}

fn job_first_user(obj: &serde_json::Map<String, serde_json::Value>) -> Option<String> {
    let job = latest_job_payload(obj)?;
    let messages = job
        .get("messages")
        .or_else(|| job.pointer("/request/messages"))?;
    first_user_from_messages(messages)
}

fn first_user_text(obj: &serde_json::Map<String, serde_json::Value>) -> String {
    obj.get("messages")
        .and_then(first_user_from_messages)
        .unwrap_or_default()
}

fn first_user_from_messages(messages: &serde_json::Value) -> Option<String> {
    let messages = messages.as_array()?;
    for message in messages {
        if message.get("role").and_then(serde_json::Value::as_str) != Some("user") {
            continue;
        }
        return Some(content_text(message.get("content")?));
    }
    None
}

fn content_text(content: &serde_json::Value) -> String {
    if let Some(s) = content.as_str() {
        return s.to_string();
    }
    let Some(blocks) = content.as_array() else {
        return String::new();
    };
    for block in blocks {
        if block.get("type").and_then(serde_json::Value::as_str) == Some("text")
            && let Some(text) = block.get("text").and_then(serde_json::Value::as_str)
        {
            return text.to_string();
        }
    }
    String::new()
}

fn session_id_of(obj: &serde_json::Map<String, serde_json::Value>) -> String {
    if let Some(job) = latest_job_payload(obj)
        && let Some(id) = job.get("session_id").and_then(serde_json::Value::as_str)
        && !id.is_empty()
    {
        return id.to_string();
    }
    if let Some(raw) = obj
        .get("metadata")
        .and_then(|m| m.get("user_id"))
        .and_then(serde_json::Value::as_str)
        && let Ok(parsed) = serde_json::from_str::<serde_json::Value>(raw)
        && let Some(id) = parsed.get("session_id").and_then(serde_json::Value::as_str)
        && !id.is_empty()
    {
        return id.to_string();
    }
    Uuid::new_v4().to_string()
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
    use serde_json::json;

    fn slot_tools() -> serde_json::Value {
        json!([
            {"name": "WebSearch"},
            {"name": "mcp__kin_runtime__slot_wait"},
            {"name": "mcp__kin_runtime__kin_done"}
        ])
    }

    fn job_result(system: serde_json::Value, user: &str, session: &str) -> String {
        json!({
            "type": "job",
            "session_id": session,
            "system": system,
            "messages": [{"role": "user", "content": user}],
            "request": {
                "system": system,
                "messages": [{"role": "user", "content": user}]
            }
        })
        .to_string()
    }

    #[test]
    fn default_header_is_billing_plus_env_without_identity_block() {
        let raw = serde_json::to_vec(&json!({
            "model": "claude-sonnet-5",
            "tools": slot_tools(),
            "system": [
                {"type": "text", "text": "x-anthropic-billing-header: cc_version=2.8.4; cc_entrypoint=cli;"},
                {"type": "text", "text": IDENTITY},
                {"type": "text", "text": "You are a persistent Kin request slot. mcp__kin_runtime__kin_done"}
            ],
            "messages": [{"role": "user", "content": "hello-slot"}]
        }))
        .unwrap();
        let out: serde_json::Value = serde_json::from_slice(&rewrite_messages_body(&raw)).unwrap();
        let blocks = out["system"].as_array().unwrap();
        assert_eq!(blocks.len(), 2);
        let billing = blocks[0]["text"].as_str().unwrap();
        assert!(billing.starts_with("x-anthropic-billing-header: cc_version=2.1.241."));
        assert!(billing.contains("cc_entrypoint=sdk-cli"));
        assert!(billing.contains(&format!("prompt_version=<{IDENTITY}>")));
        assert!(!blocks.iter().any(|b| b["text"].as_str() == Some(IDENTITY)));
        assert_eq!(
            blocks[1]["text"].as_str().unwrap(),
            "# Environment\n - Timezone: America/New_York"
        );
    }

    #[test]
    fn leftover_caller_system_is_appended() {
        let session = "41199de5-cee9-4c06-9352-9aa71290a6e0";
        let raw = serde_json::to_vec(&json!({
            "model": "claude-sonnet-5",
            "tools": slot_tools(),
            "system": [
                {"type": "text", "text": IDENTITY}
            ],
            "messages": [
                {"role": "user", "content": "bootstrap"},
                {
                    "role": "user",
                    "content": [{
                        "type": "tool_result",
                        "tool_use_id": "toolu_1",
                        "content": job_result(json!("你是一个高速收费员。"), "你好呀。", session)
                    }]
                }
            ]
        }))
        .unwrap();
        let out: serde_json::Value = serde_json::from_slice(&rewrite_messages_body(&raw)).unwrap();
        let blocks = out["system"].as_array().unwrap();
        assert_eq!(blocks.len(), 3);
        assert_eq!(blocks[2]["text"], "你是一个高速收费员。");
        let billing = blocks[0]["text"].as_str().unwrap();
        assert!(billing.contains(&format!("cc_prompt_id={session}")));
        assert!(billing.contains("prompt_version=<"));
    }

    #[test]
    fn identity_only_appended_when_caller_sends_it() {
        let raw = serde_json::to_vec(&json!({
            "tools": slot_tools(),
            "messages": [{
                "role": "user",
                "content": [{
                    "type": "tool_result",
                    "content": job_result(json!(IDENTITY), "who are you", "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee")
                }]
            }]
        }))
        .unwrap();
        let out: serde_json::Value = serde_json::from_slice(&rewrite_messages_body(&raw)).unwrap();
        let blocks = out["system"].as_array().unwrap();
        assert_eq!(blocks.len(), 3);
        assert_eq!(blocks[2]["text"], IDENTITY);
    }

    #[test]
    fn supervisor_without_mcp_tools_is_untouched() {
        let raw = serde_json::to_vec(&json!({
            "system": [{"type": "text", "text": IDENTITY}],
            "tools": [{"name": "Agent"}],
            "messages": [{"role": "user", "content": "spawn slots"}]
        }))
        .unwrap();
        assert_eq!(rewrite_messages_body(&raw), raw);
    }

    #[test]
    fn fingerprint_matches_zero_inject_helper() {
        assert_eq!(compute_fp("hello", "2.1.241").len(), 3);
        assert_eq!(compute_cch("hello", "2.1.241").len(), 5);
        let line = billing_line("hello", "41199de5-cee9-4c06-9352-9aa71290a6e0");
        assert!(line.contains("cc_prompt_id=41199de5-cee9-4c06-9352-9aa71290a6e0"));
        assert!(line.contains("cc_entrypoint=sdk-cli"));
    }
}
