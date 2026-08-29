//! Rewrite CLI outbound `system` when the loopback relay is on.
//!
//! Native path (relay off) does this inside the patched Claude Code CLI.
//! Keep this as a fallback so observe/authoritative still matches console config.

use super::super::envelope::{self, IDENTITY};

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
    let cfg = envelope::load();
    obj.insert(
        "system".into(),
        envelope::build_system(&cfg, &first_user, &session_id, leftover.as_deref()),
    );
    serde_json::to_vec(&body).unwrap_or_else(|_| raw.to_vec())
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

fn latest_job_payload(
    obj: &serde_json::Map<String, serde_json::Value>,
) -> Option<serde_json::Value> {
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
    uuid::Uuid::new_v4().to_string()
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

    fn slot_body(job: String) -> Vec<u8> {
        serde_json::to_vec(&json!({
            "model": "claude-sonnet-5",
            "stream": true,
            "tools": slot_tools(),
            "messages": [{
                "role": "user",
                "content": [{"type": "tool_result", "tool_use_id": "u1", "content": job}]
            }]
        }))
        .unwrap()
    }

    #[test]
    fn default_zero_injects_prompt_version() {
        let raw = slot_body(job_result(
            json!(null),
            "hello there",
            "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        ));
        let out: serde_json::Value = serde_json::from_slice(&rewrite_messages_body(&raw)).unwrap();
        let system = out["system"].as_array().unwrap();
        assert_eq!(system.len(), 2);
        assert!(
            system[0]["text"]
                .as_str()
                .unwrap()
                .contains("prompt_version=You are a Claude agent")
        );
        assert!(
            system[1]["text"]
                .as_str()
                .unwrap()
                .starts_with("# Environment")
        );
    }

    #[test]
    fn leftover_appended() {
        let raw = slot_body(job_result(
            json!("你是一个高速收费员。"),
            "你好呀",
            "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        ));
        let out: serde_json::Value = serde_json::from_slice(&rewrite_messages_body(&raw)).unwrap();
        let last = out["system"].as_array().unwrap().last().unwrap()["text"]
            .as_str()
            .unwrap();
        assert_eq!(last, "你是一个高速收费员。");
    }

    #[test]
    fn supervisor_without_mcp_is_untouched() {
        let raw = serde_json::to_vec(&json!({
            "model": "claude-sonnet-5",
            "system": [{"type":"text","text": IDENTITY}],
            "messages": [{"role":"user","content":"hi"}]
        }))
        .unwrap();
        assert_eq!(rewrite_messages_body(&raw), raw);
    }
}
