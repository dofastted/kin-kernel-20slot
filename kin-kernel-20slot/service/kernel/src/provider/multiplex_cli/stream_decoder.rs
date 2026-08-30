use serde_json::Value;

#[derive(Debug)]
pub enum Decoded {
    AgentSpawn { tool_use_id: String },
    Routed { parent_tool_use_id: String },
    Root,
}

pub fn parent_id(frame: &Value) -> Option<&str> {
    frame
        .get("parent_tool_use_id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .or_else(|| {
            frame
                .get("event")
                .and_then(|event| event.get("parent_tool_use_id"))
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty())
        })
}

pub fn decode(frame: &Value) -> Decoded {
    if let Some(id) = agent_spawn_id(frame) {
        return Decoded::AgentSpawn { tool_use_id: id };
    }
    if let Some(parent) = parent_id(frame) {
        return Decoded::Routed {
            parent_tool_use_id: parent.to_string(),
        };
    }
    Decoded::Root
}

fn agent_spawn_id(frame: &Value) -> Option<String> {
    if parent_id(frame).is_some() {
        return None;
    }
    let message = frame.get("message")?;
    let content = message.get("content")?.as_array()?;
    for block in content {
        let name = block.get("name").and_then(Value::as_str).unwrap_or("");
        let kind = block.get("type").and_then(Value::as_str);
        if kind == Some("tool_use")
            && (name == "Agent" || name == "kin-slot" || name.ends_with("Agent"))
            && let Some(id) = block.get("id").and_then(Value::as_str)
        {
            return Some(id.to_string());
        }
    }
    None
}

pub const MAX_LINE_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_JOB_BYTES: usize = 32 * 1024 * 1024;
