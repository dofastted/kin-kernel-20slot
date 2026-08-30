use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MessageRequest {
    pub model: String,
    pub messages: Vec<Message>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system: Option<SystemPrompt>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<ToolDefinition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thinking: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
    pub max_tokens: u32,
    #[serde(default, skip_serializing_if = "is_false")]
    pub stream: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub top_k: Option<u32>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub stop_sequences: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub betas: Vec<String>,
    #[serde(default, flatten)]
    pub extra: Map<String, Value>,
}

impl Default for MessageRequest {
    fn default() -> Self {
        Self {
            model: String::new(),
            messages: Vec::new(),
            system: None,
            tools: Vec::new(),
            tool_choice: None,
            thinking: None,
            metadata: None,
            max_tokens: 1024,
            stream: false,
            temperature: None,
            top_p: None,
            top_k: None,
            stop_sequences: Vec::new(),
            betas: Vec::new(),
            extra: Map::new(),
        }
    }
}

impl MessageRequest {
    pub fn normalize_openai_tools(&mut self) {
        for message in &mut self.messages {
            if message.role == "tool"
                && let Some(id) = message.tool_call_id.clone()
            {
                let content = match &message.content {
                    MessageContent::Text(text) => Value::String(text.clone()),
                    MessageContent::Blocks(blocks) => {
                        serde_json::to_value(blocks).unwrap_or(Value::Null)
                    }
                };
                message.role = "user".into();
                message.content = MessageContent::Blocks(vec![ContentBlock::ToolResult {
                    tool_use_id: id,
                    content,
                    is_error: false,
                }]);
                message.tool_call_id = None;
            }
            if !message.tool_calls.is_empty() {
                let mut blocks = Vec::new();
                if let MessageContent::Text(text) = &message.content
                    && !text.is_empty()
                {
                    blocks.push(ContentBlock::Text {
                        text: text.clone(),
                        cache_control: None,
                    });
                }
                if let MessageContent::Blocks(existing) = &message.content {
                    blocks.extend(existing.clone());
                }
                for call in message.tool_calls.drain(..) {
                    let input = serde_json::from_str(&call.function.arguments)
                        .unwrap_or_else(|_| json_or_string(&call.function.arguments));
                    blocks.push(ContentBlock::ToolUse {
                        id: call.id,
                        name: call.function.name,
                        input,
                    });
                }
                message.content = MessageContent::Blocks(blocks);
            }
        }
    }
}

fn json_or_string(raw: &str) -> Value {
    Value::String(raw.to_string())
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum SystemPrompt {
    Text(String),
    Blocks(Vec<Value>),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Message {
    pub role: String,
    #[serde(default, deserialize_with = "deserialize_content")]
    pub content: MessageContent,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<ChatToolCall>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum MessageContent {
    Text(String),
    Blocks(Vec<ContentBlock>),
}

impl Default for MessageContent {
    fn default() -> Self {
        Self::Text(String::new())
    }
}

fn deserialize_content<'de, D>(deserializer: D) -> Result<MessageContent, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<Value>::deserialize(deserializer)?;
    match value {
        None | Some(Value::Null) => Ok(MessageContent::Text(String::new())),
        Some(Value::String(text)) => Ok(MessageContent::Text(text)),
        Some(Value::Array(items)) => {
            let blocks = items
                .into_iter()
                .filter_map(|item| serde_json::from_value(item).ok())
                .collect();
            Ok(MessageContent::Blocks(blocks))
        }
        Some(other) => Ok(MessageContent::Text(other.to_string())),
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentBlock {
    Text {
        text: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cache_control: Option<Value>,
    },
    Image {
        source: Value,
    },
    Document {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        source: Option<Value>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        title: Option<String>,
        #[serde(flatten)]
        extra: Map<String, Value>,
    },
    ToolUse {
        id: String,
        name: String,
        #[serde(default)]
        input: Value,
    },
    ToolResult {
        tool_use_id: String,
        #[serde(default)]
        content: Value,
        #[serde(default)]
        is_error: bool,
    },
    Thinking {
        thinking: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        signature: Option<String>,
    },
    RedactedThinking {
        #[serde(default)]
        data: Value,
    },
    ServerToolUse {
        id: String,
        name: String,
        #[serde(default)]
        input: Value,
    },
    WebSearchToolResult {
        tool_use_id: String,
        #[serde(default)]
        content: Value,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ToolDefinition {
    pub name: String,
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub tool_type: Option<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    #[serde(default = "default_schema")]
    pub input_schema: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_control: Option<Value>,
    #[serde(default, flatten)]
    pub extra: Map<String, Value>,
}

fn default_schema() -> Value {
    serde_json::json!({"type": "object", "properties": {}})
}

#[derive(Clone, Debug, Serialize)]
pub struct MessageResponse {
    pub id: String,
    pub r#type: &'static str,
    pub role: &'static str,
    pub model: String,
    pub content: Vec<ContentBlock>,
    pub stop_reason: StopReason,
    pub usage: Usage,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StopReason {
    EndTurn,
    ToolUse,
    MaxTokens,
    StopSequence,
    PauseTurn,
    Refusal,
    ModelContextWindowExceeded,
    Unknown,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct Usage {
    pub input_tokens: u64,
    pub output_tokens: u64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ChatRequest {
    pub model: String,
    pub messages: Vec<Message>,
    #[serde(default)]
    pub tools: Vec<ChatTool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<Value>,
    #[serde(default = "default_chat_max_tokens")]
    pub max_tokens: u32,
    #[serde(default)]
    pub stream: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ChatTool {
    #[allow(dead_code)]
    pub r#type: String,
    pub function: ChatFunction,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ChatFunction {
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub parameters: Value,
}

#[derive(Clone, Debug, Serialize)]
pub struct ChatResponse {
    pub id: String,
    pub object: &'static str,
    pub model: String,
    pub choices: Vec<ChatChoice>,
    pub usage: ChatUsage,
}

#[derive(Clone, Debug, Serialize)]
pub struct ChatChoice {
    pub index: u32,
    pub message: ChatAssistantMessage,
    pub finish_reason: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct ChatAssistantMessage {
    pub role: &'static str,
    pub content: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<ChatToolCall>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ChatToolCall {
    pub id: String,
    #[serde(default = "function_type")]
    pub r#type: String,
    pub function: ChatToolCallFunction,
}

fn function_type() -> String {
    "function".into()
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ChatToolCallFunction {
    pub name: String,
    pub arguments: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct ChatUsage {
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
}

fn default_chat_max_tokens() -> u32 {
    1024
}

fn is_false(value: &bool) -> bool {
    !*value
}

impl From<ChatRequest> for MessageRequest {
    fn from(value: ChatRequest) -> Self {
        let tools = value
            .tools
            .into_iter()
            .map(|tool| ToolDefinition {
                name: tool.function.name,
                description: tool.function.description,
                input_schema: tool.function.parameters,
                cache_control: None,
                tool_type: None,
                extra: Map::new(),
            })
            .collect();

        let mut request = Self {
            model: value.model,
            messages: value.messages,
            tools,
            tool_choice: value.tool_choice,
            max_tokens: value.max_tokens,
            stream: value.stream,
            temperature: value.temperature,
            ..Self::default()
        };
        request.normalize_openai_tools();
        request
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn official_messages_body_roundtrip() {
        let raw = serde_json::json!({
            "model": "claude-sonnet-5",
            "max_tokens": 256,
            "stream": true,
            "temperature": 0.2,
            "top_p": 0.9,
            "top_k": 20,
            "stop_sequences": ["END"],
            "system": [
                {"type": "text", "text": "You are helpful.", "cache_control": {"type": "ephemeral"}}
            ],
            "thinking": {"type": "enabled", "budget_tokens": 2048},
            "tool_choice": {"type": "auto"},
            "metadata": {"user_id": "u1"},
            "service_tier": "auto",
            "tools": [{
                "name": "get_weather",
                "description": "weather",
                "input_schema": {"type": "object", "properties": {"city": {"type": "string"}}}
            }],
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "text", "text": "what is this?"},
                    {"type": "image", "source": {"type": "url", "url": "https://example.com/a.png"}}
                ]
            }]
        });
        let parsed: MessageRequest = serde_json::from_value(raw).unwrap();
        assert_eq!(parsed.model, "claude-sonnet-5");
        assert!(parsed.stream);
        assert_eq!(parsed.tools[0].name, "get_weather");
        assert!(parsed.system.is_some());
        assert!(parsed.thinking.is_some());
        assert_eq!(parsed.extra.get("service_tier").unwrap(), "auto");
        match &parsed.messages[0].content {
            MessageContent::Blocks(blocks) => {
                assert!(matches!(blocks[0], ContentBlock::Text { .. }));
                assert!(matches!(blocks[1], ContentBlock::Image { .. }));
            }
            _ => panic!("blocks"),
        }
    }

    #[test]
    fn openai_tool_role_normalizes() {
        let raw = serde_json::json!({
            "model": "claude-sonnet-5",
            "messages": [
                {"role": "assistant", "content": null, "tool_calls": [{
                    "id": "call_1",
                    "type": "function",
                    "function": {"name": "echo", "arguments": "{\"ok\":true}"}
                }]},
                {"role": "tool", "tool_call_id": "call_1", "content": "pong"}
            ]
        });
        let chat: ChatRequest = serde_json::from_value(raw).unwrap();
        let req = MessageRequest::from(chat);
        match &req.messages[1].content {
            MessageContent::Blocks(blocks) => match &blocks[0] {
                ContentBlock::ToolResult { tool_use_id, .. } => {
                    assert_eq!(tool_use_id, "call_1")
                }
                _ => panic!("tool_result"),
            },
            _ => panic!("blocks"),
        }
    }
}
