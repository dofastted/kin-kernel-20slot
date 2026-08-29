//! stdin/stdout frames for `execution_mode=native_slot`.
//!
//! Rust never constructs billing/identity. CLI emits Anthropic SSE inside
//! `kin_stream_event.event` and tags job_id/slot_id explicitly.

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum KinStdin {
    #[serde(rename = "kin_hello")]
    Hello {
        slots: usize,
        system_layout: String,
        timezone: String,
    },
    #[serde(rename = "kin_job_start")]
    JobStart {
        job_id: String,
        slot_id: String,
        request: Value,
    },
    #[serde(rename = "kin_tool_result")]
    ToolResult {
        job_id: String,
        slot_id: String,
        tool_use_id: String,
        content: Value,
    },
    #[serde(rename = "kin_cancel")]
    Cancel { job_id: String },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum KinStdout {
    #[serde(rename = "kin_slot_ready")]
    SlotReady { slot_id: String },
    #[serde(rename = "kin_stream_event")]
    StreamEvent {
        job_id: String,
        slot_id: String,
        event: Value,
    },
    #[serde(rename = "kin_job_done")]
    JobDone {
        job_id: String,
        slot_id: String,
        stop_reason: String,
        #[serde(default)]
        usage: Value,
    },
    #[serde(rename = "kin_job_error")]
    JobError { job_id: String, error: String },
}

pub fn encode_stdin(frame: &KinStdin) -> Result<Vec<u8>, String> {
    let mut line = serde_json::to_vec(frame).map_err(|err| err.to_string())?;
    line.push(b'\n');
    Ok(line)
}

pub fn decode_stdout_line(line: &str) -> Option<KinStdout> {
    serde_json::from_str(line).ok()
}

pub fn decode_stdout_value(frame: &Value) -> Option<KinStdout> {
    serde_json::from_value(frame.clone()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn roundtrip_job_start() {
        let frame = KinStdin::JobStart {
            job_id: "job-1".into(),
            slot_id: "s00".into(),
            request: json!({"model":"claude-sonnet-5","messages":[]}),
        };
        let raw = String::from_utf8(encode_stdin(&frame).unwrap()).unwrap();
        let parsed: KinStdin = serde_json::from_str(raw.trim()).unwrap();
        assert_eq!(parsed, frame);
    }

    #[test]
    fn stream_event_carries_ids() {
        let line = r#"{"type":"kin_stream_event","job_id":"j1","slot_id":"s03","event":{"type":"content_block_delta","delta":{"type":"text_delta","text":"Hi"}}}"#;
        let parsed = decode_stdout_line(line).unwrap();
        match parsed {
            KinStdout::StreamEvent { job_id, slot_id, event } => {
                assert_eq!(job_id, "j1");
                assert_eq!(slot_id, "s03");
                assert_eq!(event["delta"]["text"], "Hi");
            }
            other => panic!("{other:?}"),
        }
    }
}
