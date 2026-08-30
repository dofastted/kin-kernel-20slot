//! stdin/stdout frames for the `kin_*` protocol (v2), used by both
//! `execution_mode=native_slot` (`NativeAgent`) and `native_messages`
//! (`NativeMessages`).
//!
//! Rust never constructs billing/identity. CLI emits Anthropic SSE inside
//! `kin_stream_event.event` and tags job_id/slot_id explicitly. There is no
//! `kin_hello` handshake and no in-CLI tool parking (`kin_tool_result`,
//! `kin_job_parked`) — those were protocol v1 relics.

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const KIN_PROTOCOL_VERSION: u32 = 2;
pub const KIN_CAPABILITIES: &[&str] = &["multi_slot", "native_sse", "stateless"];

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum KinStdin {
    #[serde(rename = "kin_job_start")]
    JobStart {
        job_id: String,
        slot_id: String,
        request: Value,
    },
    #[serde(rename = "kin_cancel")]
    Cancel {
        job_id: String,
        #[serde(default)]
        slot_id: Option<String>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum KinStdout {
    #[serde(rename = "kin_host_ready")]
    HostReady {
        protocol_version: u32,
        slots: usize,
        system_layout: String,
        timezone: String,
        #[serde(default)]
        capabilities: Vec<String>,
        #[serde(default)]
        config_hash: Option<String>,
    },
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
    JobError {
        job_id: String,
        #[serde(default)]
        slot_id: Option<String>,
        error: String,
    },
    #[serde(rename = "kin_cancel_ack")]
    CancelAck { job_id: String, slot_id: String },
}

pub fn encode_stdin(frame: &KinStdin) -> Result<Vec<u8>, String> {
    let mut line = serde_json::to_vec(frame).map_err(|err| err.to_string())?;
    line.push(b'\n');
    Ok(line)
}

#[cfg(test)]
pub fn decode_stdout_line(line: &str) -> Option<KinStdout> {
    serde_json::from_str(line).ok()
}

pub fn decode_stdout_value(frame: &Value) -> Option<KinStdout> {
    serde_json::from_value(frame.clone()).ok()
}

pub fn slot_id(index: usize) -> String {
    format!("s{index:02}")
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
            KinStdout::StreamEvent {
                job_id,
                slot_id,
                event,
            } => {
                assert_eq!(job_id, "j1");
                assert_eq!(slot_id, "s03");
                assert_eq!(event["delta"]["text"], "Hi");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn host_ready_handshake() {
        let line = r#"{"type":"kin_host_ready","protocol_version":2,"slots":2,"system_layout":"zero","timezone":"America/New_York","capabilities":["multi_slot","native_sse","stateless"],"config_hash":"abc123"}"#;
        let parsed = decode_stdout_line(line).unwrap();
        match parsed {
            KinStdout::HostReady {
                protocol_version,
                slots,
                capabilities,
                config_hash,
                ..
            } => {
                assert_eq!(protocol_version, 2);
                assert_eq!(slots, 2);
                assert!(capabilities.iter().any(|c| c == "stateless"));
                assert_eq!(config_hash.as_deref(), Some("abc123"));
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn host_ready_without_config_hash() {
        let line = r#"{"type":"kin_host_ready","protocol_version":2,"slots":1,"system_layout":"zero","timezone":"UTC","capabilities":["multi_slot"]}"#;
        let parsed = decode_stdout_line(line).unwrap();
        match parsed {
            KinStdout::HostReady { config_hash, .. } => assert!(config_hash.is_none()),
            other => panic!("{other:?}"),
        }
    }
}
