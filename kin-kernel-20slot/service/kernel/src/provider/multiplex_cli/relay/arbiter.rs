//! Per-job body source arbiter.
//!
//! `off` never instantiates this type. `observe` starts in `StdoutFallback` so
//! the user stream stays on CLI frames while tap only fills the digest.
//! `authoritative` promotes `NoBody` → `UpstreamActive` on the first upstream
//! text/thinking delta and never falls back afterwards.

use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::config::RelayMode;

use super::sse_tap::{TAP_POISONED, TAP_USAGE};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BodyState {
    NoBody,
    UpstreamActive,
    StdoutFallback,
    Completed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArbiterEffect {
    Forward,
    Suppress,
    Ignore,
    FailJob,
}

pub struct SourceArbiter {
    mode: RelayMode,
    state: BodyState,
    failed: bool,
    /// Set once the turn reaches UpstreamActive; survives on_kin_done()
    /// moving `state` to Completed so final_text still knows the source.
    upstream_authoritative: bool,
    upstream_text: String,
    stdout_text: String,
    usage: Map<String, Value>,
    suppressed_stdout_indices: Vec<u64>,
}

impl SourceArbiter {
    pub fn new(mode: RelayMode) -> Self {
        let state = match mode {
            RelayMode::Observe => BodyState::StdoutFallback,
            _ => BodyState::NoBody,
        };
        Self {
            mode,
            state,
            failed: false,
            upstream_authoritative: false,
            upstream_text: String::new(),
            stdout_text: String::new(),
            usage: Map::new(),
            suppressed_stdout_indices: Vec::new(),
        }
    }

    #[cfg(test)]
    pub fn state(&self) -> BodyState {
        self.state
    }

    #[cfg(test)]
    pub fn failed(&self) -> bool {
        self.failed
    }

    pub fn upstream_text(&self) -> &str {
        &self.upstream_text
    }

    #[cfg(test)]
    pub fn stdout_text(&self) -> &str {
        &self.stdout_text
    }

    pub fn usage(&self) -> Option<Value> {
        if self.usage.is_empty() {
            return None;
        }
        Some(Value::Object(self.usage.clone()))
    }

    pub fn on_upstream(&mut self, event: &Value) -> ArbiterEffect {
        if self.closed() {
            return ArbiterEffect::Ignore;
        }
        if is_type(event, TAP_POISONED) {
            return self.on_tap_poisoned();
        }
        if is_type(event, TAP_USAGE) {
            if let Some(usage) = event.get("usage").cloned() {
                self.note_usage(usage);
            }
            return ArbiterEffect::Suppress;
        }
        self.push_upstream_text(event);
        self.decide_upstream(event)
    }

    pub fn on_tap_poisoned(&mut self) -> ArbiterEffect {
        if self.closed() {
            return ArbiterEffect::Ignore;
        }
        if self.state == BodyState::UpstreamActive {
            self.failed = true;
            return ArbiterEffect::FailJob;
        }
        if self.state == BodyState::NoBody {
            self.state = BodyState::StdoutFallback;
        }
        ArbiterEffect::Suppress
    }

    pub fn on_kin_done(&mut self) {
        if self.failed {
            return;
        }
        self.state = BodyState::Completed;
    }

    pub fn note_usage(&mut self, usage: Value) {
        let Value::Object(map) = usage else {
            return;
        };
        self.usage = map;
    }

    pub fn filter_stdout(&mut self, events: Vec<Value>) -> Vec<Value> {
        if self.closed() {
            return Vec::new();
        }
        self.push_stdout_text(&events);
        if self.state == BodyState::NoBody && events.iter().any(is_body_event) {
            self.state = BodyState::StdoutFallback;
        }
        if self.state != BodyState::UpstreamActive {
            return events;
        }
        self.drop_stdout_body(events)
    }

    pub fn final_text(&self, stdout: &str, fallback: &str) -> String {
        // Upstream text is authoritative only once the turn actually upgraded
        // to UpstreamActive. In observe mode the tap still accumulates text
        // for the digest comparison, but the user-visible body stays stdout.
        if self.upstream_authoritative && !self.upstream_text.is_empty() {
            return self.upstream_text.clone();
        }
        if !stdout.is_empty() {
            return stdout.to_string();
        }
        fallback.to_string()
    }

    pub fn upstream_authoritative(&self) -> bool {
        self.upstream_authoritative && !self.upstream_text.is_empty()
    }

    pub fn mismatch_digests(&self, stdout: &str) -> Option<(String, String)> {
        if self.upstream_text.is_empty() {
            return None;
        }
        let upstream = digest_hex(&self.upstream_text);
        let stdout = digest_hex(stdout);
        if upstream == stdout {
            return None;
        }
        Some((upstream, stdout))
    }

    fn closed(&self) -> bool {
        self.failed || self.state == BodyState::Completed
    }

    fn decide_upstream(&mut self, event: &Value) -> ArbiterEffect {
        if self.mode == RelayMode::Observe {
            return ArbiterEffect::Suppress;
        }
        if self.state == BodyState::StdoutFallback {
            return ArbiterEffect::Suppress;
        }
        if is_body_event(event) && self.state == BodyState::NoBody {
            self.state = BodyState::UpstreamActive;
            self.upstream_authoritative = true;
        }
        ArbiterEffect::Forward
    }

    fn push_upstream_text(&mut self, event: &Value) {
        if let Some(text) = body_text(event) {
            self.upstream_text.push_str(text);
        }
    }

    fn push_stdout_text(&mut self, events: &[Value]) {
        for event in events {
            if let Some(text) = body_text(event) {
                self.stdout_text.push_str(text);
            }
        }
    }

    fn drop_stdout_body(&mut self, events: Vec<Value>) -> Vec<Value> {
        let mut out = Vec::new();
        for event in events {
            if let Some(index) = body_start_index(&event) {
                self.suppressed_stdout_indices.push(index);
                continue;
            }
            if is_body_delta(&event) {
                continue;
            }
            if is_stop(&event) && self.stdout_index_suppressed(&event) {
                continue;
            }
            out.push(event);
        }
        out
    }

    fn stdout_index_suppressed(&self, event: &Value) -> bool {
        let Some(index) = event_index(event) else {
            return false;
        };
        self.suppressed_stdout_indices.contains(&index)
    }
}

fn is_type(event: &Value, expected: &str) -> bool {
    event.get("type").and_then(Value::as_str) == Some(expected)
}

fn is_stop(event: &Value) -> bool {
    is_type(event, "content_block_stop")
}

fn is_body_event(event: &Value) -> bool {
    is_body_start(event) || is_body_delta(event)
}

fn is_body_start(event: &Value) -> bool {
    if !is_type(event, "content_block_start") {
        return false;
    }
    matches!(
        event.pointer("/content_block/type").and_then(Value::as_str),
        Some("text" | "thinking")
    )
}

fn is_body_delta(event: &Value) -> bool {
    if !is_type(event, "content_block_delta") {
        return false;
    }
    matches!(
        event.pointer("/delta/type").and_then(Value::as_str),
        Some("text_delta" | "thinking_delta")
    )
}

fn body_start_index(event: &Value) -> Option<u64> {
    if !is_body_start(event) {
        return None;
    }
    event_index(event)
}

fn event_index(event: &Value) -> Option<u64> {
    event.get("index").and_then(Value::as_u64)
}

fn body_text(event: &Value) -> Option<&str> {
    if event.pointer("/delta/type").and_then(Value::as_str) != Some("text_delta") {
        return None;
    }
    event
        .pointer("/delta/text")
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
}

fn digest_hex(text: &str) -> String {
    Sha256::digest(text.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn text_delta(text: &str) -> Value {
        json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": { "type": "text_delta", "text": text }
        })
    }

    fn text_start() -> Value {
        json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": { "type": "text", "text": "" }
        })
    }

    fn text_stop() -> Value {
        json!({ "type": "content_block_stop", "index": 0 })
    }

    fn stage_start() -> Value {
        json!({
            "type": "content_block_start",
            "index": 1,
            "content_block": { "type": "server_tool_use", "id": "srv", "name": "web_search" }
        })
    }

    #[test]
    fn stdout_body_suppressed_after_upstream_upgrade() {
        let mut arbiter = SourceArbiter::new(RelayMode::Authoritative);
        assert_eq!(
            arbiter.on_upstream(&text_delta("hello")),
            ArbiterEffect::Forward
        );
        assert_eq!(arbiter.state(), BodyState::UpstreamActive);
        let stdout = vec![
            text_start(),
            text_delta("hello"),
            text_stop(),
            stage_start(),
        ];
        let kept = arbiter.filter_stdout(stdout);
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0]["content_block"]["type"], "server_tool_use");
        assert_eq!(arbiter.stdout_text(), "hello");
    }

    #[test]
    fn tap_poisoned_in_upstream_active_fails_instead_of_fallback() {
        let mut arbiter = SourceArbiter::new(RelayMode::Authoritative);
        assert_eq!(
            arbiter.on_upstream(&text_delta("hello")),
            ArbiterEffect::Forward
        );
        assert_eq!(arbiter.on_tap_poisoned(), ArbiterEffect::FailJob);
        assert!(arbiter.failed());
        assert_eq!(arbiter.state(), BodyState::UpstreamActive);
        assert_eq!(
            arbiter.on_upstream(&text_delta("late")),
            ArbiterEffect::Ignore
        );
        let kept = arbiter.filter_stdout(vec![text_delta("stdout")]);
        assert!(kept.is_empty());
    }

    #[test]
    fn observe_forces_stdout_fallback_and_swallows_tap_body() {
        let mut arbiter = SourceArbiter::new(RelayMode::Observe);
        assert_eq!(arbiter.state(), BodyState::StdoutFallback);
        assert_eq!(
            arbiter.on_upstream(&text_delta("from tap")),
            ArbiterEffect::Suppress
        );
        assert_eq!(arbiter.state(), BodyState::StdoutFallback);
        let stdout = vec![text_start(), text_delta("from stdout"), text_stop()];
        let kept = arbiter.filter_stdout(stdout);
        assert_eq!(kept.len(), 3);
        assert_eq!(arbiter.on_tap_poisoned(), ArbiterEffect::Suppress);
        assert!(!arbiter.failed());
    }

    #[test]
    fn completed_rejects_late_events() {
        let mut arbiter = SourceArbiter::new(RelayMode::Authoritative);
        assert_eq!(
            arbiter.on_upstream(&text_delta("hello")),
            ArbiterEffect::Forward
        );
        arbiter.on_kin_done();
        assert_eq!(arbiter.state(), BodyState::Completed);
        assert_eq!(
            arbiter.on_upstream(&text_delta("late")),
            ArbiterEffect::Ignore
        );
        assert!(
            arbiter
                .filter_stdout(vec![text_delta("late stdout")])
                .is_empty()
        );
        assert_eq!(arbiter.on_tap_poisoned(), ArbiterEffect::Ignore);
    }

    #[test]
    fn digest_match_and_mismatch() {
        let mut match_arbiter = SourceArbiter::new(RelayMode::Authoritative);
        match_arbiter.on_upstream(&text_delta("same"));
        let _ = match_arbiter.filter_stdout(vec![text_delta("same")]);
        assert_eq!(match_arbiter.mismatch_digests("same"), None);

        let mut mismatch = SourceArbiter::new(RelayMode::Authoritative);
        mismatch.on_upstream(&text_delta("upstream"));
        assert_eq!(
            mismatch
                .mismatch_digests("stdout")
                .map(|(up, down)| { (up.len(), down.len(), up != down) }),
            Some((64, 64, true))
        );
        assert!(mismatch.mismatch_digests("").is_some());
        assert_eq!(mismatch.final_text("stdout", "fallback"), "upstream");
    }
}
