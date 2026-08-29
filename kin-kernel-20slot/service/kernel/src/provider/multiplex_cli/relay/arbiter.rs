//! Per-job body source arbiter.
//!
//! `off` never instantiates this type. `observe` starts in `StdoutFallback` so
//! the user stream stays on CLI frames while tap only fills the digest.
//! `authoritative` promotes `NoBody` → `UpstreamActive` on the first upstream
//! text/thinking delta and never falls back afterwards.

use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::config::RelayMode;

use super::sse_tap::{KIN_SYNTH_MARKER, TAP_POISONED, TAP_USAGE};

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
    /// True once a tap response has attached to this turn. Only then is it
    /// worth deferring stdout body frames while waiting for upstream deltas.
    tap_attached: bool,
    /// True once a genuine (non-synthesized) upstream text delta streamed
    /// this turn. The kin_done `text` argument then duplicates/paraphrases
    /// the already-streamed body, so synthesized events must be dropped.
    saw_real_upstream_text: bool,
    /// Authoritative-mode holding pen: stdout body frames that arrived while
    /// still NoBody with a tap attached. Discarded on upgrade to
    /// UpstreamActive; released downstream if the tap never produces a body.
    deferred: Vec<Value>,
    deferred_bytes: usize,
}

/// Byte cap for deferred stdout frames (mirrors the per-job sink budget).
const DEFERRED_STDOUT_BYTES: usize = 2 * 1024 * 1024;

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
            tap_attached: false,
            saw_real_upstream_text: false,
            deferred: Vec::new(),
            deferred_bytes: 0,
        }
    }

    pub fn set_tap_attached(&mut self) {
        self.tap_attached = true;
    }

    /// Drain any deferred stdout frames. Non-empty only when the turn never
    /// upgraded to UpstreamActive; the caller must forward these to the user.
    pub fn take_deferred(&mut self) -> Vec<Value> {
        if !self.deferred.is_empty() && self.state == BodyState::NoBody {
            self.state = BodyState::StdoutFallback;
        }
        self.deferred_bytes = 0;
        std::mem::take(&mut self.deferred)
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
        if is_synth(event) {
            // The kin_done text argument restates the answer. EventFilter only
            // sees one internal response; the model may have streamed the real
            // body in an earlier response of the same turn, so the whole-turn
            // duplicate check lives here.
            if self.saw_real_upstream_text {
                return ArbiterEffect::Suppress;
            }
        } else if body_text(event).is_some() {
            self.saw_real_upstream_text = true;
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
            // Tap is gone; deferred stdout frames (if any) become the body.
            // They are released by take_deferred() on the kin_done path.
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
        // Tap emits per-response usage deltas. Each internal Messages request
        // has independent input_tokens, while output_tokens is cumulative only
        // within that response, so summing deltas gives total upstream work.
        for (key, value) in map {
            let Some(value) = value.as_u64() else {
                continue;
            };
            let current = self.usage.get(&key).and_then(Value::as_u64).unwrap_or(0);
            self.usage
                .insert(key, Value::from(current.saturating_add(value)));
        }
    }

    pub fn filter_stdout(&mut self, events: Vec<Value>) -> Vec<Value> {
        if self.closed() {
            return Vec::new();
        }
        self.push_stdout_text(&events);
        if self.state == BodyState::NoBody && events.iter().any(is_body_event) {
            if self.mode == RelayMode::Authoritative && self.tap_attached {
                // Deferred arbitration: the CLI's whole-block assistant frame
                // routinely lands before the first tap delta. Committing to
                // StdoutFallback here would permanently suppress the upstream
                // per-token stream (the state machine is one-way), so hold the
                // stdout body back until the tap either produces a body
                // (upgrade, discard these) or provably will not (release).
                return self.defer_stdout_body(events);
            }
            self.state = BodyState::StdoutFallback;
        }
        if self.state != BodyState::UpstreamActive {
            return events;
        }
        self.drop_stdout_body(events)
    }

    fn defer_stdout_body(&mut self, events: Vec<Value>) -> Vec<Value> {
        let mut pass = Vec::new();
        for event in events {
            // A content_block_stop belongs to whichever block it closes:
            // defer it alongside its deferred start/delta or it would reach
            // the client as an orphan stop for a block that never started.
            let closes_deferred = is_type(&event, "content_block_stop")
                && event
                    .get("index")
                    .and_then(Value::as_u64)
                    .is_some_and(|idx| {
                        self.deferred
                            .iter()
                            .any(|held| held.get("index").and_then(Value::as_u64) == Some(idx))
                    });
            if !is_body_event(&event) && !closes_deferred {
                pass.push(event);
                continue;
            }
            let bytes = event.to_string().len();
            if self.deferred_bytes + bytes > DEFERRED_STDOUT_BYTES {
                // Budget blown: stop waiting for the tap and fall back now.
                self.state = BodyState::StdoutFallback;
                let mut released = self.take_deferred();
                released.push(event);
                released.extend(pass);
                return released;
            }
            self.deferred_bytes += bytes;
            self.deferred.push(event);
        }
        pass
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
        // Upgrade only on visible content. Real CLI responses open with an
        // empty thinking block whose start/deltas used to claim the body:
        // the deferred stdout frames were cleared while upstream had nothing
        // to show — the empty 200s in the 20-way test.
        if visible_body_delta(event) && self.state == BodyState::NoBody {
            self.state = BodyState::UpstreamActive;
            self.upstream_authoritative = true;
            // The upstream stream owns the body now; any stdout frames held
            // back by deferred arbitration would be duplicates.
            self.deferred.clear();
            self.deferred_bytes = 0;
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

fn is_synth(event: &Value) -> bool {
    event.get(KIN_SYNTH_MARKER).and_then(Value::as_bool) == Some(true)
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

/// Non-empty text or thinking delta — content a user would actually see.
fn visible_body_delta(event: &Value) -> bool {
    if !is_type(event, "content_block_delta") {
        return false;
    }
    let text = match event.pointer("/delta/type").and_then(Value::as_str) {
        Some("text_delta") => event.pointer("/delta/text"),
        Some("thinking_delta") => event.pointer("/delta/thinking"),
        _ => return false,
    };
    text.and_then(Value::as_str).is_some_and(|t| !t.is_empty())
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
    fn deferred_arbitration_stdout_first_then_tap_streams_upstream() {
        // The race behind FAIL-1: stdout's whole-block frame lands before the
        // first tap delta. With a tap attached the arbiter must defer, then
        // upgrade and discard the stdout body once upstream deltas arrive.
        let mut arbiter = SourceArbiter::new(RelayMode::Authoritative);
        arbiter.set_tap_attached();
        let passed = arbiter.filter_stdout(vec![
            text_start(),
            text_delta("whole block"),
            text_stop(),
            stage_start(),
        ]);
        // Stage events pass through; body frames are held back.
        assert_eq!(passed.len(), 1);
        assert_eq!(passed[0]["content_block"]["type"], "server_tool_use");
        assert_eq!(arbiter.state(), BodyState::NoBody);
        // First upstream delta upgrades and discards the deferred stdout body.
        assert_eq!(
            arbiter.on_upstream(&text_delta("tok")),
            ArbiterEffect::Forward
        );
        assert_eq!(arbiter.state(), BodyState::UpstreamActive);
        assert!(arbiter.take_deferred().is_empty());
    }

    #[test]
    fn deferred_stdout_released_when_tap_never_produces_body() {
        let mut arbiter = SourceArbiter::new(RelayMode::Authoritative);
        arbiter.set_tap_attached();
        let passed =
            arbiter.filter_stdout(vec![text_start(), text_delta("only body"), text_stop()]);
        assert!(passed.is_empty());
        // kin_done path drains the deferred frames; turn falls back to stdout.
        let released = arbiter.take_deferred();
        assert_eq!(released.len(), 3);
        assert_eq!(arbiter.state(), BodyState::StdoutFallback);
        assert_eq!(released[1]["delta"]["text"], "only body");
    }

    #[test]
    fn no_tap_attached_keeps_immediate_stdout_fallback() {
        // Correlation failed: no tap will ever produce a body, so waiting
        // would only add latency. Old behavior must be preserved.
        let mut arbiter = SourceArbiter::new(RelayMode::Authoritative);
        let passed = arbiter.filter_stdout(vec![text_start(), text_delta("x"), text_stop()]);
        assert_eq!(passed.len(), 3);
        assert_eq!(arbiter.state(), BodyState::StdoutFallback);
    }

    #[test]
    fn empty_thinking_prelude_does_not_claim_body() {
        // Every real CLI response opens with an empty thinking block; if it
        // claimed the body, the deferred stdout frames were discarded while
        // upstream had nothing visible — the empty 200s in the 20-way test.
        let mut arbiter = SourceArbiter::new(RelayMode::Authoritative);
        arbiter.set_tap_attached();
        let held =
            arbiter.filter_stdout(vec![text_start(), text_delta("stdout body"), text_stop()]);
        assert!(held.is_empty());
        let start = json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": { "type": "thinking", "thinking": "", "signature": "" }
        });
        let empty_think = json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": { "type": "thinking_delta", "thinking": "" }
        });
        assert_eq!(arbiter.on_upstream(&start), ArbiterEffect::Forward);
        assert_eq!(arbiter.on_upstream(&empty_think), ArbiterEffect::Forward);
        assert_eq!(arbiter.state(), BodyState::NoBody);
        // Deferred stdout still intact and released on kin_done.
        let released = arbiter.take_deferred();
        assert_eq!(released.len(), 3);
        assert_eq!(arbiter.state(), BodyState::StdoutFallback);
    }

    #[test]
    fn nonempty_thinking_claims_body() {
        let mut arbiter = SourceArbiter::new(RelayMode::Authoritative);
        let think = json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": { "type": "thinking_delta", "thinking": "reasoning..." }
        });
        assert_eq!(arbiter.on_upstream(&think), ArbiterEffect::Forward);
        assert_eq!(arbiter.state(), BodyState::UpstreamActive);
    }

    #[test]
    fn synth_text_suppressed_after_real_upstream_text() {
        // Whole-turn guard: an earlier internal response streamed real text;
        // a later response's kin_done synthesis would duplicate the body.
        let mut arbiter = SourceArbiter::new(RelayMode::Authoritative);
        assert_eq!(
            arbiter.on_upstream(&text_delta("real body")),
            ArbiterEffect::Forward
        );
        let synth = json!({
            "type": "content_block_delta",
            "index": 3,
            "delta": { "type": "text_delta", "text": "restated" },
            "kin_synth": true
        });
        assert_eq!(arbiter.on_upstream(&synth), ArbiterEffect::Suppress);
        assert_eq!(arbiter.upstream_text(), "real body");
    }

    #[test]
    fn synth_text_claims_body_when_nothing_real_streamed() {
        let mut arbiter = SourceArbiter::new(RelayMode::Authoritative);
        arbiter.set_tap_attached();
        let held = arbiter.filter_stdout(vec![text_start(), text_delta("stdout"), text_stop()]);
        assert!(held.is_empty());
        let synth = json!({
            "type": "content_block_delta",
            "index": 3,
            "delta": { "type": "text_delta", "text": "tok" },
            "kin_synth": true
        });
        assert_eq!(arbiter.on_upstream(&synth), ArbiterEffect::Forward);
        assert_eq!(arbiter.state(), BodyState::UpstreamActive);
        // Deferred stdout discarded — synthesized stream owns the body.
        assert!(arbiter.take_deferred().is_empty());
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

    #[test]
    fn usage_deltas_accumulate_across_internal_responses() {
        let mut arbiter = SourceArbiter::new(RelayMode::Authoritative);
        arbiter.note_usage(serde_json::json!({"input_tokens":4,"output_tokens":5}));
        arbiter.note_usage(serde_json::json!({"input_tokens":6,"output_tokens":2}));
        assert_eq!(
            arbiter.usage(),
            Some(serde_json::json!({"input_tokens":10,"output_tokens":7}))
        );
    }
}
