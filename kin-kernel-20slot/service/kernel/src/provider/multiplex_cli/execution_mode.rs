use std::{env, fmt, str::FromStr};

/// How a worker drives Claude Code.
///
/// `mcp_slot` is the current Agent+MCP loop (model-visible slot_wait/kin_done).
/// `native_slot` (now `NativeAgent`) is the frozen v1 native host: the CLI
/// owns the slot loop but still parks on tool results inside the process.
/// `native_messages` is the v2 stateless target: the CLI holds no tools/
/// agents/cross-job state, Rust drives every turn as a fresh job.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ExecutionMode {
    #[default]
    McpSlot,
    NativeAgent,
    NativeMessages,
}

impl ExecutionMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::McpSlot => "mcp_slot",
            Self::NativeAgent => "native_slot",
            Self::NativeMessages => "native_messages",
        }
    }

    /// True for any mode that speaks the stdin/stdout `kin_*` protocol
    /// instead of the MCP slot_wait/kin_done tool loop.
    pub fn is_native(self) -> bool {
        matches!(self, Self::NativeAgent | Self::NativeMessages)
    }

    /// True only for the stateless v2 protocol (no in-CLI tool parking).
    pub fn is_native_messages(self) -> bool {
        matches!(self, Self::NativeMessages)
    }

    pub fn from_env() -> Result<Self, String> {
        match env::var("KIN_EXECUTION_MODE") {
            Ok(value) => value.parse(),
            Err(env::VarError::NotPresent) => Ok(Self::McpSlot),
            Err(_) => Err("KIN_EXECUTION_MODE must be valid unicode".into()),
        }
    }
}

impl fmt::Display for ExecutionMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ExecutionMode {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "mcp" | "mcp_slot" | "agent" => Ok(Self::McpSlot),
            "native" | "native_slot" | "host" => Ok(Self::NativeAgent),
            "native_messages" => Ok(Self::NativeMessages),
            other => Err(format!(
                "KIN_EXECUTION_MODE must be mcp_slot, native_slot, or native_messages (got {other})"
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_aliases() {
        assert_eq!(
            "native".parse::<ExecutionMode>().unwrap(),
            ExecutionMode::NativeAgent
        );
        assert_eq!(
            "native_slot".parse::<ExecutionMode>().unwrap(),
            ExecutionMode::NativeAgent
        );
        assert_eq!(
            "host".parse::<ExecutionMode>().unwrap(),
            ExecutionMode::NativeAgent
        );
        assert_eq!(
            "mcp_slot".parse::<ExecutionMode>().unwrap(),
            ExecutionMode::McpSlot
        );
    }

    #[test]
    fn parses_native_messages() {
        assert_eq!(
            "native_messages".parse::<ExecutionMode>().unwrap(),
            ExecutionMode::NativeMessages
        );
        assert!(ExecutionMode::NativeMessages.is_native());
        assert!(ExecutionMode::NativeMessages.is_native_messages());
        assert!(!ExecutionMode::NativeAgent.is_native_messages());
    }

    #[test]
    fn rejects_unknown() {
        assert!("bogus".parse::<ExecutionMode>().is_err());
    }
}
