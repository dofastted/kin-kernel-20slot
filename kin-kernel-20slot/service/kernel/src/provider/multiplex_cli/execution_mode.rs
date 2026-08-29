use std::{env, fmt, str::FromStr};

/// How a worker drives Claude Code.
///
/// `mcp_slot` is the current Agent+MCP loop (model-visible slot_wait/kin_done).
/// `native_slot` is the target: CLI host owns the slot loop, Anthropic never
/// sees Kin control tools.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ExecutionMode {
    #[default]
    McpSlot,
    NativeSlot,
}

impl ExecutionMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::McpSlot => "mcp_slot",
            Self::NativeSlot => "native_slot",
        }
    }

    pub fn is_native(self) -> bool {
        matches!(self, Self::NativeSlot)
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
            "native" | "native_slot" | "host" => Ok(Self::NativeSlot),
            other => Err(format!(
                "KIN_EXECUTION_MODE must be mcp_slot or native_slot (got {other})"
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_aliases() {
        assert_eq!("native".parse::<ExecutionMode>().unwrap(), ExecutionMode::NativeSlot);
        assert_eq!("mcp_slot".parse::<ExecutionMode>().unwrap(), ExecutionMode::McpSlot);
    }
}
