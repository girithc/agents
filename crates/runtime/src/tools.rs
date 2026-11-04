//! Tool registry used by the agent run loop.
//!
//! Tools are isolated behind the [`Tool`] trait; the runtime never knows
//! what a tool does internally. Built-ins are deliberately safe/local:
//! `echo`, `now` (UTC time), `uuid`.

use std::collections::HashMap;
use std::sync::Arc;

use agenthub_providers::{ToolCall, ToolSpec};
use async_trait::async_trait;
use serde_json::{json, Value};

#[async_trait]
pub trait Tool: Send + Sync {
    fn name(&self) -> &str;
    fn description(&self) -> &str;
    fn parameters(&self) -> Value;
    async fn call(&self, arguments: Value) -> Result<String, String>;
}

pub struct ToolRegistry {
    tools: HashMap<String, Arc<dyn Tool>>,
}

impl ToolRegistry {
    pub fn with_builtins() -> Self {
        let mut reg = Self {
            tools: HashMap::new(),
        };
        reg.register(Arc::new(EchoTool));
        reg.register(Arc::new(NowTool));
        reg.register(Arc::new(UuidTool));
        reg
    }

    pub fn register(&mut self, tool: Arc<dyn Tool>) {
        self.tools.insert(tool.name().to_string(), tool);
    }

    pub fn specs(&self) -> Vec<ToolSpec> {
        self.tools
            .values()
            .map(|t| ToolSpec {
                name: t.name().to_string(),
                description: t.description().to_string(),
                parameters: t.parameters(),
            })
            .collect()
    }

    /// Execute a tool call requested by the model. Always returns a string
    /// (errors are encoded as JSON error payloads) so the loop can feed the
    /// result back to the provider.
    #[tracing::instrument(name = "tool.call", skip_all, fields(tool = %call.name))]
    pub async fn call(&self, call: &ToolCall) -> String {
        match self.tools.get(&call.name) {
            None => json!({ "error": format!("unknown tool '{}'", call.name) }).to_string(),
            Some(tool) => match tool.call(call.arguments.clone()).await {
                Ok(out) => out,
                Err(e) => json!({ "error": e }).to_string(),
            },
        }
    }
}

struct EchoTool;
#[async_trait]
impl Tool for EchoTool {
    fn name(&self) -> &str {
        "echo"
    }
    fn description(&self) -> &str {
        "Returns the text provided in the 'text' argument."
    }
    fn parameters(&self) -> Value {
        json!({
            "type": "object",
            "properties": { "text": { "type": "string" } },
            "required": ["text"]
        })
    }
    async fn call(&self, args: Value) -> Result<String, String> {
        let text = args
            .get("text")
            .and_then(Value::as_str)
            .ok_or("missing 'text' argument")?;
        Ok(text.to_string())
    }
}

struct NowTool;
#[async_trait]
impl Tool for NowTool {
    fn name(&self) -> &str {
        "now"
    }
    fn description(&self) -> &str {
        "Returns the current UTC time as ISO-8601 and epoch milliseconds."
    }
    fn parameters(&self) -> Value {
        json!({ "type": "object", "properties": {} })
    }
    async fn call(&self, _args: Value) -> Result<String, String> {
        let ms = agenthub_storage::now_ms();
        Ok(json!({
            "iso8601": iso8601_from_epoch_ms(ms),
            "epoch_ms": ms
        })
        .to_string())
    }
}

struct UuidTool;
#[async_trait]
impl Tool for UuidTool {
    fn name(&self) -> &str {
        "uuid"
    }
    fn description(&self) -> &str {
        "Generates a random UUID v4."
    }
    fn parameters(&self) -> Value {
        json!({ "type": "object", "properties": {} })
    }
    async fn call(&self, _args: Value) -> Result<String, String> {
        Ok(uuid::Uuid::new_v4().to_string())
    }
}

fn iso8601_from_epoch_ms(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let millis = ms.rem_euclid(1000);
    let days = secs.div_euclid(86400);
    let sod = secs.rem_euclid(86400);
    let (y, m, d) = civil_from_days(days);
    let (h, mi, s) = (sod / 3600, (sod % 3600) / 60, sod % 60);
    format!("{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{s:02}.{millis:03}Z")
}

/// Days since 1970-01-01 to (year, month, day). Howard Hinnant's algorithm.
fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iso_conversion() {
        // 2026-01-01T00:00:00Z == 1767225600
        assert_eq!(iso8601_from_epoch_ms(1_767_225_600_000), "2026-01-01T00:00:00.000Z");
    }

    #[tokio::test]
    async fn unknown_tool_returns_error_payload() {
        let reg = ToolRegistry::with_builtins();
        let out = reg
            .call(&ToolCall {
                id: "x".into(),
                name: "nope".into(),
                arguments: json!({}),
            })
            .await;
        assert!(out.contains("unknown tool"));
    }
}
