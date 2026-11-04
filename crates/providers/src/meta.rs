//! Meta AI provider — talks to `api.meta.ai` (`/v1/responses`), serving the
//! Muse family of models (e.g. `muse-spark-1.3-contributor`).
//!
//! Wire format mirrors an OpenAI-Responses-style API:
//!
//! ```text
//! POST /v1/responses
//! Authorization: Bearer <key>
//! Accept: text/event-stream        (streaming) | application/json
//! { "model", "input": [ {role, content: [{type: input_text|output_text, text}]} ],
//!   "stream", "temperature", "max_output_tokens", "top_p", "reasoning": {"effort"} }
//! ```
//!
//! Response parsing is deliberately defensive across response shapes
//! (`output[].content[].text`, `output_text`, `choices[].message.content`).

use std::time::Duration;

use async_trait::async_trait;
use futures::{StreamExt, stream::BoxStream};
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::Instrument;

use crate::{
    CompletionRequest, CompletionResponse, Provider, ProviderError, Result, Role, StreamEvent,
    ToolCall, Usage,
};

pub const DEFAULT_BASE_URL: &str = "https://api.meta.ai";

pub struct MetaProvider {
    http: reqwest::Client,
    base_url: String,
}

impl MetaProvider {
    pub fn new(base_url: Option<String>) -> Self {
        let base_url =
            base_url.unwrap_or_else(|| std::env::var("AGENTHUB_META_BASE_URL").unwrap_or_else(|_| DEFAULT_BASE_URL.into()));
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(300))
            .build()
            .expect("reqwest client");
        Self { http, base_url }
    }

    fn endpoint(&self) -> String {
        format!("{}/v1/responses", self.base_url.trim_end_matches('/'))
    }
}

#[async_trait]
impl Provider for MetaProvider {
    fn id(&self) -> &'static str {
        "meta"
    }

    fn secret_name(&self) -> Option<String> {
        Some("meta.api_key".to_string())
    }

    async fn complete(&self, req: CompletionRequest) -> Result<CompletionResponse> {
        let span = tracing::info_span!(
            "provider.complete",
            provider = self.id(),
            run_id = req.run_id.clone().unwrap_or_default(),
            model = %req.model,
            stream = false,
        );
        self.complete_impl(req).instrument(span).await
    }

    async fn complete_stream(
        &self,
        req: CompletionRequest,
    ) -> Result<BoxStream<'static, Result<StreamEvent>>> {
        let span = tracing::info_span!(
            "provider.complete",
            provider = self.id(),
            run_id = req.run_id.clone().unwrap_or_default(),
            model = %req.model,
            stream = true,
        );
        // The reader task outlives this setup call, so it runs under a
        // clone of the span: the span stays open until the stream is fully
        // consumed and measures true end-to-end streaming duration.
        let reader_span = span.clone();
        self.complete_stream_impl(req, reader_span)
            .instrument(span)
            .await
    }
}

impl MetaProvider {
    async fn complete_impl(&self, req: CompletionRequest) -> Result<CompletionResponse> {
        let body = build_request_body(req.clone(), false);
        let key = req.api_key.clone().unwrap_or_default();

        let resp = self
            .http
            .post(self.endpoint())
            .bearer_auth(&key)
            .header("Accept", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| ProviderError::Transport(self.id(), e))?;

        let status = resp.status();
        let body_text = resp
            .text()
            .await
            .map_err(|e| ProviderError::Transport(self.id(), e))?;

        if !status.is_success() {
            return Err(ProviderError::Api {
                provider: self.id(),
                status: status.as_u16(),
                body: body_text,
            });
        }

        let value: Value = serde_json::from_str(&body_text).map_err(|e| {
            ProviderError::Malformed(self.id(), format!("non-JSON body ({e}): {body_text}"))
        })?;

        let extracted = extract_response(&value);
        tracing::debug!(
            provider = self.id(),
            text_len = extracted.text.len(),
            tool_calls = extracted.tool_calls.len(),
            "meta completion received"
        );

        Ok(CompletionResponse {
            text: extracted.text,
            reasoning: extracted.reasoning,
            tool_calls: extracted.tool_calls,
            usage: extracted.usage,
            raw: value,
        })
    }

    async fn complete_stream_impl(
        &self,
        req: CompletionRequest,
        reader_span: tracing::Span,
    ) -> Result<BoxStream<'static, Result<StreamEvent>>> {
        let body = build_request_body(req.clone(), true);
        let key = req.api_key.clone().unwrap_or_default();

        let resp = self
            .http
            .post(self.endpoint())
            .bearer_auth(&key)
            .header("Accept", "text/event-stream")
            .json(&body)
            .send()
            .await
            .map_err(|e| ProviderError::Transport(self.id(), e))?;

        let status = resp.status();
        if !status.is_success() {
            let body_text = resp
                .text()
                .await
                .unwrap_or_else(|_| "<unreadable body>".to_string());
            return Err(ProviderError::Api {
                provider: self.id(),
                status: status.as_u16(),
                body: body_text,
            });
        }

        let (tx, rx) = mpsc::channel::<Result<StreamEvent>>(64);
        tokio::spawn(
            async move {
                let mut state = SseState::default();
                let mut buf = String::new();
                let mut event_name = String::new();
                let mut data = String::new();
                let mut stream = resp.bytes_stream();

            'outer: while let Some(chunk) = stream.next().await {
                let chunk = match chunk {
                    Ok(c) => c,
                    Err(e) => {
                        let _ = tx
                            .send(Err(ProviderError::Stream("meta", format!("{e}"))))
                            .await;
                        break;
                    }
                };
                buf.push_str(&String::from_utf8_lossy(&chunk));

                while let Some(idx) = buf.find('\n') {
                    let line: String = buf[..idx].trim_end_matches('\r').to_string();
                    buf.drain(..=idx);

                    if line.is_empty() {
                        // End of one SSE event.
                        if !data.is_empty() {
                            match handle_sse_data(&mut state, &event_name, &data) {
                                SseAction::Emit(ev) => {
                                    if tx.send(ev).await.is_err() {
                                        break 'outer;
                                    }
                                    if state.done {
                                        break 'outer;
                                    }
                                }
                                SseAction::None => {}
                            }
                        }
                        event_name.clear();
                        data.clear();
                    } else if let Some(rest) = line.strip_prefix("event:") {
                        event_name = rest.trim().to_string();
                    } else if let Some(rest) = line.strip_prefix("data:") {
                        if !data.is_empty() {
                            data.push('\n');
                        }
                        data.push_str(rest.trim_start());
                    }
                    // comments (":"...) and unknown fields are ignored
                }
            }

                if !state.done {
                    // Stream closed without an explicit completion event.
                    let _ = tx
                        .send(Ok(StreamEvent::Done(CompletionResponse {
                            text: state.text.clone(),
                            reasoning: if state.reasoning.is_empty() { None } else { Some(state.reasoning.clone()) },
                            tool_calls: vec![],
                            usage: None,
                            raw: Value::Null,
                        })))
                        .await;
                }
                // The span stays open for the whole stream and closes when
                // the reader task finishes, so end_ts reflects true
                // end-to-end streaming duration.
            }
            .instrument(reader_span),
        );

        Ok(Box::pin(ReceiverStream::new(rx).map(|item| item)))
    }
}

// ------------------------------------------------------------ wire format

fn build_request_body(req: CompletionRequest, stream: bool) -> Value {
    let mut input = Vec::new();
    for m in &req.messages {
        match m.role {
            Role::System | Role::User => input.push(json!({
                "role": m.role.as_str(),
                "content": [{"type": "input_text", "text": m.content}],
            })),
            Role::Assistant => {
                for tc in &m.tool_calls {
                    input.push(json!({
                        "type": "function_call",
                        "call_id": tc.id,
                        "name": tc.name,
                        "arguments": tc.arguments.to_string(),
                    }));
                }
                if !m.content.is_empty() {
                    input.push(json!({
                        "role": "assistant",
                        "content": [{"type": "output_text", "text": m.content}],
                    }));
                }
            }
            Role::Tool => input.push(json!({
                "type": "function_call_output",
                "call_id": m.tool_call_id.clone().unwrap_or_default(),
                "output": m.content,
            })),
        }
    }

    let mut body = json!({
        "model": req.model,
        "input": input,
        "stream": stream,
        "temperature": req.temperature,
        "max_output_tokens": req.max_output_tokens,
        "top_p": req.top_p,
    });
    if let Some(effort) = &req.reasoning_effort {
        body["reasoning"] = json!({"effort": effort});
    }
    if !req.tools.is_empty() {
        body["tools"] = Value::Array(
            req.tools
                .iter()
                .map(|t| {
                    json!({
                        "type": "function",
                        "name": t.name,
                        "description": t.description,
                        "parameters": t.parameters,
                    })
                })
                .collect(),
        );
    }
    body
}

struct Extracted {
    text: String,
    reasoning: Option<String>,
    tool_calls: Vec<ToolCall>,
    usage: Option<Usage>,
}

fn extract_response(resp: &Value) -> Extracted {
    let mut text = String::new();
    let mut reasoning = String::new();
    let mut tool_calls = Vec::new();
    let mut usage = None;

    if let Some(t) = resp.get("output_text").and_then(Value::as_str) {
        text.push_str(t);
    }
    if let Some(t) = resp.get("reasoning_text").and_then(Value::as_str) {
        reasoning.push_str(t);
    }

    if let Some(items) = resp.get("output").and_then(Value::as_array) {
        for item in items {
            match item.get("type").and_then(Value::as_str) {
                Some("message") => {
                    if let Some(parts) = item.get("content").and_then(Value::as_array) {
                        for part in parts {
                            let ptype = part.get("type").and_then(Value::as_str).unwrap_or("");
                            if matches!(ptype, "output_text" | "text" | "input_text") {
                                if let Some(t) = part.get("text").and_then(Value::as_str) {
                                    text.push_str(t);
                                }
                            }
                        }
                    }
                }
                // Reasoning output items: summary parts and/or content parts.
                Some("reasoning") => {
                    if let Some(parts) = item.get("summary").and_then(Value::as_array) {
                        for part in parts {
                            if let Some(t) = part.get("text").and_then(Value::as_str) {
                                reasoning.push_str(t);
                            }
                        }
                    }
                    if let Some(parts) = item.get("content").and_then(Value::as_array) {
                        for part in parts {
                            if part.get("type").and_then(Value::as_str) == Some("reasoning_text")
                                && let Some(t) = part.get("text").and_then(Value::as_str)
                            {
                                reasoning.push_str(t);
                            }
                        }
                    }
                }
                Some("function_call") => {
                    tool_calls.push(ToolCall {
                        id: item
                            .get("call_id")
                            .or_else(|| item.get("id"))
                            .and_then(Value::as_str)
                            .unwrap_or("")
                            .to_string(),
                        name: item
                            .get("name")
                            .and_then(Value::as_str)
                            .unwrap_or("")
                            .to_string(),
                        arguments: match item.get("arguments") {
                            Some(Value::String(s)) => serde_json::from_str(s)
                                .unwrap_or_else(|_| json!({ "raw": s })),
                            Some(v @ Value::Object(_)) => v.clone(),
                            _ => json!({}),
                        },
                    });
                }
                _ => {}
            }
        }
    }

    // Chat-completions style fallback.
    if text.is_empty() {
        if let Some(c) = resp
            .pointer("/choices/0/message/content")
            .and_then(Value::as_str)
        {
            text.push_str(c);
        }
        if let Some(c) = resp
            .pointer("/choices/0/message/reasoning_content")
            .and_then(Value::as_str)
        {
            reasoning.push_str(c);
        }
    }

    if let Some(u) = resp.get("usage") {
        let input = u
            .get("input_tokens")
            .or_else(|| u.get("prompt_tokens"))
            .and_then(Value::as_i64);
        let output = u
            .get("output_tokens")
            .or_else(|| u.get("completion_tokens"))
            .and_then(Value::as_i64);
        if let (Some(i), Some(o)) = (input, output) {
            usage = Some(Usage {
                input_tokens: i,
                output_tokens: o,
            });
        }
    }

    Extracted {
        text,
        reasoning: if reasoning.is_empty() { None } else { Some(reasoning) },
        tool_calls,
        usage,
    }
}

#[derive(Default)]
struct SseState {
    text: String,
    reasoning: String,
    done: bool,
}

#[derive(Debug)]
enum SseAction {
    Emit(Result<StreamEvent>),
    None,
}

fn handle_sse_data(state: &mut SseState, event_name: &str, data: &str) -> SseAction {
    let data = data.trim();
    if data.is_empty() {
        return SseAction::None;
    }
    if data == "[DONE]" {
        state.done = true;
        return SseAction::Emit(Ok(StreamEvent::Done(CompletionResponse {
            text: state.text.clone(),
            reasoning: if state.reasoning.is_empty() { None } else { Some(state.reasoning.clone()) },
            tool_calls: vec![],
            usage: None,
            raw: Value::Null,
        })));
    }

    let v: Value = match serde_json::from_str(data) {
        Ok(v) => v,
        Err(_) => return SseAction::None, // keep-alives, comments
    };

    let ty = v
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or(event_name)
        .to_string();

    // Errors first: anything tagged error/failed.
    if ty.contains("error") || ty.ends_with(".failed") {
        return SseAction::Emit(Err(ProviderError::Api {
            provider: "meta",
            status: 0,
            body: data.to_string(),
        }));
    }

    // Reasoning/thinking deltas: e.g. `response.reasoning_summary_text.delta`
    // or `response.reasoning_text.delta`.
    if ty.contains("reasoning") && ty.contains("delta") {
        if let Some(delta) = v
            .get("delta")
            .and_then(Value::as_str)
            .or_else(|| v.get("text").and_then(Value::as_str))
        {
            state.reasoning.push_str(delta);
            return SseAction::Emit(Ok(StreamEvent::Reasoning(delta.to_string())));
        }
        return SseAction::None;
    }

    // Responses-style text deltas.
    if let Some(delta) = v.get("delta").and_then(Value::as_str) {
        if ty.contains("output_text.delta") || ty == "delta" {
            state.text.push_str(delta);
            return SseAction::Emit(Ok(StreamEvent::Delta(delta.to_string())));
        }
    }

    // Chat-completions style chunks (incl. reasoning_content).
    if let Some(delta) = v.pointer("/choices/0/delta/reasoning_content").and_then(Value::as_str) {
        state.reasoning.push_str(delta);
        return SseAction::Emit(Ok(StreamEvent::Reasoning(delta.to_string())));
    }
    if let Some(delta) = v.pointer("/choices/0/delta/content").and_then(Value::as_str) {
        state.text.push_str(delta);
        return SseAction::Emit(Ok(StreamEvent::Delta(delta.to_string())));
    }

    // Completion events.
    if ty.ends_with(".completed") || ty == "response.done" || ty == "done" {
        let inner = v.get("response").unwrap_or(&v);
        let extracted = extract_response(inner);
        let text = if extracted.text.is_empty() && !state.text.is_empty() {
            state.text.clone()
        } else {
            extracted.text
        };
        let reasoning = extracted
            .reasoning
            .filter(|r| !r.is_empty())
            .or_else(|| {
                if state.reasoning.is_empty() {
                    None
                } else {
                    Some(state.reasoning.clone())
                }
            });
        state.done = true;
        return SseAction::Emit(Ok(StreamEvent::Done(CompletionResponse {
            text,
            reasoning,
            tool_calls: extracted.tool_calls,
            usage: extracted.usage,
            raw: inner.clone(),
        })));
    }

    SseAction::None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ChatMessage;
    use serde_json::json;

    #[test]
    fn request_body_shape() {
        let req = CompletionRequest {
            model: "muse-spark-1.3-contributor".into(),
            messages: vec![
                ChatMessage::system("be helpful"),
                ChatMessage::user("hi"),
            ],
            temperature: 0.7,
            max_output_tokens: 512,
            top_p: 1.0,
            reasoning_effort: Some("medium".into()),
            tools: vec![],
            run_id: Some("r1".into()),
            api_key: Some("k".into()),
        };
        let body = build_request_body(req, false);
        assert_eq!(body["model"], "muse-spark-1.3-contributor");
        assert_eq!(body["stream"], false);
        assert_eq!(body["reasoning"]["effort"], "medium");
        assert_eq!(body["input"][0]["role"], "system");
        assert_eq!(body["input"][0]["content"][0]["type"], "input_text");
        assert_eq!(body["input"][1]["role"], "user");
    }

    #[test]
    fn parse_responses_style_output() {
        let resp = json!({
            "id": "resp_1",
            "output": [
                {"type": "reasoning", "summary": []},
                {"type": "message", "role": "assistant", "content": [
                    {"type": "output_text", "text": "Hello "}
                ]},
                {"type": "message", "role": "assistant", "content": [
                    {"type": "output_text", "text": "world"}
                ]}
            ],
            "usage": {"input_tokens": 10, "output_tokens": 5}
        });
        let ex = extract_response(&resp);
        assert_eq!(ex.text, "Hello world");
        assert_eq!(ex.usage.unwrap().output_tokens, 5);
    }

    #[test]
    fn parse_function_calls() {
        let resp = json!({
            "output": [
                {"type": "function_call", "call_id": "c1", "name": "now",
                 "arguments": "{\"tz\":\"utc\"}"}
            ]
        });
        let ex = extract_response(&resp);
        assert_eq!(ex.tool_calls.len(), 1);
        assert_eq!(ex.tool_calls[0].name, "now");
        assert_eq!(ex.tool_calls[0].arguments["tz"], "utc");
    }

    #[test]
    fn sse_delta_and_done() {
        let mut st = SseState::default();
        match handle_sse_data(&mut st, "", r#"{"type":"response.output_text.delta","delta":"Hi"}"#) {
            SseAction::Emit(Ok(StreamEvent::Delta(d))) => assert_eq!(d, "Hi"),
            other => panic!("unexpected {other:?}"),
        }
        match handle_sse_data(&mut st, "", r#"{"type":"response.completed","response":{"output":[{"type":"message","content":[{"type":"output_text","text":"Hi there"}]}]}}"#) {
            SseAction::Emit(Ok(StreamEvent::Done(resp))) => assert_eq!(resp.text, "Hi there"),
            other => panic!("unexpected {other:?}"),
        }
        assert!(st.done);
    }
}
