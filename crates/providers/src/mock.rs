//! Offline mock provider — always registered, used for tests and for
//! trying the platform without any API key. Deterministic and never
//! touches the network.

use std::time::Duration;

use async_trait::async_trait;
use futures::stream::BoxStream;
use serde_json::json;
use tracing::Instrument;

use crate::{
    CompletionRequest, CompletionResponse, Provider, Result, Role, StreamEvent, Usage,
};

pub struct MockProvider;

impl MockProvider {
    pub fn new() -> Self {
        Self
    }
}

impl Default for MockProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Provider for MockProvider {
    fn id(&self) -> &'static str {
        "mock"
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
        let reader_span = span.clone();
        self.complete_stream_impl(req, reader_span)
            .instrument(span)
            .await
    }
}

impl MockProvider {
    async fn complete_impl(&self, req: CompletionRequest) -> Result<CompletionResponse> {
        let last_user = req
            .messages
            .iter()
            .rev()
            .find(|m| m.role == Role::User)
            .map(|m| m.content.clone())
            .unwrap_or_default();

        if last_user.starts_with("#fail") {
            return Err(provider_error_for_tests());
        }
        if last_user.starts_with("#sleep") {
            tokio::time::sleep(Duration::from_secs(10)).await;
        }

        let text = format!("[mock:{}] you said: {last_user}", req.model);
        Ok(CompletionResponse {
            text,
            reasoning: None,
            tool_calls: vec![],
            usage: Some(Usage {
                input_tokens: 1,
                output_tokens: 1,
            }),
            raw: json!({"mock": true}),
        })
    }

    async fn complete_stream_impl(
        &self,
        req: CompletionRequest,
        reader_span: tracing::Span,
    ) -> Result<BoxStream<'static, Result<StreamEvent>>> {
        let last_user = req
            .messages
            .iter()
            .rev()
            .find(|m| m.role == Role::User)
            .map(|m| m.content.clone())
            .unwrap_or_default();
        let full = format!("[mock:{}] you said: {last_user}", req.model);

        let (tx, rx) = tokio::sync::mpsc::channel::<Result<StreamEvent>>(8);
        tokio::spawn(
            async move {
                let words: Vec<&str> = full.split(' ').collect();
                let mut acc = String::new();
                for (i, w) in words.iter().enumerate() {
                    let piece = if i == 0 {
                        (*w).to_string()
                    } else {
                        format!(" {w}")
                    };
                    acc.push_str(&piece);
                    if tx.send(Ok(StreamEvent::Delta(piece))).await.is_err() {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                let _ = tx
                    .send(Ok(StreamEvent::Done(CompletionResponse {
                        text: acc,
                        reasoning: None,
                        tool_calls: vec![],
                        usage: Some(Usage {
                            input_tokens: 1,
                            output_tokens: 1,
                        }),
                        raw: json!({"mock": true}),
                    })))
                    .await;
            }
            // Span stays open until the reader task finishes.
            .instrument(reader_span),
        );
        Ok(Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }
}

// Small helper so the error construction stays inside the module without
// importing the whole error type at the top.
fn provider_error_for_tests() -> crate::ProviderError {
    crate::ProviderError::Api {
        provider: "mock",
        status: 500,
        body: "mock failure requested (#fail)".into(),
    }
}
