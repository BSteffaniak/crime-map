//! Anthropic Claude provider implementation.

use serde::{Deserialize, Serialize};

use super::{ContentBlock, LlmProvider, LlmResponse, Message, MessageContent, StopReason};
use crate::AiError;

/// Anthropic Claude API provider.
pub struct AnthropicProvider {
    api_key: String,
    model: String,
    client: reqwest::Client,
}

impl AnthropicProvider {
    /// Creates a new Anthropic provider.
    #[must_use]
    pub fn new(api_key: String, model: String) -> Self {
        Self {
            api_key,
            model,
            client: reqwest::Client::new(),
        }
    }
}

/// Anthropic API request body.
#[derive(Serialize)]
struct AnthropicRequest<'a> {
    model: &'a str,
    max_tokens: u32,
    system: &'a str,
    messages: Vec<AnthropicMessage>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tools: Vec<serde_json::Value>,
}

#[derive(Serialize)]
struct AnthropicMessage {
    role: String,
    content: serde_json::Value,
}

/// Anthropic API response body.
#[derive(Deserialize)]
struct AnthropicResponse {
    content: Vec<AnthropicContentBlock>,
    stop_reason: Option<String>,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum AnthropicContentBlock {
    Text {
        text: String,
    },
    ToolUse {
        id: String,
        name: String,
        input: serde_json::Value,
    },
}

/// Anthropic API error response.
#[derive(Deserialize)]
struct AnthropicError {
    error: AnthropicErrorDetail,
}

#[derive(Deserialize)]
struct AnthropicErrorDetail {
    message: String,
}

#[async_trait::async_trait]
impl LlmProvider for AnthropicProvider {
    async fn chat(
        &self,
        system_prompt: &str,
        messages: &[Message],
        tools: &[serde_json::Value],
    ) -> Result<LlmResponse, AiError> {
        let api_messages: Vec<AnthropicMessage> = messages
            .iter()
            .map(|m| {
                let content = match &m.content {
                    MessageContent::Text(text) => serde_json::json!(text),
                    MessageContent::Blocks(blocks) => {
                        let json_blocks: Vec<serde_json::Value> = blocks
                            .iter()
                            .map(|b| match b {
                                ContentBlock::Text { text } => {
                                    serde_json::json!({ "type": "text", "text": text })
                                }
                                ContentBlock::ToolUse { id, name, input } => {
                                    serde_json::json!({
                                        "type": "tool_use",
                                        "id": id,
                                        "name": name,
                                        "input": input,
                                    })
                                }
                                ContentBlock::ToolResult {
                                    tool_use_id,
                                    content,
                                } => {
                                    serde_json::json!({
                                        "type": "tool_result",
                                        "tool_use_id": tool_use_id,
                                        "content": content,
                                    })
                                }
                            })
                            .collect();
                        serde_json::json!(json_blocks)
                    }
                };
                AnthropicMessage {
                    role: m.role.clone(),
                    content,
                }
            })
            .collect();

        // Convert tool definitions to Anthropic format
        let anthropic_tools: Vec<serde_json::Value> = tools
            .iter()
            .map(|t| {
                serde_json::json!({
                    "name": t["name"],
                    "description": t["description"],
                    "input_schema": t["parameters"],
                })
            })
            .collect();

        let request = AnthropicRequest {
            model: &self.model,
            max_tokens: 4096,
            system: system_prompt,
            messages: api_messages,
            tools: anthropic_tools,
        };

        let resp = self
            .client
            .post("https://api.anthropic.com/v1/messages")
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", "2023-06-01")
            .header("content-type", "application/json")
            .json(&request)
            .send()
            .await?;

        let status = resp.status();
        let body = resp.text().await?;

        if !status.is_success() {
            let err: AnthropicError =
                serde_json::from_str(&body).unwrap_or_else(|_| AnthropicError {
                    error: AnthropicErrorDetail {
                        message: format!("HTTP {status}: {body}"),
                    },
                });
            return Err(AiError::Provider {
                message: err.error.message,
            });
        }

        let response: AnthropicResponse = serde_json::from_str(&body)?;

        let content: Vec<ContentBlock> = response
            .content
            .into_iter()
            .map(|block| match block {
                AnthropicContentBlock::Text { text } => ContentBlock::Text { text },
                AnthropicContentBlock::ToolUse { id, name, input } => {
                    ContentBlock::ToolUse { id, name, input }
                }
            })
            .collect();

        let stop_reason = match response.stop_reason.as_deref() {
            Some("tool_use") => StopReason::ToolUse,
            Some("max_tokens") => StopReason::MaxTokens,
            _ => StopReason::EndTurn,
        };

        Ok(LlmResponse {
            content,
            stop_reason,
        })
    }
}
