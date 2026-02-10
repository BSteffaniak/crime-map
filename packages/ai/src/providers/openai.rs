//! `OpenAI` GPT provider implementation.

use serde::{Deserialize, Serialize};

use super::{ContentBlock, LlmProvider, LlmResponse, Message, MessageContent, StopReason};
use crate::AiError;

/// `OpenAI` API provider.
pub struct OpenAiProvider {
    api_key: String,
    model: String,
    client: reqwest::Client,
}

impl OpenAiProvider {
    /// Creates a new `OpenAI` provider.
    #[must_use]
    pub fn new(api_key: String, model: String) -> Self {
        Self {
            api_key,
            model,
            client: reqwest::Client::new(),
        }
    }
}

#[derive(Serialize)]
struct OpenAiRequest<'a> {
    model: &'a str,
    messages: Vec<OpenAiMessage>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tools: Vec<OpenAiTool>,
    max_tokens: u32,
}

#[derive(Serialize)]
struct OpenAiMessage {
    role: String,
    content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_calls: Option<Vec<OpenAiToolCall>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_call_id: Option<String>,
}

#[derive(Serialize, Deserialize, Clone)]
struct OpenAiToolCall {
    id: String,
    #[serde(rename = "type")]
    call_type: String,
    function: OpenAiFunction,
}

#[derive(Serialize, Deserialize, Clone)]
struct OpenAiFunction {
    name: String,
    arguments: String,
}

#[derive(Serialize)]
struct OpenAiTool {
    #[serde(rename = "type")]
    tool_type: String,
    function: OpenAiToolFunction,
}

#[derive(Serialize)]
struct OpenAiToolFunction {
    name: String,
    description: String,
    parameters: serde_json::Value,
}

#[derive(Deserialize)]
struct OpenAiResponse {
    choices: Vec<OpenAiChoice>,
}

#[derive(Deserialize)]
struct OpenAiChoice {
    message: OpenAiResponseMessage,
    finish_reason: Option<String>,
}

#[derive(Deserialize)]
struct OpenAiResponseMessage {
    content: Option<String>,
    tool_calls: Option<Vec<OpenAiToolCall>>,
}

#[derive(Deserialize)]
struct OpenAiError {
    error: OpenAiErrorDetail,
}

#[derive(Deserialize)]
struct OpenAiErrorDetail {
    message: String,
}

#[async_trait::async_trait]
impl LlmProvider for OpenAiProvider {
    #[allow(clippy::too_many_lines)]
    async fn chat(
        &self,
        system_prompt: &str,
        messages: &[Message],
        tools: &[serde_json::Value],
    ) -> Result<LlmResponse, AiError> {
        let mut api_messages = vec![OpenAiMessage {
            role: "system".to_string(),
            content: Some(system_prompt.to_string()),
            tool_calls: None,
            tool_call_id: None,
        }];

        for msg in messages {
            match &msg.content {
                MessageContent::Text(text) => {
                    api_messages.push(OpenAiMessage {
                        role: msg.role.clone(),
                        content: Some(text.clone()),
                        tool_calls: None,
                        tool_call_id: None,
                    });
                }
                MessageContent::Blocks(blocks) => {
                    // For assistant messages with tool_use, convert to OpenAI format
                    if msg.role == "assistant" {
                        let tool_calls: Vec<OpenAiToolCall> = blocks
                            .iter()
                            .filter_map(|b| {
                                if let ContentBlock::ToolUse { id, name, input } = b {
                                    Some(OpenAiToolCall {
                                        id: id.clone(),
                                        call_type: "function".to_string(),
                                        function: OpenAiFunction {
                                            name: name.clone(),
                                            arguments: serde_json::to_string(input)
                                                .unwrap_or_default(),
                                        },
                                    })
                                } else {
                                    None
                                }
                            })
                            .collect();

                        let text: String = blocks
                            .iter()
                            .filter_map(|b| {
                                if let ContentBlock::Text { text } = b {
                                    Some(text.as_str())
                                } else {
                                    None
                                }
                            })
                            .collect::<Vec<_>>()
                            .join("\n");

                        api_messages.push(OpenAiMessage {
                            role: "assistant".to_string(),
                            content: if text.is_empty() { None } else { Some(text) },
                            tool_calls: if tool_calls.is_empty() {
                                None
                            } else {
                                Some(tool_calls)
                            },
                            tool_call_id: None,
                        });
                    } else {
                        // For user messages with tool results
                        for block in blocks {
                            if let ContentBlock::ToolResult {
                                tool_use_id,
                                content,
                            } = block
                            {
                                api_messages.push(OpenAiMessage {
                                    role: "tool".to_string(),
                                    content: Some(content.clone()),
                                    tool_calls: None,
                                    tool_call_id: Some(tool_use_id.clone()),
                                });
                            }
                        }
                    }
                }
            }
        }

        // Convert tools to OpenAI format
        let openai_tools: Vec<OpenAiTool> = tools
            .iter()
            .map(|t| OpenAiTool {
                tool_type: "function".to_string(),
                function: OpenAiToolFunction {
                    name: t["name"].as_str().unwrap_or("").to_string(),
                    description: t["description"].as_str().unwrap_or("").to_string(),
                    parameters: t["parameters"].clone(),
                },
            })
            .collect();

        let request = OpenAiRequest {
            model: &self.model,
            messages: api_messages,
            tools: openai_tools,
            max_tokens: 4096,
        };

        let resp = self
            .client
            .post("https://api.openai.com/v1/chat/completions")
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .await?;

        let status = resp.status();
        let body = resp.text().await?;

        if !status.is_success() {
            let err: OpenAiError = serde_json::from_str(&body).unwrap_or_else(|_| OpenAiError {
                error: OpenAiErrorDetail {
                    message: format!("HTTP {status}: {body}"),
                },
            });
            return Err(AiError::Provider {
                message: err.error.message,
            });
        }

        let response: OpenAiResponse = serde_json::from_str(&body)?;

        let choice = response
            .choices
            .into_iter()
            .next()
            .ok_or_else(|| AiError::Provider {
                message: "No choices in OpenAI response".to_string(),
            })?;

        let mut content_blocks = Vec::new();

        if let Some(text) = choice.message.content
            && !text.is_empty()
        {
            content_blocks.push(ContentBlock::Text { text });
        }

        if let Some(tool_calls) = choice.message.tool_calls {
            for tc in tool_calls {
                let input: serde_json::Value = serde_json::from_str(&tc.function.arguments)
                    .unwrap_or_else(|_| serde_json::json!({}));
                content_blocks.push(ContentBlock::ToolUse {
                    id: tc.id,
                    name: tc.function.name,
                    input,
                });
            }
        }

        let stop_reason = match choice.finish_reason.as_deref() {
            Some("tool_calls") => StopReason::ToolUse,
            Some("length") => StopReason::MaxTokens,
            _ => {
                // If there are tool use blocks, it's a tool use stop
                if content_blocks
                    .iter()
                    .any(|b| matches!(b, ContentBlock::ToolUse { .. }))
                {
                    StopReason::ToolUse
                } else {
                    StopReason::EndTurn
                }
            }
        };

        Ok(LlmResponse {
            content: content_blocks,
            stop_reason,
        })
    }
}
