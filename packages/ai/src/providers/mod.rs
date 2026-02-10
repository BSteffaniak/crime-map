//! LLM provider abstraction and implementations.
//!
//! Supports Anthropic Claude and `OpenAI` via a common trait.

pub mod anthropic;
pub mod openai;

use serde::{Deserialize, Serialize};

use crate::AiError;

/// A message in the conversation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    /// Role: "system", "user", "assistant", or "tool".
    pub role: String,
    /// Message content.
    pub content: MessageContent,
}

/// Content of a message — either simple text or structured blocks.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MessageContent {
    /// Simple text content.
    Text(String),
    /// Structured content blocks (for tool results, etc.).
    Blocks(Vec<ContentBlock>),
}

/// A structured content block within a message.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentBlock {
    /// Text content.
    Text {
        /// The text.
        text: String,
    },
    /// A tool use request from the assistant.
    ToolUse {
        /// Unique ID for this tool use.
        id: String,
        /// Tool name.
        name: String,
        /// Tool input parameters.
        input: serde_json::Value,
    },
    /// A tool result being sent back.
    ToolResult {
        /// The `tool_use` ID this result corresponds to.
        tool_use_id: String,
        /// The result content.
        content: String,
    },
}

/// Response from the LLM provider.
#[derive(Debug, Clone)]
pub struct LlmResponse {
    /// Content blocks in the response.
    pub content: Vec<ContentBlock>,
    /// Whether the model wants to use tools (vs. providing a final answer).
    pub stop_reason: StopReason,
}

/// Why the model stopped generating.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StopReason {
    /// Model finished its response naturally.
    EndTurn,
    /// Model wants to call one or more tools.
    ToolUse,
    /// Maximum tokens reached.
    MaxTokens,
}

/// Trait for LLM providers.
#[async_trait::async_trait]
pub trait LlmProvider: Send + Sync {
    /// Send a chat completion request with tool definitions.
    ///
    /// # Errors
    ///
    /// Returns [`AiError`] if the request fails.
    async fn chat(
        &self,
        system_prompt: &str,
        messages: &[Message],
        tools: &[serde_json::Value],
    ) -> Result<LlmResponse, AiError>;
}

/// Creates an LLM provider based on environment variables.
///
/// Checks `AI_PROVIDER` (default: "anthropic") and uses the corresponding
/// API key env var (`ANTHROPIC_API_KEY` or `OPENAI_API_KEY`).
///
/// # Errors
///
/// Returns [`AiError::Config`] if the required API key is not set.
pub fn create_provider_from_env() -> Result<Box<dyn LlmProvider>, AiError> {
    let provider = std::env::var("AI_PROVIDER").unwrap_or_else(|_| "anthropic".to_string());

    match provider.to_lowercase().as_str() {
        "anthropic" | "claude" => {
            let api_key = std::env::var("ANTHROPIC_API_KEY").map_err(|_| AiError::Config {
                message: "ANTHROPIC_API_KEY environment variable not set".to_string(),
            })?;
            let model = std::env::var("AI_MODEL")
                .unwrap_or_else(|_| "claude-sonnet-4-20250514".to_string());
            Ok(Box::new(anthropic::AnthropicProvider::new(api_key, model)))
        }
        "openai" | "gpt" => {
            let api_key = std::env::var("OPENAI_API_KEY").map_err(|_| AiError::Config {
                message: "OPENAI_API_KEY environment variable not set".to_string(),
            })?;
            let model = std::env::var("AI_MODEL").unwrap_or_else(|_| "gpt-4o".to_string());
            Ok(Box::new(openai::OpenAiProvider::new(api_key, model)))
        }
        other => Err(AiError::Config {
            message: format!("Unknown AI provider: {other}. Use 'anthropic' or 'openai'."),
        }),
    }
}
