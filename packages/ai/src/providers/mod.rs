//! LLM provider abstraction and implementations.
//!
//! Supports Anthropic Claude, `OpenAI`, and AWS Bedrock via a common trait.

pub mod anthropic;
#[cfg(feature = "bedrock")]
pub mod bedrock;
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
/// If `AI_PROVIDER` is explicitly set, uses that provider. Otherwise
/// auto-detects from available credentials:
///
/// 1. `ANTHROPIC_API_KEY` set -> Anthropic Claude
/// 2. `OPENAI_API_KEY` set -> `OpenAI` GPT-4
/// 3. AWS credentials available (`AWS_ACCESS_KEY_ID`, `AWS_PROFILE`,
///    or IAM role on EC2/ECS) -> Bedrock
///
/// # Errors
///
/// Returns [`AiError::Config`] if no credentials are found or the
/// explicitly requested provider is not configured.
#[allow(clippy::unused_async)] // async is needed when bedrock feature is enabled
pub async fn create_provider_from_env() -> Result<Box<dyn LlmProvider>, AiError> {
    let provider = std::env::var("AI_PROVIDER").unwrap_or_else(|_| detect_provider());

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
        #[cfg(feature = "bedrock")]
        "bedrock" | "aws" => {
            let model = std::env::var("AI_MODEL")
                .unwrap_or_else(|_| "us.anthropic.claude-sonnet-4-20250514-v1:0".to_string());
            let region = std::env::var("AWS_REGION")
                .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
                .ok()
                .or_else(|| {
                    // Bearer token auth requires a region for endpoint resolution.
                    // Default to us-east-1 when no region is configured.
                    if std::env::var("AWS_BEARER_TOKEN_BEDROCK").is_ok() {
                        log::info!(
                            "No AWS_REGION set; defaulting to us-east-1 for Bedrock bearer token auth"
                        );
                        Some("us-east-1".to_string())
                    } else {
                        None
                    }
                });
            let provider = bedrock::BedrockProvider::new(model, region).await;
            Ok(Box::new(provider))
        }
        #[cfg(not(feature = "bedrock"))]
        "bedrock" | "aws" => Err(AiError::Config {
            message: "Bedrock support not compiled. Rebuild with --features bedrock".to_string(),
        }),
        other => Err(AiError::Config {
            message: format!(
                "Unknown AI provider: {other}. Use 'anthropic', 'openai', or 'bedrock'."
            ),
        }),
    }
}

/// Auto-detects which provider to use based on available credentials.
///
/// Checks for credentials in priority order: Bedrock bearer token,
/// Anthropic, `OpenAI`, AWS credential chain.
/// Returns a provider name string that matches the arms in
/// [`create_provider_from_env`].
fn detect_provider() -> String {
    // Bedrock bearer token is highest priority — it's the simplest
    // single-env-var setup (from the Amazon Bedrock console).
    if std::env::var("AWS_BEARER_TOKEN_BEDROCK").is_ok() {
        log::info!("Auto-detected AI provider: Bedrock (AWS_BEARER_TOKEN_BEDROCK found)");
        return "bedrock".to_string();
    }

    if std::env::var("ANTHROPIC_API_KEY").is_ok() {
        log::info!("Auto-detected AI provider: Anthropic (ANTHROPIC_API_KEY found)");
        return "anthropic".to_string();
    }

    if std::env::var("OPENAI_API_KEY").is_ok() {
        log::info!("Auto-detected AI provider: OpenAI (OPENAI_API_KEY found)");
        return "openai".to_string();
    }

    // Check for AWS credentials — any of these indicate Bedrock is available
    let has_aws_keys = std::env::var("AWS_ACCESS_KEY_ID").is_ok();
    let has_aws_profile = std::env::var("AWS_PROFILE").is_ok();
    let has_aws_role = std::env::var("AWS_ROLE_ARN").is_ok()
        || std::env::var("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI").is_ok();

    if has_aws_keys || has_aws_profile || has_aws_role {
        log::info!("Auto-detected AI provider: Bedrock (AWS credentials found)");
        return "bedrock".to_string();
    }

    log::warn!(
        "No AI credentials detected. Set one of: AWS_BEARER_TOKEN_BEDROCK, \
         ANTHROPIC_API_KEY, OPENAI_API_KEY, or AWS credentials \
         (AWS_ACCESS_KEY_ID/AWS_PROFILE). You can also set AI_PROVIDER explicitly."
    );

    // Fall back to anthropic — will produce a clear error about missing key
    "anthropic".to_string()
}
