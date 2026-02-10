//! AWS Bedrock provider implementation using the Converse API.

use aws_sdk_bedrockruntime::types::{
    self as bedrock, ContentBlock as BedrockContent, ConversationRole, Message as BedrockMessage,
    StopReason as BedrockStopReason, SystemContentBlock, Tool, ToolConfiguration, ToolInputSchema,
    ToolResultContentBlock, ToolResultStatus, ToolSpecification,
};
use aws_smithy_types::Document;

use super::{ContentBlock, LlmProvider, LlmResponse, Message, MessageContent, StopReason};
use crate::AiError;

/// AWS Bedrock provider using the Converse API.
///
/// Supports any model available on Bedrock that supports tool use
/// (Claude, Llama, Mistral, etc.). Authentication uses the standard
/// AWS credential chain (env vars, IAM role, `~/.aws/credentials`).
pub struct BedrockProvider {
    client: aws_sdk_bedrockruntime::Client,
    model_id: String,
}

impl BedrockProvider {
    /// Creates a new Bedrock provider.
    ///
    /// Loads AWS configuration from the environment (region, credentials).
    /// The `model_id` should be a Bedrock model ID such as
    /// `us.anthropic.claude-sonnet-4-20250514-v1:0`.
    pub async fn new(model_id: String, region: Option<String>) -> Self {
        let mut config_loader = aws_config::defaults(aws_config::BehaviorVersion::latest());

        if let Some(region) = region {
            config_loader = config_loader.region(aws_config::Region::new(region));
        }

        let config = config_loader.load().await;
        let client = aws_sdk_bedrockruntime::Client::new(&config);

        Self { client, model_id }
    }
}

#[async_trait::async_trait]
impl LlmProvider for BedrockProvider {
    #[allow(clippy::too_many_lines)]
    async fn chat(
        &self,
        system_prompt: &str,
        messages: &[Message],
        tools: &[serde_json::Value],
    ) -> Result<LlmResponse, AiError> {
        // Convert messages to Bedrock format
        let bedrock_messages = convert_messages(messages)?;

        // Convert tool definitions to Bedrock format
        let tool_config = convert_tools(tools);

        // Build the request
        let mut request = self
            .client
            .converse()
            .model_id(&self.model_id)
            .system(SystemContentBlock::Text(system_prompt.to_string()))
            .set_messages(Some(bedrock_messages))
            .inference_config(
                bedrock::InferenceConfiguration::builder()
                    .max_tokens(4096)
                    .build(),
            );

        if !tools.is_empty() {
            request = request.tool_config(tool_config);
        }

        let response = request.send().await.map_err(|e| AiError::Provider {
            message: format!("Bedrock Converse error: {e}"),
        })?;

        // Extract the response message
        let output = response.output().ok_or_else(|| AiError::Provider {
            message: "No output in Bedrock response".to_string(),
        })?;

        let bedrock::ConverseOutput::Message(response_msg) = output else {
            return Err(AiError::Provider {
                message: "Unexpected Bedrock output variant".to_string(),
            });
        };

        // Convert content blocks
        let mut content_blocks = Vec::new();
        for block in response_msg.content() {
            match block {
                BedrockContent::Text(text) => {
                    content_blocks.push(ContentBlock::Text { text: text.clone() });
                }
                BedrockContent::ToolUse(tool_use) => {
                    let input_json = document_to_json(tool_use.input());
                    content_blocks.push(ContentBlock::ToolUse {
                        id: tool_use.tool_use_id().to_string(),
                        name: tool_use.name().to_string(),
                        input: input_json,
                    });
                }
                _ => {
                    // Skip unsupported block types (image, audio, etc.)
                }
            }
        }

        // Map stop reason
        let stop_reason = match response.stop_reason() {
            BedrockStopReason::ToolUse => StopReason::ToolUse,
            BedrockStopReason::MaxTokens => StopReason::MaxTokens,
            _ => StopReason::EndTurn,
        };

        Ok(LlmResponse {
            content: content_blocks,
            stop_reason,
        })
    }
}

/// Converts our internal messages to Bedrock `Message` format.
fn convert_messages(messages: &[Message]) -> Result<Vec<BedrockMessage>, AiError> {
    let mut bedrock_msgs = Vec::new();

    for msg in messages {
        let role = match msg.role.as_str() {
            "user" => ConversationRole::User,
            "assistant" => ConversationRole::Assistant,
            other => {
                return Err(AiError::Provider {
                    message: format!("Unsupported message role for Bedrock: {other}"),
                });
            }
        };

        let content_blocks = match &msg.content {
            MessageContent::Text(text) => {
                vec![BedrockContent::Text(text.clone())]
            }
            MessageContent::Blocks(blocks) => {
                let mut bedrock_blocks = Vec::new();
                for block in blocks {
                    match block {
                        ContentBlock::Text { text } => {
                            bedrock_blocks.push(BedrockContent::Text(text.clone()));
                        }
                        ContentBlock::ToolUse { id, name, input } => {
                            let doc = json_to_document(input);
                            let tool_use = bedrock::ToolUseBlock::builder()
                                .tool_use_id(id.as_str())
                                .name(name.as_str())
                                .input(doc)
                                .build()
                                .map_err(|e| AiError::Provider {
                                    message: format!("Failed to build ToolUseBlock: {e}"),
                                })?;
                            bedrock_blocks.push(BedrockContent::ToolUse(tool_use));
                        }
                        ContentBlock::ToolResult {
                            tool_use_id,
                            content,
                        } => {
                            let result = bedrock::ToolResultBlock::builder()
                                .tool_use_id(tool_use_id.as_str())
                                .content(ToolResultContentBlock::Text(content.clone()))
                                .status(ToolResultStatus::Success)
                                .build()
                                .map_err(|e| AiError::Provider {
                                    message: format!("Failed to build ToolResultBlock: {e}"),
                                })?;
                            bedrock_blocks.push(BedrockContent::ToolResult(result));
                        }
                    }
                }
                bedrock_blocks
            }
        };

        let bedrock_msg = BedrockMessage::builder()
            .role(role)
            .set_content(Some(content_blocks))
            .build()
            .map_err(|e| AiError::Provider {
                message: format!("Failed to build Bedrock Message: {e}"),
            })?;

        bedrock_msgs.push(bedrock_msg);
    }

    Ok(bedrock_msgs)
}

/// Converts our JSON tool definitions to Bedrock `ToolConfiguration`.
fn convert_tools(tools: &[serde_json::Value]) -> ToolConfiguration {
    let bedrock_tools: Vec<Tool> = tools
        .iter()
        .filter_map(|t| {
            let name = t["name"].as_str()?;
            let description = t["description"].as_str().unwrap_or("");
            let params = &t["parameters"];
            let input_schema = json_to_document(params);

            let spec = ToolSpecification::builder()
                .name(name)
                .description(description)
                .input_schema(ToolInputSchema::Json(input_schema))
                .build()
                .ok()?;

            Some(Tool::ToolSpec(spec))
        })
        .collect();

    ToolConfiguration::builder()
        .set_tools(Some(bedrock_tools))
        .build()
        .unwrap_or_else(|_| {
            ToolConfiguration::builder()
                .build()
                .expect("empty ToolConfiguration should always build")
        })
}

/// Converts a `serde_json::Value` to an `aws_smithy_types::Document`.
#[allow(clippy::option_if_let_else)]
fn json_to_document(value: &serde_json::Value) -> Document {
    match value {
        serde_json::Value::Null => Document::Null,
        serde_json::Value::Bool(b) => Document::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                if i >= 0 {
                    Document::Number(aws_smithy_types::Number::PosInt(i.cast_unsigned()))
                } else {
                    Document::Number(aws_smithy_types::Number::NegInt(i))
                }
            } else if let Some(f) = n.as_f64() {
                Document::Number(aws_smithy_types::Number::Float(f))
            } else {
                Document::Null
            }
        }
        serde_json::Value::String(s) => Document::String(s.clone()),
        serde_json::Value::Array(arr) => {
            Document::Array(arr.iter().map(json_to_document).collect())
        }
        serde_json::Value::Object(obj) => {
            let map = obj
                .iter()
                .map(|(k, v)| (k.clone(), json_to_document(v)))
                .collect();
            Document::Object(map)
        }
    }
}

/// Converts an `aws_smithy_types::Document` to a `serde_json::Value`.
fn document_to_json(doc: &Document) -> serde_json::Value {
    match doc {
        Document::Null => serde_json::Value::Null,
        Document::Bool(b) => serde_json::Value::Bool(*b),
        Document::Number(n) => match *n {
            aws_smithy_types::Number::PosInt(i) => serde_json::json!(i),
            aws_smithy_types::Number::NegInt(i) => serde_json::json!(i),
            aws_smithy_types::Number::Float(f) => serde_json::Value::Number(
                serde_json::Number::from_f64(f).unwrap_or_else(|| serde_json::Number::from(0)),
            ),
        },
        Document::String(s) => serde_json::Value::String(s.clone()),
        Document::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(document_to_json).collect())
        }
        Document::Object(obj) => {
            let map: serde_json::Map<String, serde_json::Value> = obj
                .iter()
                .map(|(k, v)| (k.clone(), document_to_json(v)))
                .collect();
            serde_json::Value::Object(map)
        }
    }
}
