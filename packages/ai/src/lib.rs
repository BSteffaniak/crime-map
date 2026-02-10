#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions, clippy::cargo_common_metadata)]

//! AI agent with LLM provider abstraction and tool-use agentic loop.
//!
//! Supports Anthropic Claude, `OpenAI` GPT-4, and AWS Bedrock (feature-gated)
//! via a provider-agnostic trait. The agent loop orchestrates multi-step tool
//! calling: the LLM decides which analytics tools to invoke, the backend
//! executes them, and results are fed back until the LLM produces a final
//! answer.

pub mod agent;
pub mod providers;

use thiserror::Error;

/// Errors that can occur during AI operations.
#[derive(Debug, Error)]
pub enum AiError {
    /// HTTP request to LLM provider failed.
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    /// JSON serialization/deserialization failed.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Analytics tool execution failed.
    #[error("Tool execution error: {0}")]
    ToolExecution(#[from] crime_map_analytics::AnalyticsError),

    /// Provider-specific error.
    #[error("Provider error: {message}")]
    Provider {
        /// Description of what went wrong.
        message: String,
    },

    /// Agent loop exceeded maximum iterations.
    #[error("Agent loop exceeded maximum of {max_iterations} iterations")]
    MaxIterations {
        /// The configured maximum.
        max_iterations: u32,
    },

    /// Configuration error.
    #[error("Configuration error: {message}")]
    Config {
        /// Description.
        message: String,
    },
}

/// Events emitted by the agent during execution.
///
/// These are streamed to the frontend via SSE.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum AgentEvent {
    /// Agent is thinking / planning.
    Thinking {
        /// Brief description of what the agent is considering.
        message: String,
    },
    /// Agent is invoking a tool.
    ToolCall {
        /// Tool name.
        tool: String,
        /// Tool parameters (JSON).
        params: serde_json::Value,
    },
    /// Tool execution completed.
    ToolResult {
        /// Tool name.
        tool: String,
        /// Brief summary of the result.
        summary: String,
    },
    /// Agent has produced a final answer.
    Answer {
        /// The markdown-formatted answer.
        text: String,
    },
    /// An error occurred.
    Error {
        /// Error message.
        message: String,
    },
}
