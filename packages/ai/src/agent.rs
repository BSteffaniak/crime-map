//! Agent loop orchestration.
//!
//! Implements the agentic tool-use loop: user question -> LLM decides
//! tools -> execute tools -> feed results back -> repeat until final answer.

use crime_map_analytics::tools;
use crime_map_analytics_models::{
    ComparePeriodParams, CountIncidentsParams, ListCitiesParams, RankAreaParams,
    TopCrimeTypesParams, TrendParams, tool_definitions,
};
use switchy_database::Database;
use tokio::sync::mpsc;

use crate::providers::{ContentBlock, LlmProvider, Message, MessageContent, StopReason};
use crate::{AgentEvent, AiError};

/// Maximum number of agent loop iterations to prevent infinite loops.
const MAX_ITERATIONS: u32 = 10;

/// System prompt for the crime data AI agent.
fn build_system_prompt(context: &AgentContext) -> String {
    let cities = context
        .available_cities
        .iter()
        .map(|(city, state)| format!("{city}, {state}"))
        .collect::<Vec<_>>()
        .join("; ");

    let date_range = match (&context.min_date, &context.max_date) {
        (Some(min), Some(max)) => format!("from {min} to {max}"),
        _ => "unknown range".to_string(),
    };

    format!(
        r#"You are a crime data analyst AI assistant for a US crime map application. You help users understand crime patterns, trends, and statistics across US cities.

## Available Data
- Cities with data: {cities}
- Date range: {date_range}
- Crime categories: VIOLENT (Homicide, Sexual Assault, Robbery, Aggravated Assault, Simple Assault), PROPERTY (Burglary, Larceny/Theft, Motor Vehicle Theft, Arson, Vandalism), DRUG_NARCOTICS (Drug Possession, Drug Sales/Manufacturing, Drug Equipment), PUBLIC_ORDER (Weapons Violation, DUI, Disorderly Conduct, Trespassing, Prostitution), FRAUD_FINANCIAL (Fraud, Forgery, Embezzlement, Identity Theft), OTHER (Missing Person, Non-Criminal, Unknown)
- Severity levels: 1 (Minimal) through 5 (Critical)
- Geographic resolution: Neighborhoods (real boundary data for many cities) or census tracts as fallback. The rank_areas tool aggregates all tracts within a neighborhood and returns the neighborhood name when available.

## Instructions
1. Use the tools provided to query the crime database. Do NOT make up statistics.
2. Always use list_cities first if you're unsure whether a city is in the dataset.
3. For year-over-year comparisons, use compare_periods with appropriate date ranges.
4. For "safest neighborhood" questions, use rank_areas with safestFirst=true.
5. When comparing cities, call count_incidents for each city separately.
6. Provide specific numbers and percentages in your answers.
7. If the user asks about a city not in the dataset, tell them it's not available.
8. Format your final answer in clear markdown with key statistics bolded.
9. Today's date is {today}. When users say "2025", "this year", "last year", etc., interpret relative to today.
10. Use category names in SCREAMING_SNAKE_CASE when calling tools (e.g., "VIOLENT", "PROPERTY").
11. State abbreviations should be uppercase 2-letter codes (e.g., "IL", "DC", "CA").

Be concise but thorough. Always cite the actual numbers from tool results."#,
        cities = cities,
        date_range = date_range,
        today = chrono::Utc::now().format("%Y-%m-%d"),
    )
}

/// Context information about the available data, used to build the system prompt.
pub struct AgentContext {
    /// Available cities as (city, state) pairs.
    pub available_cities: Vec<(String, String)>,
    /// Earliest date in the dataset.
    pub min_date: Option<String>,
    /// Latest date in the dataset.
    pub max_date: Option<String>,
}

/// Runs the AI agent loop for a user question.
///
/// Sends [`AgentEvent`]s through the provided channel as the agent works.
/// The final event will be either `AgentEvent::Answer` or `AgentEvent::Error`.
///
/// # Errors
///
/// Returns [`AiError`] if the agent loop fails fatally.
pub async fn run_agent(
    provider: &dyn LlmProvider,
    db: &dyn Database,
    context: &AgentContext,
    question: &str,
    tx: mpsc::Sender<AgentEvent>,
) -> Result<(), AiError> {
    let system_prompt = build_system_prompt(context);
    let tools = tool_definitions();

    let mut messages = vec![Message {
        role: "user".to_string(),
        content: MessageContent::Text(question.to_string()),
    }];

    for iteration in 0..MAX_ITERATIONS {
        log::info!("Agent iteration {iteration}");

        let _ = tx
            .send(AgentEvent::Thinking {
                message: if iteration == 0 {
                    "Analyzing your question...".to_string()
                } else {
                    "Processing results and thinking...".to_string()
                },
            })
            .await;

        let response = provider.chat(&system_prompt, &messages, &tools).await?;

        // Check if the model wants to use tools
        if response.stop_reason == StopReason::ToolUse {
            // Extract tool calls
            if !response
                .content
                .iter()
                .any(|b| matches!(b, ContentBlock::ToolUse { .. }))
            {
                // No actual tool calls despite stop_reason — treat as final answer
                let text = extract_text(&response.content);
                let _ = tx.send(AgentEvent::Answer { text }).await;
                return Ok(());
            }

            // Add assistant message with all content blocks
            messages.push(Message {
                role: "assistant".to_string(),
                content: MessageContent::Blocks(response.content.clone()),
            });

            // Execute each tool and collect results
            let mut tool_results = Vec::new();

            for block in &response.content {
                if let ContentBlock::ToolUse { id, name, input } = block {
                    let _ = tx
                        .send(AgentEvent::ToolCall {
                            tool: name.clone(),
                            params: input.clone(),
                        })
                        .await;

                    let result = execute_tool(db, name.as_str(), input).await;

                    let (summary, result_json) = match &result {
                        Ok(json) => {
                            let summary = summarize_tool_result(name, json);
                            let raw = json.to_string();
                            // Truncate large results to avoid overwhelming the LLM
                            let truncated = if raw.len() > MAX_TOOL_RESULT_BYTES {
                                let cut = &raw[..MAX_TOOL_RESULT_BYTES];
                                format!("{cut}... (truncated, {summary})")
                            } else {
                                raw
                            };
                            (summary, truncated)
                        }
                        Err(e) => {
                            let msg = format!("Tool error: {e}");
                            (msg.clone(), msg)
                        }
                    };

                    let _ = tx
                        .send(AgentEvent::ToolResult {
                            tool: name.clone(),
                            summary: summary.clone(),
                        })
                        .await;

                    tool_results.push(ContentBlock::ToolResult {
                        tool_use_id: id.clone(),
                        content: result_json,
                    });
                }
            }

            // Add tool results as a user message
            messages.push(Message {
                role: "user".to_string(),
                content: MessageContent::Blocks(tool_results),
            });
        } else {
            // Model is done — extract final answer
            let text = extract_text(&response.content);
            let _ = tx.send(AgentEvent::Answer { text }).await;
            return Ok(());
        }
    }

    Err(AiError::MaxIterations {
        max_iterations: MAX_ITERATIONS,
    })
}

/// Extracts text content from content blocks.
fn extract_text(blocks: &[ContentBlock]) -> String {
    blocks
        .iter()
        .filter_map(|b| {
            if let ContentBlock::Text { text } = b {
                Some(text.as_str())
            } else {
                None
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Maximum size of a tool result JSON string before truncation.
/// Prevents overwhelming the LLM context window.
const MAX_TOOL_RESULT_BYTES: usize = 8000;

/// Executes a single tool by name with the given parameters.
async fn execute_tool(
    db: &dyn Database,
    name: &str,
    input: &serde_json::Value,
) -> Result<serde_json::Value, AiError> {
    match name {
        "count_incidents" => {
            let params: CountIncidentsParams = serde_json::from_value(input.clone())?;
            let result = tools::count_incidents(db, &params).await?;
            Ok(serde_json::to_value(result).unwrap_or_default())
        }
        "rank_areas" => {
            let params: RankAreaParams = serde_json::from_value(input.clone())?;
            let result = tools::rank_areas(db, &params).await?;
            Ok(serde_json::to_value(result).unwrap_or_default())
        }
        "compare_periods" => {
            let params: ComparePeriodParams = serde_json::from_value(input.clone())?;
            let result = tools::compare_periods(db, &params).await?;
            Ok(serde_json::to_value(result).unwrap_or_default())
        }
        "get_trend" => {
            let params: TrendParams = serde_json::from_value(input.clone())?;
            let result = tools::get_trend(db, &params).await?;
            Ok(serde_json::to_value(result).unwrap_or_default())
        }
        "top_crime_types" => {
            let params: TopCrimeTypesParams = serde_json::from_value(input.clone())?;
            let result = tools::top_crime_types(db, &params).await?;
            Ok(serde_json::to_value(result).unwrap_or_default())
        }
        "list_cities" => {
            let params: ListCitiesParams = serde_json::from_value(input.clone())?;
            let result = tools::list_cities(db, &params).await?;
            Ok(serde_json::to_value(result).unwrap_or_default())
        }
        other => Err(AiError::Provider {
            message: format!("Unknown tool: {other}"),
        }),
    }
}

/// Creates a brief human-readable summary of a tool result.
fn summarize_tool_result(tool_name: &str, result: &serde_json::Value) -> String {
    match tool_name {
        "count_incidents" => {
            let total = result["total"].as_u64().unwrap_or(0);
            let area = result["areaDescription"].as_str().unwrap_or("unknown area");
            format!("Found {total} incidents in {area}")
        }
        "rank_areas" => {
            let count = result["areas"].as_array().map_or(0, Vec::len);
            format!("Ranked {count} areas")
        }
        "compare_periods" => {
            let pct = result["overall"]["percentChange"].as_f64().unwrap_or(0.0);
            let direction = if pct > 0.0 { "increase" } else { "decrease" };
            format!("{:.1}% {direction}", pct.abs())
        }
        "get_trend" => {
            let points = result["data"].as_array().map_or(0, Vec::len);
            format!("{points} data points")
        }
        "top_crime_types" => {
            let total = result["total"].as_u64().unwrap_or(0);
            format!("{total} total incidents analyzed")
        }
        "list_cities" => {
            let count = result["cities"].as_array().map_or(0, Vec::len);
            format!("{count} cities available")
        }
        _ => "Result received".to_string(),
    }
}
