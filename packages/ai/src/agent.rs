//! Agent loop orchestration.
//!
//! Implements the agentic tool-use loop: user question -> LLM decides
//! tools -> execute tools -> feed results back -> repeat until final answer.
//!
//! The agent is governed by [`AgentLimits`] which provide multi-pronged
//! safeguards:
//!
//! - **Per-tool timeout**: individual tool calls that exceed the limit get
//!   an error injected back into the conversation so the LLM can retry or
//!   pivot.
//! - **Per-LLM-call timeout**: prevents hangs when the provider is slow.
//! - **Tool call budget**: soft limit nudges the agent to wrap up; hard
//!   limit forces a final answer.
//! - **Duration budget**: soft limit nudges; hard limit forces graceful
//!   termination with whatever findings the agent has so far.

use std::fmt::Write;
use std::time::{Duration, Instant};

use crime_map_analytics::tools;
use crime_map_analytics_models::{
    ComparePeriodParams, CountIncidentsParams, ListCitiesParams, RankAreaParams,
    SearchLocationsParams, TopCrimeTypesParams, TrendParams, tool_definitions,
};
use switchy_database::Database;
use tokio::sync::mpsc;

use crate::providers::{ContentBlock, LlmProvider, Message, MessageContent, StopReason};
use crate::{AgentEvent, AgentOutcome, AiError};

/// Maximum number of LLM round-trips to prevent infinite loops.
const MAX_ITERATIONS: u32 = 25;

/// Maximum size of a tool result JSON string before truncation.
/// Prevents overwhelming the LLM context window.
const MAX_TOOL_RESULT_BYTES: usize = 8000;

/// Configuration for agent resource limits.
///
/// All limits use a soft/hard pattern: the soft limit injects advisory
/// context into the conversation telling the agent to wrap up, while
/// the hard limit forces termination.
pub struct AgentLimits {
    /// Timeout for a single tool execution (e.g. a `PostGIS` query).
    /// If exceeded, the tool returns an error and the agent continues.
    pub per_tool_timeout: Duration,

    /// Timeout for a single LLM provider call.
    /// If exceeded, the agent terminates with an error.
    pub per_llm_timeout: Duration,

    /// Soft limit on total tool calls. When reached, a system message is
    /// injected telling the agent to start wrapping up.
    pub tool_call_soft_limit: u32,

    /// Hard limit on total tool calls. When reached, tool dispatch stops
    /// and the agent is forced to produce a final answer.
    pub tool_call_hard_limit: u32,

    /// Soft duration limit. When elapsed, a system message is injected
    /// telling the agent time is running low.
    pub duration_soft_limit: Duration,

    /// Hard duration limit. When elapsed, the agent is forced to produce
    /// a final answer with whatever it has gathered.
    pub duration_hard_limit: Duration,
}

impl Default for AgentLimits {
    fn default() -> Self {
        Self {
            per_tool_timeout: Duration::from_secs(30),
            per_llm_timeout: Duration::from_secs(60),
            tool_call_soft_limit: 25,
            tool_call_hard_limit: 50,
            duration_soft_limit: Duration::from_secs(90),
            duration_hard_limit: Duration::from_secs(120),
        }
    }
}

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
- Incidents are pre-attributed to Census places and tracts at ingest time, so placeGeoid filtering is fast and accurate (no runtime spatial queries needed).

## Instructions
1. Use the tools provided to query the crime database. Do NOT make up statistics.
2. Always use list_cities first if you're unsure whether a city is in the dataset.
3. For year-over-year comparisons, use compare_periods with appropriate date ranges.
4. For "safest neighborhood" questions, use rank_areas with safestFirst=true.
5. When comparing cities, call count_incidents for each city separately.
6. Provide specific numbers and percentages in your answers.
7. If the user asks about a location not directly in the dataset, use search_locations to find matching jurisdictions AND Census places. If search_locations returns a Census place with a placeGeoid, use that placeGeoid in subsequent tool calls (count_incidents, rank_areas, etc.) for precise geographic filtering within that city/town boundary. This is especially important for small cities within large counties (e.g., Capitol Heights within Prince George's County) — using placeGeoid filters to just the incidents within the city limits rather than the entire county. The placeGeoid filter uses pre-computed spatial attribution with a buffer to handle coordinate imprecision, so it captures all nearby incidents.
8. Format your final answer in clear markdown with key statistics bolded.
9. Today's date is {today}. When users say "2025", "this year", "last year", etc., interpret relative to today.
10. Use category names in SCREAMING_SNAKE_CASE when calling tools (e.g., "VIOLENT", "PROPERTY").
11. State abbreviations should be uppercase 2-letter codes (e.g., "IL", "DC", "CA").
12. Tool performance: rank_areas is the most expensive tool — it joins incidents to census tracts and aggregates by neighborhood. For large cities, always include date filters or a category filter. If rank_areas times out, fall back to count_incidents or top_crime_types which are much faster. compare_periods runs two count queries internally, so it can also be slow without filters.

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

/// Mutable state tracked across the agent loop for enforcing limits.
struct AgentBudget {
    /// When the agent started running.
    start: Instant,
    /// Total tool calls executed so far.
    tool_calls: u32,
    /// Whether the soft tool-call warning has been injected.
    tool_soft_warned: bool,
    /// Whether the soft duration warning has been injected.
    duration_soft_warned: bool,
}

impl AgentBudget {
    fn new() -> Self {
        Self {
            start: Instant::now(),
            tool_calls: 0,
            tool_soft_warned: false,
            duration_soft_warned: false,
        }
    }

    /// Returns the elapsed time since the agent started.
    fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }
}

/// Runs the AI agent loop for a user question.
///
/// If `prior_messages` is provided, the conversation continues from that
/// history. Otherwise a fresh conversation is started.
///
/// Sends [`AgentEvent`]s through the provided channel as the agent works.
/// The final event will be either `AgentEvent::Answer` or `AgentEvent::Error`.
///
/// Always returns an [`AgentOutcome`] containing the accumulated messages,
/// even on failure. This ensures the caller can persist partial progress.
#[allow(clippy::too_many_lines)]
pub async fn run_agent(
    provider: &dyn LlmProvider,
    db: &dyn Database,
    context: &AgentContext,
    question: &str,
    prior_messages: Option<Vec<Message>>,
    limits: &AgentLimits,
    tx: mpsc::Sender<AgentEvent>,
) -> AgentOutcome {
    let system_prompt = build_system_prompt(context);
    let tools = tool_definitions();
    let mut budget = AgentBudget::new();

    let mut messages = prior_messages.unwrap_or_default();
    messages.push(Message {
        role: "user".to_string(),
        content: MessageContent::Text(question.to_string()),
    });

    for iteration in 0..MAX_ITERATIONS {
        log::info!(
            "Agent iteration {iteration} (tool_calls={}, elapsed={:.1}s)",
            budget.tool_calls,
            budget.elapsed().as_secs_f64(),
        );

        // ── Duration hard limit check ─────────────────────────────────
        if budget.elapsed() >= limits.duration_hard_limit {
            log::warn!(
                "Agent hit duration hard limit ({:.0}s). Forcing final answer.",
                limits.duration_hard_limit.as_secs_f64(),
            );
            return force_final_answer(
                provider,
                &system_prompt,
                &mut messages,
                &tools,
                limits,
                &tx,
                "You have exceeded the maximum allowed time. Provide your final answer NOW \
                 using only the information you have gathered so far. Do NOT call any more tools.",
            )
            .await;
        }

        // ── Duration soft limit check ─────────────────────────────────
        if !budget.duration_soft_warned && budget.elapsed() >= limits.duration_soft_limit {
            budget.duration_soft_warned = true;
            log::info!("Agent hit duration soft limit. Injecting wrap-up advisory.");
            inject_system_context(
                &mut messages,
                &format!(
                    "NOTICE: You have been running for {:.0} seconds. You are approaching the \
                     time limit. Please finish your analysis and provide your final answer \
                     within the next 1-2 tool calls.",
                    budget.elapsed().as_secs_f64(),
                ),
            );
        }

        // ── Tool budget hard limit check ──────────────────────────────
        if budget.tool_calls >= limits.tool_call_hard_limit {
            log::warn!(
                "Agent hit tool call hard limit ({}). Forcing final answer.",
                limits.tool_call_hard_limit,
            );
            return force_final_answer(
                provider,
                &system_prompt,
                &mut messages,
                &tools,
                limits,
                &tx,
                "You have used the maximum number of tool calls. Provide your final answer NOW \
                 using only the information you have gathered so far. Do NOT call any more tools.",
            )
            .await;
        }

        let _ = tx
            .send(AgentEvent::Thinking {
                message: if iteration == 0 {
                    "Analyzing your question...".to_string()
                } else {
                    format!(
                        "Processing results and thinking... ({} tool calls, {:.0}s elapsed)",
                        budget.tool_calls,
                        budget.elapsed().as_secs_f64(),
                    )
                },
            })
            .await;

        // ── LLM call with timeout ─────────────────────────────────────
        let Ok(response) = tokio::time::timeout(
            limits.per_llm_timeout,
            provider.chat(&system_prompt, &messages, &tools),
        )
        .await
        else {
            log::error!(
                "LLM call timed out after {:.0}s",
                limits.per_llm_timeout.as_secs_f64()
            );
            return AgentOutcome {
                messages,
                result: Err(AiError::Provider {
                    message: format!(
                        "LLM provider did not respond within {:.0} seconds. Please try again.",
                        limits.per_llm_timeout.as_secs_f64()
                    ),
                }),
            };
        };
        let response = match response {
            Ok(r) => r,
            Err(e) => {
                return AgentOutcome {
                    messages,
                    result: Err(e),
                };
            }
        };

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
                return AgentOutcome {
                    messages,
                    result: Ok(()),
                };
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
                    // Check tool budget before dispatching
                    if budget.tool_calls >= limits.tool_call_hard_limit {
                        log::warn!("Tool budget exhausted, skipping tool call: {name}");
                        tool_results.push(ContentBlock::ToolResult {
                            tool_use_id: id.clone(),
                            content: "Tool call limit reached. You must provide your final \
                                      answer now using the data you have already gathered."
                                .to_string(),
                        });
                        continue;
                    }

                    let _ = tx
                        .send(AgentEvent::ToolCall {
                            tool: name.clone(),
                            params: input.clone(),
                        })
                        .await;

                    budget.tool_calls += 1;

                    // ── Per-tool timeout ───────────────────────────────
                    let result = tokio::time::timeout(
                        limits.per_tool_timeout,
                        execute_tool(db, name.as_str(), input),
                    )
                    .await
                    .unwrap_or_else(|_| {
                        log::warn!(
                            "Tool '{name}' timed out after {:.0}s",
                            limits.per_tool_timeout.as_secs_f64()
                        );
                        Err(AiError::Provider {
                            message: tool_timeout_message(name, limits.per_tool_timeout),
                        })
                    });

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

                    // ── Tool budget soft limit (inject after executing) ──
                    if !budget.tool_soft_warned && budget.tool_calls >= limits.tool_call_soft_limit
                    {
                        budget.tool_soft_warned = true;
                        log::info!(
                            "Agent hit tool call soft limit ({}). Will inject wrap-up advisory.",
                            limits.tool_call_soft_limit,
                        );
                    }
                }
            }

            // Inject soft limit advisory as a text block alongside tool results
            if budget.tool_soft_warned && !budget.duration_soft_warned {
                // Only inject once — the flag prevents re-injection
                let remaining = limits.tool_call_hard_limit - budget.tool_calls;
                let mut advisory =
                    String::from("NOTICE: You have used a significant number of tool calls. ");
                write!(
                    advisory,
                    "You have approximately {remaining} tool calls remaining. \
                     Please start forming your final answer soon."
                )
                .unwrap();
                tool_results.push(ContentBlock::ToolResult {
                    tool_use_id: "system_advisory".to_string(),
                    content: advisory,
                });
            }

            // Add tool results as a user message
            messages.push(Message {
                role: "user".to_string(),
                content: MessageContent::Blocks(tool_results),
            });
        } else {
            // Model is done — extract final answer
            let text = extract_text(&response.content);
            // Add assistant's final answer to the history
            messages.push(Message {
                role: "assistant".to_string(),
                content: MessageContent::Blocks(response.content),
            });
            let _ = tx.send(AgentEvent::Answer { text }).await;
            return AgentOutcome {
                messages,
                result: Ok(()),
            };
        }
    }

    AgentOutcome {
        messages,
        result: Err(AiError::MaxIterations {
            max_iterations: MAX_ITERATIONS,
        }),
    }
}

/// Forces the agent to produce a final answer by injecting a directive
/// and making one last LLM call with no tool dispatch.
async fn force_final_answer(
    provider: &dyn LlmProvider,
    system_prompt: &str,
    messages: &mut Vec<Message>,
    tools: &[serde_json::Value],
    limits: &AgentLimits,
    tx: &mpsc::Sender<AgentEvent>,
    directive: &str,
) -> AgentOutcome {
    inject_system_context(messages, directive);

    let _ = tx
        .send(AgentEvent::Thinking {
            message: "Wrapping up and forming final answer...".to_string(),
        })
        .await;

    // One final LLM call — we still pass tools but will ignore any tool_use
    let Ok(response) = tokio::time::timeout(
        limits.per_llm_timeout,
        provider.chat(system_prompt, messages, tools),
    )
    .await
    else {
        // Even the wrap-up call timed out. Return a generic message.
        let fallback = "I was unable to complete the analysis within the time limit. \
                        Please try a more specific question."
            .to_string();
        let _ = tx
            .send(AgentEvent::Answer {
                text: fallback.clone(),
            })
            .await;
        messages.push(Message {
            role: "assistant".to_string(),
            content: MessageContent::Text(fallback),
        });
        return AgentOutcome {
            messages: messages.clone(),
            result: Ok(()),
        };
    };
    let response = match response {
        Ok(r) => r,
        Err(e) => {
            return AgentOutcome {
                messages: messages.clone(),
                result: Err(e),
            };
        }
    };

    let text = extract_text(&response.content);
    let answer = if text.is_empty() {
        "I was unable to form a complete answer within the resource limits. \
         Please try a more specific question."
            .to_string()
    } else {
        text
    };

    messages.push(Message {
        role: "assistant".to_string(),
        content: MessageContent::Blocks(response.content),
    });
    let _ = tx.send(AgentEvent::Answer { text: answer }).await;
    AgentOutcome {
        messages: messages.clone(),
        result: Ok(()),
    }
}

/// Injects a system-level advisory message into the conversation as a
/// user message. LLM APIs don't support mid-conversation system messages,
/// so we format it as a clearly-marked user message.
fn inject_system_context(messages: &mut Vec<Message>, context: &str) {
    messages.push(Message {
        role: "user".to_string(),
        content: MessageContent::Text(format!("[SYSTEM: {context}]")),
    });
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
        "search_locations" => {
            let params: SearchLocationsParams = serde_json::from_value(input.clone())?;
            let result = tools::search_locations(db, &params).await?;
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
        "search_locations" => {
            let count = result["matches"].as_array().map_or(0, Vec::len);
            let desc = result["description"].as_str().unwrap_or("");
            if count > 0 {
                format!("{count} location(s) found: {desc}")
            } else {
                desc.to_string()
            }
        }
        _ => "Result received".to_string(),
    }
}

/// Produces a tool-specific timeout error message with actionable advice
/// so the LLM can adjust its approach instead of blindly retrying.
fn tool_timeout_message(tool_name: &str, timeout: Duration) -> String {
    let secs = timeout.as_secs_f64();
    match tool_name {
        "rank_areas" => format!(
            "Tool 'rank_areas' timed out after {secs:.0} seconds. This tool is \
             computationally expensive for large cities. Try: (1) adding date filters \
             to narrow the time window, (2) filtering by a specific category like \
             VIOLENT or PROPERTY, (3) using count_incidents or top_crime_types instead \
             which are faster, or (4) using a placeGeoid for a smaller geographic area."
        ),
        "compare_periods" => format!(
            "Tool 'compare_periods' timed out after {secs:.0} seconds. This tool runs \
             two count queries internally. Try shorter time periods or add a category \
             filter to reduce the data scanned."
        ),
        _ => format!(
            "Tool '{tool_name}' timed out after {secs:.0} seconds. Try adding more \
             filters (date range, category, state) to narrow the query scope."
        ),
    }
}
