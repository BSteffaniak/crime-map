---
name: ai-conversation-audit
description: Systematic audit of AI conversation quality to improve tool usage, system prompts, and data coverage
---

# AI Conversation Audit Skill

## Overview

The crime-map project has an AI chat feature where users ask natural language questions about crime data. The AI agent uses analytical tools (`count_incidents`, `rank_areas`, `compare_periods`, `get_trend`, `top_crime_types`, `list_cities`, `search_locations`) to query the database and produce answers.

All conversations are persisted in `data/conversations.db` (SQLite) and accessible via the `cargo conversations` CLI. This skill provides a systematic workflow for auditing those conversations to identify and fix issues with:

- **Tool selection**: Did the AI call the right tools for the question?
- **Tool parameters**: Were the parameters correct (city/state casing, date ranges, placeGeoid usage)?
- **Result interpretation**: Did the AI accurately describe what the data showed?
- **Answer quality**: Were claims supported by data? Were caveats appropriate?
- **Data coverage**: Did a query fail because we lack data for a region?

## Starting a Session

Always begin an audit session with these steps:

1. **List recent conversations:**

   ```
   cargo conversations list --limit 20
   ```

   This shows conversation IDs, message counts, last update time, and titles (the first user question).

2. **Pick conversations to audit.** Prioritize:
   - Conversations with many messages (complex multi-turn interactions have more failure modes)
   - Conversations with titles suggesting geographic specificity (e.g., "crime in Capitol Heights" — these test spatial filtering)
   - Conversations with titles suggesting comparisons (e.g., "compare Denver and Chicago" — these test multi-tool orchestration)

3. **Read the full conversation:**

   ```
   cargo conversations show <id>
   ```

   This displays the conversation in a structured format showing user questions, tool calls with parameters, tool results, and assistant answers.

4. **For deep analysis, export as JSON:**

   ```
   cargo conversations export <id>
   ```

   This gives the raw `Message` objects with full `ContentBlock` detail.

## Audit Checklist

For each conversation, evaluate the following:

### A. Tool Selection

- Did the AI use `search_locations` before querying a location it hadn't seen before?
- Did it use `list_cities` when unsure about data availability?
- For "safest/most dangerous" questions, did it use `rank_areas` (not manual counting)?
- For trend questions, did it use `get_trend` (not manual `count_incidents` over multiple periods)?
- For comparisons, did it use `compare_periods` or call the appropriate tool for each city?
- Did it avoid redundant tool calls (e.g., calling `count_incidents` when `top_crime_types` would answer the question directly)?

### B. Tool Parameters

- **City/state**: Are city names properly cased? Are state abbreviations uppercase two-letter codes?
- **Date ranges**: Are `dateFrom`/`dateTo` in ISO 8601 format? Do they cover the right period?
- **placeGeoid**: When querying a Census place (small city/town within a larger county), did the AI use `placeGeoid` from `search_locations` results instead of filtering by city name?
- **Category filters**: Are crime category names valid (`VIOLENT`, `PROPERTY`, `OTHER`)?
- **Missing parameters**: Did the AI omit required parameters? Did it include unnecessary ones?

### C. Result Interpretation

- Did the AI correctly read numeric values from tool results?
- Did it calculate percentages and per-capita rates correctly?
- Did it note when a tool returned no data (vs. zero incidents)?
- Did it correctly identify the top/bottom items from ranked results?

### D. Answer Quality

- Are all claims in the answer supported by tool results shown in the conversation?
- Does the AI fabricate statistics not returned by any tool?
- Are caveats appropriate? (e.g., "this data only covers 2023-2024" when relevant)
- Does it mention data limitations when a query returns suspiciously few results?
- Is the answer well-structured with clear sections?

### E. Data Coverage Issues

- Did any tool call return empty results for a location that should have data?
- Did `search_locations` fail to find a location that exists?
- Were incident counts unexpectedly low for a populated area?
- Are there error messages from tool calls indicating missing data?

## Recording Findings

After auditing each conversation, document findings in the following categories:

### System Prompt Improvements

If the AI repeatedly makes the same mistake (e.g., not using `search_locations` first), suggest a specific addition to the system prompt in `packages/ai/src/agent.rs`.

Example: "Add instruction: 'When a user asks about a specific neighborhood or small town, always call search_locations first to check if a Census place GEOID is available for precise filtering.'"

### Tool Definition Improvements

If tool parameters are confusing or the AI misuses a tool, suggest changes to the tool JSON schema definitions in `packages/analytics/models/src/lib.rs`.

Example: "The `dateFrom` parameter description should clarify that dates must be in YYYY-MM-DD format, not relative terms like 'last year'."

### Tool Implementation Improvements

If tool results are incorrect or missing expected data, suggest changes to the tool implementations in `packages/analytics/src/tools.rs`.

Example: "The `rank_areas` tool should return the total incident count alongside per-capita rates so the AI can assess statistical significance."

### Data Coverage Gaps

If a query fails because the database lacks data for a region, note the gap.

Example: "No data for Atlanta, GA — need to add a source for Atlanta PD or Fulton County."

## CLI Reference

### List conversations

```
cargo conversations list [--limit N]
```

Shows recent conversations sorted by last update time. Default limit is 20.

Output columns: ID, message count, updated timestamp, title.

### Show conversation

```
cargo conversations show <id>
```

Displays the full conversation in human-readable format. Supports ID prefix matching (e.g., `cargo conversations show a1b2` if the ID starts with `a1b2`).

Format:
- `--- USER ---` followed by the question
- `--- ASSISTANT ---` followed by text, `[TOOL CALL: name]` with params, `[TOOL RESULT]` with data
- Tool results longer than 500 characters are truncated in display

### Export conversation

```
cargo conversations export <id>
```

Outputs the full conversation as a JSON array of `Message` objects. Useful for programmatic analysis or sharing with others.

### Delete conversation

```
cargo conversations delete <id>
```

Permanently removes a conversation and all its messages.

## Session Workflow

1. List conversations and pick 2-3 to audit
2. For each conversation:
   a. Read the full conversation with `cargo conversations show`
   b. Walk through the audit checklist (sections A-E)
   c. Note specific findings with suggested fixes
3. Summarize all findings at the end:
   - List specific code changes needed (file + description)
   - Prioritize by impact (how many conversations would improve)
   - Note any data coverage gaps discovered
