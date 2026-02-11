---
description: Audit AI conversation quality and suggest improvements
subtask: true
---

Load the `ai-conversation-audit` skill, then run an audit session.

Start by listing recent conversations with `cargo conversations list --limit 20` to see what's available. Pick 2-3 conversations to audit, prioritizing those with:

1. Complex multi-turn interactions (high message count)
2. Geographic specificity (questions about specific cities/neighborhoods)
3. Comparison or trend questions

For each conversation, read it with `cargo conversations show <id>` and work through the full audit checklist. Document all findings with specific, actionable suggestions for improving the system prompt, tool definitions, tool implementations, or data coverage.

At the end, provide a prioritized summary of recommended changes.
