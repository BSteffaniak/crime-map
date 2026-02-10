---
description: Run a full source discovery session
subtask: true
---

Load the `source-discovery` skill, then run a full discovery session.

Start by running `cargo discover status` to see the current state of the discovery database. Then review the search log with `cargo discover search-log list` to understand what has already been explored.

Based on the current state, systematically search for new crime data sources. Prioritize:

1. Investigating any existing leads with `new` status
2. Exploring regions and cities that have no coverage yet
3. Trying discovery strategies that haven't been used recently

For each search or investigation, record all findings in the discovery database using the appropriate `cargo discover` CLI commands. Never skip recording a search or lead.

Execute 3-5 searches or investigations per session. At the end, summarize what was found and suggest next steps for the following session.
