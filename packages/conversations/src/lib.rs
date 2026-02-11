#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions, clippy::cargo_common_metadata)]

//! Persistent AI conversation storage backed by `SQLite`.
//!
//! Stores conversation history in `data/conversations.db` so that:
//! - Conversations survive server restarts
//! - They are accessible via the `cargo conversations` CLI for auditing
//!
//! Uses `switchy_database` for all database operations, following the same
//! patterns as the discover and generate packages.

pub mod interactive;

use std::fmt::Write as _;
use std::path::Path;

use crime_map_ai::providers::{Message, MessageContent};
use moosicbox_json_utils::database::ToValue as _;
use switchy_database::{Database, DatabaseValue};
use switchy_database_connection::init_sqlite_rusqlite;
use thiserror::Error;

/// Default path for the conversations database.
pub const DEFAULT_DB_PATH: &str = "data/conversations.db";

/// Maximum title length (truncated from first user message).
const MAX_TITLE_LENGTH: usize = 100;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors from conversation storage operations.
#[derive(Debug, Error)]
pub enum ConversationError {
    /// A database query or command failed.
    #[error("Database error: {0}")]
    Database(String),

    /// An I/O operation failed.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// JSON serialization/deserialization failed.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

// ---------------------------------------------------------------------------
// Result types
// ---------------------------------------------------------------------------

/// Summary of a conversation for listing.
#[derive(Debug, Clone)]
pub struct ConversationSummary {
    /// Conversation UUID.
    pub id: String,
    /// Title (first user question, truncated).
    pub title: Option<String>,
    /// When the conversation was created.
    pub created_at: String,
    /// When the conversation was last updated.
    pub updated_at: String,
    /// Total number of messages.
    pub message_count: i64,
}

/// A single stored message.
#[derive(Debug, Clone)]
pub struct StoredMessage {
    /// Ordering within the conversation.
    pub sequence: i32,
    /// Role: "user" or "assistant".
    pub role: String,
    /// JSON-serialized `MessageContent`.
    pub content: String,
    /// When this message was stored.
    pub created_at: String,
}

// ---------------------------------------------------------------------------
// Database lifecycle
// ---------------------------------------------------------------------------

/// Opens (or creates) the conversations `SQLite` database and ensures
/// the schema exists.
///
/// # Errors
///
/// Returns [`ConversationError`] if the database cannot be opened or
/// schema creation fails.
pub async fn open_db(path: &Path) -> Result<Box<dyn Database>, ConversationError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let db =
        init_sqlite_rusqlite(Some(path)).map_err(|e| ConversationError::Database(e.to_string()))?;

    ensure_schema(db.as_ref()).await?;

    Ok(db)
}

/// Creates all tables if they don't already exist.
async fn ensure_schema(db: &dyn Database) -> Result<(), ConversationError> {
    db.exec_raw(
        "CREATE TABLE IF NOT EXISTS conversations (
            id          TEXT PRIMARY KEY,
            title       TEXT,
            created_at  TEXT NOT NULL,
            updated_at  TEXT NOT NULL
        )",
    )
    .await
    .map_err(|e| ConversationError::Database(e.to_string()))?;

    db.exec_raw(
        "CREATE TABLE IF NOT EXISTS messages (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            conversation_id TEXT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
            sequence        INTEGER NOT NULL,
            role            TEXT NOT NULL,
            content         TEXT NOT NULL,
            created_at      TEXT NOT NULL,
            UNIQUE(conversation_id, sequence)
        )",
    )
    .await
    .map_err(|e| ConversationError::Database(e.to_string()))?;

    db.exec_raw(
        "CREATE INDEX IF NOT EXISTS idx_messages_conversation
         ON messages (conversation_id, sequence)",
    )
    .await
    .map_err(|e| ConversationError::Database(e.to_string()))?;

    db.exec_raw(
        "CREATE INDEX IF NOT EXISTS idx_conversations_updated
         ON conversations (updated_at)",
    )
    .await
    .map_err(|e| ConversationError::Database(e.to_string()))?;

    // Enable foreign key enforcement (SQLite has it off by default)
    db.exec_raw("PRAGMA foreign_keys = ON")
        .await
        .map_err(|e| ConversationError::Database(e.to_string()))?;

    Ok(())
}

// ---------------------------------------------------------------------------
// CRUD operations
// ---------------------------------------------------------------------------

/// Saves a conversation and its full message history to the database.
///
/// Creates the conversation row if it doesn't exist, updates it otherwise.
/// Replaces all messages (delete + re-insert) since the agent returns the
/// complete history each time.
///
/// # Errors
///
/// Returns [`ConversationError`] if any database operation fails.
pub async fn save_conversation(
    db: &dyn Database,
    id: &str,
    messages: &[Message],
) -> Result<(), ConversationError> {
    let now = chrono::Utc::now().to_rfc3339();

    // Extract title from first user message
    let title = messages
        .iter()
        .find(|m| m.role == "user")
        .and_then(|m| match &m.content {
            MessageContent::Text(t) => Some(truncate_title(t)),
            MessageContent::Blocks(blocks) => blocks.iter().find_map(|b| {
                if let crime_map_ai::providers::ContentBlock::Text { text } = b {
                    Some(truncate_title(text))
                } else {
                    None
                }
            }),
        });

    // Upsert conversation
    db.exec_raw_params(
        "INSERT INTO conversations (id, title, created_at, updated_at)
         VALUES ($1, $2, $3, $3)
         ON CONFLICT (id) DO UPDATE SET
           title = COALESCE(conversations.title, excluded.title),
           updated_at = excluded.updated_at",
        &[
            DatabaseValue::String(id.to_string()),
            title.map_or(DatabaseValue::Null, DatabaseValue::String),
            DatabaseValue::String(now.clone()),
        ],
    )
    .await
    .map_err(|e| ConversationError::Database(e.to_string()))?;

    // Delete existing messages
    db.exec_raw_params(
        "DELETE FROM messages WHERE conversation_id = $1",
        &[DatabaseValue::String(id.to_string())],
    )
    .await
    .map_err(|e| ConversationError::Database(e.to_string()))?;

    // Insert all messages
    for (i, msg) in messages.iter().enumerate() {
        let content_json = serde_json::to_string(&msg.content)?;

        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        let seq = i as i32;

        db.exec_raw_params(
            "INSERT INTO messages (conversation_id, sequence, role, content, created_at)
             VALUES ($1, $2, $3, $4, $5)",
            &[
                DatabaseValue::String(id.to_string()),
                DatabaseValue::Int32(seq),
                DatabaseValue::String(msg.role.clone()),
                DatabaseValue::String(content_json),
                DatabaseValue::String(now.clone()),
            ],
        )
        .await
        .map_err(|e| ConversationError::Database(e.to_string()))?;
    }

    Ok(())
}

/// Loads the message history for a conversation.
///
/// Returns `None` if the conversation doesn't exist.
///
/// # Errors
///
/// Returns [`ConversationError`] if the database operation fails.
pub async fn load_messages(
    db: &dyn Database,
    conversation_id: &str,
) -> Result<Option<Vec<Message>>, ConversationError> {
    let rows = db
        .query_raw_params(
            "SELECT role, content FROM messages
             WHERE conversation_id = $1
             ORDER BY sequence",
            &[DatabaseValue::String(conversation_id.to_string())],
        )
        .await
        .map_err(|e| ConversationError::Database(e.to_string()))?;

    if rows.is_empty() {
        return Ok(None);
    }

    let mut messages = Vec::with_capacity(rows.len());
    for row in &rows {
        let role: String = row.to_value("role").unwrap_or_default();
        let content_json: String = row.to_value("content").unwrap_or_default();
        let content: MessageContent = serde_json::from_str(&content_json)?;
        messages.push(Message { role, content });
    }

    Ok(Some(messages))
}

/// Lists recent conversations with summary information.
///
/// Results are ordered by most recently updated first. Use `offset` to
/// paginate through results.
///
/// # Errors
///
/// Returns [`ConversationError`] if the database operation fails.
pub async fn list_conversations(
    db: &dyn Database,
    limit: u32,
    offset: u32,
) -> Result<Vec<ConversationSummary>, ConversationError> {
    let rows = db
        .query_raw_params(
            "SELECT c.id, c.title, c.created_at, c.updated_at,
                    (SELECT COUNT(*) FROM messages m WHERE m.conversation_id = c.id) as message_count
             FROM conversations c
             ORDER BY c.updated_at DESC
             LIMIT $1 OFFSET $2",
            &[
                DatabaseValue::Int32(i32::try_from(limit).unwrap_or(i32::MAX)),
                DatabaseValue::Int32(i32::try_from(offset).unwrap_or(0)),
            ],
        )
        .await
        .map_err(|e| ConversationError::Database(e.to_string()))?;

    let mut summaries = Vec::with_capacity(rows.len());
    for row in &rows {
        summaries.push(ConversationSummary {
            id: row.to_value("id").unwrap_or_default(),
            title: row.to_value("title").unwrap_or(None),
            created_at: row.to_value("created_at").unwrap_or_default(),
            updated_at: row.to_value("updated_at").unwrap_or_default(),
            message_count: row.to_value("message_count").unwrap_or(0),
        });
    }

    Ok(summaries)
}

/// Loads all messages for a conversation in a displayable format.
///
/// # Errors
///
/// Returns [`ConversationError`] if the database operation fails.
pub async fn get_conversation_messages(
    db: &dyn Database,
    conversation_id: &str,
) -> Result<Option<Vec<StoredMessage>>, ConversationError> {
    let rows = db
        .query_raw_params(
            "SELECT sequence, role, content, created_at FROM messages
             WHERE conversation_id = $1
             ORDER BY sequence",
            &[DatabaseValue::String(conversation_id.to_string())],
        )
        .await
        .map_err(|e| ConversationError::Database(e.to_string()))?;

    if rows.is_empty() {
        return Ok(None);
    }

    let mut messages = Vec::with_capacity(rows.len());
    for row in &rows {
        messages.push(StoredMessage {
            sequence: row.to_value("sequence").unwrap_or(0),
            role: row.to_value("role").unwrap_or_default(),
            content: row.to_value("content").unwrap_or_default(),
            created_at: row.to_value("created_at").unwrap_or_default(),
        });
    }

    Ok(Some(messages))
}

/// Deletes a conversation and all its messages.
///
/// # Errors
///
/// Returns [`ConversationError`] if the database operation fails.
pub async fn delete_conversation(
    db: &dyn Database,
    conversation_id: &str,
) -> Result<bool, ConversationError> {
    let deleted = db
        .exec_raw_params(
            "DELETE FROM conversations WHERE id = $1",
            &[DatabaseValue::String(conversation_id.to_string())],
        )
        .await
        .map_err(|e| ConversationError::Database(e.to_string()))?;

    Ok(deleted > 0)
}

/// Returns the total number of stored conversations.
///
/// # Errors
///
/// Returns [`ConversationError`] if the database operation fails.
pub async fn count_conversations(db: &dyn Database) -> Result<u64, ConversationError> {
    let rows = db
        .query_raw_params("SELECT COUNT(*) as cnt FROM conversations", &[])
        .await
        .map_err(|e| ConversationError::Database(e.to_string()))?;

    let count: i64 = rows.first().map_or(0, |r| r.to_value("cnt").unwrap_or(0));

    #[allow(clippy::cast_sign_loss)]
    Ok(count as u64)
}

/// Resolves a conversation ID, supporting prefix matching.
///
/// If the given ID is a full UUID (36+ chars), returns it directly.
/// Otherwise, searches for a unique conversation whose ID starts with
/// the given prefix.
///
/// # Errors
///
/// Returns an error if no conversation matches the prefix, or if
/// multiple conversations match (ambiguous).
pub async fn resolve_id(db: &dyn Database, id: &str) -> Result<String, Box<dyn std::error::Error>> {
    // If it looks like a full UUID, use it directly
    if id.len() >= 36 {
        return Ok(id.to_string());
    }

    // Prefix search
    let rows = db
        .query_raw_params(
            "SELECT id FROM conversations WHERE id LIKE $1 || '%' LIMIT 2",
            &[DatabaseValue::String(id.to_string())],
        )
        .await
        .map_err(|e| format!("Database error: {e}"))?;

    match rows.len() {
        0 => Err(format!("No conversation found matching prefix: {id}").into()),
        1 => {
            let full_id: String = rows
                .first()
                .map_or(String::new(), |r| r.to_value("id").unwrap_or_default());
            Ok(full_id)
        }
        _ => Err(format!("Multiple conversations match prefix '{id}'. Be more specific.").into()),
    }
}

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------

/// Prefix used for injected system advisory messages in the conversation.
const SYSTEM_MSG_PREFIX: &str = "[SYSTEM: ";

/// Returns `true` if the text is an injected system advisory message
/// (e.g. budget/timeout warnings), not a real user message.
fn is_system_advisory(text: &str) -> bool {
    text.starts_with(SYSTEM_MSG_PREFIX)
}

/// Formats a conversation for human-readable display.
///
/// Shows user questions, tool calls (name + params), tool results (summary),
/// and assistant answers in a structured format.
///
/// When `show_system` is `false`, injected system advisory messages (budget
/// and timeout warnings) are hidden from the output.
#[must_use]
pub fn format_conversation(messages: &[StoredMessage], show_system: bool) -> String {
    let mut output = String::new();

    for msg in messages {
        let content: Result<MessageContent, _> = serde_json::from_str(&msg.content);

        match (&*msg.role, content) {
            ("user", Ok(MessageContent::Text(ref text))) if is_system_advisory(text) => {
                if show_system {
                    writeln!(output, "--- SYSTEM ---").unwrap();
                    // Strip the [SYSTEM: ...] wrapper for cleaner display
                    let inner = text
                        .strip_prefix(SYSTEM_MSG_PREFIX)
                        .and_then(|s| s.strip_suffix(']'))
                        .unwrap_or(text);
                    writeln!(output, "{inner}").unwrap();
                    writeln!(output).unwrap();
                }
            }
            ("user", Ok(MessageContent::Text(text))) => {
                writeln!(output, "--- USER ---").unwrap();
                writeln!(output, "{text}").unwrap();
                writeln!(output).unwrap();
            }
            ("assistant", Ok(MessageContent::Text(text))) => {
                writeln!(output, "--- ASSISTANT ---").unwrap();
                writeln!(output, "{text}").unwrap();
                writeln!(output).unwrap();
            }
            ("assistant", Ok(MessageContent::Blocks(blocks))) => {
                writeln!(output, "--- ASSISTANT ---").unwrap();
                for block in &blocks {
                    match block {
                        crime_map_ai::providers::ContentBlock::Text { text } => {
                            writeln!(output, "{text}").unwrap();
                        }
                        crime_map_ai::providers::ContentBlock::ToolUse { name, input, .. } => {
                            writeln!(output, "[TOOL CALL: {name}]").unwrap();
                            if let Ok(pretty) = serde_json::to_string_pretty(input) {
                                writeln!(output, "{pretty}").unwrap();
                            }
                        }
                        crime_map_ai::providers::ContentBlock::ToolResult { content, .. } => {
                            writeln!(output, "[TOOL RESULT]").unwrap();
                            // Truncate long tool results for display
                            if content.len() > 500 {
                                writeln!(output, "{}...", &content[..500]).unwrap();
                            } else {
                                writeln!(output, "{content}").unwrap();
                            }
                        }
                    }
                }
                writeln!(output).unwrap();
            }
            ("user", Ok(MessageContent::Blocks(blocks))) => {
                // Tool results sent as user messages
                for block in &blocks {
                    if let crime_map_ai::providers::ContentBlock::ToolResult {
                        tool_use_id,
                        content,
                    } = block
                    {
                        writeln!(output, "--- TOOL RESULT (for {tool_use_id}) ---").unwrap();
                        if content.len() > 500 {
                            writeln!(output, "{}...", &content[..500]).unwrap();
                        } else {
                            writeln!(output, "{content}").unwrap();
                        }
                        writeln!(output).unwrap();
                    }
                }
            }
            _ => {
                writeln!(output, "--- {} ---", msg.role.to_uppercase()).unwrap();
                writeln!(output, "{}", msg.content).unwrap();
                writeln!(output).unwrap();
            }
        }
    }

    output
}

/// Truncates a string to `MAX_TITLE_LENGTH` characters.
fn truncate_title(s: &str) -> String {
    let trimmed = s.trim();
    if trimmed.len() <= MAX_TITLE_LENGTH {
        trimmed.to_string()
    } else {
        format!("{}...", &trimmed[..MAX_TITLE_LENGTH - 3])
    }
}
