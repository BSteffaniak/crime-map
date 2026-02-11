//! Interactive TUI for browsing AI conversation history.
//!
//! Provides a menu-driven interface with paginated conversation selection
//! using `dialoguer`.

use std::path::Path;

use dialoguer::{Confirm, Input, Select};
use switchy_database::Database;

use crate::{
    ConversationSummary, DEFAULT_DB_PATH, count_conversations, delete_conversation,
    format_conversation, get_conversation_messages, list_conversations, load_messages, open_db,
};

/// Number of conversations to display per page in the picker.
const PAGE_SIZE: u32 = 20;

/// Top-level actions in the conversations interactive menu.
enum ConversationAction {
    List,
    Show,
    Export,
    Delete,
}

impl ConversationAction {
    const ALL: &[Self] = &[Self::List, Self::Show, Self::Export, Self::Delete];

    #[must_use]
    const fn label(&self) -> &'static str {
        match self {
            Self::List => "List recent conversations",
            Self::Show => "Show a conversation",
            Self::Export => "Export a conversation (JSON)",
            Self::Delete => "Delete a conversation",
        }
    }
}

/// Runs the interactive conversations menu.
///
/// Opens the conversations database and presents a menu for browsing,
/// viewing, exporting, and deleting AI conversation history.
///
/// # Errors
///
/// Returns an error if the database connection, user prompts, or any
/// operation fails.
pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let db = open_db(Path::new(DEFAULT_DB_PATH)).await?;

    let labels: Vec<&str> = ConversationAction::ALL
        .iter()
        .map(ConversationAction::label)
        .collect();

    let idx = Select::new()
        .with_prompt("Conversations")
        .items(&labels)
        .default(0)
        .interact()?;

    match ConversationAction::ALL[idx] {
        ConversationAction::List => handle_list(db.as_ref()).await?,
        ConversationAction::Show => handle_show(db.as_ref()).await?,
        ConversationAction::Export => handle_export(db.as_ref()).await?,
        ConversationAction::Delete => handle_delete(db.as_ref()).await?,
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// Lists conversations in a table, prompting for a limit.
async fn handle_list(db: &dyn Database) -> Result<(), Box<dyn std::error::Error>> {
    let limit_str: String = Input::new()
        .with_prompt("Max conversations to show")
        .default("20".to_string())
        .interact_text()?;
    let limit: u32 = limit_str.parse().unwrap_or(20);

    let conversations = list_conversations(db, limit, 0).await?;

    if conversations.is_empty() {
        println!("No conversations found.");
        return Ok(());
    }

    println!();
    println!("{:<38} {:<6} {:<22} TITLE", "ID", "MSGS", "UPDATED");
    println!("{}", "-".repeat(100));

    for conv in &conversations {
        let title = conv.title.as_deref().unwrap_or("(no title)");
        let display_title = if title.len() > 50 {
            format!("{}...", &title[..47])
        } else {
            title.to_string()
        };

        let date = &conv.updated_at;
        let short_date = if date.len() >= 19 { &date[..19] } else { date };

        println!(
            "{:<38} {:<6} {:<22} {}",
            conv.id, conv.message_count, short_date, display_title
        );
    }

    println!("\n{} conversation(s)", conversations.len());
    Ok(())
}

/// Shows a conversation in human-readable format using the paginated picker.
async fn handle_show(db: &dyn Database) -> Result<(), Box<dyn std::error::Error>> {
    let Some(conv_id) = pick_conversation(db).await? else {
        return Ok(());
    };

    let show_system = Confirm::new()
        .with_prompt("Show system advisory messages?")
        .default(false)
        .interact()?;

    let messages = get_conversation_messages(db, &conv_id).await?;

    if let Some(msgs) = messages {
        println!("\nConversation: {conv_id}\n");
        print!("{}", format_conversation(&msgs, show_system));
    } else {
        println!("Conversation not found.");
    }

    Ok(())
}

/// Exports a conversation as pretty-printed JSON using the paginated picker.
async fn handle_export(db: &dyn Database) -> Result<(), Box<dyn std::error::Error>> {
    let Some(conv_id) = pick_conversation(db).await? else {
        return Ok(());
    };

    let messages = load_messages(db, &conv_id).await?;

    if let Some(msgs) = messages {
        let json = serde_json::to_string_pretty(&msgs)?;
        println!("{json}");
    } else {
        println!("Conversation not found.");
    }

    Ok(())
}

/// Deletes a conversation after confirmation using the paginated picker.
async fn handle_delete(db: &dyn Database) -> Result<(), Box<dyn std::error::Error>> {
    let Some(conv_id) = pick_conversation(db).await? else {
        return Ok(());
    };

    let confirmed = Confirm::new()
        .with_prompt(format!("Delete conversation {conv_id}?"))
        .default(false)
        .interact()?;

    if confirmed {
        let deleted = delete_conversation(db, &conv_id).await?;
        if deleted {
            println!("Deleted.");
        } else {
            println!("Conversation not found.");
        }
    } else {
        println!("Cancelled.");
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Paginated conversation picker
// ---------------------------------------------------------------------------

/// Sentinel values returned by the picker to indicate navigation actions
/// rather than a conversation selection.
enum PickerItem {
    Conversation(usize),
    PreviousPage,
    NextPage,
}

/// Presents a paginated `Select` menu of conversations.
///
/// Loads conversations one page at a time. Navigation items ("Previous
/// page" / "Next page") are appended inline.
///
/// Returns `None` if there are no conversations or the user cancels.
async fn pick_conversation(
    db: &dyn Database,
) -> Result<Option<String>, Box<dyn std::error::Error>> {
    let total = count_conversations(db).await?;

    if total == 0 {
        println!("No conversations found.");
        return Ok(None);
    }

    #[allow(clippy::cast_possible_truncation)]
    let total_u32 = total as u32;
    let total_pages = total_u32.div_ceil(PAGE_SIZE);
    let mut page = 0u32;

    loop {
        let offset = page * PAGE_SIZE;
        let conversations = list_conversations(db, PAGE_SIZE, offset).await?;

        if conversations.is_empty() {
            println!("No conversations on this page.");
            return Ok(None);
        }

        let (labels, items) = build_picker_page(&conversations, page, total_pages, total_u32);

        let idx = Select::new()
            .with_prompt(format!(
                "Select a conversation (page {}/{})",
                page + 1,
                total_pages
            ))
            .items(&labels)
            .default(usize::from(page > 0))
            .interact()?;

        match &items[idx] {
            PickerItem::Conversation(conv_idx) => {
                return Ok(Some(conversations[*conv_idx].id.clone()));
            }
            PickerItem::PreviousPage => {
                page = page.saturating_sub(1);
            }
            PickerItem::NextPage => {
                page += 1;
            }
        }
    }
}

/// Builds the labels and item mappings for one page of the conversation
/// picker.
fn build_picker_page(
    conversations: &[ConversationSummary],
    page: u32,
    total_pages: u32,
    total_count: u32,
) -> (Vec<String>, Vec<PickerItem>) {
    let mut labels: Vec<String> = Vec::new();
    let mut items: Vec<PickerItem> = Vec::new();

    // "Previous page" at top if not first page
    if page > 0 {
        labels.push(format!(
            "\u{2190} Previous page ({total_count} total conversations)"
        ));
        items.push(PickerItem::PreviousPage);
    }

    // Conversation entries
    for (i, conv) in conversations.iter().enumerate() {
        let title = conv.title.as_deref().unwrap_or("(no title)");
        let display_title = if title.len() > 60 {
            format!("{}...", &title[..57])
        } else {
            title.to_string()
        };

        let short_id = if conv.id.len() >= 8 {
            &conv.id[..8]
        } else {
            &conv.id
        };

        labels.push(format!(
            "{short_id} \u{2014} {display_title} ({} msgs)",
            conv.message_count
        ));
        items.push(PickerItem::Conversation(i));
    }

    // "Next page" at bottom if not last page
    if (page + 1) < total_pages {
        labels.push(format!(
            "Next page \u{2192} ({total_count} total conversations)"
        ));
        items.push(PickerItem::NextPage);
    }

    (labels, items)
}
