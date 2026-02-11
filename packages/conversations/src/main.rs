#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![warn(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]
#![allow(clippy::multiple_crate_versions, clippy::cargo_common_metadata)]

//! CLI for browsing and auditing AI conversations.
//!
//! ```text
//! cargo conversations list [--limit 20]
//! cargo conversations show <id>
//! cargo conversations export <id>
//! cargo conversations delete <id>
//! ```
//!
//! Running `cargo conversations` with no subcommand enters interactive mode.
//!
//! This binary is exposed via the cargo alias defined in `.cargo/config.toml`.

use std::path::Path;

use clap::{Parser, Subcommand};
use crime_map_conversations::{
    DEFAULT_DB_PATH, delete_conversation, format_conversation, get_conversation_messages,
    list_conversations, load_messages, open_db, resolve_id,
};

#[derive(Parser)]
#[command(
    name = "crime_map_conversations",
    about = "Browse and audit AI conversation history"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// List recent conversations
    List {
        /// Maximum number of conversations to show
        #[arg(long, default_value = "20")]
        limit: u32,
    },
    /// Show a conversation in readable format
    Show {
        /// Conversation ID (UUID or prefix)
        id: String,
        /// Include internal system advisory messages (budget/timeout warnings)
        #[arg(long)]
        show_system: bool,
    },
    /// Export a conversation as JSON
    Export {
        /// Conversation ID (UUID or prefix)
        id: String,
    },
    /// Delete a conversation
    Delete {
        /// Conversation ID
        id: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    pretty_env_logger::init();
    let cli = Cli::parse();

    let Some(command) = cli.command else {
        return crime_map_conversations::interactive::run().await;
    };

    let db = open_db(Path::new(DEFAULT_DB_PATH)).await?;

    match command {
        Commands::List { limit } => {
            let conversations = list_conversations(db.as_ref(), limit, 0).await?;

            if conversations.is_empty() {
                println!("No conversations found.");
                return Ok(());
            }

            println!("{:<38} {:<6} {:<22} TITLE", "ID", "MSGS", "UPDATED");
            println!("{}", "-".repeat(100));

            for conv in &conversations {
                let title = conv.title.as_deref().unwrap_or("(no title)");
                // Truncate title for display
                let display_title = if title.len() > 50 {
                    format!("{}...", &title[..47])
                } else {
                    title.to_string()
                };

                // Parse and format the date more compactly
                let date = &conv.updated_at;
                let short_date = if date.len() >= 19 { &date[..19] } else { date };

                println!(
                    "{:<38} {:<6} {:<22} {}",
                    conv.id, conv.message_count, short_date, display_title
                );
            }

            println!("\n{} conversation(s)", conversations.len());
        }
        Commands::Show { id, show_system } => {
            let resolved = resolve_id(db.as_ref(), &id).await?;
            let messages = get_conversation_messages(db.as_ref(), &resolved).await?;

            if let Some(msgs) = messages {
                println!("Conversation: {resolved}\n");
                print!("{}", format_conversation(&msgs, show_system));
            } else {
                eprintln!("Conversation not found: {id}");
                std::process::exit(1);
            }
        }
        Commands::Export { id } => {
            let resolved = resolve_id(db.as_ref(), &id).await?;
            let messages = load_messages(db.as_ref(), &resolved).await?;

            if let Some(msgs) = messages {
                let json = serde_json::to_string_pretty(&msgs)?;
                println!("{json}");
            } else {
                eprintln!("Conversation not found: {id}");
                std::process::exit(1);
            }
        }
        Commands::Delete { id } => {
            let deleted = delete_conversation(db.as_ref(), &id).await?;
            if deleted {
                println!("Deleted conversation: {id}");
            } else {
                eprintln!("Conversation not found: {id}");
                std::process::exit(1);
            }
        }
    }

    Ok(())
}
