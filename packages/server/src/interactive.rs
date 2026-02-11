//! Interactive mode for the server.
//!
//! Prompts the user for bind address and port before starting the server.

use dialoguer::{Confirm, Input};

/// Runs the server in interactive mode, prompting for configuration.
///
/// Asks the user for a bind address and port, sets the corresponding
/// environment variables (`BIND_ADDR`, `PORT`), and delegates to
/// [`super::run_server`].
///
/// # Errors
///
/// Returns an `std::io::Result` error if the underlying server fails to
/// start.
#[allow(clippy::future_not_send)]
pub async fn run() -> std::io::Result<()> {
    println!("Crime Map Server");
    println!();

    let bind_addr: String = Input::new()
        .with_prompt("Bind address")
        .default("127.0.0.1".to_string())
        .interact_text()
        .unwrap_or_else(|_| "127.0.0.1".to_string());

    let port_str: String = Input::new()
        .with_prompt("Port")
        .default("8080".to_string())
        .interact_text()
        .unwrap_or_else(|_| "8080".to_string());

    // SAFETY: We are single-threaded at this point (before server starts) and
    // these variables are only read once during server initialisation.
    unsafe {
        std::env::set_var("BIND_ADDR", &bind_addr);
        std::env::set_var("PORT", &port_str);
    }

    if !Confirm::new()
        .with_prompt(format!("Start server on {bind_addr}:{port_str}?"))
        .default(true)
        .interact()
        .unwrap_or(true)
    {
        println!("Cancelled.");
        return Ok(());
    }

    super::run_server().await
}
