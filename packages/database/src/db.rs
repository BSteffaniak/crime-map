//! Database connection utilities.

use switchy_database::Database;
use switchy_database_connection::Credentials;

/// Creates a new database connection from the `DATABASE_URL` environment
/// variable.
///
/// Configures a 120-second `statement_timeout` so stalled queries fail with
/// an error instead of hanging indefinitely.
///
/// # Errors
///
/// Returns an error if the `DATABASE_URL` is not set or the connection fails.
pub async fn connect_from_env() -> Result<Box<dyn Database>, Box<dyn std::error::Error>> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5440/crime_map".to_string());

    // Strip query parameters (e.g., ?sslmode=require&channel_binding=require)
    // that the Credentials parser doesn't understand. TLS is handled by the
    // native-tls connector automatically.
    let url_base = url.split('?').next().unwrap_or(&url);

    let creds = Credentials::from_url(url_base)?;
    let db = switchy_database_connection::init_postgres_raw_native_tls(creds).await?;

    // Prevent queries from hanging indefinitely on remote databases (e.g. Neon).
    // 120s is generous for even the largest bulk INSERT chunks.
    db.exec_raw("SET statement_timeout = '120s'").await?;

    Ok(db)
}
