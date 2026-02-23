//! Database connection utilities.

use switchy_database::Database;
use switchy_database_connection::Credentials;

/// Creates a new database connection from the `DATABASE_URL` environment
/// variable.
///
/// # Errors
///
/// Returns an error if the `DATABASE_URL` is not set or the connection fails.
pub async fn connect_from_env() -> Result<Box<dyn Database>, Box<dyn std::error::Error>> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5440/crime_map".to_string());

    let creds = Credentials::from_url(&url)?;
    let db = switchy_database_connection::init_postgres_raw_native_tls(creds).await?;
    Ok(db)
}
