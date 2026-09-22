pub mod history;
pub mod prioritized;

use sqlx::{migrate::Migrator, sqlite::SqliteConnectOptions, SqlitePool};
use std::sync::OnceLock;
use tracing::info;

pub(crate) static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

const DB_FILE: &str = "jd_history.db";

static POOL: OnceLock<SqlitePool> = OnceLock::new();

/// Shared database handle
pub fn pool() -> Option<&'static SqlitePool> {
    POOL.get()
}

pub async fn init() -> Result<(), sqlx::Error> {
    if POOL.get().is_some() {
        return Ok(());
    }
    let _ = POOL.set(open_and_migrate().await?);
    info!("history database ready");
    Ok(())
}

async fn open_and_migrate() -> Result<SqlitePool, sqlx::Error> {
    let options = SqliteConnectOptions::new()
        .filename(DB_FILE)
        .create_if_missing(true);
    let pool = SqlitePool::connect_with(options).await?;
    MIGRATOR.run(&pool).await?;
    Ok(pool)
}
