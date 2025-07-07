pub mod handlers;
pub mod model;
use sqlx::{migrate::MigrateDatabase, Sqlite, SqlitePool};

pub async fn connect_db() -> Result<SqlitePool, sqlx::Error> {
    const DB_URL: &str = "sqlite://jd_history.db";
    if !Sqlite::database_exists(DB_URL).await.unwrap_or(false) {
        println!("Creating database {}", DB_URL);
        match Sqlite::create_database(DB_URL).await {
            Ok(_) => println!("Create db success"),
            Err(error) => panic!("error: {}", error),
        }
    } else {
        println!("Database already exists");
    }

    let db = match SqlitePool::connect(DB_URL).await {
        Ok(pool) => pool,
        Err(e) => return Err(e),
    };

    let crate_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let migrations = std::path::Path::new(&crate_dir).join("./migrations");

    let migration_results = sqlx::migrate::Migrator::new(migrations)
        .await
        .unwrap()
        .run(&db)
        .await;
    match migration_results {
        Ok(_) => println!("Migration success"),
        Err(error) => {
            panic!("error: {}", error);
        }
    }
    Ok(db)
}
