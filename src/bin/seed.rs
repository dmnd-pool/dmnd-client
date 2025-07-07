use sqlx::{migrate::MigrateDatabase, sqlite::SqlitePool, Sqlite};
use std::env;

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    // Accept number of rows from CLI
    let args: Vec<String> = env::args().collect();
    let row_count = args
        .get(1)
        .and_then(|arg| arg.parse::<usize>().ok())
        .unwrap_or(10);

    let pool = connect_db().await?;

    for i in 0..row_count {
        let template_id = 1 + i as i32;
        let channel_id = 100 + i as i32;
        let request_id = 200 + i as i32;
        let job_id = 300 + i as i32;
        let mining_job_token = format!("token_{}", i);
        let txids = vec!["txid1", "txid2"];
        let txids_json = serde_json::to_string(&txids).unwrap();
        let txid_count = txids.len() as i32;
        let now = chrono::Utc::now().naive_utc();

        sqlx::query!(
            r#"
        INSERT INTO job_declarations (
            template_id, channel_id, request_id, job_id,
            mining_job_token, txids_json, txid_count,
            created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
            template_id,
            channel_id,
            request_id,
            job_id,
            mining_job_token,
            txids_json,
            txid_count,
            now,
            now,
        )
        .execute(&pool)
        .await?;
    }

    println!("Seeded {} rows into `job_declarations`.", row_count);
    Ok(())
}

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
