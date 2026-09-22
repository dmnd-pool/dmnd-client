use bitcoin::{hashes::Hash, Txid};
use serde::Serialize;
use sqlx::{Row, SqlitePool};
use tracing::{error, info, warn};

/// Default number of blocks to keep.
const DEFAULT_KEEP_BLOCKS: i64 = 5;

const KEEP_BLOCKS_KEY: &str = "history_keep_blocks";

/// One recorded declaration.
#[derive(Debug, Serialize)]
pub struct Declaration {
    pub id: i64,
    pub template_id: i64,
    pub channel_id: i64,
    pub mining_job_token: String,
    pub height: Option<i64>,
    pub total_fees_sat: Option<i64>,
    pub total_weight: Option<i64>,
    pub txid_count: i64,
    pub created_at: i64,
}

#[derive(Debug, Serialize)]
pub struct Page {
    pub jobs: Vec<Declaration>,
    pub total: i64,
    pub page: i64,
    pub per_page: i64,
    pub total_pages: i64,
}

/// Record a declaration the pool has accepted.
pub async fn record(
    template_id: u64,
    channel_id: u32,
    job_id: u32,
    mining_job_token: Option<Vec<u8>>,
) {
    let Some(pool) = crate::db::pool() else {
        return;
    };

    let snapshot = crate::block_templates::by_id(template_id);
    if snapshot.is_none() {
        warn!(
            template_id,
            "recording a declaration whose candidate is already gone; \
             the transaction list and block context are not available"
        );
    }

    // Raw 32-byte txids, concatenated.
    let txids: Vec<u8> = snapshot
        .as_ref()
        .map(|snapshot| {
            snapshot
                .transactions
                .iter()
                .flat_map(|tx| tx.txid.to_byte_array())
                .collect()
        })
        .unwrap_or_default();
    let txid_count = (txids.len() / 32) as i64;

    let token = mining_job_token.unwrap_or_default();

    let insert = async {
        let mut transaction = pool.begin().await?;
        sqlx::query(
            "INSERT INTO job_declarations
                 (template_id, channel_id, mining_job_token, height, total_fees_sat,
                  total_weight, txid_count, txids, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(template_id as i64)
        .bind(channel_id as i64)
        .bind(token)
        .bind(snapshot.as_ref().and_then(|s| s.height.map(i64::from)))
        .bind(
            snapshot
                .as_ref()
                .and_then(|s| s.total_fees_sat)
                .and_then(|fees| i64::try_from(fees).ok()),
        )
        .bind(
            snapshot
                .as_ref()
                .and_then(|s| i64::try_from(s.total_weight).ok()),
        )
        .bind(txid_count)
        .bind(txids)
        .bind(crate::block_templates::unix_now() as i64)
        .execute(&mut *transaction)
        .await?;

        if let Some(snapshot) = snapshot.as_ref() {
            crate::db::prioritized::mark_declared(&mut transaction, &snapshot.transactions).await?;
        }

        transaction.commit().await
    }
    .await;

    match insert {
        Ok(_) => info!(
            template_id,
            job_id, "recorded the declaration in the history"
        ),
        Err(error) => error!(%error, template_id, "failed to record the declaration"),
    }
}

/// Prune once the tip has moved.
pub fn prune_for_new_tip() {
    let Some(pool) = crate::db::pool() else {
        return;
    };
    let pool = pool.clone();
    tokio::spawn(async move { prune(&pool).await });
}

async fn prune(pool: &SqlitePool) {
    let Some(keep) = keep_blocks(pool).await else {
        return;
    };
    // Rows with no height (undecodable coinbase prefix) are never pruned.
    let pruned = sqlx::query(
        "DELETE FROM job_declarations
          WHERE height IS NOT NULL
            AND height < (SELECT MIN(height) FROM
                           (SELECT DISTINCT height FROM job_declarations
                             WHERE height IS NOT NULL
                          ORDER BY height DESC LIMIT ?))",
    )
    .bind(keep)
    .execute(pool)
    .await;

    match pruned {
        Ok(result) if result.rows_affected() > 0 => info!(
            removed = result.rows_affected(),
            keep_blocks = keep,
            "pruned declarations for blocks past the retention"
        ),
        Ok(_) => {}
        Err(error) => error!(%error, "failed to prune the declaration history"),
    }
}

/// How many blocks of history to keep, or `None` to keep everything.
pub async fn keep_blocks(pool: &SqlitePool) -> Option<i64> {
    let stored: Option<String> = sqlx::query_scalar("SELECT value FROM meta WHERE key = ?")
        .bind(KEEP_BLOCKS_KEY)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten();
    stored.and_then(|value| value.parse().ok())
}

/// Set retention. `None` keeps everything.
pub async fn set_keep_blocks(pool: &SqlitePool, keep: Option<i64>) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value")
        .bind(KEEP_BLOCKS_KEY)
        .bind(keep.map(|keep| keep.to_string()))
        .execute(pool)
        .await?;
    prune(pool).await;
    Ok(())
}

/// The default a fresh database starts with.
pub const fn default_keep_blocks() -> i64 {
    DEFAULT_KEEP_BLOCKS
}

/// One page of declarations, newest first.
pub async fn page(pool: &SqlitePool, page: i64, per_page: i64) -> Result<Page, sqlx::Error> {
    let per_page = per_page.clamp(1, 100);
    let page = page.max(1);
    let total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM job_declarations")
        .fetch_one(pool)
        .await?;

    let rows = sqlx::query(
        "SELECT id, template_id, channel_id, mining_job_token, height, total_fees_sat,
                total_weight, txid_count, created_at
           FROM job_declarations
          ORDER BY created_at DESC, id DESC
          LIMIT ? OFFSET ?",
    )
    .bind(per_page)
    .bind((page - 1).saturating_mul(per_page))
    .fetch_all(pool)
    .await?;

    let jobs = rows
        .into_iter()
        .map(|row| Declaration {
            id: row.get("id"),
            template_id: row.get("template_id"),
            channel_id: row.get("channel_id"),
            mining_job_token: bytes_to_hex(&row.get::<Vec<u8>, _>("mining_job_token")),
            height: row.get("height"),
            total_fees_sat: row.get("total_fees_sat"),
            total_weight: row.get("total_weight"),
            txid_count: row.get("txid_count"),
            created_at: row.get("created_at"),
        })
        .collect();

    Ok(Page {
        jobs,
        total,
        page,
        per_page,
        total_pages: (total + per_page - 1) / per_page,
    })
}

pub async fn txids(pool: &SqlitePool, template_id: i64) -> Result<Vec<String>, sqlx::Error> {
    let stored: Option<Vec<u8>> = sqlx::query_scalar(
        "SELECT txids FROM job_declarations
          WHERE template_id = ?
          ORDER BY created_at DESC, id DESC
          LIMIT 1",
    )
    .bind(template_id)
    .fetch_optional(pool)
    .await?
    .flatten();

    Ok(stored
        .unwrap_or_default()
        .chunks_exact(32)
        .filter_map(|bytes| Txid::from_slice(bytes).ok())
        .map(|txid| txid.to_string())
        .collect())
}

/// Delete every declaration.
pub async fn clear(pool: &SqlitePool) -> Result<u64, sqlx::Error> {
    let removed = sqlx::query("DELETE FROM job_declarations")
        .execute(pool)
        .await?
        .rows_affected();
    let _ = sqlx::query("VACUUM").execute(pool).await;
    info!(removed, "cleared the declaration history");
    Ok(removed)
}

/// Hex for display; tokens are stored as raw bytes.
fn bytes_to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn database() -> SqlitePool {
        let pool = SqlitePool::connect("sqlite::memory:")
            .await
            .expect("in-memory sqlite");
        crate::db::MIGRATOR.run(&pool).await.expect("migrate");
        pool
    }

    /// Insert a declaration with `transactions` txids at `height`.
    async fn declare(pool: &SqlitePool, template_id: i64, height: i64, transactions: u8) {
        let txids: Vec<u8> = (0..transactions).flat_map(|n| [n; 32]).collect();
        sqlx::query(
            "INSERT INTO job_declarations
                 (template_id, channel_id, mining_job_token, height, total_fees_sat,
                  total_weight, txid_count, txids, created_at)
             VALUES (?, 1, ?, ?, 10, 20, ?, ?, ?)",
        )
        .bind(template_id)
        .bind(vec![0xabu8; 32])
        .bind(height)
        .bind(i64::from(transactions))
        .bind(txids)
        .bind(template_id)
        .execute(pool)
        .await
        .expect("insert");
    }

    #[tokio::test]
    async fn retention_keeps_whole_blocks_and_clearing_leaves_nothing() {
        let pool = database().await;
        // Four blocks, two declarations each.
        for (template_id, height) in (0..8).map(|i| (i, 900_000 + i / 2)) {
            declare(&pool, template_id, height, 3).await;
        }

        assert_eq!(keep_blocks(&pool).await, Some(DEFAULT_KEEP_BLOCKS));
        assert_eq!(page(&pool, 1, 100).await.expect("page").total, 8);

        // Retention counts blocks, not rows.
        set_keep_blocks(&pool, Some(2)).await.expect("set");
        let kept = page(&pool, 1, 100).await.expect("page");
        assert_eq!(kept.total, 4);
        let heights: Vec<Option<i64>> = kept.jobs.iter().map(|job| job.height).collect();
        assert!(heights.iter().all(|height| *height >= Some(900_002)));

        // Null keeps everything.
        set_keep_blocks(&pool, None).await.expect("set");
        assert_eq!(keep_blocks(&pool).await, None);
        assert_eq!(page(&pool, 1, 100).await.expect("page").total, 4);

        assert_eq!(clear(&pool).await.expect("clear"), 4);
        assert_eq!(page(&pool, 1, 100).await.expect("page").total, 0);
    }
}
