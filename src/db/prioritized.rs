use bitcoin::{hashes::Hash, Txid};
use serde::Serialize;
use sqlx::{Row, Sqlite, SqlitePool, Transaction};
use tracing::error;

/// The two values the `source` column takes, named once so a typo cannot quietly
/// invent a third cache.
pub const MANUAL: &str = "manual";
pub const MEMPOOL_SPACE: &str = "mempool_space";

pub async fn record(source: &str, txid: &Txid, fee_delta: i64) {
    if let Some(pool) = crate::db::pool() {
        if let Err(error) = execute(pool, source, txid, fee_delta).await {
            error!(%error, %txid, "failed to update prioritized transaction history");
        }
    }
}

async fn execute(
    pool: &SqlitePool,
    source: &str,
    txid: &Txid,
    fee_delta: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO prioritized_transactions (txid, source, fee_delta, created_at)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(txid, source) DO UPDATE SET fee_delta = excluded.fee_delta",
    )
    .bind(txid.to_byte_array().to_vec())
    .bind(source)
    .bind(fee_delta)
    .bind(crate::block_templates::unix_now() as i64)
    .execute(pool)
    .await?;
    Ok(())
}

/// One stored prioritization, as the dashboard shows it.
#[derive(Debug, Serialize)]
pub struct Prioritization {
    pub txid: String,
    pub source: String,
    pub fee_delta: i64,
    pub declared: bool,
    pub created_at: i64,
}

#[derive(Debug, Serialize)]
pub struct Page {
    pub transactions: Vec<Prioritization>,
    pub total: i64,
    pub page: i64,
    pub per_page: i64,
    pub total_pages: i64,
}

fn read(row: sqlx::sqlite::SqliteRow) -> Prioritization {
    Prioritization {
        txid: Txid::from_slice(&row.get::<Vec<u8>, _>("txid"))
            .map(|txid| txid.to_string())
            .unwrap_or_default(),
        source: row.get("source"),
        fee_delta: row.get("fee_delta"),
        declared: row.get::<i64, _>("declared") != 0,
        created_at: row.get("created_at"),
    }
}

/// One page of prioritizations, newest first.
pub async fn page(pool: &SqlitePool, page: i64, per_page: i64) -> Result<Page, sqlx::Error> {
    let per_page = per_page.clamp(1, 100);
    let page = page.max(1);
    let total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM prioritized_transactions")
        .fetch_one(pool)
        .await?;

    let rows = sqlx::query(
        "SELECT txid, source, fee_delta, declared, created_at
           FROM prioritized_transactions
          ORDER BY created_at DESC, rowid DESC
          LIMIT ? OFFSET ?",
    )
    .bind(per_page)
    .bind((page - 1).saturating_mul(per_page))
    .fetch_all(pool)
    .await?;

    let transactions = rows.into_iter().map(read).collect();

    Ok(Page {
        transactions,
        total,
        page,
        per_page,
        total_pages: (total + per_page - 1) / per_page,
    })
}

pub async fn mark_declared(
    transaction: &mut Transaction<'_, Sqlite>,
    transactions: &[crate::block_templates::TemplateTx],
) -> Result<(), sqlx::Error> {
    let caches = [
        (
            MANUAL,
            crate::prioritized_transactions::PRIORITIZED_TRANSACTIONS.snapshot_txids(),
        ),
        (
            MEMPOOL_SPACE,
            crate::prioritized_transactions::MEMPOOL_DOT_SPACE_ACCELERATED.snapshot_txids(),
        ),
    ];

    for tx in transactions {
        for (source, prioritized) in &caches {
            if !prioritized.contains(&tx.txid) {
                continue;
            }
            sqlx::query(
                "UPDATE prioritized_transactions SET declared = 1
                  WHERE txid = ? AND source = ?",
            )
            .bind(tx.txid.to_byte_array().to_vec())
            .bind(*source)
            .execute(&mut **transaction)
            .await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::Row;

    async fn database() -> SqlitePool {
        let pool = SqlitePool::connect("sqlite::memory:")
            .await
            .expect("in-memory sqlite");
        crate::db::MIGRATOR.run(&pool).await.expect("migrate");
        pool
    }

    /// One row as (fee delta, declared), if it is there.
    async fn row(pool: &SqlitePool, source: &str, txid: &Txid) -> Option<(i64, i64)> {
        sqlx::query(
            "SELECT fee_delta, declared FROM prioritized_transactions
              WHERE txid = ? AND source = ?",
        )
        .bind(txid.to_byte_array().to_vec())
        .bind(source)
        .fetch_optional(pool)
        .await
        .expect("select")
        .map(|row| (row.get("fee_delta"), row.get("declared")))
    }

    async fn record(pool: &SqlitePool, source: &str, txid: &Txid, fee_delta: i64) {
        execute(pool, source, txid, fee_delta)
            .await
            .expect("execute");
    }

    fn txid(byte: u8) -> Txid {
        Txid::from_byte_array([byte; 32])
    }

    #[tokio::test]
    async fn each_cache_is_tracked_on_its_own_and_repeating_a_record_is_free() {
        let pool = database().await;
        let shared = txid(1);

        // The same transaction in both caches is two rows, each with its own delta.
        record(&pool, MANUAL, &shared, 500).await;
        record(&pool, MEMPOOL_SPACE, &shared, 700).await;
        assert_eq!(row(&pool, MANUAL, &shared).await, Some((500, 0)));
        assert_eq!(row(&pool, MEMPOOL_SPACE, &shared).await, Some((700, 0)));

        // Storing again replaces the fee delta rather than adding a row, and leaves
        // the other cache alone. Every poll may do this.
        record(&pool, MANUAL, &shared, 600).await;
        record(&pool, MANUAL, &shared, 600).await;
        assert_eq!(row(&pool, MANUAL, &shared).await, Some((600, 0)));
        assert_eq!(row(&pool, MEMPOOL_SPACE, &shared).await, Some((700, 0)));

        // The dashboard reads whole rows back.
        let other = txid(2);
        record(&pool, MANUAL, &other, 900).await;
        let listed = page(&pool, 1, 10).await.expect("page");
        assert_eq!((listed.total, listed.total_pages), (3, 1));
        let manual = listed
            .transactions
            .iter()
            .find(|row| row.txid == other.to_string() && row.source == "manual")
            .expect("the row is listed");
        assert_eq!(manual.fee_delta, 900);
        assert!(!manual.declared);

        crate::prioritized_transactions::PRIORITIZED_TRANSACTIONS.record(shared, 600);
        crate::prioritized_transactions::MEMPOOL_DOT_SPACE_ACCELERATED.record(shared, 700);
        let template_tx = crate::block_templates::TemplateTx {
            txid: shared,
            weight: 400,
            vsize: 100,
            fee_sat: Some(1_000),
        };
        let mut transaction = pool.begin().await.expect("begin");
        mark_declared(&mut transaction, &[template_tx])
            .await
            .expect("mark declared priorities");
        transaction.commit().await.expect("commit");

        assert_eq!(row(&pool, MANUAL, &shared).await, Some((600, 1)));
        assert_eq!(row(&pool, MEMPOOL_SPACE, &shared).await, Some((700, 1)));

        crate::prioritized_transactions::PRIORITIZED_TRANSACTIONS.remove(&shared);
        crate::prioritized_transactions::MEMPOOL_DOT_SPACE_ACCELERATED.remove(&shared);
    }

    #[tokio::test]
    async fn a_hostile_page_number_does_not_overflow() {
        let pool = database().await;
        let listed = page(&pool, i64::MAX, 100).await.expect("page");
        assert!(listed.transactions.is_empty());
    }
}
