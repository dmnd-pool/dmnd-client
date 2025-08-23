use crate::db::model::{
    JobDeclarationInsert, JobDeclarationWithTxids, JobHistoryItem, JobHistoryResponse,
    JobTxidsResponse, Settings, SettingsRequest, SettingsResponse,
};
use crate::db::settings_updater::SettingsUpdater;
use sqlx::SqlitePool;

pub struct JobDeclarationHandler {
    pool: SqlitePool,
}

impl JobDeclarationHandler {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Insert a new job declaration record with txids
    pub async fn insert_job_declaration(
        &self,
        job_declaration: &JobDeclarationInsert,
    ) -> Result<i64, sqlx::Error> {
        let txids_json = serde_json::to_string(&job_declaration.txids)
            .map_err(|e| sqlx::Error::Encode(Box::new(e)))?;
        let txid_count = job_declaration.txids.len() as i64;

        let result = sqlx::query(
            r#"
            INSERT OR REPLACE INTO job_declarations 
            (template_id, channel_id, request_id, job_id, mining_job_token, txids_json, txid_count, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            "#,
        )
        .bind(job_declaration.template_id)
        .bind(job_declaration.channel_id)
        .bind(job_declaration.request_id)
        .bind(job_declaration.job_id)
        .bind(&job_declaration.mining_job_token)
        .bind(&txids_json)
        .bind(txid_count)
        .execute(&self.pool)
        .await?;

        Ok(result.last_insert_rowid())
    }

    /// Database function: Get job declaration history with pagination
    pub async fn get_job_history(
        &self,
        page: i64,
        per_page: i64,
    ) -> Result<JobHistoryResponse, sqlx::Error> {
        let per_page = per_page.min(100).max(1); // Limit per_page to 100 max, 1 min
        let offset = (page - 1) * per_page;

        // Get total count
        let total: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*) FROM job_declarations
            "#,
        )
        .fetch_one(&self.pool)
        .await?;

        // Get paginated results
        let jobs: Vec<JobHistoryItem> = sqlx::query_as(
            r#"
            SELECT 
                id,
                template_id,
                channel_id,
                request_id,
                job_id,
                mining_job_token,
                txid_count,
                created_at,
                updated_at
            FROM job_declarations
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?
            "#,
        )
        .bind(per_page)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;

        let total_pages = (total + per_page - 1) / per_page;

        Ok(JobHistoryResponse {
            jobs,
            total,
            page,
            per_page,
            total_pages,
        })
    }

    /// Database function: Get txids for a specific template
    pub async fn get_job_txids(&self, template_id: i64) -> Result<JobTxidsResponse, sqlx::Error> {
        // Get job declaration with txids
        let job_declaration: Option<JobDeclarationWithTxids> = sqlx::query_as(
            r#"
            SELECT id, template_id, channel_id, request_id, job_id, mining_job_token, 
                   txids_json, txid_count, created_at, updated_at
            FROM job_declarations
            WHERE template_id = ?
            ORDER BY created_at DESC
            LIMIT 1
            "#,
        )
        .bind(template_id)
        .fetch_optional(&self.pool)
        .await?;

        match job_declaration {
            Some(job) => {
                let txids = if let Some(txids_json) = job.txids_json {
                    serde_json::from_str::<Vec<String>>(&txids_json).unwrap_or_else(|_| Vec::new())
                } else {
                    Vec::new()
                };

                Ok(JobTxidsResponse {
                    template_id,
                    txids,
                    total: job.txid_count,
                })
            }
            None => Ok(JobTxidsResponse {
                template_id,
                txids: Vec::new(),
                total: 0,
            }),
        }
    }
}

pub struct SettingsHandler {
    pool: SqlitePool,
}

impl SettingsHandler {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Get settings, create default settings if none exist
    pub async fn get_or_create_settings(&self) -> Result<SettingsResponse, sqlx::Error> {
        // Try to get existing settings (there should only be one row)
        let existing_settings: Option<Settings> = sqlx::query_as(
            r#"
            SELECT id, created_at, updated_at,
                   auto_selection_enabled, selection_strategy, min_fee_rate, max_size, min_base_fee,
                   max_ancestor_count, max_descendant_count, exclude_bip125_replaceable, exclude_unbroadcast,
                   max_transaction_count, require_template, clear_existing_selections, periodic_enabled,
                   periodic_interval, auto_job_declaration, auto_scroll_to_table, show_notifications,
                   pause_on_selection, clear_selection_on_job_declaration, preserve_existing_selections,
                   auto_clean_invalid_transactions
            FROM settings
            LIMIT 1
            "#,
        )
        .fetch_optional(&self.pool)
        .await?;

        match existing_settings {
            Some(settings) => Ok(settings.into()),
            None => {
                // Create default settings
                let result = sqlx::query(
                    r#"
                    INSERT INTO settings DEFAULT VALUES
                    "#,
                )
                .execute(&self.pool)
                .await?;

                // Fetch the newly created settings
                let settings: Settings = sqlx::query_as(
                    r#"
                    SELECT id, created_at, updated_at,
                           auto_selection_enabled, selection_strategy, min_fee_rate, max_size, min_base_fee,
                           max_ancestor_count, max_descendant_count, exclude_bip125_replaceable, exclude_unbroadcast,
                           max_transaction_count, require_template, clear_existing_selections, periodic_enabled,
                           periodic_interval, auto_job_declaration, auto_scroll_to_table, show_notifications,
                           pause_on_selection, clear_selection_on_job_declaration, preserve_existing_selections,
                           auto_clean_invalid_transactions
                    FROM settings
                    WHERE id = ?
                    "#,
                )
                .bind(result.last_insert_rowid())
                .fetch_one(&self.pool)
                .await?;

                Ok(settings.into())
            }
        }
    }

    pub async fn update_settings(
        &self,
        settings_request: &SettingsRequest,
    ) -> Result<SettingsResponse, sqlx::Error> {
        // Ensure settings exist first
        self.get_or_create_settings().await?;

        let updater = SettingsUpdater::new(self.pool.clone());
        updater.update_fields(settings_request).await?;
        self.get_or_create_settings().await
    }
}
