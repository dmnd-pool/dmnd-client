use crate::db::model::SettingsRequest;
use sqlx::SqlitePool;

pub struct SettingsUpdater {
    pool: SqlitePool,
}

impl SettingsUpdater {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    async fn update_field<T>(&self, field_name: &str, value: Option<T>) -> Result<(), sqlx::Error>
    where
        T: for<'q> sqlx::Encode<'q, sqlx::Sqlite> + sqlx::Type<sqlx::Sqlite> + Send,
    {
        if let Some(val) = value {
            let sql = format!(
                "UPDATE settings SET {} = ?, updated_at = CURRENT_TIMESTAMP",
                field_name
            );
            sqlx::query(&sql).bind(val).execute(&self.pool).await?;
        }
        Ok(())
    }

    pub async fn update_fields(
        &self,
        settings_request: &SettingsRequest,
    ) -> Result<(), sqlx::Error> {
        // Auto Selection Settings
        self.update_field(
            "auto_selection_enabled",
            settings_request.auto_selection_enabled,
        )
        .await?;
        self.update_field(
            "selection_strategy",
            settings_request.selection_strategy.as_ref(),
        )
        .await?;
        self.update_field("min_fee_rate", settings_request.min_fee_rate)
            .await?;
        self.update_field("max_size", settings_request.max_size)
            .await?;
        self.update_field("min_base_fee", settings_request.min_base_fee)
            .await?;
        self.update_field("max_ancestor_count", settings_request.max_ancestor_count)
            .await?;
        self.update_field(
            "max_descendant_count",
            settings_request.max_descendant_count,
        )
        .await?;
        self.update_field(
            "exclude_bip125_replaceable",
            settings_request.exclude_bip125_replaceable,
        )
        .await?;
        self.update_field("exclude_unbroadcast", settings_request.exclude_unbroadcast)
            .await?;
        self.update_field(
            "max_transaction_count",
            settings_request.max_transaction_count,
        )
        .await?;
        self.update_field("require_template", settings_request.require_template)
            .await?;
        self.update_field(
            "clear_existing_selections",
            settings_request.clear_existing_selections,
        )
        .await?;
        self.update_field("periodic_enabled", settings_request.periodic_enabled)
            .await?;
        self.update_field("periodic_interval", settings_request.periodic_interval)
            .await?;
        self.update_field(
            "auto_job_declaration",
            settings_request.auto_job_declaration,
        )
        .await?;
        self.update_field(
            "preserve_existing_selections",
            settings_request.preserve_existing_selections,
        )
        .await?;

        // General Settings
        self.update_field(
            "auto_scroll_to_table",
            settings_request.auto_scroll_to_table,
        )
        .await?;
        self.update_field("show_notifications", settings_request.show_notifications)
            .await?;
        self.update_field("pause_on_selection", settings_request.pause_on_selection)
            .await?;
        self.update_field(
            "clear_selection_on_job_declaration",
            settings_request.clear_selection_on_job_declaration,
        )
        .await?;
        self.update_field(
            "auto_clean_invalid_transactions",
            settings_request.auto_clean_invalid_transactions,
        )
        .await?;

        Ok(())
    }
}
