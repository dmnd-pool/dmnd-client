use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct JobDeclarationWithTxids {
    pub id: i64,
    pub template_id: i64,
    pub channel_id: i64,
    pub request_id: i64,
    pub job_id: i64,
    pub mining_job_token: String,
    pub txids_json: Option<String>, // JSON array of txids
    pub txid_count: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct JobHistoryItem {
    pub id: i64,
    pub template_id: i64,
    pub channel_id: i64,
    pub request_id: i64,
    pub job_id: i64,
    pub mining_job_token: String,
    pub txid_count: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct JobHistoryResponse {
    pub jobs: Vec<JobHistoryItem>,
    pub total: i64,
    pub page: i64,
    pub per_page: i64,
    pub total_pages: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct JobTxidsResponse {
    pub template_id: i64,
    pub txids: Vec<String>, // Just return the txids as strings
    pub total: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct JobDeclarationInsert {
    pub template_id: i64,
    pub channel_id: i64,
    pub request_id: i64,
    pub job_id: i64,
    pub mining_job_token: String,
    pub txids: Vec<String>, // Include txids in the insert
}

// Settings Models
#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct Settings {
    pub id: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,

    // Auto Selection Settings
    pub auto_selection_enabled: bool,
    pub selection_strategy: String,
    pub min_fee_rate: f64,
    pub max_size: i64,
    pub min_base_fee: f64,
    pub max_ancestor_count: i64,
    pub max_descendant_count: i64,
    pub exclude_bip125_replaceable: bool,
    pub exclude_unbroadcast: bool,
    pub max_transaction_count: i64,
    pub require_template: bool,
    pub clear_existing_selections: bool,
    pub periodic_enabled: bool,
    pub periodic_interval: i64,
    pub auto_job_declaration: bool,

    // General Settings
    pub auto_scroll_to_table: bool,
    pub show_notifications: bool,
    pub pause_on_selection: bool,
    pub clear_selection_on_job_declaration: bool,
    pub preserve_existing_selections: bool,
    pub auto_clean_invalid_transactions: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SettingsRequest {
    // Auto Selection Settings
    pub auto_selection_enabled: Option<bool>,
    pub selection_strategy: Option<String>,
    pub min_fee_rate: Option<f64>,
    pub max_size: Option<i64>,
    pub min_base_fee: Option<f64>,
    pub max_ancestor_count: Option<i64>,
    pub max_descendant_count: Option<i64>,
    pub exclude_bip125_replaceable: Option<bool>,
    pub exclude_unbroadcast: Option<bool>,
    pub max_transaction_count: Option<i64>,
    pub require_template: Option<bool>,
    pub clear_existing_selections: Option<bool>,
    pub periodic_enabled: Option<bool>,
    pub periodic_interval: Option<i64>,
    pub auto_job_declaration: Option<bool>,

    // General Settings
    pub auto_scroll_to_table: Option<bool>,
    pub show_notifications: Option<bool>,
    pub pause_on_selection: Option<bool>,
    pub clear_selection_on_job_declaration: Option<bool>,
    pub preserve_existing_selections: Option<bool>,
    pub auto_clean_invalid_transactions: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SettingsResponse {
    pub id: i64,
    pub created_at: String,
    pub updated_at: String,

    // Auto Selection Settings
    pub auto_selection_enabled: bool,
    pub selection_strategy: String,
    pub min_fee_rate: f64,
    pub max_size: i64,
    pub min_base_fee: f64,
    pub max_ancestor_count: i64,
    pub max_descendant_count: i64,
    pub exclude_bip125_replaceable: bool,
    pub exclude_unbroadcast: bool,
    pub max_transaction_count: i64,
    pub require_template: bool,
    pub clear_existing_selections: bool,
    pub periodic_enabled: bool,
    pub periodic_interval: i64,
    pub auto_job_declaration: bool,

    // General Settings
    pub auto_scroll_to_table: bool,
    pub show_notifications: bool,
    pub pause_on_selection: bool,
    pub clear_selection_on_job_declaration: bool,
    pub preserve_existing_selections: bool,
    pub auto_clean_invalid_transactions: bool,
}

impl From<Settings> for SettingsResponse {
    fn from(settings: Settings) -> Self {
        Self {
            id: settings.id,
            created_at: settings.created_at.to_rfc3339(),
            updated_at: settings.updated_at.to_rfc3339(),
            auto_selection_enabled: settings.auto_selection_enabled,
            selection_strategy: settings.selection_strategy,
            min_fee_rate: settings.min_fee_rate,
            max_size: settings.max_size,
            min_base_fee: settings.min_base_fee,
            max_ancestor_count: settings.max_ancestor_count,
            max_descendant_count: settings.max_descendant_count,
            exclude_bip125_replaceable: settings.exclude_bip125_replaceable,
            exclude_unbroadcast: settings.exclude_unbroadcast,
            max_transaction_count: settings.max_transaction_count,
            require_template: settings.require_template,
            clear_existing_selections: settings.clear_existing_selections,
            periodic_enabled: settings.periodic_enabled,
            periodic_interval: settings.periodic_interval,
            auto_job_declaration: settings.auto_job_declaration,
            auto_scroll_to_table: settings.auto_scroll_to_table,
            show_notifications: settings.show_notifications,
            pause_on_selection: settings.pause_on_selection,
            clear_selection_on_job_declaration: settings.clear_selection_on_job_declaration,
            preserve_existing_selections: settings.preserve_existing_selections,
            auto_clean_invalid_transactions: settings.auto_clean_invalid_transactions,
        }
    }
}
