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
