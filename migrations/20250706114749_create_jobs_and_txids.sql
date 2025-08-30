-- Add migration script here
-- Create table for job declarations with txids stored as JSON array
CREATE TABLE IF NOT EXISTS job_declarations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    template_id INTEGER NOT NULL,
    channel_id INTEGER NOT NULL,
    request_id INTEGER NOT NULL,
    job_id INTEGER NOT NULL,
    mining_job_token TEXT NOT NULL,
    txids_json TEXT,
    txid_count INTEGER DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
-- Create indexes for better performance
CREATE INDEX idx_job_declarations_template_id ON job_declarations(template_id);
CREATE INDEX idx_job_declarations_created_at ON job_declarations(created_at);
CREATE INDEX idx_job_declarations_request_id ON job_declarations(request_id);