-- Add migration script here
-- Create table for user settings
CREATE TABLE IF NOT EXISTS settings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    -- Auto Selection Settings
    auto_selection_enabled BOOLEAN NOT NULL DEFAULT 0,
    selection_strategy TEXT NOT NULL DEFAULT 'fee_rate',
    min_fee_rate REAL NOT NULL DEFAULT 1.0,
    max_size INTEGER NOT NULL DEFAULT 1000000,
    min_base_fee REAL NOT NULL DEFAULT 0.0,
    max_ancestor_count INTEGER NOT NULL DEFAULT 25,
    max_descendant_count INTEGER NOT NULL DEFAULT 25,
    exclude_bip125_replaceable BOOLEAN NOT NULL DEFAULT 0,
    exclude_unbroadcast BOOLEAN NOT NULL DEFAULT 0,
    max_transaction_count INTEGER NOT NULL DEFAULT 100,
    require_template BOOLEAN NOT NULL DEFAULT 0,
    clear_existing_selections BOOLEAN NOT NULL DEFAULT 1,
    periodic_enabled BOOLEAN NOT NULL DEFAULT 0,
    periodic_interval INTEGER NOT NULL DEFAULT 30,
    auto_job_declaration BOOLEAN NOT NULL DEFAULT 0,
    -- General Settings
    auto_scroll_to_table BOOLEAN NOT NULL DEFAULT 1,
    show_notifications BOOLEAN NOT NULL DEFAULT 1,
    pause_on_selection BOOLEAN NOT NULL DEFAULT 0,
    clear_selection_on_job_declaration BOOLEAN NOT NULL DEFAULT 0,
    preserve_existing_selections BOOLEAN NOT NULL DEFAULT 1,
    auto_clean_invalid_transactions BOOLEAN NOT NULL DEFAULT 1
);