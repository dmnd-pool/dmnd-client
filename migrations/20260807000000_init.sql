-- Identifiers are stored as raw bytes, timestamps as unix seconds.

-- One row per declaration the pool accepted. `height` is null only when the
-- coinbase prefix would not decode. Retention keeps this table small, so no indexes.
CREATE TABLE job_declarations (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    template_id      INTEGER NOT NULL,
    channel_id       INTEGER NOT NULL,
    mining_job_token BLOB    NOT NULL,
    height           INTEGER,
    total_fees_sat   INTEGER,
    total_weight     INTEGER,
    txid_count       INTEGER NOT NULL,
    -- 32 raw bytes per txid, concatenated. Only read by the txids endpoint.
    txids            BLOB,
    created_at       INTEGER NOT NULL
);

-- Transactions the operator asked bitcoind to prioritise, kept so the list
-- survives a proxy restart. Rows are deleted when bitcoind stops reporting them.
CREATE TABLE prioritized_transactions (
    txid       BLOB PRIMARY KEY,
    tx         BLOB NOT NULL,
    created_at INTEGER NOT NULL
);

-- Settings that survive a restart.
CREATE TABLE meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);

-- Blocks of history to keep. NULL keeps everything.
INSERT INTO meta (key, value) VALUES ('history_keep_blocks', '5');
