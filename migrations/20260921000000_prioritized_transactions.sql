DROP TABLE IF EXISTS prioritized_transactions;

CREATE TABLE prioritized_transactions (
    txid         BLOB    NOT NULL,
    -- 'manual' or 'mempool_space'.
    source       TEXT    NOT NULL,
    fee_delta    INTEGER NOT NULL,
    -- 1 once the transaction appeared in a template we declared.
    declared     INTEGER NOT NULL DEFAULT 0,
    created_at   INTEGER NOT NULL,
    PRIMARY KEY (txid, source)
);
