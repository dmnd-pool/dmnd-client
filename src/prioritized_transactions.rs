use std::{
    collections::HashMap,
    sync::{Mutex, OnceLock},
};

use bitcoin::{blockdata::transaction::Transaction, Txid};

static PRIORITIZED_TRANSACTIONS: OnceLock<Mutex<HashMap<Txid, Transaction>>> = OnceLock::new();

fn transactions() -> &'static Mutex<HashMap<Txid, Transaction>> {
    PRIORITIZED_TRANSACTIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(crate) fn record(transaction: Transaction) {
    let txid = transaction.compute_txid();
    transactions()
        .lock()
        .expect("prioritized transactions mutex poisoned")
        .insert(txid, transaction);
}

pub(crate) fn snapshot() -> Vec<(Txid, Transaction)> {
    transactions()
        .lock()
        .expect("prioritized transactions mutex poisoned")
        .iter()
        .map(|(txid, transaction)| (*txid, transaction.clone()))
        .collect()
}

pub(crate) fn snapshot_txids() -> Vec<Txid> {
    transactions()
        .lock()
        .expect("prioritized transactions mutex poisoned")
        .keys()
        .copied()
        .collect()
}

pub(crate) fn remove(txid: &Txid) {
    transactions()
        .lock()
        .expect("prioritized transactions mutex poisoned")
        .remove(txid);
}
