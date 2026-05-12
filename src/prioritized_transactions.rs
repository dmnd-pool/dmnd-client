use std::{
    collections::HashSet,
    sync::{Mutex, OnceLock},
};

static PRIORITIZED_TXIDS: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();

fn txids() -> &'static Mutex<HashSet<String>> {
    PRIORITIZED_TXIDS.get_or_init(|| Mutex::new(HashSet::new()))
}

pub(crate) fn record(txid: &str) {
    txids()
        .lock()
        .expect("prioritized transactions mutex poisoned")
        .insert(txid.to_string());
}

pub(crate) fn snapshot() -> Vec<String> {
    txids()
        .lock()
        .expect("prioritized transactions mutex poisoned")
        .iter()
        .cloned()
        .collect()
}

pub(crate) fn remove(txid: &str) {
    txids()
        .lock()
        .expect("prioritized transactions mutex poisoned")
        .remove(txid);
}
