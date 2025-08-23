use crate::api::mempool::MempoolTransaction;
use crate::api::routes::AutoSelectParams;

use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};

pub(super) async fn get_cpu_and_memory_usage() -> (f32, u64) {
    let mut system = System::new_all();

    // First refresh to get initial values
    system.refresh_processes_specifics(
        ProcessesToUpdate::All,
        true,
        ProcessRefreshKind::nothing().with_cpu().with_memory(),
    );

    // Wait for a measurable interval
    tokio::time::sleep(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL).await;

    // Second refresh to get the difference
    system.refresh_processes_specifics(
        ProcessesToUpdate::All,
        true,
        ProcessRefreshKind::nothing().with_cpu().with_memory(),
    );

    let pid = std::process::id();
    if let Some(process) = system.process(Pid::from_u32(pid)) {
        let cpu_usage = process.cpu_usage();
        let cpu_nums = system.cpus().len() as f32; // get the number of cpu

        let normalized_cpu_usage = if cpu_nums > 0.0 {
            cpu_usage / cpu_nums
        } else {
            0.0
        };

        let memory = process.memory();
        (normalized_cpu_usage, memory)
    } else {
        (0.0, 0)
    }
}

pub fn filter_mempool_transaction(tx: &MempoolTransaction, params: &AutoSelectParams) -> bool {
    // Filter by minimum fee rate
    if let Some(min_fee_rate) = params.min_fee_rate {
        if tx.fee_rate < min_fee_rate {
            return false;
        }
    }

    // Filter by maximum size (vsize)
    if let Some(max_size) = params.max_size {
        if tx.vsize > max_size {
            return false;
        }
    }

    // Filter by minimum base fee
    if let Some(min_base_fee) = params.min_base_fee {
        let base_fee_sats = tx.fees.base.to_sat();
        if base_fee_sats < min_base_fee {
            return false;
        }
    }

    // Filter by maximum ancestor count
    if let Some(max_ancestor_count) = params.max_ancestor_count {
        if tx.ancestor_count > max_ancestor_count {
            return false;
        }
    }

    // Filter by maximum descendant count
    if let Some(max_descendant_count) = params.max_descendant_count {
        if tx.descendant_count > max_descendant_count {
            return false;
        }
    }

    // Filter by BIP125 replaceable flag
    if let Some(exclude_bip125) = params.exclude_bip125_replaceable {
        if exclude_bip125 && tx.bip125_replaceable {
            return false;
        }
    }

    // Filter by unbroadcast flag
    if let Some(exclude_unbroadcast) = params.exclude_unbroadcast {
        if exclude_unbroadcast && tx.unbroadcast {
            return false;
        }
    }

    true
}
