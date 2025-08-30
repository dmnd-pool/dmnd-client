use crate::api::mempool::MempoolTransaction;
use bitcoin::Txid;
use serde::Serialize;
use std::collections::{HashMap, HashSet, VecDeque};
use tracing::{debug, info, warn};

/// Configuration for transaction selection algorithm
#[derive(Debug, Clone)]
pub struct TransactionSelectionConfig {
    /// Maximum block weight (4,000,000 weight units)
    pub max_block_weight: u64,
    /// Maximum number of transactions to include
    pub max_transactions: usize,
    /// Minimum fee rate (satoshis per vbyte)
    pub min_fee_rate: f64,
    /// Maximum ancestor count for any transaction
    pub max_ancestor_count: u64,
    /// Maximum descendant count for any transaction  
    pub max_descendant_count: u64,
    /// Reserve weight for coinbase transaction and other overhead
    pub reserved_weight: u64,
    /// Selection strategy to use
    pub selection_strategy: SelectionStrategy,
}

/// Available selection strategies
#[derive(Debug, Clone)]
pub enum SelectionStrategy {
    /// Maximize total fees collected
    MaximizeFees,
    /// Maximize number of transactions included
    MaximizeCount,
    /// Balanced approach considering both fees and count
    Balanced,
}

impl TransactionSelectionConfig {
    pub fn new() -> Self {
        Self {
            max_block_weight: 4_000_000,
            max_transactions: 10000,
            min_fee_rate: 1.0, // 1 sat/vbyte minimum
            max_ancestor_count: 25,
            max_descendant_count: 25,
            reserved_weight: 4000, // Reserve for coinbase and overhead
            selection_strategy: SelectionStrategy::MaximizeFees,
        }
    }

    /// Create configuration from AutoSelectParams
    pub fn from_params(params: &crate::api::routes::AutoSelectParams) -> Self {
        let mut config = Self::new();

        // Apply parameter overrides to config
        if let Some(min_fee_rate) = params.min_fee_rate {
            config.min_fee_rate = min_fee_rate;
        }
        if let Some(max_ancestor_count) = params.max_ancestor_count {
            config.max_ancestor_count = max_ancestor_count;
        }
        if let Some(max_descendant_count) = params.max_descendant_count {
            config.max_descendant_count = max_descendant_count;
        }
        if let Some(max_tx_count) = params.max_transaction_count {
            config.max_transactions = max_tx_count;
        }
        // Set selection strategy
        if let Some(strategy) = &params.selection_strategy {
            // Convert from routes::SelectionStrategy to transaction_selector::SelectionStrategy
            config.selection_strategy = match strategy {
                crate::api::routes::SelectionStrategy::MaximizeFees => {
                    SelectionStrategy::MaximizeFees
                }
                crate::api::routes::SelectionStrategy::MaximizeCount => {
                    SelectionStrategy::MaximizeCount
                }
                crate::api::routes::SelectionStrategy::Balanced => SelectionStrategy::Balanced,
            };
        }
        println!("Divyansh Strategy: {:?}", config.selection_strategy);

        config
    }
}

/// Represents a transaction with its dependency information
#[derive(Debug, Clone)]
pub struct TransactionNode {
    pub txid: Txid,
    pub fee_rate: f64, // satoshis per vbyte
    pub ancestors: HashSet<Txid>,
    pub descendants: HashSet<Txid>,
    pub total_fee: u64,
    pub total_weight: u64,
    pub is_included: bool,
}

impl TransactionNode {
    pub fn new(transaction: MempoolTransaction) -> Result<Self, String> {
        let txid = transaction
            .txid
            .parse::<Txid>()
            .map_err(|e| format!("Failed to parse txid: {}", e))?;
        let fee_rate = transaction.fee_rate;

        let ancestors: Result<HashSet<Txid>, _> = transaction
            .depends
            .into_iter()
            .map(|s| {
                s.parse::<Txid>()
                    .map_err(|e| format!("Failed to parse ancestor txid {}: {}", s, e))
            })
            .collect();
        let ancestors = ancestors?;

        let descendants: Result<HashSet<Txid>, _> = transaction
            .spent_by
            .into_iter()
            .map(|s| {
                s.parse::<Txid>()
                    .map_err(|e| format!("Failed to parse descendant txid {}: {}", s, e))
            })
            .collect();
        let descendants = descendants?;

        Ok(Self {
            txid,
            fee_rate,
            ancestors,
            descendants,
            total_fee: transaction.fees.base.to_sat(),
            total_weight: transaction.weight.unwrap_or(transaction.vsize * 4),
            is_included: false,
        })
    }
}

/// Transaction selection result
#[derive(Debug, Serialize)]
pub struct SelectionResult {
    pub selected_txids: Vec<String>,
    pub total_fee: u64,
    pub total_weight: u64,
    pub transaction_count: usize,
    pub selection_method: String,
    pub stats: SelectionStats,
}

#[derive(Debug, Serialize)]
pub struct SelectionStats {
    pub total_mempool_transactions: usize,
    pub valid_transactions_after_filtering: usize,
    pub conflicts_resolved: usize,
    pub orphaned_transactions_removed: usize,
    pub selection_time_ms: u64,
}

/// Main transaction selection engine
pub struct TransactionSelector {
    config: TransactionSelectionConfig,
}

impl TransactionSelector {
    pub fn new(config: TransactionSelectionConfig) -> Self {
        Self { config }
    }

    /// Select transactions for mining using the configured strategy with validation
    pub async fn select_transactions(
        &self,
        mempool_transactions: Vec<MempoolTransaction>,
    ) -> Result<SelectionResult, String> {
        let start_time = std::time::Instant::now();
        let initial_count = mempool_transactions.len();

        // Step 1: Build validated transaction DAG
        let mut transaction_map = self.build_transaction_dag(mempool_transactions)?;
        let valid_count = transaction_map.len();

        // Step 2: Select transactions using the configured strategy
        let selected_txids = self.select_transactions_by_strategy(&mut transaction_map)?;

        // Step 3: Validate selection ordering (ensure parents come before children)
        let final_txids = self.validate_and_order_selection(&transaction_map, selected_txids)?;

        // Step 4: Calculate totals
        let (total_fee, total_weight) = self.calculate_totals(&transaction_map, &final_txids);

        let selection_time = start_time.elapsed().as_millis() as u64;

        let result = SelectionResult {
            selected_txids: final_txids.iter().map(|tx| tx.to_string()).collect(),
            total_fee,
            total_weight,
            transaction_count: final_txids.len(),
            selection_method: format!("{:?}", self.config.selection_strategy),
            stats: SelectionStats {
                total_mempool_transactions: initial_count,
                valid_transactions_after_filtering: valid_count,
                conflicts_resolved: 0,            // Always 0 for RPC mempool
                orphaned_transactions_removed: 0, // Bitcoin Core handles this
                selection_time_ms: selection_time,
            },
        };

        info!(
            "Transaction selection completed: {} transactions selected, total fee: {} sats, total weight: {} WU",
            result.transaction_count, result.total_fee, result.total_weight
        );

        Ok(result)
    }

    /// Validate and order the selected transactions to ensure mining validity
    fn validate_and_order_selection(
        &self,
        transaction_map: &HashMap<Txid, TransactionNode>,
        selected_txids: Vec<Txid>,
    ) -> Result<Vec<Txid>, String> {
        let selected_set: HashSet<Txid> = selected_txids.iter().cloned().collect();

        // Verify all dependencies are satisfied
        for &txid in &selected_txids {
            if let Some(node) = transaction_map.get(&txid) {
                for &ancestor in &node.ancestors {
                    if !selected_set.contains(&ancestor) {
                        return Err(format!(
                            "Transaction {} selected but its ancestor {} is not included",
                            txid, ancestor
                        ));
                    }
                }
            }
        }

        // Use topological sort to ensure correct ordering
        let ordered_txids = self.topo_sort(transaction_map, &selected_set);

        if ordered_txids.len() != selected_txids.len() {
            return Err(format!(
                "Topological sort failed: expected {} transactions, got {}",
                selected_txids.len(),
                ordered_txids.len()
            ));
        }

        Ok(ordered_txids)
    }

    /// Build a DAG from mempool transactions with validation
    /// Ensures that selected transactions will form a valid set for mining
    pub fn build_transaction_dag(
        &self,
        mempool: Vec<MempoolTransaction>,
    ) -> Result<HashMap<Txid, TransactionNode>, String> {
        let mut tx_map: HashMap<Txid, TransactionNode> = HashMap::new();
        let mut all_mempool_txids: HashSet<String> = HashSet::new();

        // Step 1: Create initial map of all transactions and track all txids
        let mut initial_nodes: HashMap<Txid, TransactionNode> = HashMap::new();
        for mtx in mempool.iter() {
            all_mempool_txids.insert(mtx.txid.clone());
            if let Ok(node) = TransactionNode::new(mtx.clone()) {
                initial_nodes.insert(node.txid, node);
            }
        }

        // Step 2: Apply filtering but validate dependencies
        for mtx in mempool.into_iter() {
            let fee_rate = mtx.fees.base.to_sat() as f64 / mtx.vsize as f64;

            // Apply Bitcoin Core compatible filtering
            if fee_rate < self.config.min_fee_rate
                || mtx.ancestor_count > self.config.max_ancestor_count
                || mtx.descendant_count > self.config.max_descendant_count
            {
                continue;
            }

            // Check if this transaction has dependencies outside our mempool
            let has_external_deps = mtx
                .depends
                .iter()
                .any(|dep| !all_mempool_txids.contains(dep));
            if has_external_deps {
                warn!(
                    "Transaction {} has dependencies outside mempool - excluding to ensure validity",
                    mtx.txid
                );
                continue;
            }

            let node = TransactionNode::new(mtx)
                .map_err(|e| format!("Failed to create transaction node: {}", e))?;
            tx_map.insert(node.txid, node);
        }

        // Step 3: Validate that all dependencies are satisfied
        let existing_txids: HashSet<Txid> = tx_map.keys().cloned().collect();
        let mut invalid_transactions = Vec::new();

        for (txid, node) in &tx_map {
            // Check if all ancestors are in our filtered set
            for ancestor in &node.ancestors {
                if !existing_txids.contains(ancestor) {
                    invalid_transactions.push(*txid);
                    warn!(
                        "Transaction {} depends on filtered ancestor {} - marking invalid",
                        txid, ancestor
                    );
                    break;
                }
            }
        }

        // Remove transactions with missing dependencies
        for txid in invalid_transactions {
            tx_map.remove(&txid);
        }

        // Step 4: Clean up dependency edges after validation
        let final_txids: HashSet<Txid> = tx_map.keys().cloned().collect();
        for node in tx_map.values_mut() {
            let original_ancestor_count = node.ancestors.len();
            let original_descendant_count = node.descendants.len();

            node.ancestors
                .retain(|parent_txid| final_txids.contains(parent_txid));
            node.descendants
                .retain(|child_txid| final_txids.contains(child_txid));

            // Log if we removed any edges (useful for debugging)
            if node.ancestors.len() != original_ancestor_count
                || node.descendants.len() != original_descendant_count
            {
                debug!(
                    "Cleaned edges for {}: ancestors {} -> {}, descendants {} -> {}",
                    node.txid,
                    original_ancestor_count,
                    node.ancestors.len(),
                    original_descendant_count,
                    node.descendants.len()
                );
            }
        }

        Ok(tx_map)
    }

    /// Topological sort for a set of txids (acyclic, using Kahn’s algorithm).
    fn topo_sort(
        &self,
        nodes: &HashMap<Txid, TransactionNode>,
        subset: &HashSet<Txid>,
    ) -> Vec<Txid> {
        // Build in‑degree map restricted to subset
        let mut indegree = HashMap::with_capacity(subset.len());
        let mut children: HashMap<Txid, Vec<Txid>> = HashMap::new();
        for &txid in subset {
            indegree.insert(txid, 0);
        }
        for &txid in subset {
            for &p in &nodes[&txid].ancestors {
                if subset.contains(&p) {
                    *indegree.get_mut(&txid).unwrap() += 1;
                    children.entry(p).or_default().push(txid);
                }
            }
        }
        // Initialize queue with zero in‑degree
        let mut queue: VecDeque<_> = indegree
            .iter()
            .filter_map(|(&txid, &deg)| if deg == 0 { Some(txid) } else { None })
            .collect();

        let mut sorted = Vec::with_capacity(subset.len());
        while let Some(txid) = queue.pop_front() {
            sorted.push(txid);
            if let Some(ch) = children.remove(&txid) {
                for c in ch {
                    let deg = indegree.get_mut(&c).unwrap();
                    *deg -= 1;
                    if *deg == 0 {
                        queue.push_back(c);
                    }
                }
            }
        }
        if sorted.len() != subset.len() {
            warn!("Cycle detected in ancestor graph subset – falling back to arbitrary order");
            let sorted_set: HashSet<Txid> = sorted.iter().cloned().collect();
            for &txid in subset {
                if !sorted_set.contains(&txid) {
                    sorted.push(txid);
                }
            }
        }
        sorted
    }

    /// Select transactions based on the configured strategy
    pub fn select_transactions_by_strategy(
        &self,
        transaction_map: &mut HashMap<Txid, TransactionNode>,
    ) -> Result<Vec<Txid>, String> {
        let sorted_transactions = self.generate_sorted_transaction_list(transaction_map)?;
        self.select_sorted_transactions(transaction_map, sorted_transactions)
    }

    /// Generate sorted transactions based on the strategy's priority function
    fn generate_sorted_transaction_list(
        &self,
        transaction_map: &HashMap<Txid, TransactionNode>,
    ) -> Result<Vec<Txid>, String> {
        let mut sortable_transactions: Vec<(Txid, f64)> = transaction_map
            .iter()
            .filter_map(|(&txid, node)| {
                if node.is_included {
                    None
                } else {
                    let sort_metric = self.calculate_transaction_priority(node);
                    Some((txid, sort_metric))
                }
            })
            .collect();

        // Sort based on strategy (higher priority values first, except for MaximizeCount which uses weight directly)
        match self.config.selection_strategy {
            SelectionStrategy::MaximizeCount => {
                // For count maximization, sort by weight (ascending)
                let mut transactions_by_weight: Vec<(Txid, u64)> = transaction_map
                    .iter()
                    .filter_map(|(&txid, node)| {
                        if node.is_included {
                            None
                        } else {
                            Some((txid, node.total_weight))
                        }
                    })
                    .collect();
                transactions_by_weight.sort_by_key(|&(_, weight)| weight);
                return Ok(transactions_by_weight
                    .into_iter()
                    .map(|(txid, _)| txid)
                    .collect());
            }
            _ => {
                sortable_transactions
                    .sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
            }
        }

        Ok(sortable_transactions
            .into_iter()
            .map(|(txid, _)| txid)
            .collect())
    }

    /// Calculate priority value for a transaction based on the current strategy
    fn calculate_transaction_priority(&self, node: &TransactionNode) -> f64 {
        match self.config.selection_strategy {
            SelectionStrategy::MaximizeFees => {
                if node.fee_rate.is_nan() {
                    0.0
                } else {
                    node.fee_rate
                }
            }
            SelectionStrategy::Balanced => {
                // Improved balanced algorithm using efficiency ratio
                self.calculate_balanced_score(node)
            }
            SelectionStrategy::MaximizeCount => {
                // This case is handled separately in generate_sorted_candidates
                node.total_weight as f64
            }
        }
    }

    /// Calculate balanced score that optimizes both fees and transaction count
    fn calculate_balanced_score(&self, node: &TransactionNode) -> f64 {
        let fee_rate = if node.fee_rate.is_nan() {
            0.0
        } else {
            node.fee_rate
        };

        // Method 1: Multi-objective score (60% fees, 40% efficiency)
        let fee_efficiency = fee_rate / 100.0; // Normalize assuming max ~100 sat/vB
        let space_efficiency = 25.0 / node.total_weight as f64; // Normalize assuming min ~25 weight

        let fee_weight = 0.6;
        let count_weight = 0.4;

        let multi_objective_score = fee_weight * fee_efficiency + count_weight * space_efficiency;

        // Method 2: Efficiency ratio (geometric mean)
        let fee_per_weight = node.total_fee as f64 / node.total_weight as f64;
        let space_ratio = 1000.0 / node.total_weight as f64; // Scale for better comparison
        let efficiency_ratio = (fee_per_weight * space_ratio).sqrt();

        // Method 3: Logarithmic scaling (less harsh on large transactions)
        let log_weight_penalty = 1.0 / (1.0 + (node.total_weight as f64 / 1000.0).ln());
        let log_balanced = fee_rate * log_weight_penalty;

        // Combine all methods with weights
        let combined_score = 0.4 * multi_objective_score * 1000.0 +  // Scale up for comparison
            0.4 * efficiency_ratio +
            0.2 * log_balanced;

        combined_score
    }

    /// Core selection logic that processes a sorted list of transactions
    fn select_sorted_transactions(
        &self,
        transaction_map: &mut HashMap<Txid, TransactionNode>,
        sorted_txids: Vec<Txid>,
    ) -> Result<Vec<Txid>, String> {
        let mut selected_txids = Vec::new();
        let mut current_block_weight = self.config.reserved_weight;
        let max_block_weight = self.config.max_block_weight;
        let max_transactions = self.config.max_transactions;
        let mut current_tx_count = 0;

        for txid in sorted_txids {
            if current_tx_count >= max_transactions {
                break;
            }

            // Get required ancestors and compute bundle metrics
            // bundle -> transaction plus all of its required ancestors
            let (ancestor_txids, bundle_weight, bundle_count) =
                self.compute_bundle_metrics(transaction_map, txid)?;

            // Check if bundle fits within limits
            if current_block_weight + bundle_weight > max_block_weight
                || current_tx_count + bundle_count > max_transactions
            {
                continue;
            }
            // Include the bundle (ancestors + transaction)
            let included = self.include_bundle_transactions(
                transaction_map,
                txid,
                ancestor_txids,
                &mut current_block_weight,
                &mut current_tx_count,
            )?;
            selected_txids.extend(included);
        }

        Ok(selected_txids)
    }

    /// Compute bundle metrics (ancestors, weight, count) for a transaction
    fn compute_bundle_metrics(
        &self,
        transaction_map: &HashMap<Txid, TransactionNode>,
        txid: Txid,
    ) -> Result<(Vec<Txid>, u64, usize), String> {
        // Gather all unincluded ancestors using BFS
        let mut ancestors_to_process = HashSet::new();
        let mut bfs_queue = VecDeque::new();
        bfs_queue.extend(&transaction_map[&txid].ancestors);

        while let Some(ancestor_txid) = bfs_queue.pop_front() {
            if !ancestors_to_process.insert(ancestor_txid) {
                continue;
            }
            if let Some(node) = transaction_map.get(&ancestor_txid) {
                if !node.is_included {
                    bfs_queue.extend(&node.ancestors);
                }
            }
        }

        let required_ancestors = self.topo_sort(transaction_map, &ancestors_to_process);

        // Calculate bundle weight and count
        let mut bundle_weight = 0;
        let mut bundle_count = 1; // for this transaction

        for &ancestor in &required_ancestors {
            if !transaction_map[&ancestor].is_included {
                bundle_weight += transaction_map[&ancestor].total_weight;
                bundle_count += 1;
            }
        }
        bundle_weight += transaction_map[&txid].total_weight;

        Ok((required_ancestors, bundle_weight, bundle_count))
    }

    /// Include a transaction bundle (ancestors + transaction) and update state
    fn include_bundle_transactions(
        &self,
        transaction_map: &mut HashMap<Txid, TransactionNode>,
        txid: Txid,
        ancestor_txids: Vec<Txid>,
        current_block_weight: &mut u64,
        current_tx_count: &mut usize,
    ) -> Result<Vec<Txid>, String> {
        let mut included_txids = Vec::new();

        // Include ancestors first
        for ancestor in ancestor_txids {
            if let Some(node) = transaction_map.get_mut(&ancestor) {
                if !node.is_included {
                    included_txids.push(ancestor);
                    *current_block_weight += node.total_weight;
                    node.is_included = true;
                    *current_tx_count += 1;
                }
            }
        }

        // Include the transaction itself
        if let Some(node) = transaction_map.get_mut(&txid) {
            if !node.is_included {
                included_txids.push(txid);
                *current_block_weight += node.total_weight;
                node.is_included = true;
                *current_tx_count += 1;
            }
        }

        Ok(included_txids)
    }

    /// Sum fees & weight for a given list
    pub fn calculate_totals(
        &self,
        transaction_map: &HashMap<Txid, TransactionNode>,
        selected_txids: &[Txid],
    ) -> (u64, u64) {
        let mut total_fee = 0;
        let mut total_weight = 0;
        for &txid in selected_txids {
            if let Some(node) = transaction_map.get(&txid) {
                total_fee += node.total_fee;
                total_weight += node.total_weight;
            }
        }
        (total_fee, total_weight)
    }
}
