# Transaction Auto-Selection Configuration

This document describes the transaction auto-selection system in the demand-cli, which automatically selects transactions from the mempool for mining based on configurable strategies and parameters.

## Overview

The auto-selection system goes beyond simple fee maximization to provide multiple selection strategies that miners can choose based on their specific needs and network conditions. The system filters the mempool based on configurable parameters and then applies the selected strategy to optimize transaction selection.

## API Endpoint

**Endpoint:** `GET /api/auto-select`

**Description:** Returns a list of automatically selected transactions from the mempool based on the provided parameters and strategy.

## Query Parameters

### Basic Filtering Parameters

| Parameter                  | Type      | Description                              | Default  |
| -------------------------- | --------- | ---------------------------------------- | -------- |
| `minFeeRate`               | `number`  | Minimum fee rate (sat/vbyte)             | `1.0`    |
| `maxSize`                  | `number`  | Maximum transaction size (vbytes)        | No limit |
| `minBaseFee`               | `number`  | Minimum base fee (satoshis)              | No limit |
| `maxAncestorCount`         | `number`  | Maximum ancestor count                   | `25`     |
| `maxDescendantCount`       | `number`  | Maximum descendant count                 | `25`     |
| `excludeBip125Replaceable` | `boolean` | Exclude BIP125 replaceable transactions  | `false`  |
| `excludeUnbroadcast`       | `boolean` | Exclude unbroadcast transactions         | `false`  |
| `maxTransactionCount`      | `number`  | Maximum number of transactions to select | `10000`  |

### Selection Strategy

| Parameter           | Type     | Description               | Default        |
| ------------------- | -------- | ------------------------- | -------------- |
| `selectionStrategy` | `string` | Selection strategy to use | `maximizeFees` |

## Selection Strategies

### 1. `maximizeFees` (Default)

**Purpose:** Maximize total fees collected from the selected transactions.

**Algorithm:** Sorts transactions by fee rate (highest first) to optimize for maximum revenue.

**Best for:** Mining pools focused on revenue optimization.

```bash
GET /api/auto-select?selectionStrategy=maximizeFees&minFeeRate=5
```

### 2. `maximizeCount`

**Purpose:** Include as many transactions as possible within block limits.

**Algorithm:** Sorts transactions by smallest weight first to fit maximum number of transactions.

**Best for:** Supporting network transaction throughput and reducing mempool congestion.

```bash
GET /api/auto-select?selectionStrategy=maximizeCount&maxTransactionCount=1000
```

### 3. `balanced`

**Purpose:** Optimally balance between maximizing fees and transaction count using advanced multi-objective optimization.

**Algorithm:** Uses a sophisticated hybrid scoring system that combines multiple mathematical approaches:

1. **Multi-Objective Score (40% weight)**: Weighted combination of normalized fee efficiency (60%) and space efficiency (40%)
2. **Efficiency Ratio (40% weight)**: Geometric mean of fee-per-weight and space utilization ratios
3. **Logarithmic Scaling (20% weight)**: Less harsh penalty on larger transactions using logarithmic weight scaling

The combined formula: `0.4 × multi_objective_score × 1000 + 0.4 × efficiency_ratio + 0.2 × log_balanced`

Where:

- `multi_objective_score = 0.6 × (fee_rate/100) + 0.4 × (25/weight)`
- `efficiency_ratio = √(fee_per_weight × 1000/weight)`
- `log_balanced = fee_rate × (1/(1 + ln(weight/1000)))`

**Best for:** True dual optimization that maximizes both revenue and network throughput efficiently. Ideal for miners who want optimal balance without sacrificing either objective.

```bash
GET /api/auto-select?selectionStrategy=balanced&maxTransactionCount=800
```

## Usage Examples

### Basic Usage

````bash
# Default behavior (maximize fees)
GET /api/auto-select

# High fee rate transactions only
GET /api/auto-select?selectionStrategy=maximizeFees&minFeeRate=10

# Balanced selection with custom limits
```bash
GET /api/auto-select?selectionStrategy=balanced&maxTransactionCount=800&minFeeRate=2
````

### Advanced Configuration

```bash
# Complex filtering with multiple parameters
GET /api/auto-select?selectionStrategy=balanced&minFeeRate=1&maxSize=100000&minBaseFee=1000&maxAncestorCount=25&maxDescendantCount=25&excludeBip125Replaceable=false&excludeUnbroadcast=false&maxTransactionCount=100

# Exclude problematic transactions
GET /api/auto-select?selectionStrategy=balanced&excludeBip125Replaceable=true&excludeUnbroadcast=true

# Small block optimization
GET /api/auto-select?selectionStrategy=maximizeFees&maxSize=500000&maxTransactionCount=200
```

## Response Format

The API returns a JSON response with the following structure:

```json
{
  "success": true,
  "message": null,
  "data": [
    {
      "txid": "abc123...",
      "vsize": 250,
      "weight": 1000,
      "time": 1691234567,
      "height": 800000,
      "descendant_count": 5,
      "descendant_size": 1250,
      "ancestor_count": 2,
      "ancestor_size": 500,
      "wtxid": "def456...",
      "fees": {
        "base": "0.00001000",
        "modified": "0.00001000",
        "ancestor": "0.00002000",
        "descendant": "0.00003000"
      },
      "feeRate": 4.0,
      "depends": ["parent1", "parent2"],
      "spent_by": ["child1", "child2"],
      "bip125_replaceable": true,
      "unbroadcast": false
    }
  ]
}
```

### Response Fields

Each transaction object contains:

- `txid`: Transaction ID
- `vsize`: Virtual size in vbytes
- `weight`: Transaction weight (4x vsize for non-witness data)
- `time`: Time transaction entered mempool (Unix timestamp)
- `height`: Block height when transaction entered mempool
- `descendant_count`: Number of descendant transactions
- `descendant_size`: Total size of descendant transactions (vbytes)
- `ancestor_count`: Number of ancestor transactions
- `ancestor_size`: Total size of ancestor transactions (vbytes)
- `wtxid`: Witness transaction ID
- `fees`: Fee information object with amounts in BTC string format
- `feeRate`: Fee rate in sat/vbyte
- `depends`: Array of parent transaction IDs this transaction depends on
- `spent_by`: Array of child transaction IDs that spend this transaction's outputs
- `bip125_replaceable`: Whether transaction signals BIP125 Replace-by-Fee
- `unbroadcast`: Whether transaction is unbroadcast (local to this node)

## Error Handling

The API returns appropriate HTTP status codes and error messages:

### Common Errors

- **500 Internal Server Error**: Auto-selection failed due to internal error
- **503 Service Unavailable**: Bitcoin RPC not available

### Error Response Format

```json
{
  "success": false,
  "message": "Auto-selection failed: Bitcoin RPC client not available",
  "data": null
}
```

## Implementation Details

### Transaction Selection Process

1. **Mempool Fetching**: Retrieves all mempool transactions via Bitcoin Core RPC
2. **Filtering**: Applies user-specified filters (fee rate, size, ancestor limits, etc.)
3. **DAG Building**: Constructs a directed acyclic graph of transaction dependencies
4. **Strategy Application**: Applies the selected strategy algorithm to choose transactions
5. **Validation**: Ensures all dependencies are satisfied and transactions are ordered correctly
6. **Bundle Processing**: Groups transactions with their required ancestors for validity

### Dependency Handling

The system automatically handles transaction dependencies using topological sorting to ensure:

- Parent transactions are included before children
- No orphaned transactions in the selection
- Valid transaction ordering for block construction
- Bundle inclusion (transaction + all required ancestors)

### Performance Considerations

- Uses efficient algorithms to handle large mempools (10,000+ transactions)
- Filtering is applied before expensive selection algorithms
- Topological sorting ensures O(V + E) complexity for dependency resolution
- Bundle metrics are computed once per transaction to avoid redundant calculations
- Hybrid balanced scoring provides O(1) computation per transaction while maintaining mathematical rigor

### Memory Pool Integration

- Directly integrates with Bitcoin Core's mempool via RPC
- Uses `getrawmempoolverbose` for complete transaction information
- Respects Bitcoin Core's transaction validation and policy rules
- Handles external dependencies by excluding transactions with unconfirmed parents outside the mempool

## Algorithm Details

### Selection Strategies Implementation

1. **MaximizeFees**: Sorts by `fee_rate` (descending)
2. **MaximizeCount**: Sorts by `total_weight` (ascending)
3. **Balanced**: Uses advanced hybrid scoring combining:
   - Multi-objective optimization (fee efficiency + space efficiency)
   - Geometric mean efficiency ratio (fee-per-weight × space-ratio)
   - Logarithmic weight penalty for smoother large transaction handling

### Balanced Strategy Mathematical Details

The balanced strategy implements a sophisticated multi-faceted approach:

#### Component 1: Multi-Objective Score (40% weight)

```
multi_objective_score = 0.6 × (fee_rate/100) + 0.4 × (25/weight)
```

- Normalizes fee rates assuming typical max ~100 sat/vB
- Normalizes space efficiency assuming min weight ~25 units
- 60/40 split favors fees slightly while ensuring space efficiency

#### Component 2: Efficiency Ratio (40% weight)

```
efficiency_ratio = √(fee_per_weight × 1000/weight)
```

- Uses geometric mean to balance fee-per-weight vs space utilization
- Prevents extreme bias toward either very small or very profitable transactions
- Scale factor of 1000 normalizes for typical weight ranges

#### Component 3: Logarithmic Scaling (20% weight)

```
log_balanced = fee_rate × (1/(1 + ln(weight/1000)))
```

- Provides gentler penalty for larger transactions than square root
- Allows high-fee large transactions to remain competitive
- Logarithmic scaling creates smooth transition between optimization goals

#### Final Score Calculation

```
combined_score = 0.4 × multi_objective_score × 1000 + 0.4 × efficiency_ratio + 0.2 × log_balanced
```

This hybrid approach ensures true dual optimization rather than simple fee-rate weighting.### Bundle Selection Algorithm

For each candidate transaction:

1. Identify all unincluded ancestor transactions using BFS
2. Calculate total bundle weight and transaction count
3. Check if bundle fits within block size and count limits
4. If yes, include entire bundle using topological ordering
5. Update block state and continue to next candidate
