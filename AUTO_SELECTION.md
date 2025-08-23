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

**Purpose:** Balance between fees and transaction count efficiency.

**Algorithm:** Uses a balanced score: `fee_rate × (1 / √weight)` to consider both profitability and efficiency.

**Best for:** General-purpose mining with good balance of revenue and network support.

```bash
GET /api/auto-select?selectionStrategy=balanced&maxTransactionCount=800
```

## Usage Examples

### Basic Usage

```bash
# Default behavior (maximize fees)
GET /api/auto-select

# High fee rate transactions only
GET /api/auto-select?selectionStrategy=maximizeFees&minFeeRate=10

# Balanced selection with custom limits
GET /api/auto-select?selectionStrategy=balanced&maxTransactionCount=500&minFeeRate=2
```

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

### Memory Pool Integration

- Directly integrates with Bitcoin Core's mempool via RPC
- Uses `getrawmempoolverbose` for complete transaction information
- Respects Bitcoin Core's transaction validation and policy rules
- Handles external dependencies by excluding transactions with unconfirmed parents outside the mempool

## Algorithm Details

### Selection Strategies Implementation

1. **MaximizeFees**: Sorts by `fee_rate` (descending)
2. **MaximizeCount**: Sorts by `total_weight` (ascending)
3. **Balanced**: Sorts by `fee_rate × (1 / √weight)` (descending)### Bundle Selection Algorithm

For each candidate transaction:

1. Identify all unincluded ancestor transactions using BFS
2. Calculate total bundle weight and transaction count
3. Check if bundle fits within block size and count limits
4. If yes, include entire bundle using topological ordering
5. Update block state and continue to next candidate
