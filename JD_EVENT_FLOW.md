# JD Event Notification and Job Declaration Flow

This document explains the complete flow of JD (Job Declaration) event notifications and how job declarations from the frontend are processed and stored in the database.

## Overview

The demand-cli system implements a sophisticated Job Declaration protocol that enables custom transaction selection for mining pools. The flow involves:

1. **Template Reception** - Receiving new mining templates from the Template Provider
2. **Frontend Notification** - Broadcasting template events to connected WebSocket clients
3. **Transaction List Submission** - Frontend submits custom transaction lists
4. **Job Declaration Processing** - Converting transactions into mining jobs
5. **Pool Communication** - Declaring jobs to the mining pool
6. **Database Storage** - Persisting job declaration data

## System Architecture

```
Template Provider → TemplateNotification → Frontend (WebSocket)
                                              ↓
Database ← Job Response ← Mining Pool ← Job Declaration
```

## Detailed Flow

### 1. Template Reception and Event Broadcasting

#### 1.1 New Template Processing

- **Location**: `src/jd_client/job_declarator/mod.rs::on_new_template()`
- **Trigger**: When a new mining template is received from the Template Provider
- **Process**:

  ```rust
  // Create template notification
  let template_notification = NewTemplateNotification::new(
      "NewTemplate".to_string(),
      format!("Send the transaction list for template id: {}", template.template_id),
      template.template_id,
      timestamp
  );

  // Broadcast to WebSocket clients
  jd_event_broadcaster.send(template_notification)
  ```

#### 1.2 WebSocket Event Streaming

- **Location**: `src/dashboard/jd_event_ws.rs`
- **Endpoint**: `ws://localhost:3001/ws/jd/stream`
- **Data Structure**:
  ```rust
  pub struct NewTemplateNotification {
      pub event: String,        // Event type (e.g., "NewTemplate")
      pub message: String,      // Human-readable message
      pub template_id: u64,     // Template identifier
      pub timestamp: u64,       // Unix timestamp
  }
  ```

### 2. Frontend Transaction List Submission

#### 2.1 API Endpoint

- **Endpoint**: `POST /api/job-declaration`
- **Location**: `src/api/mempool.rs::submit_tx_list()`
- **Request Format**:
  ```json
  {
    "template_id": 12345,
    "txids": [
      "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
      "fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321"
    ]
  }
  ```

#### 2.2 Transaction Validation Process

1. **TXID Parsing**: Validate each transaction ID format
2. **RPC Lookup**: Fetch raw transaction data from Bitcoin node
3. **Serialization**: Convert transactions to binary format
4. **Error Handling**: Track invalid transactions

```rust
// Validation loop
for txid_str in txids {
    let txid = txid_str.parse::<Txid>()?;
    let raw_tx = rpc.get_raw_transaction(&txid, None)?;
    let serialized = encode::serialize(&raw_tx).try_into()?;
    txs.push(serialized);
    valid_txids.push(txid);
}
```

### 3. Job Declaration Processing

#### 3.1 Internal Communication

- **Channel**: `TxListWithResponse` type with oneshot response channel
- **Flow**: API → JobDeclarator → Mining Upstream → Pool

```rust
// Send transaction list with response channel
let (response_sender, response_receiver) = oneshot::channel();
tx_list_sender.send((seq, Some(response_sender))).await?;
```

#### 3.2 Job Declaration Creation

- **Location**: `src/jd_client/job_declarator/mod.rs`
- **Process**:
  1. Wait for transaction list from frontend (with timeout)
  2. Create `DeclareMiningJob` message
  3. Store job response tracking data
  4. Send job declaration to pool

```rust
// Create job declaration
let declare_job = DeclareMiningJob {
    request_id: id,
    mining_job_token: token,
    version: template.version,
    coinbase_prefix,
    coinbase_suffix,
    tx_list: tx_ids,
    excess_data,
};
```

### 4. Pool Communication and Response

#### 4.1 Mining Pool Communication

- **Location**: `src/jd_client/mining_upstream/upstream.rs`
- **Message**: `SetCustomMiningJob` sent to pool
- **Response**: `SetCustomMiningJobSuccess` received from pool

#### 4.2 Response Handling

```rust
// Handle successful job declaration
pub async fn handle_job_declaration_success(
    success_msg: SetCustomMiningJobSuccess,
) {
    // Update job data with pool-assigned IDs
    job_data.channel_id = Some(success_msg.channel_id);
    job_data.job_id = Some(success_msg.job_id);

    // Send response back to API endpoint
    response_sender.send(job_data);
}
```

### 5. Database Storage

#### 5.1 Database Schema

- **Table**: `job_declarations`
- **Location**: `migrations/20250706114749_create_jobs_and_txids.sql`

```sql
CREATE TABLE job_declarations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    template_id INTEGER NOT NULL,
    channel_id INTEGER NOT NULL,
    request_id INTEGER NOT NULL,
    job_id INTEGER NOT NULL,
    mining_job_token TEXT NOT NULL,
    txids_json TEXT,              -- JSON array of transaction IDs
    txid_count INTEGER DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

#### 5.2 Storage Process

- **Location**: `src/api/mempool.rs::store_jd_in_db()`
- **Trigger**: After successful job declaration response
- **Data Stored**:
  - Template ID, Channel ID, Request ID, Job ID
  - Mining job token (hex-encoded)
  - Transaction IDs as JSON array
  - Transaction count and timestamps

```rust
let job_declaration = JobDeclarationInsert {
    template_id: template_id as i64,
    channel_id: channel_id as i64,
    request_id: request_id as i64,
    job_id: job_id as i64,
    mining_job_token: mining_job_token.clone(),
    txids: txid_strings,
};

handler.insert_job_declaration(&job_declaration).await?;
```

## Event Types and Notifications

### WebSocket Event Types

1. **"NewTemplate"** - New mining template available
2. **"RequestTransactionDataSuccess"** - Transaction list received successfully
3. **"RequestTransactionDataTimeout"** - Transaction list request timed out

### Notification Flow

```
Template Provider → Notification → WebSocket Broadcast → Frontend
                                                              ↓
                                                        Transaction Selection
                                                              ↓
                                                    Job Declaration API Call
                                                              ↓
                                                    Database Storage
```

## Error Handling and Timeouts

### Transaction List Timeout

- **Timeout**: Configurable via `Configuration::custom_job_timeout()`
- **Fallback**: Use template provider's transaction list if available
- **Error State**: Update proxy state to `PoolState::Down` if no fallback

### Job Declaration Timeout

- **Timeout**: 30 seconds for job declaration response
- **Error Response**: `REQUEST_TIMEOUT` status with error message

## API Endpoints for Monitoring

### Get Job History

- **Endpoint**: `GET /api/job-history?page=1&per_page=10`
- **Purpose**: Retrieve paginated job declaration history

### Get Job Transaction IDs

- **Endpoint**: `GET /api/job-txids/{template_id}`
- **Purpose**: Get transaction IDs for specific template

### Auto-Select Transactions

- **Endpoint**: `GET /api/auto-select`
- **Purpose**: Automatically select optimal transactions from mempool

## Data Structures

### JobDeclarationData

```rust
pub struct JobDeclarationData {
    pub template_id: Option<u64>,
    pub channel_id: Option<u32>,
    pub req_id: Option<u32>,
    pub job_id: Option<u32>,
    pub mining_job_token: Option<String>, // hex encoded
}
```

### API Response Format

```json
{
  "success": true,
  "data": {
    "template_id": 12345,
    "submitted_tx_count": 2,
    "invalid_txids": [],
    "job_declaration": {
      "template_id": 12345,
      "channel_id": 1,
      "req_id": 1,
      "job_id": 100,
      "mining_job_token": "abcdef..."
    }
  }
}
```
