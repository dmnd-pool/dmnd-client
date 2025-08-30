# Settings API Documentation

This document describes the Settings API endpoints.

## Database Schema

The settings are stored in a `settings` table with the following structure:

```sql
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
```

## API Endpoints

### 1. GET /api/settings

Retrieve settings of user. If no settings exist, default settings will be automatically created and returned.

**Request:**

```bash
curl -X GET http://localhost:3001/api/settings
```

**Response:**

```json
{
  "success": true,
  "message": null,
  "data": {
    "id": 1,
    "created_at": "2025-08-19T07:54:38+00:00",
    "updated_at": "2025-08-19T07:54:38+00:00",
    "auto_selection_enabled": false,
    "selection_strategy": "fee_rate",
    "min_fee_rate": 1.0,
    "max_size": 1000000,
    "min_base_fee": 0.0,
    "max_ancestor_count": 25,
    "max_descendant_count": 25,
    "exclude_bip125_replaceable": false,
    "exclude_unbroadcast": false,
    "max_transaction_count": 100,
    "require_template": false,
    "clear_existing_selections": true,
    "periodic_enabled": false,
    "periodic_interval": 30,
    "auto_job_declaration": false,
    "auto_scroll_to_table": true,
    "show_notifications": true,
    "pause_on_selection": false,
    "clear_selection_on_job_declaration": false,
    "preserve_existing_selections": true,
    "auto_clean_invalid_transactions": true
  }
}
```

### 2. POST /api/settings

Update settings. Only provided fields will be updated; omitted fields retain their current values. The updated settings are returned in the response.

**Request:**

```bash
curl -X POST http://localhost:3001/api/settings \
  -H "Content-Type: application/json" \
  -d '{
    "auto_selection_enabled": true,
    "selection_strategy": "maximizeFees",
    "min_fee_rate": 5.0,
    "max_size": 500000,
    "periodic_enabled": true,
    "periodic_interval": 60
  }'
```

**Response:**

```json
{
  "success": true,
  "message": null,
  "data": {
    "id": 1,
    "created_at": "2025-08-19T07:54:38+00:00",
    "updated_at": "2025-08-19T07:54:58+00:00",
    "auto_selection_enabled": true,
    "selection_strategy": "maximizeFees",
    "min_fee_rate": 5.0,
    "max_size": 500000,
    "min_base_fee": 0.0,
    "max_ancestor_count": 25,
    "max_descendant_count": 25,
    "exclude_bip125_replaceable": false,
    "exclude_unbroadcast": false,
    "max_transaction_count": 100,
    "require_template": false,
    "clear_existing_selections": true,
    "periodic_enabled": true,
    "periodic_interval": 60,
    "auto_job_declaration": false,
    "auto_scroll_to_table": true,
    "show_notifications": true,
    "pause_on_selection": false,
    "clear_selection_on_job_declaration": false,
    "preserve_existing_selections": true,
    "auto_clean_invalid_transactions": true
  }
}
```

**Note:** This endpoint supports partial updates. You can send only the fields you want to change, and other settings will remain unchanged. The response contains the complete updated settings object.

## Settings Fields Description

### Auto Selection Settings

- `auto_selection_enabled`: Enable/disable automatic transaction selection
- `selection_strategy`: Strategy for selecting transactions. Available options:
  - `"maximizeFees"`: Prioritizes transactions with highest fees
  - `"maximizeCount"`: Prioritizes including maximum number of transactions
  - `"balanced"`: Uses a balanced approach considering both fees and transaction count
  - `"fee_rate"`: Default strategy based on fee rate (fallback)
- `min_fee_rate`: Minimum fee rate for transaction selection (sat/vB)
- `max_size`: Maximum block size in bytes
- `min_base_fee`: Minimum base fee for transactions (satoshis)
- `max_ancestor_count`: Maximum number of ancestor transactions
- `max_descendant_count`: Maximum number of descendant transactions
- `exclude_bip125_replaceable`: Exclude BIP-125 replaceable transactions
- `exclude_unbroadcast`: Exclude unbroadcast transactions
- `max_transaction_count`: Maximum number of transactions to select
- `require_template`: Require template for job declaration
- `clear_existing_selections`: Clear existing selections when starting auto-selection
- `periodic_enabled`: Enable periodic auto-selection
- `periodic_interval`: Interval in seconds for periodic selection
- `auto_job_declaration`: Enable automatic job declaration after selection

### General Settings

- `auto_scroll_to_table`: Enable auto-scroll to transaction table in the UI
- `show_notifications`: Enable system notifications
- `pause_on_selection`: Pause processing during transaction selection
- `clear_selection_on_job_declaration`: Clear transaction selection after job declaration

## Error Handling

All endpoints return a consistent response format following this structure:

```json
{
  "success": boolean,
  "message": string | null,
  "data": object | null
}
```

**Response Fields:**

- `success`: Indicates if the request was successful
- `message`: Error message (only present when success is false)
- `data`: Response data (only present when success is true)

**HTTP Status Codes:**

- `200 OK`: Successful operation
- `500 INTERNAL_SERVER_ERROR`: Database or server error
- `503 SERVICE_UNAVAILABLE`: Database not available

**Example Error Response:**

```json
{
  "success": false,
  "message": "Database not available",
  "data": null
}
```

**Important Notes:**

- The system automatically creates default settings if none exist when GET is called
- All settings updates are applied atomically
- The database connection must be available for these endpoints to function
