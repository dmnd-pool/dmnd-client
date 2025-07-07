# Job Declaration History API Examples

This document provides examples of how to use the job declaration history API endpoints.

## Prerequisites

1. The demand-cli server must be running
2. Bitcoin RPC must be configured (for job declaration storage)
3. The database must be initialized (happens automatically on startup)

## API Endpoints

### 1. Get Job History

**Endpoint:** `GET /api/job-history`

**Optional Query Parameters:**

- `page` (default: 1) - Page number
- `per_page` (default: 10, max: 100) - Number of results per page

**Examples:**

```bash
# Get first page of job history
curl "http://localhost:3001/api/job-history"

# Get second page with 25 results per page
curl "http://localhost:3001/api/job-history?page=2&per_page=25"

# Get all recent job declarations
curl "http://localhost:3001/api/job-history?per_page=100"
```

**Response:**

```json
{
  "success": true,
  "data": {
    "jobs": [
      {
        "id": 1,
        "template_id": 12345,
        "channel_id": 1,
        "request_id": 1,
        "job_id": 100,
        "mining_job_token": "abcdef123456789abcdef123456789abcdef123456789abcdef123456789abcdef",
        "created_at": "2025-01-06T16:00:00Z",
        "updated_at": "2025-01-06T16:00:00Z",
        "txid_count": 42
      }
    ],
    "total": 1,
    "page": 1,
    "per_page": 10,
    "total_pages": 1
  }
}
```

### 2. Get Job Txids

**Endpoint:** `GET /api/job-txids/{template_id}`

**Path Parameters:**

- `template_id` (required) - The template ID to get txids for

**Examples:**

```bash
# Get txids for template 12345
curl "http://localhost:3001/api/job-txids/12345"
```

**Response:**

```json
{
  "success": true,
  "data": {
    "template_id": 12345,
    "txids": [
      "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
      "fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321"
    ],
    "total": 2
  }
}
```

### 3. Submit Job Declaration (Enhanced)

**Endpoint:** `POST /api/job-declaration`

This endpoint now automatically stores job declaration data in the database when a successful response is received.

**Request Body:**

```json
{
  "template_id": 12345,
  "txids": [
    "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
    "fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321"
  ]
}
```

**Example:**

```bash
curl -X POST "http://localhost:3001/api/job-declaration" \
  -H "Content-Type: application/json" \
  -d '{
    "template_id": 12345,
    "txids": [
      "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
      "fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321"
    ]
  }'
```

**Response:**

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
      "mining_job_token": "abcdef123456789abcdef123456789abcdef123456789abcdef123456789abcdef"
    }
  }
}
```

## Error Responses

All endpoints return appropriate error responses:

**400 Bad Request:**

```json
{
  "success": false,
  "message": "Invalid parameters"
}
```

**404 Not Found:**

```json
{
  "success": false,
  "message": "Template not found"
}
```

**500 Internal Server Error:**

```json
{
  "success": false,
  "message": "Failed to get job history: database error"
}
```

**503 Service Unavailable:**

```json
{
  "success": false,
  "message": "Database not available"
}
```
