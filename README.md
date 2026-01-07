# Typesense Full Reindex Script

A one-time script to republish all documents from the PostgreSQL database to Typesense for all collections, ensuring schema consistency and new fields are populated.

## Background

This script was created to address scenarios where:
- New fields were added to Typesense collection schemas
- Existing documents do not have these fields
- The normal indexer is change-driven and will not republish unchanged records
- Typesense has no built-in "reindex" feature

The solution is to upsert all documents again with new fields included.

## Features

- **Full Collection Reindex**: Reindexes all 5 collections in dependency order
- **Reference Integrity**: Processes collections in order to satisfy reference constraints
- **Paginated Database Reads**: Handles large datasets without memory issues
- **Exact Mapping Logic**: Uses the same field mappings as the existing indexer
- **Bulk Upserts**: Uses Typesense's bulk import endpoint with `action=upsert`
- **Reference Handling**: Uses `dirty_values=coerce_or_drop` for graceful reference handling
- **Retry Logic**: Automatic retry with exponential backoff for transient failures
- **Progress Tracking**: Logs progress every N records with running totals
- **Safety Threshold**: Exits non-zero if failure rate exceeds threshold
- **Idempotent**: Safe to re-run; upserts overwrite existing documents

## Reindex Order

Collections are reindexed in dependency order to satisfy reference constraints:

1. **Ledgers** - No references
2. **Identities** - No references
3. **Balances** - References ledgers and identities
4. **Reconciliations** - No references
5. **Transactions** - References balances

## File Structure

```
typesense-reindex/
├── main.go             # Entry point, config, shared utilities
├── ledgers.go          # Ledger fetching and transformation
├── identities.go       # Identity fetching and transformation
├── balances.go         # Balance fetching and transformation
├── reconciliations.go  # Reconciliation fetching and transformation
├── transactions.go     # Transaction fetching and transformation
├── go.mod
└── README.md
```

## Configuration

Edit the `config` struct at the top of `main.go`:

```go
var config = struct {
    // Database connection string (PostgreSQL)
    DatabaseDNS string

    // Typesense configuration
    TypesenseHost     string
    TypesensePort     string
    TypesenseProtocol string // "http" or "https"
    TypesenseAPIKey   string

    // Batch settings
    BatchSize int   // Number of records per DB query page
    BulkSize  int   // Number of documents per Typesense bulk import

    // Progress and safety
    ProgressInterval      int64   // Log progress every N records
    MaxFailureRatePercent float64 // Exit if failure rate exceeds this (0-100)

    // Retry settings
    MaxRetries    int           // Max retries for transient failures
    RetryBaseWait time.Duration // Base wait time between retries
}{
    DatabaseDNS: "postgres://postgres:password@localhost:5432/blnk?sslmode=disable",

    TypesenseHost:     "localhost",
    TypesensePort:     "8108",
    TypesenseProtocol: "http",
    TypesenseAPIKey:   "blnk-api-key",

    BatchSize: 5000,
    BulkSize:  1000,

    ProgressInterval:      50000,
    MaxFailureRatePercent: 5.0,

    MaxRetries:    3,
    RetryBaseWait: 2 * time.Second,
}
```

## Usage

### Prerequisites

1. Go 1.21 or later
2. PostgreSQL driver: `go get github.com/lib/pq`

### Running the Script

```bash
cd typesense-reindex

# Install dependencies (first time only)
go mod tidy

# Run the script (compiles all .go files)
go run .
```

**Note**: Use `go run .` (not `go run main.go`) to compile all Go files together.

### Expected Runtime Behavior

```
[2026-01-07 14:30:00] INFO: === Typesense Full Reindex Script ===
[2026-01-07 14:30:00] INFO: Database: ***@localhost:5432/blnk?sslmode=disable
[2026-01-07 14:30:00] INFO: Typesense: http://localhost:8108
[2026-01-07 14:30:00] INFO: Batch Size: 5000, Bulk Size: 1000
[2026-01-07 14:30:00] INFO: Database connection established
[2026-01-07 14:30:00] INFO: Verifying Typesense collections...
[2026-01-07 14:30:00] INFO: All Typesense collections verified
[2026-01-07 14:30:00] INFO: 
[2026-01-07 14:30:00] INFO: ========== STEP 1: REINDEX LEDGERS ==========
[2026-01-07 14:30:00] INFO: Total ledgers to reindex: 5
[2026-01-07 14:30:00] INFO: Ledger reindex completed in 100ms
[2026-01-07 14:30:00] INFO: 
[2026-01-07 14:30:00] INFO: ========== STEP 2: REINDEX IDENTITIES ==========
[2026-01-07 14:30:00] INFO: Total identities to reindex: 100
[2026-01-07 14:30:01] INFO: Identity reindex completed in 500ms
[2026-01-07 14:30:01] INFO: 
[2026-01-07 14:30:01] INFO: ========== STEP 3: REINDEX BALANCES ==========
[2026-01-07 14:30:01] INFO: Total balances to reindex: 4208
[2026-01-07 14:30:03] INFO: Balance reindex completed in 2s
[2026-01-07 14:30:03] INFO: 
[2026-01-07 14:30:03] INFO: ========== STEP 4: REINDEX RECONCILIATIONS ==========
[2026-01-07 14:30:03] INFO: Total reconciliations to reindex: 0
[2026-01-07 14:30:03] INFO: No reconciliations to reindex
[2026-01-07 14:30:03] INFO: 
[2026-01-07 14:30:03] INFO: ========== STEP 5: REINDEX TRANSACTIONS ==========
[2026-01-07 14:30:03] INFO: Total transactions to reindex: 340000
[2026-01-07 14:30:10] INFO: Progress: 50000/340000 transactions (14.7%), 16667 docs/sec
...
[2026-01-07 14:30:30] INFO: Transaction reindex completed in 28s
[2026-01-07 14:30:30] INFO: 
[2026-01-07 14:30:30] INFO: === Full reindex completed successfully ===
```

## Field Mapping Reference

### Ledgers

| DB Column | Typesense Field | Type | Notes |
|-----------|-----------------|------|-------|
| ledger_id | id, ledger_id | string | Document ID |
| name | name | string | |
| created_at | created_at | int64 | Unix epoch seconds |
| meta_data | meta_data | object | JSON object |

### Identities

| DB Column | Typesense Field | Type | Notes |
|-----------|-----------------|------|-------|
| identity_id | id, identity_id | string | Document ID |
| identity_type | identity_type | string | |
| organization_name | organization_name | string | |
| category | category | string | |
| first_name | first_name | string | |
| last_name | last_name | string | |
| other_names | other_names | string | |
| gender | gender | string | |
| email_address | email_address | string | |
| phone_number | phone_number | string | |
| nationality | nationality | string | |
| street | street | string | |
| country | country | string | |
| state | state | string | |
| post_code | post_code | string | |
| city | city | string | |
| dob | dob | int64 | Unix epoch seconds, falls back to created_at |
| created_at | created_at | int64 | Unix epoch seconds |
| meta_data | meta_data | object | JSON object |

### Balances

| DB Column | Typesense Field | Type | Notes |
|-----------|-----------------|------|-------|
| balance_id | id, balance_id | string | Document ID |
| balance | balance | string | BigInt as string |
| credit_balance | credit_balance | string | BigInt as string |
| debit_balance | debit_balance | string | BigInt as string |
| inflight_balance | inflight_balance | string | BigInt as string |
| inflight_credit_balance | inflight_credit_balance | string | BigInt as string |
| inflight_debit_balance | inflight_debit_balance | string | BigInt as string |
| currency | currency | string | |
| currency_multiplier | precision, currency_multiplier | float | Both fields populated |
| ledger_id | ledger_id | string | Reference to ledgers |
| identity_id | identity_id | string | Reference to identities (only if not empty) |
| indicator | indicator | string | |
| version | version | int64 | |
| created_at | created_at | int64 | Unix epoch seconds |
| - | inflight_expires_at | int64 | Falls back to created_at (not in DB) |
| meta_data | meta_data | object | JSON object |

### Reconciliations

| DB Column | Typesense Field | Type | Notes |
|-----------|-----------------|------|-------|
| reconciliation_id | id, reconciliation_id | string | Document ID |
| upload_id | upload_id | string | |
| status | status | string | |
| matched_transactions | matched_transactions | int32 | |
| unmatched_transactions | unmatched_transactions | int32 | |
| started_at | started_at | int64 | Unix epoch seconds |
| completed_at | completed_at | int64 | Falls back to started_at |

### Transactions

| DB Column | Typesense Field | Type | Notes |
|-----------|-----------------|------|-------|
| transaction_id | id, transaction_id | string | Document ID |
| parent_transaction | parent_transaction | string | |
| source | source | string | Reference to balance |
| destination | destination | string | Reference to balance |
| reference | reference | string | |
| currency | currency | string | |
| description | description | string | |
| status | status | string | |
| hash | hash | string | |
| amount | amount | float | |
| amount | amount_string | string | Formatted from amount |
| precise_amount | precise_amount | string | BigInt as string |
| precision | precision | int64 | |
| rate | rate | float | |
| created_at | created_at | int64 | Unix epoch seconds |
| effective_date | effective_date | int64 | Falls back to created_at |
| scheduled_for | scheduled_for | int64 | Falls back to created_at |
| meta_data | meta_data | object | JSON object |
| - | sources | string[] | Empty array default |
| - | destinations | string[] | Empty array default |

### Derived Transaction Fields (not stored in DB)

| Typesense Field | Type | Derivation |
|-----------------|------|------------|
| allow_overdraft | bool | Defaults to `false` |
| inflight | bool | `status == 'INFLIGHT'` OR `meta_data.inflight == true` |
| inflight_expiry_date | int64 | Falls back to `created_at` |
| atomic | bool | Defaults to `false` |
| skip_queue | bool | Defaults to `false` |
| overdraft_limit | float | Defaults to `0` |

## Non-Goals

This script does NOT:
- Drop or recreate collections
- Export from Typesense
- Modify database data
- Rely on Typesense doing any automatic migration
- Modify the main Blnk codebase

## License

Same as Blnk Finance - Apache 2.0
