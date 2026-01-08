// Typesense Reindex Script - Transaction Operations

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Transaction Model
// Data structure for transaction records

type transaction struct {
	TransactionID     string
	ParentTransaction string
	Source            string
	Destination       string
	Reference         string
	Currency          string
	Description       string
	Status            string
	Hash              string
	Amount            float64
	PreciseAmount     string
	Precision         int64
	Rate              float64
	CreatedAt         time.Time
	EffectiveDate     *time.Time
	ScheduledFor      *time.Time
	MetaData          map[string]interface{}
	Sources           []string
	Destinations      []string
}

// Transaction Reindex
// Main reindexing logic for transactions collection

func reindexTransactions(ctx context.Context, db *sql.DB) bool {
	// Build count query - only index APPLIED, INFLIGHT, REJECTED, SCHEDULED, and VOID transactions
	countQuery := "SELECT COUNT(*) FROM blnk.transactions WHERE status IN ('APPLIED', 'INFLIGHT', 'REJECTED', 'SCHEDULED', 'VOID')"
	countQuery, countParams := buildTimeRangeClause(countQuery, 1)
	
	var totalCount int64
	err := db.QueryRowContext(ctx, countQuery, countParams...).Scan(&totalCount)
	if err != nil {
		log("ERROR", "Failed to count transactions: %v", err)
		return false
	}
	log("INFO", "Total transactions to reindex: %d (APPLIED, INFLIGHT, REJECTED, SCHEDULED, VOID only)", totalCount)

	if totalCount == 0 {
		log("INFO", "No transactions to reindex")
		return true
	}

	var (
		totalScanned   int64
		totalSucceeded int64
		totalFailed    int64
		offset         int64
	)

	startTime := time.Now()

	for {
		// Fetch batch from DB
		transactions, err := fetchTransactionBatch(ctx, db, offset)
		if err != nil {
			log("ERROR", "Failed to fetch transaction batch: %v", err)
			return false
		}

		if len(transactions) == 0 {
			break
		}

		// Transform to Typesense documents
		documents := make([]map[string]interface{}, 0, len(transactions))
		for _, txn := range transactions {
			doc := transformTransaction(txn)
			documents = append(documents, doc)
		}

		// Split into chunks for concurrent processing
		chunks := make([][]map[string]interface{}, 0)
		for i := 0; i < len(documents); i += config.BulkSize {
			end := i + config.BulkSize
			if end > len(documents) {
				end = len(documents)
			}
			chunks = append(chunks, documents[i:end])
		}

		// Process chunks concurrently
		succeeded, failed := processBatchesConcurrently(ctx, "transactions", chunks)
		totalSucceeded += succeeded
		totalFailed += failed

		totalScanned += int64(len(transactions))
		offset += int64(len(transactions))

		// Progress logging
		if totalScanned%config.ProgressInterval == 0 || totalScanned == totalCount {
			elapsed := time.Since(startTime)
			rate := float64(totalScanned) / elapsed.Seconds()
			log("INFO", "Progress: %d/%d transactions (%.1f%%), %.0f docs/sec, succeeded: %d, failed: %d",
				totalScanned, totalCount,
				float64(totalScanned)/float64(totalCount)*100,
				rate, totalSucceeded, totalFailed)
		}

		// Check failure rate
		if totalScanned > 0 {
			failureRate := float64(totalFailed) / float64(totalScanned) * 100
			if failureRate > config.MaxFailureRatePercent {
				log("ERROR", "Failure rate %.2f%% exceeds threshold %.2f%%, aborting",
					failureRate, config.MaxFailureRatePercent)
				return false
			}
		}

		// Short batch means we're done
		if len(transactions) < config.BatchSize {
			break
		}
	}

	elapsed := time.Since(startTime)
	log("INFO", "Transaction reindex completed in %v", elapsed)
	log("INFO", "Total scanned: %d, succeeded: %d, failed: %d", totalScanned, totalSucceeded, totalFailed)

	return totalFailed == 0 || float64(totalFailed)/float64(totalScanned)*100 <= config.MaxFailureRatePercent
}

// Database Operations
// Functions for fetching transaction data from PostgreSQL

func fetchTransactionBatch(ctx context.Context, db *sql.DB, offset int64) ([]*transaction, error) {
	baseQuery := `
		SELECT 
			transaction_id,
			COALESCE(parent_transaction, '') as parent_transaction,
			COALESCE(source, '') as source,
			COALESCE(destination, '') as destination,
			COALESCE(reference, '') as reference,
			COALESCE(currency, '') as currency,
			COALESCE(description, '') as description,
			COALESCE(status, '') as status,
			COALESCE(hash, '') as hash,
			COALESCE(amount, 0) as amount,
			COALESCE(precise_amount::text, '0') as precise_amount,
			COALESCE(precision, 1) as precision,
			COALESCE(rate, 1) as rate,
			created_at,
			effective_date,
			scheduled_for,
			COALESCE(meta_data, '{}') as meta_data
		FROM blnk.transactions
		WHERE status IN ('APPLIED', 'INFLIGHT', 'REJECTED', 'SCHEDULED', 'VOID')
	`
	
	// Build time range clause (appends to existing WHERE clause)
	query, timeParams := buildTimeRangeClause(baseQuery, 1)
	query += " ORDER BY created_at ASC"
	
	// Combine parameters: time range params first, then batch size and offset
	params := append(timeParams, config.BatchSize, offset)
	// Adjust LIMIT and OFFSET parameter numbers
	paramCount := len(timeParams)
	query += fmt.Sprintf(" LIMIT $%d OFFSET $%d", paramCount+1, paramCount+2)

	rows, err := db.QueryContext(ctx, query, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var transactions []*transaction
	for rows.Next() {
		txn := &transaction{}
		var metaDataJSON []byte
		var effectiveDate sql.NullTime
		var scheduledFor sql.NullTime

		err := rows.Scan(
			&txn.TransactionID,
			&txn.ParentTransaction,
			&txn.Source,
			&txn.Destination,
			&txn.Reference,
			&txn.Currency,
			&txn.Description,
			&txn.Status,
			&txn.Hash,
			&txn.Amount,
			&txn.PreciseAmount,
			&txn.Precision,
			&txn.Rate,
			&txn.CreatedAt,
			&effectiveDate,
			&scheduledFor,
			&metaDataJSON,
		)
		if err != nil {
			return nil, err
		}

		// Handle nullable dates
		if effectiveDate.Valid {
			txn.EffectiveDate = &effectiveDate.Time
		}
		if scheduledFor.Valid {
			txn.ScheduledFor = &scheduledFor.Time
		}

		// Parse metadata
		if len(metaDataJSON) > 0 {
			if err := json.Unmarshal(metaDataJSON, &txn.MetaData); err != nil {
				txn.MetaData = make(map[string]interface{})
			}
		} else {
			txn.MetaData = make(map[string]interface{})
		}

		// Initialize arrays
		txn.Sources = []string{}
		txn.Destinations = []string{}

		transactions = append(transactions, txn)
	}

	return transactions, rows.Err()
}

// Transformation
// Functions for converting transaction records to Typesense documents

func transformTransaction(txn *transaction) map[string]interface{} {
	doc := map[string]interface{}{
		"id":                 txn.TransactionID, // Typesense document ID
		"transaction_id":     txn.TransactionID,
		"parent_transaction": txn.ParentTransaction,
		"source":             txn.Source,
		"destination":        txn.Destination,
		"reference":          txn.Reference,
		"currency":           txn.Currency,
		"description":        txn.Description,
		"status":             txn.Status,
		"hash":               txn.Hash,
		"amount":             txn.Amount,
		"precision":          txn.Precision,
		"rate":               txn.Rate,
	}

	// amount_string - format amount as string
	doc["amount_string"] = formatAmountString(txn.Amount)

	// Derive inflight from status or metadata
	doc["inflight"] = deriveInflightFlag(txn)

	// These fields are not stored in DB - default to false/0
	doc["allow_overdraft"] = false
	doc["atomic"] = false
	doc["skip_queue"] = false
	doc["overdraft_limit"] = float64(0)

	// Convert precise_amount - BigInt fields stored as strings
	doc["precise_amount"] = convertBigIntToString(txn.PreciseAmount)

	// Time fields - convert to Unix epoch seconds (int64)
	doc["created_at"] = txn.CreatedAt.Unix()

	// effective_date: use if set, otherwise fall back to created_at
	doc["effective_date"] = computeEffectiveDate(txn)

	// scheduled_for: use if set, otherwise fall back to created_at
	doc["scheduled_for"] = computeScheduledFor(txn)

	// inflight_expiry_date: not stored in DB, fall back to created_at
	doc["inflight_expiry_date"] = txn.CreatedAt.Unix()

	// Handle meta_data
	if txn.MetaData != nil {
		doc["meta_data"] = txn.MetaData
	} else {
		doc["meta_data"] = make(map[string]interface{})
	}

	// sources and destinations are required fields but not in database
	// Set to empty arrays - Typesense requires these fields even if empty
	doc["sources"] = []string{}
	doc["destinations"] = []string{}

	return doc
}

// deriveInflightFlag determines if a transaction is inflight based on status and metadata
func deriveInflightFlag(txn *transaction) bool {
	if txn.Status == "INFLIGHT" {
		return true
	}

	if txn.Status == "QUEUED" && txn.MetaData != nil {
		if inflightVal, ok := txn.MetaData["inflight"]; ok {
			if inflight, ok := inflightVal.(bool); ok && inflight {
				return true
			}
		}
	}

	return false
}

// computeEffectiveDate derives the effective_date field value
func computeEffectiveDate(txn *transaction) int64 {
	if txn.EffectiveDate != nil && !txn.EffectiveDate.IsZero() {
		return txn.EffectiveDate.Unix()
	}
	return txn.CreatedAt.Unix()
}

// computeScheduledFor derives the scheduled_for field value
func computeScheduledFor(txn *transaction) int64 {
	if txn.ScheduledFor != nil && !txn.ScheduledFor.IsZero() {
		return txn.ScheduledFor.Unix()
	}
	return txn.CreatedAt.Unix()
}

// formatAmountString converts float amount to string representation
func formatAmountString(amount float64) string {
	str := fmt.Sprintf("%.10f", amount)
	if strings.Contains(str, ".") {
		str = strings.TrimRight(str, "0")
		str = strings.TrimRight(str, ".")
	}
	return str
}
