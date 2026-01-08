// Typesense Reindex Script - Balance Operations

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// Balance Model
// Data structure for balance records

type balance struct {
	BalanceID             string
	Balance               string // big.Int stored as string
	CreditBalance         string
	DebitBalance          string
	InflightBalance       string
	InflightCreditBalance string
	InflightDebitBalance  string
	Currency              string
	CurrencyMultiplier    float64
	LedgerID              string
	IdentityID            string
	Indicator             string
	Version               int64
	CreatedAt             time.Time
	MetaData              map[string]interface{}
}

// Balance Reindex
// Main reindexing logic for balances collection

func reindexBalances(ctx context.Context, db *sql.DB) bool {
	// Build count query with time range filtering
	countQuery := "SELECT COUNT(*) FROM blnk.balances"
	countQuery, countParams := buildTimeRangeClause(countQuery, 1)
	
	var totalCount int64
	err := db.QueryRowContext(ctx, countQuery, countParams...).Scan(&totalCount)
	if err != nil {
		log("ERROR", "Failed to count balances: %v", err)
		return false
	}
	log("INFO", "Total balances to reindex: %d", totalCount)

	if totalCount == 0 {
		log("INFO", "No balances to reindex")
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
		balances, err := fetchBalanceBatch(ctx, db, offset)
		if err != nil {
			log("ERROR", "Failed to fetch balance batch: %v", err)
			return false
		}

		if len(balances) == 0 {
			break
		}

		// Transform to Typesense documents
		documents := make([]map[string]interface{}, 0, len(balances))
		for _, bal := range balances {
			documents = append(documents, transformBalance(bal))
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
		succeeded, failed := processBatchesConcurrently(ctx, "balances", chunks)
		totalSucceeded += succeeded
		totalFailed += failed

		totalScanned += int64(len(balances))
		offset += int64(len(balances))

		// Progress logging
		if totalScanned%config.ProgressInterval == 0 || totalScanned == totalCount {
			elapsed := time.Since(startTime)
			rate := float64(totalScanned) / elapsed.Seconds()
			log("INFO", "Progress: %d/%d balances (%.1f%%), %.0f docs/sec, succeeded: %d, failed: %d",
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
		if len(balances) < config.BatchSize {
			break
		}
	}

	elapsed := time.Since(startTime)
	log("INFO", "Balance reindex completed in %v", elapsed)
	log("INFO", "Total scanned: %d, succeeded: %d, failed: %d", totalScanned, totalSucceeded, totalFailed)

	return totalFailed == 0 || float64(totalFailed)/float64(totalScanned)*100 <= config.MaxFailureRatePercent
}

// Database Operations
// Functions for fetching balance data from PostgreSQL

func fetchBalanceBatch(ctx context.Context, db *sql.DB, offset int64) ([]*balance, error) {
	// Note: inflight_expires_at is NOT stored in the DB - it's computed at runtime
	baseQuery := `
		SELECT 
			balance_id,
			COALESCE(balance::text, '0') as balance,
			COALESCE(credit_balance::text, '0') as credit_balance,
			COALESCE(debit_balance::text, '0') as debit_balance,
			COALESCE(inflight_balance::text, '0') as inflight_balance,
			COALESCE(inflight_credit_balance::text, '0') as inflight_credit_balance,
			COALESCE(inflight_debit_balance::text, '0') as inflight_debit_balance,
			COALESCE(currency, '') as currency,
			COALESCE(currency_multiplier, 1) as currency_multiplier,
			COALESCE(ledger_id, '') as ledger_id,
			COALESCE(identity_id, '') as identity_id,
			COALESCE(indicator, '') as indicator,
			COALESCE(version, 0) as version,
			created_at,
			COALESCE(meta_data, '{}') as meta_data
		FROM blnk.balances
	`
	
	// Build time range clause
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

	var balances []*balance
	for rows.Next() {
		bal := &balance{}
		var metaDataJSON []byte

		err := rows.Scan(
			&bal.BalanceID,
			&bal.Balance,
			&bal.CreditBalance,
			&bal.DebitBalance,
			&bal.InflightBalance,
			&bal.InflightCreditBalance,
			&bal.InflightDebitBalance,
			&bal.Currency,
			&bal.CurrencyMultiplier,
			&bal.LedgerID,
			&bal.IdentityID,
			&bal.Indicator,
			&bal.Version,
			&bal.CreatedAt,
			&metaDataJSON,
		)
		if err != nil {
			return nil, err
		}

		// inflight_expires_at is not in DB, will be set to created_at in transform

		// Parse metadata
		if len(metaDataJSON) > 0 {
			if err := json.Unmarshal(metaDataJSON, &bal.MetaData); err != nil {
				bal.MetaData = make(map[string]interface{})
			}
		} else {
			bal.MetaData = make(map[string]interface{})
		}

		balances = append(balances, bal)
	}

	return balances, rows.Err()
}

// Transformation
// Functions for converting balance records to Typesense documents

func transformBalance(bal *balance) map[string]interface{} {
	doc := map[string]interface{}{
		"id":                      bal.BalanceID, // Typesense document ID
		"balance_id":              bal.BalanceID,
		"balance":                 convertBigIntToString(bal.Balance),
		"credit_balance":          convertBigIntToString(bal.CreditBalance),
		"debit_balance":           convertBigIntToString(bal.DebitBalance),
		"inflight_balance":        convertBigIntToString(bal.InflightBalance),
		"inflight_credit_balance": convertBigIntToString(bal.InflightCreditBalance),
		"inflight_debit_balance":  convertBigIntToString(bal.InflightDebitBalance),
		"currency":                bal.Currency,
		"precision":               bal.CurrencyMultiplier,
		"currency_multiplier":     bal.CurrencyMultiplier, // Both fields exist in Typesense
		"ledger_id":               bal.LedgerID,
		"indicator":               bal.Indicator,
		"version":                 bal.Version,
		"created_at":              bal.CreatedAt.Unix(),
	}

	// Only include identity_id when not empty (it's a reference field)
	if bal.IdentityID != "" {
		doc["identity_id"] = bal.IdentityID
	}

	// inflight_expires_at is not stored in DB - use created_at as fallback
	doc["inflight_expires_at"] = bal.CreatedAt.Unix()

	// Handle meta_data
	if bal.MetaData != nil {
		doc["meta_data"] = bal.MetaData
	} else {
		doc["meta_data"] = make(map[string]interface{})
	}

	return doc
}
