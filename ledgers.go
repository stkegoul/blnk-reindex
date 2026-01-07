// Typesense Reindex Script - Ledger Operations

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"
)

// ============================================================================
// LEDGER MODEL
// ============================================================================

type ledger struct {
	LedgerID  string
	Name      string
	CreatedAt time.Time
	MetaData  map[string]interface{}
}

// ============================================================================
// LEDGER REINDEX
// ============================================================================

func reindexLedgers(ctx context.Context, db *sql.DB) bool {
	var totalCount int64
	err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM blnk.ledgers").Scan(&totalCount)
	if err != nil {
		log("ERROR", "Failed to count ledgers: %v", err)
		return false
	}
	log("INFO", "Total ledgers to reindex: %d", totalCount)

	if totalCount == 0 {
		log("INFO", "No ledgers to reindex")
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
		ledgers, err := fetchLedgerBatch(ctx, db, offset)
		if err != nil {
			log("ERROR", "Failed to fetch ledger batch: %v", err)
			return false
		}

		if len(ledgers) == 0 {
			break
		}

		documents := make([]map[string]interface{}, 0, len(ledgers))
		for _, l := range ledgers {
			documents = append(documents, transformLedger(l))
		}

		for i := 0; i < len(documents); i += config.BulkSize {
			end := i + config.BulkSize
			if end > len(documents) {
				end = len(documents)
			}
			chunk := documents[i:end]

			succeeded, failed := bulkUpsertWithRetry(ctx, "ledgers", chunk)
			totalSucceeded += int64(succeeded)
			totalFailed += int64(failed)
		}

		totalScanned += int64(len(ledgers))
		offset += int64(len(ledgers))

		if totalScanned%config.ProgressInterval == 0 || totalScanned == totalCount {
			elapsed := time.Since(startTime)
			rate := float64(totalScanned) / elapsed.Seconds()
			log("INFO", "Progress: %d/%d ledgers (%.1f%%), %.0f docs/sec, succeeded: %d, failed: %d",
				totalScanned, totalCount,
				float64(totalScanned)/float64(totalCount)*100,
				rate, totalSucceeded, totalFailed)
		}

		if totalScanned > 0 {
			failureRate := float64(totalFailed) / float64(totalScanned) * 100
			if failureRate > config.MaxFailureRatePercent {
				log("ERROR", "Failure rate %.2f%% exceeds threshold %.2f%%, aborting",
					failureRate, config.MaxFailureRatePercent)
				return false
			}
		}

		if len(ledgers) < config.BatchSize {
			break
		}
	}

	elapsed := time.Since(startTime)
	log("INFO", "Ledger reindex completed in %v", elapsed)
	log("INFO", "Total scanned: %d, succeeded: %d, failed: %d", totalScanned, totalSucceeded, totalFailed)

	return totalFailed == 0 || float64(totalFailed)/float64(totalScanned)*100 <= config.MaxFailureRatePercent
}

// ============================================================================
// DATABASE OPERATIONS
// ============================================================================

func fetchLedgerBatch(ctx context.Context, db *sql.DB, offset int64) ([]*ledger, error) {
	query := `
		SELECT 
			ledger_id,
			COALESCE(name, '') as name,
			created_at,
			COALESCE(meta_data, '{}') as meta_data
		FROM blnk.ledgers
		ORDER BY created_at ASC
		LIMIT $1 OFFSET $2
	`

	rows, err := db.QueryContext(ctx, query, config.BatchSize, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ledgers []*ledger
	for rows.Next() {
		l := &ledger{}
		var metaDataJSON []byte

		err := rows.Scan(
			&l.LedgerID,
			&l.Name,
			&l.CreatedAt,
			&metaDataJSON,
		)
		if err != nil {
			return nil, err
		}

		if len(metaDataJSON) > 0 {
			if err := json.Unmarshal(metaDataJSON, &l.MetaData); err != nil {
				l.MetaData = make(map[string]interface{})
			}
		} else {
			l.MetaData = make(map[string]interface{})
		}

		ledgers = append(ledgers, l)
	}

	return ledgers, rows.Err()
}

// ============================================================================
// TRANSFORMATION
// ============================================================================

func transformLedger(l *ledger) map[string]interface{} {
	doc := map[string]interface{}{
		"id":         l.LedgerID,
		"ledger_id":  l.LedgerID,
		"name":       l.Name,
		"created_at": l.CreatedAt.Unix(),
	}

	// Only include meta_data if it has content (field is optional in schema)
	if l.MetaData != nil && len(l.MetaData) > 0 {
		doc["meta_data"] = l.MetaData
	}

	return doc
}

