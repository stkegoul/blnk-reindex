// Typesense Reindex Script - Reconciliation Operations

package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// ============================================================================
// RECONCILIATION MODEL
// ============================================================================

type reconciliation struct {
	ReconciliationID      string
	UploadID              string
	Status                string
	MatchedTransactions   int32
	UnmatchedTransactions int32
	StartedAt             time.Time
	CompletedAt           *time.Time
}

// ============================================================================
// RECONCILIATION REINDEX
// ============================================================================

func reindexReconciliations(ctx context.Context, db *sql.DB) bool {
	// Build count query with time range filtering (using started_at for reconciliations)
	countQuery := "SELECT COUNT(*) FROM blnk.reconciliations"
	var totalCount int64
	if config.TimeRangeEnabled {
		var conditions []string
		var params []interface{}
		if config.TimeRangeStart != nil {
			conditions = append(conditions, "started_at >= $1")
			params = append(params, *config.TimeRangeStart)
		}
		if config.TimeRangeEnd != nil {
			paramIdx := len(params) + 1
			conditions = append(conditions, fmt.Sprintf("started_at <= $%d", paramIdx))
			params = append(params, *config.TimeRangeEnd)
		}
		if len(conditions) > 0 {
			countQuery += " WHERE " + strings.Join(conditions, " AND ")
		}
		err := db.QueryRowContext(ctx, countQuery, params...).Scan(&totalCount)
		if err != nil {
			log("ERROR", "Failed to count reconciliations: %v", err)
			return false
		}
	} else {
		err := db.QueryRowContext(ctx, countQuery).Scan(&totalCount)
		if err != nil {
			log("ERROR", "Failed to count reconciliations: %v", err)
			return false
		}
	}
	log("INFO", "Total reconciliations to reindex: %d", totalCount)

	if totalCount == 0 {
		log("INFO", "No reconciliations to reindex")
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
		reconciliations, err := fetchReconciliationBatch(ctx, db, offset)
		if err != nil {
			log("ERROR", "Failed to fetch reconciliation batch: %v", err)
			return false
		}

		if len(reconciliations) == 0 {
			break
		}

		documents := make([]map[string]interface{}, 0, len(reconciliations))
		for _, r := range reconciliations {
			documents = append(documents, transformReconciliation(r))
		}

		for i := 0; i < len(documents); i += config.BulkSize {
			end := i + config.BulkSize
			if end > len(documents) {
				end = len(documents)
			}
			chunk := documents[i:end]

			succeeded, failed := bulkUpsertWithRetry(ctx, "reconciliations", chunk)
			totalSucceeded += int64(succeeded)
			totalFailed += int64(failed)
		}

		totalScanned += int64(len(reconciliations))
		offset += int64(len(reconciliations))

		if totalScanned%config.ProgressInterval == 0 || totalScanned == totalCount {
			elapsed := time.Since(startTime)
			rate := float64(totalScanned) / elapsed.Seconds()
			log("INFO", "Progress: %d/%d reconciliations (%.1f%%), %.0f docs/sec, succeeded: %d, failed: %d",
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

		if len(reconciliations) < config.BatchSize {
			break
		}
	}

	elapsed := time.Since(startTime)
	log("INFO", "Reconciliation reindex completed in %v", elapsed)
	log("INFO", "Total scanned: %d, succeeded: %d, failed: %d", totalScanned, totalSucceeded, totalFailed)

	return totalFailed == 0 || float64(totalFailed)/float64(totalScanned)*100 <= config.MaxFailureRatePercent
}

// ============================================================================
// DATABASE OPERATIONS
// ============================================================================

func fetchReconciliationBatch(ctx context.Context, db *sql.DB, offset int64) ([]*reconciliation, error) {
	baseQuery := `
		SELECT 
			reconciliation_id,
			COALESCE(upload_id, '') as upload_id,
			COALESCE(status, '') as status,
			COALESCE(matched_transactions, 0) as matched_transactions,
			COALESCE(unmatched_transactions, 0) as unmatched_transactions,
			started_at,
			completed_at
		FROM blnk.reconciliations
	`
	
	// Build time range clause (using started_at for reconciliations)
	var query string
	var params []interface{}
	if config.TimeRangeEnabled {
		var conditions []string
		if config.TimeRangeStart != nil {
			conditions = append(conditions, "started_at >= $1")
			params = append(params, *config.TimeRangeStart)
		}
		if config.TimeRangeEnd != nil {
			paramIdx := len(params) + 1
			conditions = append(conditions, fmt.Sprintf("started_at <= $%d", paramIdx))
			params = append(params, *config.TimeRangeEnd)
		}
		if len(conditions) > 0 {
			query = baseQuery + " WHERE " + strings.Join(conditions, " AND ")
		} else {
			query = baseQuery
		}
	} else {
		query = baseQuery
	}
	
	query += " ORDER BY started_at ASC"
	
	// Combine parameters: time range params first, then batch size and offset
	paramCount := len(params)
	params = append(params, config.BatchSize, offset)
	query += fmt.Sprintf(" LIMIT $%d OFFSET $%d", paramCount+1, paramCount+2)

	rows, err := db.QueryContext(ctx, query, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var reconciliations []*reconciliation
	for rows.Next() {
		r := &reconciliation{}
		var completedAt sql.NullTime

		err := rows.Scan(
			&r.ReconciliationID,
			&r.UploadID,
			&r.Status,
			&r.MatchedTransactions,
			&r.UnmatchedTransactions,
			&r.StartedAt,
			&completedAt,
		)
		if err != nil {
			return nil, err
		}

		if completedAt.Valid {
			r.CompletedAt = &completedAt.Time
		}

		reconciliations = append(reconciliations, r)
	}

	return reconciliations, rows.Err()
}

// ============================================================================
// TRANSFORMATION
// ============================================================================

func transformReconciliation(r *reconciliation) map[string]interface{} {
	doc := map[string]interface{}{
		"id":                     r.ReconciliationID,
		"reconciliation_id":      r.ReconciliationID,
		"upload_id":              r.UploadID,
		"status":                 r.Status,
		"matched_transactions":   r.MatchedTransactions,
		"unmatched_transactions": r.UnmatchedTransactions,
		"started_at":             r.StartedAt.Unix(),
	}

	// Handle completed_at - convert to Unix timestamp
	if r.CompletedAt != nil && !r.CompletedAt.IsZero() {
		doc["completed_at"] = r.CompletedAt.Unix()
	} else {
		doc["completed_at"] = r.StartedAt.Unix() // Fall back to started_at
	}

	return doc
}

