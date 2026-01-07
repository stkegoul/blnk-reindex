// Typesense Reindex Script - Main Entry Point
// Reindexes all collections in dependency order:
// 1. Ledgers (no references)
// 2. Identities (no references)
// 3. Balances (references ledgers, identities)
// 4. Reconciliations (no references)
// 5. Transactions (references balances)

package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

// ============================================================================
// CONFIGURATION
// ============================================================================

var config = struct {
	// Database
	DatabaseDNS string

	// Typesense
	TypesenseHost     string
	TypesensePort     string
	TypesenseProtocol string
	TypesenseAPIKey   string

	// Processing
	BatchSize        int   // Records to fetch from DB per query
	BulkSize         int   // Documents to send per Typesense import request
	ProgressInterval int64 // Log progress every N records

	// Safety
	MaxFailureRatePercent float64

	// Retry
	MaxRetries    int
	RetryBaseWait time.Duration

	// Collections to skip (set to skip already-completed collections)
	CollectionsToSkip []string
}{
	// Database connection - use localhost when running from host machine
	DatabaseDNS: "postgres://postgres:password@localhost:5432/blnk?sslmode=disable",

	// Typesense connection
	TypesenseHost:     "localhost",
	TypesensePort:     "8108",
	TypesenseProtocol: "http",
	TypesenseAPIKey:   "blnk-api-key",

	// Processing - tuned for very large datasets (1M+ records)
	BatchSize:        10000,
	BulkSize:         2000,
	ProgressInterval: 100000,

	// Safety - abort if more than 5% of documents fail
	MaxFailureRatePercent: 5.0,

	// Retry config for transient failures
	MaxRetries:    3,
	RetryBaseWait: 2 * time.Second,

	// Skip collections that were already successfully reindexed
	// Example: []string{"ledgers", "identities", "balances", "reconciliations"}
	CollectionsToSkip: []string{"ledgers", "identities", "balances", "reconciliations"},
}

// ============================================================================
// LOGGING
// ============================================================================

func log(level, format string, args ...interface{}) {
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	msg := fmt.Sprintf(format, args...)
	fmt.Printf("[%s] %s: %s\n", timestamp, level, msg)
}

// ============================================================================
// HTTP CLIENT
// ============================================================================

var httpClient = &http.Client{Timeout: 60 * time.Second}

// ============================================================================
// TYPESENSE OPERATIONS
// ============================================================================

func verifyTypesenseCollection(collectionName string) error {
	url := fmt.Sprintf("%s://%s:%s/collections/%s",
		config.TypesenseProtocol, config.TypesenseHost, config.TypesensePort, collectionName)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("X-TYPESENSE-API-KEY", config.TypesenseAPIKey)

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return fmt.Errorf("collection '%s' does not exist", collectionName)
	}
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// removeReferencesFromTransactionsSchema removes references from sources and destinations fields
// by dropping and recreating the collection with modified schema
func removeReferencesFromTransactionsSchema(ctx context.Context) error {
	log("INFO", "Removing references from sources and destinations fields in transactions collection...")

	// Get current schema
	url := fmt.Sprintf("%s://%s:%s/collections/transactions",
		config.TypesenseProtocol, config.TypesenseHost, config.TypesensePort)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("X-TYPESENSE-API-KEY", config.TypesenseAPIKey)

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to get schema: HTTP %d: %s", resp.StatusCode, string(body))
	}

	var currentSchema map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&currentSchema); err != nil {
		return fmt.Errorf("failed to decode schema: %w", err)
	}

	// Modify fields to remove references from sources and destinations
	fields, ok := currentSchema["fields"].([]interface{})
	if !ok {
		return fmt.Errorf("invalid schema format: fields not found")
	}

	modifiedFields := make([]map[string]interface{}, 0, len(fields))
	for _, field := range fields {
		fieldMap, ok := field.(map[string]interface{})
		if !ok {
			continue
		}

		fieldName, _ := fieldMap["name"].(string)
		if fieldName == "sources" || fieldName == "destinations" {
			// Remove reference from this field
			delete(fieldMap, "reference")
			log("INFO", "Removed reference from field: %s", fieldName)
		}

		modifiedFields = append(modifiedFields, fieldMap)
	}

	// Create new schema with modified fields
	newSchema := map[string]interface{}{
		"name":                  currentSchema["name"],
		"fields":                modifiedFields,
		"default_sorting_field": currentSchema["default_sorting_field"],
		"enable_nested_fields":  currentSchema["enable_nested_fields"],
	}

	// Delete existing collection
	deleteURL := fmt.Sprintf("%s://%s:%s/collections/transactions",
		config.TypesenseProtocol, config.TypesenseHost, config.TypesensePort)

	req, err = http.NewRequestWithContext(ctx, "DELETE", deleteURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create delete request: %w", err)
	}
	req.Header.Set("X-TYPESENSE-API-KEY", config.TypesenseAPIKey)

	resp, err = httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("delete request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 && resp.StatusCode != 404 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to delete collection: HTTP %d: %s", resp.StatusCode, string(body))
	}

	log("INFO", "Deleted transactions collection")

	// Wait a moment for deletion to complete
	time.Sleep(1 * time.Second)

	// Create collection with modified schema
	schemaJSON, err := json.Marshal(newSchema)
	if err != nil {
		return fmt.Errorf("failed to marshal schema: %w", err)
	}

	createURL := fmt.Sprintf("%s://%s:%s/collections",
		config.TypesenseProtocol, config.TypesenseHost, config.TypesensePort)

	req, err = http.NewRequestWithContext(ctx, "POST", createURL, bytes.NewBuffer(schemaJSON))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-TYPESENSE-API-KEY", config.TypesenseAPIKey)

	resp, err = httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("create request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 201 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to create collection: HTTP %d: %s", resp.StatusCode, string(body))
	}

	log("INFO", "Recreated transactions collection without references on sources/destinations")
	return nil
}

func bulkUpsertWithRetry(ctx context.Context, collectionName string, documents []map[string]interface{}) (succeeded, failed int) {
	var lastErr error

	for attempt := 0; attempt <= config.MaxRetries; attempt++ {
		if attempt > 0 {
			waitTime := config.RetryBaseWait * time.Duration(1<<(attempt-1))
			log("WARN", "Retry attempt %d after %v", attempt, waitTime)
			time.Sleep(waitTime)
		}

		succeeded, failed, err := bulkUpsert(ctx, collectionName, documents)
		if err == nil {
			return succeeded, failed
		}

		lastErr = err

		if !isRetryableError(err) {
			log("ERROR", "Non-retryable error: %v", err)
			return 0, len(documents)
		}

		log("WARN", "Retryable error: %v", err)
	}

	log("ERROR", "All retries exhausted: %v", lastErr)
	return 0, len(documents)
}

func bulkUpsert(ctx context.Context, collectionName string, documents []map[string]interface{}) (succeeded, failed int, err error) {
	if len(documents) == 0 {
		return 0, 0, nil
	}

	// Use bulk import for all collections (including transactions)
	// Build JSONL payload
	var buffer bytes.Buffer
	for _, doc := range documents {
		jsonBytes, err := json.Marshal(doc)
		if err != nil {
			log("ERROR", "Failed to marshal document: %v", err)
			failed++
			continue
		}
		buffer.Write(jsonBytes)
		buffer.WriteByte('\n')
	}

	// Build request URL with action=upsert
	// dirty_values=coerce_or_drop handles reference validation failures
	url := fmt.Sprintf("%s://%s:%s/collections/%s/documents/import?action=upsert&dirty_values=coerce_or_drop",
		config.TypesenseProtocol, config.TypesenseHost, config.TypesensePort, collectionName)

	req, err := http.NewRequestWithContext(ctx, "POST", url, &buffer)
	if err != nil {
		return 0, len(documents), fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/x-ndjson")
	req.Header.Set("X-TYPESENSE-API-KEY", config.TypesenseAPIKey)

	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, len(documents), fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	// Check for HTTP-level errors (retryable)
	if resp.StatusCode == 429 || resp.StatusCode >= 500 {
		body, _ := io.ReadAll(resp.Body)
		return 0, len(documents), fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		// #region agent log
		logFile, _ := os.OpenFile("/Users/antgspakr/Documents/code/projects/.cursor/debug.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		logEntry := map[string]interface{}{
			"sessionId":    "debug-session",
			"runId":        "run1",
			"location":     "main.go:193",
			"message":      "HTTP error response from Typesense",
			"data":         map[string]interface{}{"statusCode": resp.StatusCode, "body": string(body), "collection": collectionName},
			"timestamp":    time.Now().UnixMilli(),
			"hypothesisId": "F",
		}
		json.NewEncoder(logFile).Encode(logEntry)
		logFile.Close()
		// #endregion
		return 0, len(documents), fmt.Errorf("HTTP %d (non-retryable): %s", resp.StatusCode, string(body))
	}

	// Parse JSONL response - each line is the result for one document
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, len(documents), fmt.Errorf("failed to read response: %w", err)
	}

	// #region agent log
	logFile, _ := os.OpenFile("/Users/antgspakr/Documents/code/projects/.cursor/debug.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	defer logFile.Close()
	logEntry := map[string]interface{}{
		"sessionId":    "debug-session",
		"runId":        "run1",
		"location":     "main.go:204",
		"message":      "Parsing Typesense bulk response",
		"data":         map[string]interface{}{"collection": collectionName, "responseLength": len(body), "responsePreview": string(body[:min(500, len(body))])},
		"timestamp":    time.Now().UnixMilli(),
		"hypothesisId": "F",
	}
	json.NewEncoder(logFile).Encode(logEntry)
	// #endregion

	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	failureCount := 0
	for lineIdx, line := range lines {
		if line == "" {
			continue
		}

		var result map[string]interface{}
		if err := json.Unmarshal([]byte(line), &result); err != nil {
			// #region agent log
			logEntry = map[string]interface{}{
				"sessionId":    "debug-session",
				"runId":        "run1",
				"location":     "main.go:215",
				"message":      "Failed to parse response line",
				"data":         map[string]interface{}{"lineIndex": lineIdx, "line": line, "error": err.Error()},
				"timestamp":    time.Now().UnixMilli(),
				"hypothesisId": "F",
			}
			json.NewEncoder(logFile).Encode(logEntry)
			// #endregion
			log("ERROR", "Failed to parse response line: %v", err)
			failed++
			continue
		}

		if success, ok := result["success"].(bool); ok && success {
			succeeded++
		} else {
			failed++
			failureCount++
			errMsg := ""
			if e, ok := result["error"].(string); ok {
				errMsg = e
			}
			docID := ""
			docData := map[string]interface{}{}
			if doc, ok := result["document"].(map[string]interface{}); ok {
				if id, ok := doc["id"].(string); ok {
					docID = id
				}
				// Capture document structure for first few failures - especially source/destination
				if failureCount <= 5 {
					docData = doc
					// Extract source/destination for verification
					source := ""
					destination := ""
					if s, ok := doc["source"].(string); ok {
						source = s
					}
					if d, ok := doc["destination"].(string); ok {
						destination = d
					}

					// #region agent log
					logEntry = map[string]interface{}{
						"sessionId": "debug-session",
						"runId":     "run1",
						"location":  "main.go:275",
						"message":   "Document upsert failed - checking balance references",
						"data": map[string]interface{}{
							"docId":         docID,
							"error":         errMsg,
							"source":        source,
							"destination":   destination,
							"document":      docData,
							"fullResult":    result,
							"failureNumber": failureCount,
						},
						"timestamp":    time.Now().UnixMilli(),
						"hypothesisId": "A",
					}
					json.NewEncoder(logFile).Encode(logEntry)
					// #endregion

					// Verify if the referenced balance_ids exist in Typesense
					if source != "" || destination != "" {
						// Query Typesense to check if these balance_ids exist
						checkBalanceIDs := []string{}
						if source != "" {
							checkBalanceIDs = append(checkBalanceIDs, source)
						}
						if destination != "" {
							checkBalanceIDs = append(checkBalanceIDs, destination)
						}

						for _, balanceID := range checkBalanceIDs {
							checkURL := fmt.Sprintf("%s://%s:%s/collections/balances/documents/%s",
								config.TypesenseProtocol, config.TypesenseHost, config.TypesensePort, balanceID)
							checkReq, _ := http.NewRequestWithContext(ctx, "GET", checkURL, nil)
							checkReq.Header.Set("X-TYPESENSE-API-KEY", config.TypesenseAPIKey)
							checkResp, checkErr := httpClient.Do(checkReq)
							exists := false
							if checkErr == nil && checkResp.StatusCode == 200 {
								exists = true
								checkResp.Body.Close()
							} else if checkResp != nil {
								checkResp.Body.Close()
							}

							// #region agent log
							logEntry = map[string]interface{}{
								"sessionId": "debug-session",
								"runId":     "run1",
								"location":  "main.go:310",
								"message":   "Verified balance_id existence in Typesense",
								"data": map[string]interface{}{
									"balanceId":     balanceID,
									"exists":        exists,
									"transactionId": docID,
								},
								"timestamp":    time.Now().UnixMilli(),
								"hypothesisId": "A",
							}
							json.NewEncoder(logFile).Encode(logEntry)
							// #endregion
						}
					}
				} else {
					// #region agent log
					logEntry = map[string]interface{}{
						"sessionId":    "debug-session",
						"runId":        "run1",
						"location":     "main.go:240",
						"message":      "Document upsert failed",
						"data":         map[string]interface{}{"docId": docID, "error": errMsg, "failureNumber": failureCount},
						"timestamp":    time.Now().UnixMilli(),
						"hypothesisId": "A,B,C,D,E",
					}
					json.NewEncoder(logFile).Encode(logEntry)
					// #endregion
				}
			}

			log("ERROR", "Document %s failed: %s", docID, errMsg)
		}
	}

	return succeeded, failed, nil
}

func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "HTTP 429") ||
		strings.Contains(errStr, "HTTP 5") ||
		strings.Contains(errStr, "connection refused") ||
		strings.Contains(errStr, "timeout") ||
		strings.Contains(errStr, "EOF")
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

func convertBigIntToString(value string) string {
	if value == "" {
		return "0"
	}
	return value
}

func maskConnectionString(connStr string) string {
	if idx := strings.Index(connStr, "@"); idx > 0 {
		return "***@" + connStr[idx+1:]
	}
	return connStr
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ============================================================================
// MAIN
// ============================================================================

func main() {
	log("INFO", "=== Typesense Full Reindex Script ===")
	log("INFO", "Database: %s", maskConnectionString(config.DatabaseDNS))
	log("INFO", "Typesense: %s://%s:%s", config.TypesenseProtocol, config.TypesenseHost, config.TypesensePort)
	log("INFO", "Batch Size: %d, Bulk Size: %d", config.BatchSize, config.BulkSize)

	ctx := context.Background()

	// Connect to database
	db, err := sql.Open("postgres", config.DatabaseDNS)
	if err != nil {
		log("ERROR", "Failed to open database: %v", err)
		os.Exit(1)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		log("ERROR", "Failed to ping database: %v", err)
		os.Exit(1)
	}
	log("INFO", "Database connection established")

	// Remove references from sources and destinations in transactions collection
	if err := removeReferencesFromTransactionsSchema(ctx); err != nil {
		log("ERROR", "Failed to remove references from transactions schema: %v", err)
		os.Exit(1)
	}

	// Verify all collections exist
	log("INFO", "Verifying Typesense collections...")
	collections := []string{"ledgers", "identities", "balances", "reconciliations", "transactions"}
	for _, c := range collections {
		if err := verifyTypesenseCollection(c); err != nil {
			log("ERROR", "Collection '%s' check failed: %v", c, err)
			os.Exit(1)
		}
	}
	log("INFO", "All Typesense collections verified")

	// Reindex in dependency order
	steps := []struct {
		name    string
		reindex func(context.Context, *sql.DB) bool
	}{
		{"LEDGERS", reindexLedgers},
		{"IDENTITIES", reindexIdentities},
		{"BALANCES", reindexBalances},
		{"RECONCILIATIONS", reindexReconciliations},
		{"TRANSACTIONS", reindexTransactions},
	}

	// Build skip map for fast lookup
	skipMap := make(map[string]bool)
	for _, name := range config.CollectionsToSkip {
		skipMap[strings.ToUpper(name)] = true
	}

	stepNum := 1
	for _, step := range steps {
		if skipMap[step.name] {
			log("INFO", "")
			log("INFO", "========== STEP %d: SKIPPING %s (already completed) ==========", stepNum, step.name)
			stepNum++
			continue
		}

		log("INFO", "")
		log("INFO", "========== STEP %d: REINDEX %s ==========", stepNum, step.name)
		stepNum++
		if !step.reindex(ctx, db) {
			log("ERROR", "%s reindexing failed, aborting", step.name)
			os.Exit(1)
		}

		// Add a short delay after balance reindexing to ensure documents are committed
		// before transactions try to reference them
		if step.name == "BALANCES" {
			log("INFO", "Waiting 5 seconds for balance documents to be committed...")
			time.Sleep(5 * time.Second)
		}
	}

	log("INFO", "")
	log("INFO", "=== Full reindex completed successfully ===")
}
