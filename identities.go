// Typesense Reindex Script - Identity Operations

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// Identity Model
// Data structure for identity records

type identity struct {
	IdentityID       string
	IdentityType     string
	OrganizationName string
	Category         string
	FirstName        string
	LastName         string
	OtherNames       string
	Gender           string
	EmailAddress     string
	PhoneNumber      string
	Nationality      string
	Street           string
	Country          string
	State            string
	PostCode         string
	City             string
	DOB              *time.Time
	CreatedAt        time.Time
	MetaData         map[string]interface{}
}

// Identity Reindex
// Main reindexing logic for identities collection

func reindexIdentities(ctx context.Context, db *sql.DB) bool {
	// Build count query with time range filtering
	countQuery := "SELECT COUNT(*) FROM blnk.identity"
	countQuery, countParams := buildTimeRangeClause(countQuery, 1)
	
	var totalCount int64
	err := db.QueryRowContext(ctx, countQuery, countParams...).Scan(&totalCount)
	if err != nil {
		log("ERROR", "Failed to count identities: %v", err)
		return false
	}
	log("INFO", "Total identities to reindex: %d", totalCount)

	if totalCount == 0 {
		log("INFO", "No identities to reindex")
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
		identities, err := fetchIdentityBatch(ctx, db, offset)
		if err != nil {
			log("ERROR", "Failed to fetch identity batch: %v", err)
			return false
		}

		if len(identities) == 0 {
			break
		}

		documents := make([]map[string]interface{}, 0, len(identities))
		for _, i := range identities {
			documents = append(documents, transformIdentity(i))
		}

		for i := 0; i < len(documents); i += config.BulkSize {
			end := i + config.BulkSize
			if end > len(documents) {
				end = len(documents)
			}
			chunk := documents[i:end]

			succeeded, failed := bulkUpsertWithRetry(ctx, "identities", chunk)
			totalSucceeded += int64(succeeded)
			totalFailed += int64(failed)
		}

		totalScanned += int64(len(identities))
		offset += int64(len(identities))

		if totalScanned%config.ProgressInterval == 0 || totalScanned == totalCount {
			elapsed := time.Since(startTime)
			rate := float64(totalScanned) / elapsed.Seconds()
			log("INFO", "Progress: %d/%d identities (%.1f%%), %.0f docs/sec, succeeded: %d, failed: %d",
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

		if len(identities) < config.BatchSize {
			break
		}
	}

	elapsed := time.Since(startTime)
	log("INFO", "Identity reindex completed in %v", elapsed)
	log("INFO", "Total scanned: %d, succeeded: %d, failed: %d", totalScanned, totalSucceeded, totalFailed)

	return totalFailed == 0 || float64(totalFailed)/float64(totalScanned)*100 <= config.MaxFailureRatePercent
}

// Database Operations
// Functions for fetching identity data from PostgreSQL

func fetchIdentityBatch(ctx context.Context, db *sql.DB, offset int64) ([]*identity, error) {
	baseQuery := `
		SELECT 
			identity_id,
			COALESCE(identity_type, '') as identity_type,
			COALESCE(organization_name, '') as organization_name,
			COALESCE(category, '') as category,
			COALESCE(first_name, '') as first_name,
			COALESCE(last_name, '') as last_name,
			COALESCE(other_names, '') as other_names,
			COALESCE(gender, '') as gender,
			COALESCE(email_address, '') as email_address,
			COALESCE(phone_number, '') as phone_number,
			COALESCE(nationality, '') as nationality,
			COALESCE(street, '') as street,
			COALESCE(country, '') as country,
			COALESCE(state, '') as state,
			COALESCE(post_code, '') as post_code,
			COALESCE(city, '') as city,
			dob,
			created_at,
			COALESCE(meta_data, '{}') as meta_data
		FROM blnk.identity
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

	var identities []*identity
	for rows.Next() {
		i := &identity{}
		var metaDataJSON []byte
		var dob sql.NullTime

		err := rows.Scan(
			&i.IdentityID,
			&i.IdentityType,
			&i.OrganizationName,
			&i.Category,
			&i.FirstName,
			&i.LastName,
			&i.OtherNames,
			&i.Gender,
			&i.EmailAddress,
			&i.PhoneNumber,
			&i.Nationality,
			&i.Street,
			&i.Country,
			&i.State,
			&i.PostCode,
			&i.City,
			&dob,
			&i.CreatedAt,
			&metaDataJSON,
		)
		if err != nil {
			return nil, err
		}

		if dob.Valid {
			i.DOB = &dob.Time
		}

		if len(metaDataJSON) > 0 {
			if err := json.Unmarshal(metaDataJSON, &i.MetaData); err != nil {
				i.MetaData = make(map[string]interface{})
			}
		} else {
			i.MetaData = make(map[string]interface{})
		}

		identities = append(identities, i)
	}

	return identities, rows.Err()
}

// Transformation
// Functions for converting identity records to Typesense documents

func transformIdentity(i *identity) map[string]interface{} {
	doc := map[string]interface{}{
		"id":                i.IdentityID,
		"identity_id":       i.IdentityID,
		"identity_type":     i.IdentityType,
		"organization_name": i.OrganizationName,
		"category":          i.Category,
		"first_name":        i.FirstName,
		"last_name":         i.LastName,
		"other_names":       i.OtherNames,
		"gender":            i.Gender,
		"email_address":     i.EmailAddress,
		"phone_number":      i.PhoneNumber,
		"nationality":       i.Nationality,
		"street":            i.Street,
		"country":           i.Country,
		"state":             i.State,
		"post_code":         i.PostCode,
		"city":              i.City,
		"created_at":        i.CreatedAt.Unix(),
	}

	// Handle DOB - convert to Unix timestamp
	if i.DOB != nil && !i.DOB.IsZero() {
		doc["dob"] = i.DOB.Unix()
	} else {
		doc["dob"] = i.CreatedAt.Unix() // Fall back to created_at
	}

	if i.MetaData != nil {
		doc["meta_data"] = i.MetaData
	} else {
		doc["meta_data"] = make(map[string]interface{})
	}

	return doc
}
