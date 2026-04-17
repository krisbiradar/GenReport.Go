//go:build !cgo

package services

import (
	"genreport/internal/models"
)

// checkReadOnlyPostgres is the fallback for builds with CGO_ENABLED=0.
// It uses a robust keyword-normaliser because pg_query requires CGO.
func checkReadOnlyPostgres(rawSQL string) (models.QueryValidationResult, bool) {
	return checkReadOnlyByKeyword(rawSQL, "PostgreSQL")
}
