//go:build !cgo

package services

import (
	"testing"
)

func TestCheckReadOnlyPostgres_ParseErrorFallback(t *testing.T) {
	// In the non-CGO fallback (which uses the keyword normaliser), malformed SQL 
	// that doesn't contain mutating keywords is passed by Phase 1 and deferred 
	// to the DB engine in Phase 2.
	acceptedSqls := []string{
		"THIS IS NOT SQL AT ALL !!!",
		"SELECT FROM FROM FROM",
	}
	for _, q := range acceptedSqls {
		t.Run(q, func(t *testing.T) {
			result, ok := checkReadOnlyPostgres(q)
			assertReadOnly(t, result, ok) // Actually returns OK because it's harmless
		})
	}

	// However, some strings explicitly trigger parse errors in the keyword check
	// because they lack any identifiable verbs.
	rejectedSqls := []string{
		"( (",
		"",
	}
	for _, q := range rejectedSqls {
		t.Run(q, func(t *testing.T) {
			result, ok := checkReadOnlyPostgres(q)
			assertParseError(t, result, ok) // Fails keyword normaliser limits
		})
	}
}
