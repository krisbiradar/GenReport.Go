//go:build cgo

package services

import (
	"testing"
)

func TestCheckReadOnlyPostgres_ParseError(t *testing.T) {
	sqls := []string{
		// Note: `SELECT ??? garbage` is actually valid PG syntax (??? is an operator).
		// Use SQL that is structurally invalid for the pg parser.
		"THIS IS NOT SQL AT ALL !!!",
		"SELECT FROM FROM FROM",
		"( (",
	}
	for _, q := range sqls {
		t.Run(q, func(t *testing.T) {
			result, ok := checkReadOnlyPostgres(q)
			assertParseError(t, result, ok)
		})
	}
}
