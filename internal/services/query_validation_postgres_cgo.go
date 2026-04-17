//go:build cgo

package services

import (
	"fmt"

	pg_query "github.com/pganalyze/pg_query_go/v6"

	"genreport/internal/models"
)

// checkReadOnlyPostgres uses the official libpg_query C library (via cgo) to
// parse the SQL into a full AST and inspects every top-level statement node.
func checkReadOnlyPostgres(rawSQL string) (models.QueryValidationResult, bool) {
	tree, err := pg_query.Parse(rawSQL)
	if err != nil {
		return models.QueryValidationResult{
			Status:      models.QueryValidationStatusParseError,
			Description: fmt.Sprintf("PostgreSQL parse error: %s", err.Error()),
		}, false
	}

	for _, stmt := range tree.Stmts {
		node := stmt.Stmt
		if node == nil {
			continue
		}
		switch n := node.Node.(type) {
		case *pg_query.Node_SelectStmt,
			*pg_query.Node_ExplainStmt:
			// allowed
		case *pg_query.Node_CopyStmt: // COPY TO stdout is read-only
			if n.CopyStmt.IsFrom {
				return models.QueryValidationResult{
					Status:      models.QueryValidationStatusNotReadOnly,
					Description: "statement is not a read-only operation (COPY FROM detected)",
				}, false
			}
			// allowed
		default:
			return models.QueryValidationResult{
				Status:      models.QueryValidationStatusNotReadOnly,
				Description: "statement is not a read-only operation (PostgreSQL AST check)",
			}, false
		}
	}

	return models.QueryValidationResult{}, true
}
