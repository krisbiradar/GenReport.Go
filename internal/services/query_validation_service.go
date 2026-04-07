package services

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"vitess.io/vitess/go/vt/sqlparser"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/microsoft/go-mssqldb"
	"github.com/rs/zerolog"
	_ "github.com/sijms/go-ora/v2"
	"gorm.io/gorm"

	"genreport/internal/models"
	"genreport/internal/security"
)

// QueryValidationService validates SQL queries for read-only compliance and
// dry-runs them against a rolled-back transaction on the target database.
type QueryValidationService struct {
	gormDB    *gorm.DB
	masterKey string
	logger    zerolog.Logger
}

func NewQueryValidationService(gormDB *gorm.DB, masterKey string, logger zerolog.Logger) *QueryValidationService {
	return &QueryValidationService{
		gormDB:    gormDB,
		masterKey: masterKey,
		logger:    logger,
	}
}

// Validate performs the two-phase validation:
//  1. Static read-only check via the provider's dedicated parser.
//  2. Dry-run inside a rolled-back transaction on the real database.
func (s *QueryValidationService) Validate(ctx context.Context, req models.QueryValidationRequest) models.QueryValidationResult {
	// ── Resolve the database record ──────────────────────────────────────────
	var dbRecord models.Database
	if err := s.gormDB.First(&dbRecord, "id = ?", req.DatabaseID).Error; err != nil {
		s.logger.Error().Err(err).Str("databaseId", req.DatabaseID).Msg("query validation: database not found")
		return models.QueryValidationResult{
			Status:      models.QueryValidationStatusExecutionError,
			Description: fmt.Sprintf("database record not found: %s", req.DatabaseID),
		}
	}

	// ── Determine provider kind from the DB record ───────────────────────────
	provider := dbRecord.Provider

	// MongoDB is not SQL — unsupported.
	if provider == models.DbProviderMongoClient {
		return models.QueryValidationResult{
			Status:      models.QueryValidationStatusUnsupported,
			Description: "MongoDB is not a SQL database; query validation is not supported for this provider",
		}
	}

	// ── Phase 1: static read-only check ─────────────────────────────────────
	if result, ok := checkReadOnly(req.SQL, provider); !ok {
		return result
	}

	// ── Decrypt + prepare connection string ──────────────────────────────────
	connString, err := s.resolveConnectionString(dbRecord)
	if err != nil {
		s.logger.Error().Err(err).Str("db", dbRecord.Name).Msg("query validation: failed to resolve connection string")
		return models.QueryValidationResult{
			Status:      models.QueryValidationStatusExecutionError,
			Description: "could not resolve database connection string",
		}
	}

	// ── Phase 2: dry-run ─────────────────────────────────────────────────────
	return dryRun(ctx, req.SQL, provider, connString, s.logger)
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase 1: per-provider read-only static analysis
// ─────────────────────────────────────────────────────────────────────────────

// checkReadOnly returns (result, true) when the SQL passes the read-only check,
// or (rejectionResult, false) when validation fails.
func checkReadOnly(rawSQL string, provider models.DbProvider) (models.QueryValidationResult, bool) {
	switch provider {
	case models.DbProviderNpgSql:
		return checkReadOnlyPostgres(rawSQL)
	case models.DbProviderMySqlConnector:
		return checkReadOnlyMySQL(rawSQL)
	case models.DbProviderSqlClient:
		return checkReadOnlySQLServer(rawSQL)
	case models.DbProviderOracle:
		return checkReadOnlyOracle(rawSQL)
	default:
		return models.QueryValidationResult{
			Status:      models.QueryValidationStatusUnsupported,
			Description: "provider is not supported",
		}, false
	}
}

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

// checkReadOnlyMySQL uses Vitess' production-grade MySQL parser.
func checkReadOnlyMySQL(rawSQL string) (models.QueryValidationResult, bool) {
	parser, err := sqlparser.New(sqlparser.Options{})
	if err != nil {
		return models.QueryValidationResult{
			Status:      models.QueryValidationStatusParseError,
			Description: fmt.Sprintf("MySQL parser init error: %s", err.Error()),
		}, false
	}

	stmt, err := parser.Parse(rawSQL)
	if err != nil {
		return models.QueryValidationResult{
			Status:      models.QueryValidationStatusParseError,
			Description: fmt.Sprintf("MySQL parse error: %s", err.Error()),
		}, false
	}

	stmtType := sqlparser.ASTToStatementType(stmt)
	if stmtType.IsReadStatement() {
		return models.QueryValidationResult{}, true
	}

	return models.QueryValidationResult{
		Status:      models.QueryValidationStatusNotReadOnly,
		Description: fmt.Sprintf("statement is not a read-only operation (MySQL Vitess AST check; detected type: %s)", stmtType),
	}, false
}


// checkReadOnlySQLServer uses a robust keyword-normaliser because there is no
// widely available open-source T-SQL AST parser for Go with the reliability
// of pg_query or Vitess.
func checkReadOnlySQLServer(rawSQL string) (models.QueryValidationResult, bool) {
	return checkReadOnlyByKeyword(rawSQL, "SQL Server")
}

// checkReadOnlyOracle uses the same keyword-normaliser strategy.
func checkReadOnlyOracle(rawSQL string) (models.QueryValidationResult, bool) {
	return checkReadOnlyByKeyword(rawSQL, "Oracle")
}

var (
	// sqlLineComment matches -- ... to end of line.
	sqlLineComment = regexp.MustCompile(`--[^\n]*`)
	// sqlBlockComment matches /* ... */ (non-greedy).
	sqlBlockComment = regexp.MustCompile(`(?s)/\*.*?\*/`)
	// sqlStringLiteral matches single-quoted strings (handles '' escaping).
	sqlStringLiteral = regexp.MustCompile(`'(?:[^']|'')*'`)
	// sqlWhitespace collapses runs of whitespace.
	sqlWhitespace = regexp.MustCompile(`\s+`)

	// allowedFirstVerbs are the only statement-starting keywords that indicate
	// a read-only query for SQL Server and Oracle.
	allowedFirstVerbs = map[string]bool{
		"SELECT":   true,
		"WITH":     true, // CTE — body is examined separately via first non-CTE verb check
		"EXEC":     false, // stored-proc calls can mutate
		"EXECUTE":  false,
		"SHOW":     true,
		"DESCRIBE": true,
		"DESC":     true,
		"EXPLAIN":  true,
		"PRINT":    true, // informational
	}

	// mutatingVerbs are keywords that definitively signal a write operation.
	mutatingVerbs = map[string]bool{
		"INSERT":   true,
		"UPDATE":   true,
		"DELETE":   true,
		"MERGE":    true,
		"UPSERT":   true,
		"DROP":     true,
		"CREATE":   true,
		"ALTER":    true,
		"TRUNCATE": true,
		"REPLACE":  true,
		"CALL":     true,
		"EXEC":     true,
		"EXECUTE":  true,
		"GRANT":    true,
		"REVOKE":   true,
		"DENY":     true,
	}
)

// checkReadOnlyByKeyword strips comments and string literals from the SQL,
// then inspects the first meaningful keyword to determine read-only intent.
// It also scans the full token stream for any mutating verbs.
func checkReadOnlyByKeyword(rawSQL string, providerLabel string) (models.QueryValidationResult, bool) {
	// 1. Strip comments (block first, then line — order matters).
	clean := sqlBlockComment.ReplaceAllString(rawSQL, " ")
	clean = sqlLineComment.ReplaceAllString(clean, " ")
	// 2. Remove string literals to avoid false positives ("DROP TABLE" inside a string).
	clean = sqlStringLiteral.ReplaceAllString(clean, "''")
	// 3. Normalise whitespace.
	clean = strings.TrimSpace(sqlWhitespace.ReplaceAllString(clean, " "))

	if clean == "" {
		return models.QueryValidationResult{
			Status:      models.QueryValidationStatusParseError,
			Description: fmt.Sprintf("%s: SQL is empty after stripping comments", providerLabel),
		}, false
	}

	// 4. Split into tokens (uppercase for case-insensitive matching).
	tokens := strings.Fields(strings.ToUpper(clean))

	// 5. Find the first verb — skip parentheses that appear before WITH/SELECT.
	firstVerb := ""
	for _, tok := range tokens {
		// Strip leading/trailing parens from token
		stripped := strings.Trim(tok, "();,")
		if stripped == "" {
			continue
		}
		firstVerb = stripped
		break
	}

	if firstVerb == "" {
		return models.QueryValidationResult{
			Status:      models.QueryValidationStatusParseError,
			Description: fmt.Sprintf("%s: could not determine first SQL verb", providerLabel),
		}, false
	}

	// 6. A WITH (CTE) prefix is fine; scan past it to find the *trailing* DML verb.
	//    We must track parenthesis depth so that verbs appearing *inside*
	//    a CTE definition (e.g. AS (SELECT …)) are ignored — only verbs at
	//    depth 0 represent the final statement (SELECT / INSERT / DELETE …).
	if firstVerb == "WITH" {
		cteDepth := 0
		seenCTEOpen := false // true once we encounter the first '('
		for _, tok := range tokens[1:] {
			// Update depth based on parens embedded anywhere in the token.
			for _, ch := range tok {
				if ch == '(' {
					cteDepth++
					seenCTEOpen = true
				} else if ch == ')' {
					cteDepth--
				}
			}
			// Only examine verbs at depth 0 *after* we've seen at least one
			// CTE body open-paren (to skip the cte_name token itself).
			if seenCTEOpen && cteDepth == 0 {
				stripped := strings.Trim(tok, "();,")
				if stripped == "" {
					continue
				}
				if stripped == "SELECT" || stripped == "WITH" {
					// Trailing DML is read-only.
					return models.QueryValidationResult{}, true
				}
				if mutatingVerbs[stripped] {
					return models.QueryValidationResult{
						Status:      models.QueryValidationStatusNotReadOnly,
						Description: fmt.Sprintf("%s: CTE trailing statement is mutating (%s)", providerLabel, stripped),
					}, false
				}
			}
		}
		// If we never hit depth-0 trailing verb, the WITH had no usable DML.
		return models.QueryValidationResult{
			Status:      models.QueryValidationStatusNotReadOnly,
			Description: fmt.Sprintf("%s: CTE does not contain a trailing SELECT statement", providerLabel),
		}, false
	}

	// 7. Check if first verb is explicitly allowed or mutating.
	if mutatingVerbs[firstVerb] {
		return models.QueryValidationResult{
			Status:      models.QueryValidationStatusNotReadOnly,
			Description: fmt.Sprintf("%s: statement starts with mutating verb %q", providerLabel, firstVerb),
		}, false
	}

	if ok, known := allowedFirstVerbs[firstVerb]; known {
		if !ok {
			return models.QueryValidationResult{
				Status:      models.QueryValidationStatusNotReadOnly,
				Description: fmt.Sprintf("%s: statement starts with potentially mutating verb %q", providerLabel, firstVerb),
			}, false
		}
	}

	// 8. Full token scan: reject if any mutating verb appears at statement boundary.
	//    Simplified: flag any mutating verb not inside parentheses depth > 0.
	depth := 0
	for _, tok := range tokens {
		for _, ch := range tok {
			if ch == '(' {
				depth++
			} else if ch == ')' {
				depth--
			}
		}
		stripped := strings.Trim(tok, "();,")
		if depth == 0 && mutatingVerbs[stripped] {
			return models.QueryValidationResult{
				Status:      models.QueryValidationStatusNotReadOnly,
				Description: fmt.Sprintf("%s: contains mutating keyword %q", providerLabel, stripped),
			}, false
		}
	}

	return models.QueryValidationResult{}, true
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase 2: dry-run inside a rolled-back transaction
// ─────────────────────────────────────────────────────────────────────────────

func dryRun(ctx context.Context, rawSQL string, provider models.DbProvider, connString string, logger zerolog.Logger) models.QueryValidationResult {
	driverName, prepFn := providerDriverConfig(provider, connString)

	// prepFn translates ADO.NET or raw DSN into a driver-compatible DSN.
	dsn := prepFn(connString)

	db, err := sql.Open(driverName, dsn)
	if err != nil {
		logger.Error().Err(err).Str("driver", driverName).Msg("query validation: failed to open connection")
		return models.QueryValidationResult{
			Status:      models.QueryValidationStatusExecutionError,
			Description: fmt.Sprintf("failed to open database connection: %s", err.Error()),
		}
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		return models.QueryValidationResult{
			Status:      models.QueryValidationStatusExecutionError,
			Description: fmt.Sprintf("database unreachable: %s", err.Error()),
		}
	}

	// Begin transaction to ensure execution and rollback occur on the exact same pool connection.
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return models.QueryValidationResult{
			Status:      models.QueryValidationStatusExecutionError,
			Description: fmt.Sprintf("failed to begin transaction: %s", err.Error()),
		}
	}
	// Always rollback to prevent persistence.
	defer tx.Rollback()

	// Execute the query. Empty result set is fine — we only care whether it
	// errors at the DB level.
	rows, err := tx.QueryContext(ctx, rawSQL)
	if err != nil {
		return models.QueryValidationResult{
			Status:      models.QueryValidationStatusExecutionError,
			Description: fmt.Sprintf("query execution error: %s", err.Error()),
		}
	}
	defer rows.Close()

	// Drain rows to detect any deferred errors (e.g. from streaming cursors).
	for rows.Next() {
	}
	if err := rows.Err(); err != nil {
		return models.QueryValidationResult{
			Status:      models.QueryValidationStatusExecutionError,
			Description: fmt.Sprintf("query result error: %s", err.Error()),
		}
	}

	return models.QueryValidationResult{
		Status:      models.QueryValidationStatusOK,
		Description: "query is read-only and executed successfully (no rows returned on empty replica is expected)",
	}
}

// providerDriverConfig returns the SQL driver name, and a DSN preparation function
// for the given provider.
func providerDriverConfig(provider models.DbProvider, connString string) (
	driverName string,
	prepFn func(string) string,
) {
	switch provider {
	case models.DbProviderNpgSql:
		return "pgx",
			func(cs string) string { return prepareConnectionString(cs, "postgres") }
	case models.DbProviderSqlClient:
		return "sqlserver",
			func(cs string) string { return prepareConnectionString(cs, "sqlserver") }
	case models.DbProviderMySqlConnector:
		return "mysql",
			func(cs string) string { return prepareConnectionString(cs, "mysql") }
	case models.DbProviderOracle:
		return "oracle",
			func(cs string) string { return prepareConnectionString(cs, "oracle") }
	default:
		return "", func(cs string) string { return cs }
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

// resolveConnectionString decrypts and normalises the connection string stored
// in the database record, mirroring the logic used in SchemaSyncJob.
func (s *QueryValidationService) resolveConnectionString(dbRecord models.Database) (string, error) {
	connString := strings.TrimSpace(dbRecord.ConnectionString)
	if connString == "" {
		return "", fmt.Errorf("connection string is empty for database %q", dbRecord.Name)
	}

	if s.masterKey != "" &&
		len(connString) > 20 &&
		!strings.Contains(connString, "host=") &&
		!strings.Contains(connString, "://") &&
		!strings.Contains(connString, "Server=") {

		// Try the two credential-type labels used by the C# encryptor.
		if dec, err := security.Decrypt(connString, "ConnectionString", s.masterKey); err == nil && dec != "" {
			return dec, nil
		}
		if dec, err := security.Decrypt(connString, "DatabaseConnectionString", s.masterKey); err == nil && dec != "" {
			return dec, nil
		}
		s.logger.Warn().Str("db", dbRecord.Name).Msg("query validation: decryption failed — using raw connection string")
	}

	return connString, nil
}
