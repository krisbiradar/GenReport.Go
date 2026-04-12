package services

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/mattn/go-sqlite3"
	_ "github.com/microsoft/go-mssqldb"
	"github.com/rs/zerolog"
	_ "github.com/sijms/go-ora/v2"
	"gorm.io/gorm"

	"genreport/internal/models"
	"genreport/internal/security"
)

// ReportQueryService executes a user SQL query against the target database
// and materialises the results into a temporary SQLite file.
type ReportQueryService struct {
	gormDB    *gorm.DB
	masterKey string
	logger    zerolog.Logger
}

func NewReportQueryService(gormDB *gorm.DB, masterKey string, logger zerolog.Logger) *ReportQueryService {
	return &ReportQueryService{
		gormDB:    gormDB,
		masterKey: masterKey,
		logger:    logger,
	}
}

// Execute runs the query from the job request, streams the results into a
// SQLite temp file, and returns the file path. The caller is responsible for
// removing the file when it is no longer needed.
func (s *ReportQueryService) Execute(ctx context.Context, req models.ReportJobRequest) (string, error) {
	// ── 1. Look up the database record ───────────────────────────────────────
	var dbRecord models.Database
	if err := s.gormDB.First(&dbRecord, "id = ?", req.DatabaseConnectionID).Error; err != nil {
		return "", fmt.Errorf("database record %q not found: %w", req.DatabaseConnectionID, err)
	}

	// ── 2. Resolve (decrypt) the connection string ────────────────────────────
	connString, err := s.resolveConnectionString(dbRecord)
	if err != nil {
		return "", fmt.Errorf("could not resolve connection string: %w", err)
	}

	// ── 3. Determine SQL driver ───────────────────────────────────────────────
	driverName, dsnFn := s.driverConfig(dbRecord.Provider)
	if driverName == "" {
		return "", fmt.Errorf("unsupported database provider: %d", dbRecord.Provider)
	}
	dsn := dsnFn(connString)

	// ── 4. Open source DB connection ──────────────────────────────────────────
	srcDB, err := sql.Open(driverName, dsn)
	if err != nil {
		return "", fmt.Errorf("failed to open source database connection: %w", err)
	}
	defer srcDB.Close()

	queryCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	if err := srcDB.PingContext(queryCtx); err != nil {
		return "", fmt.Errorf("source database unreachable: %w", err)
	}

	// ── 5. Execute the query ──────────────────────────────────────────────────
	rows, err := srcDB.QueryContext(queryCtx, req.Query)
	if err != nil {
		return "", fmt.Errorf("query execution failed: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return "", fmt.Errorf("failed to read column names: %w", err)
	}

	// ── 6. Create a temp SQLite file ──────────────────────────────────────────
	tmpFile, err := os.CreateTemp("", "report-*.sqlite")
	if err != nil {
		return "", fmt.Errorf("failed to create temp SQLite file: %w", err)
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close() // SQLite will re-open it by path

	sqliteDB, err := sql.Open("sqlite3", tmpPath)
	if err != nil {
		os.Remove(tmpPath)
		return "", fmt.Errorf("failed to open SQLite file: %w", err)
	}
	defer sqliteDB.Close()

	// ── 7. Create results table in SQLite ─────────────────────────────────────
	quotedCols := make([]string, len(cols))
	placeholders := make([]string, len(cols))
	for i, c := range cols {
		quotedCols[i] = fmt.Sprintf(`"%s" TEXT`, sanitizeIdentifier(c))
		placeholders[i] = "?"
	}
	createSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS results (%s)`, strings.Join(quotedCols, ", "))
	if _, err := sqliteDB.ExecContext(ctx, createSQL); err != nil {
		os.Remove(tmpPath)
		return "", fmt.Errorf("failed to create results table in SQLite: %w", err)
	}

	// ── 8. Insert rows in a single transaction for performance ────────────────
	tx, err := sqliteDB.BeginTx(ctx, nil)
	if err != nil {
		os.Remove(tmpPath)
		return "", fmt.Errorf("failed to begin SQLite transaction: %w", err)
	}

	insertSQL := fmt.Sprintf(
		`INSERT INTO results VALUES (%s)`,
		strings.Join(placeholders, ", "),
	)
	stmt, err := tx.PrepareContext(ctx, insertSQL)
	if err != nil {
		tx.Rollback()
		os.Remove(tmpPath)
		return "", fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	vals := make([]any, len(cols))
	valPtrs := make([]any, len(cols))
	for i := range vals {
		valPtrs[i] = &vals[i]
	}

	rowCount := 0
	for rows.Next() {
		if err := rows.Scan(valPtrs...); err != nil {
			tx.Rollback()
			os.Remove(tmpPath)
			return "", fmt.Errorf("failed to scan row: %w", err)
		}
		strVals := make([]any, len(vals))
		for i, v := range vals {
			if v == nil {
				strVals[i] = nil
			} else {
				strVals[i] = fmt.Sprintf("%v", v)
			}
		}
		if _, err := stmt.ExecContext(ctx, strVals...); err != nil {
			tx.Rollback()
			os.Remove(tmpPath)
			return "", fmt.Errorf("failed to insert row into SQLite: %w", err)
		}
		rowCount++
	}
	if err := rows.Err(); err != nil {
		tx.Rollback()
		os.Remove(tmpPath)
		return "", fmt.Errorf("error iterating result rows: %w", err)
	}

	if err := tx.Commit(); err != nil {
		os.Remove(tmpPath)
		return "", fmt.Errorf("failed to commit SQLite transaction: %w", err)
	}

	s.logger.Info().
		Str("sessionId", req.SessionID).
		Str("databaseConnectionId", req.DatabaseConnectionID).
		Int("rows", rowCount).
		Str("sqlitePath", tmpPath).
		Msg("report query executed and written to SQLite")

	return tmpPath, nil
}

// resolveConnectionString decrypts the stored connection string if needed,
// mirroring the pattern used in QueryValidationService.
func (s *ReportQueryService) resolveConnectionString(dbRecord models.Database) (string, error) {
	connString := strings.TrimSpace(dbRecord.ConnectionString)
	if connString == "" {
		return "", fmt.Errorf("connection string is empty for database %q", dbRecord.Name)
	}

	if s.masterKey != "" &&
		len(connString) > 20 &&
		!strings.Contains(connString, "host=") &&
		!strings.Contains(connString, "://") &&
		!strings.Contains(connString, "Server=") {

		if dec, err := security.Decrypt(connString, "ConnectionString", s.masterKey); err == nil && dec != "" {
			return dec, nil
		}
		if dec, err := security.Decrypt(connString, "DatabaseConnectionString", s.masterKey); err == nil && dec != "" {
			return dec, nil
		}
		s.logger.Warn().Str("db", dbRecord.Name).Msg("report query: decryption failed — using raw connection string")
	}

	return connString, nil
}

// driverConfig returns the SQL driver name and a DSN preparation function,
// mirroring providerDriverConfig from QueryValidationService.
func (s *ReportQueryService) driverConfig(provider models.DbProvider) (string, func(string) string) {
	switch provider {
	case models.DbProviderNpgSql:
		return "pgx", func(cs string) string { return prepareConnectionString(cs, "postgres") }
	case models.DbProviderSqlClient:
		return "sqlserver", func(cs string) string { return prepareConnectionString(cs, "sqlserver") }
	case models.DbProviderMySqlConnector:
		return "mysql", func(cs string) string { return prepareConnectionString(cs, "mysql") }
	case models.DbProviderOracle:
		return "oracle", func(cs string) string { return prepareConnectionString(cs, "oracle") }
	default:
		return "", func(cs string) string { return cs }
	}
}

// sanitizeIdentifier strips characters that are illegal in SQLite column names.
func sanitizeIdentifier(name string) string {
	var b strings.Builder
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
		} else {
			b.WriteRune('_')
		}
	}
	return b.String()
}
