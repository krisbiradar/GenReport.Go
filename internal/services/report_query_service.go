package services

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"strings"

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

// ReportQueryService executes a SQL query against a target database on a
// cursor/batch basis, materialises all rows into a temporary SQLite file,
// and POSTs that file (multipart) together with the original request JSON
// to the configured callback endpoint.
type ReportQueryService struct {
	gormDB      *gorm.DB
	masterKey   string
	callbackURL string
	batchSize   int
	logger      zerolog.Logger
}

func NewReportQueryService(
	gormDB *gorm.DB,
	masterKey string,
	callbackURL string,
	batchSize int,
	logger zerolog.Logger,
) *ReportQueryService {
	return &ReportQueryService{
		gormDB:      gormDB,
		masterKey:   masterKey,
		callbackURL: callbackURL,
		batchSize:   batchSize,
		logger:      logger,
	}
}

// Execute runs the full pipeline for a single report request:
//  1. Look up the database record.
//  2. Decrypt + prepare the connection string.
//  3. Open a transaction on the target DB (always rolled back — read only).
//  4. Stream rows in batches of s.batchSize into a temp SQLite file.
//  5. POST the SQLite file + original JSON to the callback URL.
//  6. Delete the temp file (deferred).
func (s *ReportQueryService) Execute(ctx context.Context, msg models.ReportQueryMessage) error {
	log := s.logger.With().
		Str("userId", msg.UserID).
		Str("databaseId", msg.DatabaseID).
		Str("sessionTitle", msg.SessionTitle).
		Logger()

	// ── 1. Resolve database record ────────────────────────────────────────────
	var dbRecord models.Database
	if err := s.gormDB.First(&dbRecord, "id = ?", msg.DatabaseID).Error; err != nil {
		return fmt.Errorf("database record not found (%s): %w", msg.DatabaseID, err)
	}

	if dbRecord.Provider == models.DbProviderMongoClient {
		return fmt.Errorf("MongoDB is not supported for report query execution")
	}

	// ── 2. Decrypt connection string ──────────────────────────────────────────
	connString, err := s.resolveConnStringForDB(dbRecord)
	if err != nil {
		return fmt.Errorf("failed to resolve connection string: %w", err)
	}

	// ── 3. Open target database connection ────────────────────────────────────
	driverName, prepFn := providerDriverConfig(dbRecord.Provider, connString)
	if driverName == "" {
		return fmt.Errorf("unsupported database provider: %d", dbRecord.Provider)
	}
	dsn := prepFn(connString)

	targetDB, err := sql.Open(driverName, dsn)
	if err != nil {
		return fmt.Errorf("failed to open target database connection: %w", err)
	}
	defer targetDB.Close()

	if err := targetDB.PingContext(ctx); err != nil {
		return fmt.Errorf("target database unreachable: %w", err)
	}

	// ── 4. Create temp SQLite file ────────────────────────────────────────────
	sqliteFile, err := os.CreateTemp("", "report_*.sqlite")
	if err != nil {
		return fmt.Errorf("failed to create temp SQLite file: %w", err)
	}
	sqlitePath := sqliteFile.Name()
	sqliteFile.Close() // sqlite3 driver opens by path
	defer func() {
		if removeErr := os.Remove(sqlitePath); removeErr != nil && !os.IsNotExist(removeErr) {
			log.Warn().Err(removeErr).Str("path", sqlitePath).Msg("report: failed to delete temp SQLite file")
		}
	}()

	log.Info().Str("sqlite", sqlitePath).Msg("report: created temp SQLite file")

	// ── 5. Execute query and stream into SQLite ───────────────────────────────
	rowCount, err := s.streamToSQLite(ctx, targetDB, msg.Query, sqlitePath, log)
	if err != nil {
		return fmt.Errorf("query execution failed: %w", err)
	}

	log.Info().Int64("rows", rowCount).Msg("report: query complete, all rows written to SQLite")

	// ── 6. POST to callback ───────────────────────────────────────────────────
	if err := s.postCallback(ctx, msg, sqlitePath, log); err != nil {
		return fmt.Errorf("callback failed: %w", err)
	}

	log.Info().Msg("report: callback successful")
	return nil
}

// streamToSQLite opens a read-only transaction on targetDB, runs rawSQL,
// and writes every result row into a "results" table in the SQLite file at path.
// Rows are committed in batches of s.batchSize to limit memory pressure.
func (s *ReportQueryService) streamToSQLite(
	ctx context.Context,
	targetDB *sql.DB,
	rawSQL string,
	sqlitePath string,
	log zerolog.Logger,
) (int64, error) {
	// ── Open SQLite destination ───────────────────────────────────────────────
	sqliteDB, err := sql.Open("sqlite3", sqlitePath)
	if err != nil {
		return 0, fmt.Errorf("failed to open SQLite file: %w", err)
	}
	defer sqliteDB.Close()

	// ── Execute query inside a rolled-back transaction on the target DB ───────
	// ReadOnly: false — some drivers (e.g. go-mssqldb) reject driver-level
	// read-only transactions. The deferred Rollback prevents any writes.
	tx, err := targetDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: false})
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction on target DB: %w", err)
	}
	defer tx.Rollback() // always rolled back — we never commit the target tx

	rows, err := tx.QueryContext(ctx, rawSQL)
	if err != nil {
		return 0, fmt.Errorf("query error: %w", err)
	}
	defer rows.Close()

	// ── Introspect columns ────────────────────────────────────────────────────
	colNames, err := rows.Columns()
	if err != nil {
		return 0, fmt.Errorf("failed to read column names: %w", err)
	}
	if len(colNames) == 0 {
		return 0, fmt.Errorf("query returned no columns")
	}

	// ── Create SQLite results table ───────────────────────────────────────────
	colDefs := make([]string, len(colNames))
	quotedCols := make([]string, len(colNames))
	placeholders := make([]string, len(colNames))
	for i, c := range colNames {
		safe := strings.ReplaceAll(c, `"`, `""`)
		colDefs[i] = fmt.Sprintf(`"%s" TEXT`, safe)
		quotedCols[i] = fmt.Sprintf(`"%s"`, safe)
		placeholders[i] = "?"
	}

	createDDL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "results" (%s)`, strings.Join(colDefs, ", "))
	if _, err := sqliteDB.ExecContext(ctx, createDDL); err != nil {
		return 0, fmt.Errorf("failed to create SQLite results table: %w", err)
	}

	insertSQL := fmt.Sprintf(
		`INSERT INTO "results" (%s) VALUES (%s)`,
		strings.Join(quotedCols, ", "),
		strings.Join(placeholders, ", "),
	)

	// ── Stream rows into SQLite in batches ────────────────────────────────────
	values := make([]interface{}, len(colNames))
	valuePtrs := make([]interface{}, len(colNames))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	var totalRows int64
	batchNum := 1

	// Helper: begin a fresh SQLite batch transaction + prepared statement.
	beginBatch := func() (*sql.Tx, *sql.Stmt, error) {
		bTx, err := sqliteDB.BeginTx(ctx, nil)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to begin SQLite batch transaction: %w", err)
		}
		bStmt, err := bTx.PrepareContext(ctx, insertSQL)
		if err != nil {
			bTx.Rollback()
			return nil, nil, fmt.Errorf("failed to prepare SQLite insert statement: %w", err)
		}
		return bTx, bStmt, nil
	}

	bTx, bStmt, err := beginBatch()
	if err != nil {
		return 0, err
	}

	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			bStmt.Close()
			bTx.Rollback()
			return totalRows, fmt.Errorf("failed to scan row %d: %w", totalRows+1, err)
		}

		// Convert []byte values to string — SQLite stores everything as TEXT.
		args := make([]interface{}, len(values))
		for i, v := range values {
			if b, ok := v.([]byte); ok {
				args[i] = string(b)
			} else {
				args[i] = v
			}
		}

		if _, err := bStmt.ExecContext(ctx, args...); err != nil {
			bStmt.Close()
			bTx.Rollback()
			return totalRows, fmt.Errorf("failed to insert row %d into SQLite: %w", totalRows+1, err)
		}
		totalRows++

		// Commit batch every s.batchSize rows and start a new one.
		if int(totalRows)%s.batchSize == 0 {
			bStmt.Close()
			if err := bTx.Commit(); err != nil {
				return totalRows, fmt.Errorf("failed to commit SQLite batch %d: %w", batchNum, err)
			}
			log.Debug().Int64("rows", totalRows).Int("batch", batchNum).Msg("report: batch committed")
			batchNum++

			bTx, bStmt, err = beginBatch()
			if err != nil {
				return totalRows, err
			}
		}
	}

	// Commit the final (partial) batch.
	bStmt.Close()
	if err := bTx.Commit(); err != nil {
		return totalRows, fmt.Errorf("failed to commit final SQLite batch: %w", err)
	}

	if err := rows.Err(); err != nil {
		return totalRows, fmt.Errorf("row iteration error: %w", err)
	}

	return totalRows, nil
}

// postCallback POSTs the SQLite file as multipart/form-data to s.callbackURL.
// Form fields:
//   - "request" — the original ReportQueryMessage serialised as JSON
//   - "file"    — the SQLite file (binary)
func (s *ReportQueryService) postCallback(
	ctx context.Context,
	msg models.ReportQueryMessage,
	sqlitePath string,
	log zerolog.Logger,
) error {
	reqJSON, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal request JSON: %w", err)
	}

	f, err := os.Open(sqlitePath)
	if err != nil {
		return fmt.Errorf("failed to open SQLite file for upload: %w", err)
	}
	defer f.Close()

	var body bytes.Buffer
	mw := multipart.NewWriter(&body)

	// Part 1 — JSON metadata
	if err := mw.WriteField("request", string(reqJSON)); err != nil {
		return fmt.Errorf("failed to write request field: %w", err)
	}

	// Part 2 — SQLite file
	filename := fmt.Sprintf("report_%s.sqlite", sanitiseReportFilename(msg.SessionTitle))
	fw, err := mw.CreateFormFile("file", filename)
	if err != nil {
		return fmt.Errorf("failed to create multipart file field: %w", err)
	}
	if _, err := io.Copy(fw, f); err != nil {
		return fmt.Errorf("failed to copy SQLite into multipart body: %w", err)
	}
	if err := mw.Close(); err != nil {
		return fmt.Errorf("failed to close multipart writer: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.callbackURL, &body)
	if err != nil {
		return fmt.Errorf("failed to build callback HTTP request: %w", err)
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("callback HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("callback returned non-2xx status %d: %s", resp.StatusCode, string(snippet))
	}

	log.Info().Int("httpStatus", resp.StatusCode).Str("callbackUrl", s.callbackURL).Msg("report: callback POST succeeded")
	return nil
}

// resolveConnStringForDB decrypts and normalises the connection string stored
// in the database record, mirroring the logic in QueryValidationService.
func (s *ReportQueryService) resolveConnStringForDB(dbRecord models.Database) (string, error) {
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
		s.logger.Warn().Str("db", dbRecord.Name).Msg("report: decryption failed — using raw connection string")
	}

	return connString, nil
}

// sanitiseReportFilename replaces characters that are unsafe in filenames with
// underscores, and caps the length at 64 characters.
func sanitiseReportFilename(s string) string {
	replacer := strings.NewReplacer(
		"/", "_", "\\", "_", ":", "_", "*", "_",
		"?", "_", `"`, "_", "<", "_", ">", "_",
		"|", "_", " ", "_",
	)
	result := replacer.Replace(s)
	if len(result) > 64 {
		result = result[:64]
	}
	if result == "" {
		result = "report"
	}
	return result
}
