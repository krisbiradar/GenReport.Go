package services

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"genreport/internal/models"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/microsoft/go-mssqldb"
	"github.com/rs/zerolog"
	_ "github.com/sijms/go-ora/v2"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type SchemaMetadata struct {
	Name          string
	Type          string // "table", "view", "collection"
	SchemaText    string // full DDL → stored in full_schema
	EmbeddingText string // slim column:type [FK] text → stored in embedding_text
}

type RoutineMetadata struct {
	Name        string
	Type        string // "procedure", "function"
	RoutineText string
}

type SchemaExtractor interface {
	Extract(ctx context.Context, connString string, logger zerolog.Logger) ([]SchemaMetadata, []RoutineMetadata, error)
}

func GetExtractorForProvider(provider models.DbProvider) (SchemaExtractor, error) {
	switch provider {
	case models.DbProviderNpgSql:
		return &PostgresExtractor{}, nil
	case models.DbProviderSqlClient:
		return &SQLServerExtractor{}, nil
	case models.DbProviderMySqlConnector:
		return &MySQLExtractor{}, nil
	case models.DbProviderOracle:
		return &OracleExtractor{}, nil
	case models.DbProviderMongoClient:
		return &MongoDBExtractor{}, nil
	default:
		return nil, fmt.Errorf("unsupported provider type: %v", provider)
	}
}

// ==============
// PostgreSQL Extractor
// ==============
type PostgresExtractor struct{}

func (e *PostgresExtractor) Extract(ctx context.Context, connString string, logger zerolog.Logger) ([]SchemaMetadata, []RoutineMetadata, error) {
	connString = prepareConnectionString(connString, "postgres")

	db, err := sql.Open("pgx", connString)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open pgx connection: %w", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		return nil, nil, fmt.Errorf("failed to ping postgres: %w", err)
	}

	var schemas []SchemaMetadata
	var routines []RoutineMetadata

	// 1. Tables / Views — compact DDL built from information_schema (no pg_dump)
	tableRows, err := db.QueryContext(ctx, `
		SELECT table_schema, table_name, table_type
		FROM information_schema.tables
		WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
		  AND table_type IN ('BASE TABLE', 'VIEW')
		  AND table_name NOT IN ('__EFMigrationHistory', '__EFMigrationsHistory')
		ORDER BY table_schema, table_name
	`)
	if err != nil {
		logger.Warn().Err(err).Msg("postgres: failed to query tables/views")
	} else {
		defer tableRows.Close()
		for tableRows.Next() {
			var tableSchema, tableName, tableType string
			if err := tableRows.Scan(&tableSchema, &tableName, &tableType); err != nil {
				logger.Warn().Err(err).Msg("postgres: failed to scan table row")
				continue
			}

			t := "table"
			if tableType == "VIEW" {
				t = "view"
			}

			ddl, buildErr := buildPostgresCompactDDL(ctx, db, tableSchema, tableName, t)
			if buildErr != nil {
				logger.Warn().Err(buildErr).Str("table", tableName).Msg("postgres: failed to build compact DDL, skipping")
				continue
			}
			// Views embed their SQL body; tables get a lean column:type [FK] text.
			embedText := ddl
			if t == "table" {
				if et, etErr := buildPostgresTableEmbeddingText(ctx, db, tableSchema, tableName); etErr == nil && et != "" {
					embedText = et
				} else if etErr != nil {
					logger.Warn().Err(etErr).Str("table", tableName).Msg("postgres: failed to build table embedding text, using full DDL")
				}
			}
			schemas = append(schemas, SchemaMetadata{Name: tableName, Type: t, SchemaText: ddl, EmbeddingText: embedText})
		}
		if err := tableRows.Err(); err != nil {
			logger.Warn().Err(err).Msg("postgres: error iterating table rows")
		}
	}

	// 2. Routines (Functions/Procedures)
	// Exclude:
	//  - internal/C language builtins (language = 'internal' or 'c')
	//  - anything installed by an extension (pg_depend deptype='e')
	// These can show up even in 'public' when connecting as a superuser.
	routineRows, err := db.QueryContext(ctx, `
		SELECT p.proname as name,
			CASE WHEN p.prokind = 'p' THEN 'procedure' ELSE 'function' END as type,
			pg_get_functiondef(p.oid) as definition
		FROM pg_proc p
		JOIN pg_language l ON l.oid = p.prolang
		WHERE p.pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
		  AND p.prokind IN ('p', 'f')
		  AND l.lanname NOT IN ('internal', 'c')
		  AND NOT EXISTS (
			  SELECT 1 FROM pg_depend d
			  JOIN pg_extension e ON d.refobjid = e.oid
			  WHERE d.objid = p.oid AND d.deptype = 'e'
		  )
	`)
	if err != nil {
		logger.Warn().Err(err).Msg("postgres: failed to query routines")
	} else {
		defer routineRows.Close()
		for routineRows.Next() {
			var name, rtType string
			var definition sql.NullString
			if err := routineRows.Scan(&name, &rtType, &definition); err != nil {
				logger.Warn().Err(err).Msg("postgres: failed to scan routine row")
				continue
			}
			if !definition.Valid || definition.String == "" {
				logger.Warn().Str("routine", name).Msg("postgres: empty routine definition, skipping")
				continue
			}
			routines = append(routines, RoutineMetadata{Name: name, Type: rtType, RoutineText: definition.String})
		}
		if err := routineRows.Err(); err != nil {
			logger.Warn().Err(err).Msg("postgres: error iterating routine rows")
		}
	}

	logger.Info().Int("schemas", len(schemas)).Int("routines", len(routines)).Msg("postgres: extraction complete")
	return schemas, routines, nil
}

// buildPostgresCompactDDL produces a compact CREATE TABLE/VIEW statement with
// column names + data types, plus any foreign key relationships.
// This is intentionally small — it is stored as embedding_text for vector search.
func buildPostgresCompactDDL(ctx context.Context, db *sql.DB, schema, table, objType string) (string, error) {
	var sb strings.Builder

	if objType == "view" {
		// For views: use pg_get_viewdef for the definition
		var viewDef sql.NullString
		row := db.QueryRowContext(ctx,
			`SELECT pg_get_viewdef(format('%I.%I', $1, $2)::regclass, true)`,
			schema, table,
		)
		if err := row.Scan(&viewDef); err != nil || !viewDef.Valid {
			// Fallback: just record it as an empty view
			fmt.Fprintf(&sb, "CREATE VIEW %s.%s AS <definition unavailable>;", schema, table)
			return sb.String(), nil
		}
		fmt.Fprintf(&sb, "CREATE VIEW %s.%s AS\n%s", schema, table, strings.TrimSpace(viewDef.String))
		return sb.String(), nil
	}

	// ── Columns ────────────────────────────────────────────────────────────────
	colRows, err := db.QueryContext(ctx, `
		SELECT column_name,
			   CASE
				 WHEN data_type = 'character varying' THEN 'varchar(' || COALESCE(character_maximum_length::text, 'max') || ')'
				 WHEN data_type = 'character'         THEN 'char(' || COALESCE(character_maximum_length::text, 'max') || ')'
				 WHEN data_type = 'numeric'           THEN 'numeric(' || COALESCE(numeric_precision::text,'?') || ',' || COALESCE(numeric_scale::text,'?') || ')'
				 ELSE data_type
			   END AS col_type,
			   is_nullable
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position
	`, schema, table)
	if err != nil {
		return "", fmt.Errorf("failed to query columns: %w", err)
	}
	defer colRows.Close()

	var colDefs []string
	for colRows.Next() {
		var colName, colType, isNullable string
		if err := colRows.Scan(&colName, &colType, &isNullable); err != nil {
			continue
		}
		nullStr := ""
		if isNullable == "NO" {
			nullStr = " NOT NULL"
		}
		colDefs = append(colDefs, fmt.Sprintf("  %s %s%s", colName, colType, nullStr))
	}
	if err := colRows.Err(); err != nil {
		return "", fmt.Errorf("column row error: %w", err)
	}
	if len(colDefs) == 0 {
		return "", fmt.Errorf("no columns found for %s.%s", schema, table)
	}

	fmt.Fprintf(&sb, "CREATE TABLE %s.%s (\n%s\n);", schema, table, strings.Join(colDefs, ",\n"))

	// ── Foreign Keys ───────────────────────────────────────────────────────────
	fkRows, err := db.QueryContext(ctx, `
		SELECT
			kcu.column_name,
			ccu.table_schema AS foreign_schema,
			ccu.table_name   AS foreign_table,
			ccu.column_name  AS foreign_column
		FROM information_schema.table_constraints     AS tc
		JOIN information_schema.key_column_usage      AS kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
		JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name
		WHERE tc.constraint_type = 'FOREIGN KEY'
		  AND tc.table_schema = $1
		  AND tc.table_name   = $2
		ORDER BY kcu.column_name
	`, schema, table)
	if err == nil {
		defer fkRows.Close()
		var fkLines []string
		for fkRows.Next() {
			var col, fSchema, fTable, fCol string
			if err := fkRows.Scan(&col, &fSchema, &fTable, &fCol); err != nil {
				continue
			}
			fkLines = append(fkLines, fmt.Sprintf("  -- FK: %s -> %s.%s(%s)", col, fSchema, fTable, fCol))
		}
		if len(fkLines) > 0 {
			sb.WriteString("\n-- Foreign keys:\n")
			sb.WriteString(strings.Join(fkLines, "\n"))
		}
	}

	return sb.String(), nil
}

// ==============
// SQL Server Extractor
// ==============
type SQLServerExtractor struct{}

func (e *SQLServerExtractor) Extract(ctx context.Context, connString string, logger zerolog.Logger) ([]SchemaMetadata, []RoutineMetadata, error) {
	connString = prepareConnectionString(connString, "sqlserver")
	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open sqlserver connection: %w", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		return nil, nil, fmt.Errorf("failed to ping sqlserver: %w", err)
	}

	var schemas []SchemaMetadata
	var routines []RoutineMetadata

	// 1a. Tables — single query: columns + FK refs via sys.foreign_key_columns join.
	tableRows, err := db.QueryContext(ctx, `
		SELECT t.name,
		       c.name                    AS col_name,
		       type_name(c.user_type_id) AS data_type,
		       ISNULL(rt.name, '')       AS ref_table,
		       ISNULL(rc.name, '')       AS ref_col
		FROM sys.tables t
		JOIN sys.columns c ON c.object_id = t.object_id
		LEFT JOIN sys.foreign_key_columns fkc
		    ON fkc.parent_object_id = c.object_id AND fkc.parent_column_id = c.column_id
		LEFT JOIN sys.tables rt ON rt.object_id = fkc.referenced_object_id
		LEFT JOIN sys.columns rc
		    ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id
		WHERE t.name NOT IN ('__EFMigrationHistory')
		ORDER BY t.name, c.column_id
	`)
	if err != nil {
		logger.Warn().Err(err).Msg("sqlserver: failed to query tables")
	} else {
		defer tableRows.Close()
		type ssCol struct{ name, dataType, refTable, refCol string }
		tableColMap := make(map[string][]ssCol)
		for tableRows.Next() {
			var tableName, colName, dataType, refTable, refCol string
			if err := tableRows.Scan(&tableName, &colName, &dataType, &refTable, &refCol); err != nil {
				logger.Warn().Err(err).Msg("sqlserver: failed to scan table row")
				continue
			}
			tableColMap[tableName] = append(tableColMap[tableName], ssCol{colName, dataType, refTable, refCol})
		}
		if err := tableRows.Err(); err != nil {
			logger.Warn().Err(err).Msg("sqlserver: error iterating table rows")
		}
		for name, cols := range tableColMap {
			schemaCols := make([]string, 0, len(cols))
			embedLines := make([]string, 0, len(cols))
			for _, col := range cols {
				schemaCols = append(schemaCols, fmt.Sprintf("[%s] %s", col.name, col.dataType))
				line := col.name + ": " + col.dataType
				if col.refTable != "" {
					line += fmt.Sprintf(" -- FK: %s.%s", col.refTable, col.refCol)
				}
				embedLines = append(embedLines, line)
			}
			schemaText := fmt.Sprintf("CREATE TABLE [%s] (\n  %s\n);", name, strings.Join(schemaCols, ",\n  "))
			embedText := "table: " + name + "\n" + strings.Join(embedLines, "\n")
			schemas = append(schemas, SchemaMetadata{Name: name, Type: "table", SchemaText: schemaText, EmbeddingText: embedText})
		}
	}

	// 1b. Views (Using OBJECT_DEFINITION)
	viewRows, err := db.QueryContext(ctx, `
		SELECT name, OBJECT_DEFINITION(object_id)
		FROM sys.views
	`)
	if err != nil {
		logger.Warn().Err(err).Msg("sqlserver: failed to query views")
	} else {
		defer viewRows.Close()
		for viewRows.Next() {
			var viewName string
			var definition sql.NullString
			if err := viewRows.Scan(&viewName, &definition); err != nil {
				logger.Warn().Err(err).Msg("sqlserver: failed to scan view row")
				continue
			}
			if !definition.Valid {
				logger.Warn().Str("view", viewName).Msg("sqlserver: null definition for view, skipping")
				continue
			}
			schemas = append(schemas, SchemaMetadata{Name: viewName, Type: "view", SchemaText: definition.String, EmbeddingText: definition.String})
		}
		if err := viewRows.Err(); err != nil {
			logger.Warn().Err(err).Msg("sqlserver: error iterating view rows")
		}
	}

	// 2. Routines (Using OBJECT_DEFINITION)
	routineRows, err := db.QueryContext(ctx, `
		SELECT name, 
			CASE WHEN type = 'P' THEN 'procedure' ELSE 'function' END,
			OBJECT_DEFINITION(object_id)
		FROM sys.objects 
		WHERE type IN ('P', 'FN', 'IF', 'TF')
		  AND is_ms_shipped = 0
	`)
	if err != nil {
		logger.Warn().Err(err).Msg("sqlserver: failed to query routines")
	} else {
		defer routineRows.Close()
		for routineRows.Next() {
			var name, rtType string
			var definition sql.NullString
			if err := routineRows.Scan(&name, &rtType, &definition); err != nil {
				logger.Warn().Err(err).Msg("sqlserver: failed to scan routine row")
				continue
			}
			if !definition.Valid {
				logger.Warn().Str("routine", name).Msg("sqlserver: null definition for routine, skipping")
				continue
			}
			routines = append(routines, RoutineMetadata{Name: name, Type: rtType, RoutineText: definition.String})
		}
		if err := routineRows.Err(); err != nil {
			logger.Warn().Err(err).Msg("sqlserver: error iterating routine rows")
		}
	}

	logger.Info().Int("schemas", len(schemas)).Int("routines", len(routines)).Msg("sqlserver: extraction complete")
	return schemas, routines, nil
}

// ==============
// MySQL Extractor
// ==============
type MySQLExtractor struct{}

func (e *MySQLExtractor) Extract(ctx context.Context, connString string, logger zerolog.Logger) ([]SchemaMetadata, []RoutineMetadata, error) {
	connString = prepareConnectionString(connString, "mysql")
	db, err := sql.Open("mysql", connString)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open mysql connection: %w", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		return nil, nil, fmt.Errorf("failed to ping mysql: %w", err)
	}

	var schemas []SchemaMetadata
	var routines []RoutineMetadata

	// 1. Tables/Views
	tableRows, err := db.QueryContext(ctx, "SHOW FULL TABLES WHERE Table_type = 'BASE TABLE' OR Table_type = 'VIEW'")
	if err != nil {
		logger.Warn().Err(err).Msg("mysql: failed to query tables/views")
	} else {
		defer tableRows.Close()
		for tableRows.Next() {
			var tableName, tableType string
			if err := tableRows.Scan(&tableName, &tableType); err != nil {
				logger.Warn().Err(err).Msg("mysql: failed to scan table row")
				continue
			}
			t := "table"
			query := fmt.Sprintf("SHOW CREATE TABLE `%s`", tableName)
			if tableType == "VIEW" {
				t = "view"
				query = fmt.Sprintf("SHOW CREATE VIEW `%s`", tableName)
			}

			ddl, err := getMySQLDDL(ctx, db, query)
			if err != nil {
				logger.Warn().Err(err).Str("table", tableName).Msg("mysql: failed to get DDL for object, skipping")
				continue
			}
			if ddl == "" {
				logger.Warn().Str("table", tableName).Msg("mysql: empty DDL for object, skipping")
				continue
			}
			embedText := ddl // views embed their SQL body
			if t == "table" {
				if et, etErr := buildMySQLTableEmbeddingText(ctx, db, tableName); etErr == nil && et != "" {
					embedText = et
				} else if etErr != nil {
					logger.Warn().Err(etErr).Str("table", tableName).Msg("mysql: failed to build table embedding text, using full DDL")
				}
			}
			schemas = append(schemas, SchemaMetadata{Name: tableName, Type: t, SchemaText: ddl, EmbeddingText: embedText})
		}
		if err := tableRows.Err(); err != nil {
			logger.Warn().Err(err).Msg("mysql: error iterating table rows")
		}
	}

	// 2. Routines (Functions/Procedures)
	// information_schema.routines only contains user-defined routines, but if
	// root owns the DB we still exclude the mysql/sys/information_schema schemas.
	routineRows, err := db.QueryContext(ctx, `
		SELECT routine_name, routine_type 
		FROM information_schema.routines 
		WHERE routine_schema = DATABASE()
		  AND routine_schema NOT IN ('mysql', 'sys', 'information_schema', 'performance_schema')
	`)
	if err != nil {
		logger.Warn().Err(err).Msg("mysql: failed to query routines")
	} else {
		defer routineRows.Close()
		for routineRows.Next() {
			var name, rtType string
			if err := routineRows.Scan(&name, &rtType); err != nil {
				logger.Warn().Err(err).Msg("mysql: failed to scan routine row")
				continue
			}
			rtTypeLower := strings.ToLower(rtType)
			query := fmt.Sprintf("SHOW CREATE %s `%s`", strings.ToUpper(rtType), name)

			ddl, err := getMySQLDDL(ctx, db, query)
			if err != nil {
				logger.Warn().Err(err).Str("routine", name).Msg("mysql: failed to get DDL for routine, skipping")
				continue
			}
			if ddl == "" {
				logger.Warn().Str("routine", name).Msg("mysql: empty DDL for routine, skipping")
				continue
			}
			routines = append(routines, RoutineMetadata{Name: name, Type: rtTypeLower, RoutineText: ddl})
		}
		if err := routineRows.Err(); err != nil {
			logger.Warn().Err(err).Msg("mysql: error iterating routine rows")
		}
	}

	logger.Info().Int("schemas", len(schemas)).Int("routines", len(routines)).Msg("mysql: extraction complete")
	return schemas, routines, nil
}

// Helper to pull the "Create ..." statement block natively.
func getMySQLDDL(ctx context.Context, db *sql.DB, query string) (string, error) {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	if !rows.Next() {
		return "", fmt.Errorf("no result")
	}
	cols, err := rows.Columns()
	if err != nil {
		return "", err
	}
	values := make([]interface{}, len(cols))
	valuePtrs := make([]interface{}, len(cols))
	for i := range values {
		valuePtrs[i] = &values[i]
	}
	if err := rows.Scan(valuePtrs...); err != nil {
		return "", err
	}

	for i, colName := range cols {
		cn := strings.ToLower(colName)
		if strings.HasPrefix(cn, "create ") {
			if b, ok := values[i].([]byte); ok {
				return string(b), nil
			}
			if s, ok := values[i].(string); ok {
				return s, nil
			}
		}
	}
	return "", fmt.Errorf("ddl column not found")
}

// ==============
// Oracle Extractor
// ==============
type OracleExtractor struct{}

func (e *OracleExtractor) Extract(ctx context.Context, connString string, logger zerolog.Logger) ([]SchemaMetadata, []RoutineMetadata, error) {
	connString = prepareConnectionString(connString, "oracle")
	db, err := sql.Open("oracle", connString)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open oracle connection: %w", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		return nil, nil, fmt.Errorf("failed to ping oracle: %w", err)
	}

	// Need to initialize dbms_metadata session params occasionally
	_, _ = db.ExecContext(ctx, "BEGIN DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'SQLTERMINATOR',true); END;")

	var schemas []SchemaMetadata
	var routines []RoutineMetadata

	// 1. Tables/Views
	tableRows, err := db.QueryContext(ctx, `
		SELECT object_name, object_type 
		FROM user_objects
		WHERE object_type IN ('TABLE', 'VIEW')
	`)
	if err != nil {
		logger.Warn().Err(err).Msg("oracle: failed to query tables/views")
	} else {
		defer tableRows.Close()
		for tableRows.Next() {
			var objName, objType string
			if err := tableRows.Scan(&objName, &objType); err != nil {
				logger.Warn().Err(err).Msg("oracle: failed to scan table row")
				continue
			}
			var ddl sql.NullString
			ddlRow := db.QueryRowContext(ctx, "SELECT DBMS_METADATA.GET_DDL(:1, :2) FROM DUAL", objType, objName)
			if err := ddlRow.Scan(&ddl); err != nil {
				logger.Warn().Err(err).Str("object", objName).Msg("oracle: failed to get DDL, skipping")
				continue
			}
			if !ddl.Valid || ddl.String == "" {
				logger.Warn().Str("object", objName).Msg("oracle: empty DDL, skipping")
				continue
			}
			objTypeLower := strings.ToLower(objType)
			embedText := ddl.String // views embed their SQL body
			if objTypeLower == "table" {
				if et, etErr := buildOracleTableEmbeddingText(ctx, db, objName); etErr == nil && et != "" {
					embedText = et
				} else if etErr != nil {
					logger.Warn().Err(etErr).Str("object", objName).Msg("oracle: failed to build table embedding text, using full DDL")
				}
			}
			schemas = append(schemas, SchemaMetadata{Name: objName, Type: objTypeLower, SchemaText: ddl.String, EmbeddingText: embedText})
		}
		if err := tableRows.Err(); err != nil {
			logger.Warn().Err(err).Msg("oracle: error iterating table rows")
		}
	}

	// 2. Routines (Functions/Procedures)
	// Filter out Oracle-shipped packages by well-known name prefixes.
	// user_objects is user-scoped but SYS/SYSTEM users own system objects too.
	routineRows, err := db.QueryContext(ctx, `
		SELECT object_name, object_type 
		FROM user_objects
		WHERE object_type IN ('PROCEDURE', 'FUNCTION')
		  AND object_name NOT LIKE 'DBMS_%'
		  AND object_name NOT LIKE 'UTL_%'
		  AND object_name NOT LIKE 'SYS_%'
		  AND object_name NOT LIKE 'SDO_%'
		  AND object_name NOT LIKE 'OWA_%'
		  AND object_name NOT LIKE 'HTF_%'
		  AND object_name NOT LIKE 'HTP_%'
		  AND object_name NOT LIKE 'APEX_%'
		  AND object_name NOT LIKE 'WWV_%'
	`)
	if err != nil {
		logger.Warn().Err(err).Msg("oracle: failed to query routines")
	} else {
		defer routineRows.Close()
		for routineRows.Next() {
			var objName, objType string
			if err := routineRows.Scan(&objName, &objType); err != nil {
				logger.Warn().Err(err).Msg("oracle: failed to scan routine row")
				continue
			}
			var ddl sql.NullString
			ddlRow := db.QueryRowContext(ctx, "SELECT DBMS_METADATA.GET_DDL(:1, :2) FROM DUAL", objType, objName)
			if err := ddlRow.Scan(&ddl); err != nil {
				logger.Warn().Err(err).Str("routine", objName).Msg("oracle: failed to get DDL, skipping")
				continue
			}
			if !ddl.Valid || ddl.String == "" {
				logger.Warn().Str("routine", objName).Msg("oracle: empty DDL, skipping")
				continue
			}
			routines = append(routines, RoutineMetadata{Name: objName, Type: strings.ToLower(objType), RoutineText: ddl.String})
		}
		if err := routineRows.Err(); err != nil {
			logger.Warn().Err(err).Msg("oracle: error iterating routine rows")
		}
	}

	logger.Info().Int("schemas", len(schemas)).Int("routines", len(routines)).Msg("oracle: extraction complete")
	return schemas, routines, nil
}

// ==============
// MongoDB Extractor
// ==============
type MongoDBExtractor struct{}

func (e *MongoDBExtractor) Extract(ctx context.Context, connString string, logger zerolog.Logger) ([]SchemaMetadata, []RoutineMetadata, error) {
	connString = prepareConnectionString(connString, "mongodb")
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(connString))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open mongodb connection: %w", err)
	}
	defer client.Disconnect(ctx)

	var schemas []SchemaMetadata

	dbNames, err := client.ListDatabaseNames(ctx, bson.M{})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to list databases: %w", err)
	}

	for _, dbName := range dbNames {
		if dbName == "admin" || dbName == "local" || dbName == "config" {
			continue
		}
		collections, err := client.Database(dbName).ListCollectionNames(ctx, bson.M{})
		if err != nil {
			logger.Warn().Err(err).Str("db", dbName).Msg("mongodb: failed to list collections for database, skipping")
			continue
		}
		for _, colName := range collections {
			text := fmt.Sprintf("Collection: %s\nDatabase: %s\nType: NoSQL Document Store", colName, dbName)
			schemas = append(schemas, SchemaMetadata{
				Name:          colName,
				Type:          "collection",
				SchemaText:    text,
				EmbeddingText: text,
			})
		}
	}

	logger.Info().Int("schemas", len(schemas)).Msg("mongodb: extraction complete")
	return schemas, nil, nil
}

// prepareConnectionString detects C# ADO.NET-style connection strings
// (e.g. "Server=host;Database=db;User Id=user;Password=pass;")
// and converts them into the DSN format expected by each Go SQL driver.
// If the string is already in a recognised Go format it is returned as-is.
func prepareConnectionString(cs string, driver string) string {
	cs = strings.TrimSpace(cs)

	// If it already looks like a URL or a Go key=value DSN, pass it through.
	if strings.Contains(cs, "://") ||
		strings.Contains(cs, "host=") ||
		strings.Contains(cs, "Host=") {
		return cs
	}

	// Try to parse as a C# ADO.NET semicolon-separated key=value string.
	// Keys are case-insensitive; we normalise to lowercase.
	parsed := parseAdoNet(cs)
	if len(parsed) == 0 {
		return cs
	}

	switch driver {
	case "postgres":
		// pgx / lib/pq DSN: "host=X port=Y dbname=Z user=U password=P sslmode=disable"
		host := coalesce(parsed, "server", "host", "data source")
		port := coalesce(parsed, "port")
		dbname := coalesce(parsed, "database", "initial catalog")
		user := coalesce(parsed, "user id", "uid", "user")
		password := coalesce(parsed, "password", "pwd")
		sslmode := coalesce(parsed, "sslmode")
		if sslmode == "" {
			sslmode = "disable"
		}
		dsn := fmt.Sprintf("host=%s dbname=%s user=%s password=%s sslmode=%s",
			host, dbname, user, password, sslmode)
		if port != "" {
			dsn += " port=" + port
		}
		return dsn

	case "mysql":
		// go-sql-driver/mysql DSN: "user:pass@tcp(host:port)/dbname?parseTime=true"
		host := coalesce(parsed, "server", "host", "data source")
		port := coalesce(parsed, "port")
		if port == "" {
			port = "3306"
		}
		dbname := coalesce(parsed, "database", "initial catalog")
		user := coalesce(parsed, "user id", "uid", "user")
		password := coalesce(parsed, "password", "pwd")
		return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
			user, password, host, port, dbname)

	case "sqlserver":
		// go-mssqldb already accepts ADO.NET-style strings, but it uses
		// "server" / "user id" keys natively — just normalise and return.
		// Convert "Data Source" → "server" if present.
		if ds, ok := parsed["data source"]; ok {
			parsed["server"] = ds
		}
		var parts []string
		for k, v := range parsed {
			parts = append(parts, fmt.Sprintf("%s=%s", k, v))
		}
		return strings.Join(parts, ";")

	case "oracle":
		// go-ora DSN: "oracle://user:pass@host:port/service"
		host := coalesce(parsed, "data source", "server", "host")
		port := coalesce(parsed, "port")
		if port == "" {
			port = "1521"
		}
		user := coalesce(parsed, "user id", "uid", "user")
		password := coalesce(parsed, "password", "pwd")
		service := coalesce(parsed, "database", "initial catalog", "service name")
		return fmt.Sprintf("oracle://%s:%s@%s:%s/%s",
			user, password, host, port, service)
	}

	return cs
}

// parseAdoNet splits a semicolon-separated "Key=Value;Key=Value" string into
// a lowercase-keyed map.
func parseAdoNet(cs string) map[string]string {
	m := make(map[string]string)
	for _, part := range strings.Split(cs, ";") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		idx := strings.IndexByte(part, '=')
		if idx < 0 {
			continue
		}
		k := strings.ToLower(strings.TrimSpace(part[:idx]))
		v := strings.TrimSpace(part[idx+1:])
		m[k] = v
	}
	return m
}

// coalesce returns the first non-empty value found for any of the given keys
// in the map (all lookups are already lowercase).
func coalesce(m map[string]string, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[strings.ToLower(k)]; ok && v != "" {
			return v
		}
	}
	return ""
}


// StripRoutineForEmbedding returns a compact, embedding-friendly representation
// of a routine definition. It scans the text line-by-line and stops at the
// first standalone body-start keyword (AS, BEGIN, IS) or a Postgres-style
// dollar-quote opener (AS $$), keeping only the signature section.
// The full body is preserved separately in FullSchema.
func StripRoutineForEmbedding(fullText string) string {
	if fullText == "" {
		return ""
	}
	text := strings.TrimSpace(fullText)
	lines := strings.Split(text, "\n")

	var sigLines []string
	for _, line := range lines {
		upper := strings.ToUpper(strings.TrimSpace(line))

		// Standalone body-start keywords on their own line
		if upper == "AS" || upper == "BEGIN" || upper == "IS" {
			break
		}
		// Postgres dollar-quoted body: "AS $$" or "AS $tag$"
		if strings.HasPrefix(upper, "AS $$") || strings.HasPrefix(upper, "AS $") {
			break
		}
		// Inline body start at end of line: ") AS" or ") IS"
		if strings.HasSuffix(upper, ") AS") || strings.HasSuffix(upper, ") IS") {
			sigLines = append(sigLines, line)
			break
		}

		sigLines = append(sigLines, line)
	}

	result := strings.TrimSpace(strings.Join(sigLines, "\n"))
	if result == "" {
		// Fallback: first 500 chars of original so we always have something
		if len(text) > 500 {
			return text[:500]
		}
		return text
	}
	// Cap at 2000 chars — plenty for a signature, keeps token cost low
	if len(result) > 2000 {
		result = result[:2000]
	}
	return result
}

// ─────────────────────────────────────────────────────────────────────────────
// Per-provider table embedding text builders
//
// Each function queries the provider's system catalogs directly to produce:
//   table: <name>
//   col_name: data_type
//   col_name: data_type -- FK: referenced_table.referenced_col
//
// This is far more accurate than regex-parsing DDL strings.
// ─────────────────────────────────────────────────────────────────────────────

// buildPostgresTableEmbeddingText queries information_schema for column types
// and FK relationships for a single Postgres table.
func buildPostgresTableEmbeddingText(ctx context.Context, db *sql.DB, schema, table string) (string, error) {
	// ── FK map: column → "FK: ref_table.ref_col" ─────────────────────────────
	fkMap := make(map[string]string)
	fkRows, err := db.QueryContext(ctx, `
		SELECT kcu.column_name, ccu.table_name, ccu.column_name
		FROM information_schema.table_constraints     AS tc
		JOIN information_schema.key_column_usage      AS kcu
		     ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
		JOIN information_schema.constraint_column_usage AS ccu
		     ON ccu.constraint_name = tc.constraint_name
		WHERE tc.constraint_type = 'FOREIGN KEY'
		  AND tc.table_schema = $1
		  AND tc.table_name   = $2
	`, schema, table)
	if err == nil {
		defer fkRows.Close()
		for fkRows.Next() {
			var col, refTable, refCol string
			if err := fkRows.Scan(&col, &refTable, &refCol); err == nil {
				fkMap[col] = fmt.Sprintf("FK: %s.%s", refTable, refCol)
			}
		}
	}

	// ── Columns ───────────────────────────────────────────────────────────────
	colRows, err := db.QueryContext(ctx, `
		SELECT column_name,
		       CASE
				 WHEN data_type = 'character varying' THEN 'varchar(' || COALESCE(character_maximum_length::text, 'max') || ')'
				 WHEN data_type = 'character'         THEN 'char(' || COALESCE(character_maximum_length::text, 'max') || ')'
				 WHEN data_type = 'numeric'           THEN 'numeric(' || COALESCE(numeric_precision::text,'?') || ',' || COALESCE(numeric_scale::text,'?') || ')'
				 ELSE data_type
			   END AS col_type
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position
	`, schema, table)
	if err != nil {
		return "", fmt.Errorf("postgres: failed to query columns for table embedding %s.%s: %w", schema, table, err)
	}
	defer colRows.Close()

	var sb strings.Builder
	sb.WriteString("table: ")
	sb.WriteString(table)
	sb.WriteString("\n")
	for colRows.Next() {
		var colName, colType string
		if err := colRows.Scan(&colName, &colType); err != nil {
			continue
		}
		sb.WriteString(colName)
		sb.WriteString(": ")
		sb.WriteString(colType)
		if fk, ok := fkMap[colName]; ok {
			sb.WriteString(" -- ")
			sb.WriteString(fk)
		}
		sb.WriteString("\n")
	}
	return strings.TrimSpace(sb.String()), nil
}

// buildMySQLTableEmbeddingText queries information_schema.COLUMNS and
// KEY_COLUMN_USAGE for a single MySQL/MariaDB table.
func buildMySQLTableEmbeddingText(ctx context.Context, db *sql.DB, tableName string) (string, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT
		    c.COLUMN_NAME,
		    c.COLUMN_TYPE,
		    COALESCE(kcu.REFERENCED_TABLE_NAME,  '') AS ref_table,
		    COALESCE(kcu.REFERENCED_COLUMN_NAME, '') AS ref_col
		FROM information_schema.COLUMNS c
		LEFT JOIN information_schema.KEY_COLUMN_USAGE kcu
		    ON  kcu.TABLE_SCHEMA  = c.TABLE_SCHEMA
		    AND kcu.TABLE_NAME    = c.TABLE_NAME
		    AND kcu.COLUMN_NAME   = c.COLUMN_NAME
		    AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
		WHERE c.TABLE_SCHEMA = DATABASE() AND c.TABLE_NAME = ?
		ORDER BY c.ORDINAL_POSITION
	`, tableName)
	if err != nil {
		return "", fmt.Errorf("mysql: failed to query columns+FKs for %s: %w", tableName, err)
	}
	defer rows.Close()

	var sb strings.Builder
	sb.WriteString("table: ")
	sb.WriteString(tableName)
	sb.WriteString("\n")
	for rows.Next() {
		var colName, colType, refTable, refCol string
		if err := rows.Scan(&colName, &colType, &refTable, &refCol); err != nil {
			continue
		}
		sb.WriteString(colName)
		sb.WriteString(": ")
		sb.WriteString(colType)
		if refTable != "" {
			sb.WriteString(fmt.Sprintf(" -- FK: %s.%s", refTable, refCol))
		}
		sb.WriteString("\n")
	}
	return strings.TrimSpace(sb.String()), nil
}

// buildOracleTableEmbeddingText queries user_tab_columns + user_constraints
// to produce the lean embedding text for a single Oracle table.
func buildOracleTableEmbeddingText(ctx context.Context, db *sql.DB, tableName string) (string, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT
		    tc.COLUMN_NAME,
		    tc.DATA_TYPE ||
		        CASE
		            WHEN tc.DATA_PRECISION IS NOT NULL
		                THEN '(' || tc.DATA_PRECISION || ',' || NVL(tc.DATA_SCALE, 0) || ')'
		            WHEN tc.CHAR_LENGTH > 0
		                THEN '(' || tc.CHAR_LENGTH || ')'
		            ELSE ''
		        END AS data_type,
		    NVL(fk.ref_table, '') AS ref_table,
		    NVL(fk.ref_col,   '') AS ref_col
		FROM user_tab_columns tc
		LEFT JOIN (
		    SELECT ucc.table_name, ucc.column_name,
		           ucc2.table_name  AS ref_table,
		           ucc2.column_name AS ref_col
		    FROM user_constraints uc
		    JOIN user_cons_columns ucc  ON uc.constraint_name   = ucc.constraint_name
		    JOIN user_cons_columns ucc2 ON uc.r_constraint_name = ucc2.constraint_name
		    WHERE uc.constraint_type = 'R'
		) fk ON fk.table_name = tc.table_name AND fk.column_name = tc.column_name
		WHERE tc.table_name = :1
		ORDER BY tc.column_id
	`, tableName)
	if err != nil {
		return "", fmt.Errorf("oracle: failed to query columns+FKs for %s: %w", tableName, err)
	}
	defer rows.Close()

	var sb strings.Builder
	sb.WriteString("table: ")
	sb.WriteString(tableName)
	sb.WriteString("\n")
	for rows.Next() {
		var colName, dataType, refTable, refCol string
		if err := rows.Scan(&colName, &dataType, &refTable, &refCol); err != nil {
			continue
		}
		sb.WriteString(colName)
		sb.WriteString(": ")
		sb.WriteString(dataType)
		if refTable != "" {
			sb.WriteString(fmt.Sprintf(" -- FK: %s.%s", refTable, refCol))
		}
		sb.WriteString("\n")
	}
	return strings.TrimSpace(sb.String()), nil
}
