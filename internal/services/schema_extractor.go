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
	_ "github.com/sijms/go-ora/v2"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type SchemaMetadata struct {
	Name       string
	Type       string // "table", "view", "collection"
	SchemaText string
}

type RoutineMetadata struct {
	Name        string
	Type        string // "procedure", "function"
	RoutineText string
}

type SchemaExtractor interface {
	Extract(ctx context.Context, connString string) ([]SchemaMetadata, []RoutineMetadata, error)
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
// Tables  → full column DDL (type with length/precision, NOT NULL, DEFAULT) + PRIMARY KEY constraint
// Views   → actual view body via pg_get_viewdef
// Routines→ full definition via pg_get_functiondef (unchanged)
type PostgresExtractor struct{}

func (e *PostgresExtractor) Extract(ctx context.Context, connString string) ([]SchemaMetadata, []RoutineMetadata, error) {
	connString = prepareConnectionString(connString, "postgres")
	db, err := sql.Open("pgx", connString)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open pgx connection: %w", err)
	}
	defer db.Close()

	var schemas []SchemaMetadata
	var routines []RoutineMetadata

	// ── 1. Tables: columns with full type + nullability + defaults ─────────────
	type pgCol struct {
		colType    string
		isNullable bool
		defaultVal sql.NullString
	}
	tableColMap := make(map[string][]string) // table → ordered col names
	tableColDef := make(map[string][]pgCol)  // table → col metadata

	colRows, err := db.QueryContext(ctx, `
		SELECT
			c.table_name,
			c.column_name,
			CASE
				WHEN c.character_maximum_length IS NOT NULL
					THEN c.udt_name || '(' || c.character_maximum_length || ')'
				WHEN c.data_type IN ('numeric','decimal') AND c.numeric_precision IS NOT NULL
					THEN c.udt_name || '(' || c.numeric_precision || ',' || c.numeric_scale || ')'
				ELSE c.udt_name
			END AS col_type,
			c.is_nullable,
			c.column_default
		FROM information_schema.columns c
		JOIN information_schema.tables t
			ON c.table_name = t.table_name AND c.table_schema = t.table_schema
		WHERE c.table_schema NOT IN ('information_schema', 'pg_catalog')
		  AND t.table_type = 'BASE TABLE'
		ORDER BY c.table_name, c.ordinal_position
	`)
	if err == nil {
		defer colRows.Close()
		for colRows.Next() {
			var tbl, colName, colType, isNullable string
			var colDefault sql.NullString
			if err := colRows.Scan(&tbl, &colName, &colType, &isNullable, &colDefault); err == nil {
				tableColMap[tbl] = append(tableColMap[tbl], colName)
				tableColDef[tbl] = append(tableColDef[tbl], pgCol{
					colType:    colType,
					isNullable: isNullable == "YES",
					defaultVal: colDefault,
				})
			}
		}
	}

	// ── 2. Tables: PRIMARY KEY constraints ────────────────────────────────────
	pkMap := make(map[string][]string)
	pkRows, err := db.QueryContext(ctx, `
		SELECT tc.table_name, kcu.column_name
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
			ON tc.constraint_name = kcu.constraint_name
			AND tc.table_schema   = kcu.table_schema
		WHERE tc.constraint_type = 'PRIMARY KEY'
		  AND tc.table_schema NOT IN ('information_schema', 'pg_catalog')
		ORDER BY tc.table_name, kcu.ordinal_position
	`)
	if err == nil {
		defer pkRows.Close()
		for pkRows.Next() {
			var tbl, col string
			if err := pkRows.Scan(&tbl, &col); err == nil {
				pkMap[tbl] = append(pkMap[tbl], col)
			}
		}
	}

	// Build CREATE TABLE DDL
	for tbl, colNames := range tableColMap {
		cols := tableColDef[tbl]
		var parts []string
		for i, col := range cols {
			line := fmt.Sprintf("  %s %s", colNames[i], col.colType)
			if !col.isNullable {
				line += " NOT NULL"
			}
			if col.defaultVal.Valid {
				line += " DEFAULT " + col.defaultVal.String
			}
			parts = append(parts, line)
		}
		if pks, ok := pkMap[tbl]; ok && len(pks) > 0 {
			parts = append(parts, fmt.Sprintf("  CONSTRAINT pk_%s PRIMARY KEY (%s)",
				strings.ToLower(tbl), strings.Join(pks, ", ")))
		}
		script := fmt.Sprintf("CREATE TABLE %s (\n%s\n);", tbl, strings.Join(parts, ",\n"))
		schemas = append(schemas, SchemaMetadata{Name: tbl, Type: "table", SchemaText: script})
	}

	// ── 3. Views: actual view body via pg_views ────────────────────────────────
	viewRows, err := db.QueryContext(ctx, `
		SELECT viewname, definition
		FROM pg_views
		WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
	`)
	if err == nil {
		defer viewRows.Close()
		for viewRows.Next() {
			var name, def string
			if err := viewRows.Scan(&name, &def); err == nil {
				script := fmt.Sprintf("CREATE VIEW %s AS\n%s", name, strings.TrimSpace(def))
				schemas = append(schemas, SchemaMetadata{Name: name, Type: "view", SchemaText: script})
			}
		}
	}

	// ── 4. Routines: full definition via pg_get_functiondef ───────────────────
	routineRows, err := db.QueryContext(ctx, `
		SELECT proname,
			CASE WHEN prokind = 'p' THEN 'procedure' ELSE 'function' END,
			pg_get_functiondef(oid)
		FROM pg_proc
		WHERE pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
		  AND prokind IN ('p', 'f')
	`)
	if err == nil {
		defer routineRows.Close()
		for routineRows.Next() {
			var name, rtType string
			var definition sql.NullString
			if err := routineRows.Scan(&name, &rtType, &definition); err == nil && definition.Valid {
				routines = append(routines, RoutineMetadata{Name: name, Type: rtType, RoutineText: definition.String})
			}
		}
	}

	return schemas, routines, nil
}

// ==============
// SQL Server Extractor
// ==============
// Tables  → full column DDL (type with length/precision, IDENTITY, NOT NULL, DEFAULT) + PRIMARY KEY constraint
// Views   → actual CREATE VIEW text via sys.sql_modules
// Routines→ full CREATE PROCEDURE/FUNCTION text via sys.sql_modules (unchanged)
type SQLServerExtractor struct{}

func (e *SQLServerExtractor) Extract(ctx context.Context, connString string) ([]SchemaMetadata, []RoutineMetadata, error) {
	connString = prepareConnectionString(connString, "sqlserver")
	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open sqlserver connection: %w", err)
	}
	defer db.Close()

	var schemas []SchemaMetadata
	var routines []RoutineMetadata

	// ── 1. Tables: enhanced column info ───────────────────────────────────────
	type ssCol struct {
		name       string
		colType    string
		isNullable bool
		isIdentity bool
		defaultVal sql.NullString
	}
	tableColsMap := make(map[string][]ssCol)

	colRows, err := db.QueryContext(ctx, `
		SELECT
			t.name AS table_name,
			c.name AS col_name,
			CASE
				WHEN tp.name IN ('varchar','char','binary','varbinary') AND c.max_length = -1
					THEN tp.name + '(MAX)'
				WHEN tp.name IN ('varchar','char','binary','varbinary')
					THEN tp.name + '(' + CAST(c.max_length AS VARCHAR(10)) + ')'
				WHEN tp.name IN ('nvarchar','nchar') AND c.max_length = -1
					THEN tp.name + '(MAX)'
				WHEN tp.name IN ('nvarchar','nchar')
					THEN tp.name + '(' + CAST(c.max_length / 2 AS VARCHAR(10)) + ')'
				WHEN tp.name IN ('decimal','numeric')
					THEN tp.name + '(' + CAST(c.precision AS VARCHAR(10)) + ',' + CAST(c.scale AS VARCHAR(10)) + ')'
				ELSE tp.name
			END AS col_type,
			c.is_nullable,
			c.is_identity,
			ISNULL(dc.definition, '') AS col_default
		FROM sys.tables t
		JOIN sys.columns c  ON t.object_id = c.object_id
		JOIN sys.types   tp ON c.user_type_id = tp.user_type_id
		LEFT JOIN sys.default_constraints dc
			ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
		ORDER BY t.name, c.column_id
	`)
	if err == nil {
		defer colRows.Close()
		for colRows.Next() {
			var tbl, colName, colType, colDefault string
			var isNullable, isIdentity bool
			if err := colRows.Scan(&tbl, &colName, &colType, &isNullable, &isIdentity, &colDefault); err == nil {
				var def sql.NullString
				if colDefault != "" {
					def = sql.NullString{String: colDefault, Valid: true}
				}
				tableColsMap[tbl] = append(tableColsMap[tbl], ssCol{
					name:       colName,
					colType:    colType,
					isNullable: isNullable,
					isIdentity: isIdentity,
					defaultVal: def,
				})
			}
		}
	}

	// ── 2. Tables: PRIMARY KEY constraints ────────────────────────────────────
	ssPKMap := make(map[string][]string)
	pkRows, err := db.QueryContext(ctx, `
		SELECT t.name, c.name
		FROM sys.key_constraints kc
		JOIN sys.tables t         ON kc.parent_object_id = t.object_id
		JOIN sys.index_columns ic ON kc.parent_object_id = ic.object_id AND kc.unique_index_id = ic.index_id
		JOIN sys.columns c        ON ic.object_id = c.object_id AND ic.column_id = c.column_id
		WHERE kc.type = 'PK'
		ORDER BY t.name, ic.key_ordinal
	`)
	if err == nil {
		defer pkRows.Close()
		for pkRows.Next() {
			var tbl, col string
			if err := pkRows.Scan(&tbl, &col); err == nil {
				ssPKMap[tbl] = append(ssPKMap[tbl], col)
			}
		}
	}

	// Build CREATE TABLE DDL
	for tbl, cols := range tableColsMap {
		var parts []string
		for _, col := range cols {
			line := fmt.Sprintf("  %s %s", col.name, col.colType)
			if col.isIdentity {
				line += " IDENTITY(1,1)"
			}
			if !col.isNullable {
				line += " NOT NULL"
			} else {
				line += " NULL"
			}
			if col.defaultVal.Valid {
				line += " DEFAULT " + col.defaultVal.String
			}
			parts = append(parts, line)
		}
		if pks, ok := ssPKMap[tbl]; ok && len(pks) > 0 {
			parts = append(parts, fmt.Sprintf("  CONSTRAINT PK_%s PRIMARY KEY (%s)",
				tbl, strings.Join(pks, ", ")))
		}
		script := fmt.Sprintf("CREATE TABLE %s (\n%s\n);", tbl, strings.Join(parts, ",\n"))
		schemas = append(schemas, SchemaMetadata{Name: tbl, Type: "table", SchemaText: script})
	}

	// ── 3. Views: actual CREATE VIEW text via sys.sql_modules ─────────────────
	viewRows, err := db.QueryContext(ctx, `
		SELECT v.name, m.definition
		FROM sys.views v
		JOIN sys.sql_modules m ON v.object_id = m.object_id
	`)
	if err == nil {
		defer viewRows.Close()
		for viewRows.Next() {
			var name string
			var definition sql.NullString
			if err := viewRows.Scan(&name, &definition); err == nil && definition.Valid {
				schemas = append(schemas, SchemaMetadata{Name: name, Type: "view", SchemaText: definition.String})
			}
		}
	}

	// ── 4. Routines: full definition via sys.sql_modules (unchanged) ──────────
	routineRows, err := db.QueryContext(ctx, `
		SELECT o.name,
			CASE WHEN o.type = 'P' THEN 'procedure' ELSE 'function' END,
			m.definition
		FROM sys.objects o
		JOIN sys.sql_modules m ON o.object_id = m.object_id
		WHERE o.type IN ('P', 'FN', 'IF', 'TF')
	`)
	if err == nil {
		defer routineRows.Close()
		for routineRows.Next() {
			var name, rtType string
			var definition sql.NullString
			if err := routineRows.Scan(&name, &rtType, &definition); err == nil && definition.Valid {
				routines = append(routines, RoutineMetadata{Name: name, Type: rtType, RoutineText: definition.String})
			}
		}
	}

	return schemas, routines, nil
}

// ==============
// MySQL Extractor
// ==============
// Tables  → SHOW CREATE TABLE  (verbatim DDL including indexes, constraints)
// Views   → SHOW CREATE TABLE  (MySQL returns full CREATE VIEW body)
// Routines→ SHOW CREATE PROCEDURE / SHOW CREATE FUNCTION
type MySQLExtractor struct{}

func (e *MySQLExtractor) Extract(ctx context.Context, connString string) ([]SchemaMetadata, []RoutineMetadata, error) {
	connString = prepareConnectionString(connString, "mysql")
	db, err := sql.Open("mysql", connString)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open mysql connection: %w", err)
	}
	defer db.Close()

	var schemas []SchemaMetadata
	var routines []RoutineMetadata

	// ── 1+2. Tables & Views: enumerate names, then SHOW CREATE TABLE ──────────
	nameRows, err := db.QueryContext(ctx, `
		SELECT table_name, table_type
		FROM information_schema.tables
		WHERE table_schema = DATABASE()
		ORDER BY table_name
	`)
	if err == nil {
		defer nameRows.Close()
		type tableEntry struct {
			name string
			kind string // "BASE TABLE" or "VIEW"
		}
		var entries []tableEntry
		for nameRows.Next() {
			var name, kind string
			if err := nameRows.Scan(&name, &kind); err == nil {
				entries = append(entries, tableEntry{name, kind})
			}
		}
		_ = nameRows.Close()

		for _, entry := range entries {
			objType := "table"
			if strings.Contains(strings.ToUpper(entry.kind), "VIEW") {
				objType = "view"
			}

			var dummy, createSQL string
			row := db.QueryRowContext(ctx, "SHOW CREATE TABLE `"+entry.name+"`")
			if err := row.Scan(&dummy, &createSQL); err == nil {
				schemas = append(schemas, SchemaMetadata{
					Name:       entry.name,
					Type:       objType,
					SchemaText: createSQL,
				})
			}
		}
	}

	// ── 3. Routines: SHOW CREATE PROCEDURE/FUNCTION ───────────────────────────
	routineNameRows, err := db.QueryContext(ctx, `
		SELECT routine_name, routine_type
		FROM information_schema.routines
		WHERE routine_schema = DATABASE()
		ORDER BY routine_name
	`)
	if err == nil {
		defer routineNameRows.Close()
		type routineEntry struct {
			name   string
			rtType string
		}
		var rEntries []routineEntry
		for routineNameRows.Next() {
			var name, rtType string
			if err := routineNameRows.Scan(&name, &rtType); err == nil {
				rEntries = append(rEntries, routineEntry{name, rtType})
			}
		}
		_ = routineNameRows.Close()

		for _, r := range rEntries {
			keyword := strings.ToUpper(r.rtType) // "PROCEDURE" or "FUNCTION"
			rows, err := db.QueryContext(ctx, "SHOW CREATE "+keyword+" `"+r.name+"`")
			if err != nil {
				continue
			}
			cols, _ := rows.Columns()
			// SHOW CREATE PROCEDURE returns: (Name, sql_mode, Create Procedure, ...)
			// The CREATE script is always in column index 2
			if len(cols) >= 3 && rows.Next() {
				vals := make([]sql.NullString, len(cols))
				ptrs := make([]any, len(cols))
				for i := range vals {
					ptrs[i] = &vals[i]
				}
				if err := rows.Scan(ptrs...); err == nil && vals[2].Valid {
					routines = append(routines, RoutineMetadata{
						Name:        r.name,
						Type:        strings.ToLower(r.rtType),
						RoutineText: vals[2].String,
					})
				}
			}
			rows.Close()
		}
	}

	return schemas, routines, nil
}

// ==============
// Oracle Extractor
// ==============
// Tables  → full column DDL (type with length/precision, NULLABLE, DEFAULT) + PRIMARY KEY constraint
// Views   → actual view body via USER_VIEWS.TEXT
// Routines→ full source via USER_SOURCE (unchanged)
type OracleExtractor struct{}

func (e *OracleExtractor) Extract(ctx context.Context, connString string) ([]SchemaMetadata, []RoutineMetadata, error) {
	connString = prepareConnectionString(connString, "oracle")
	db, err := sql.Open("oracle", connString)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open oracle connection: %w", err)
	}
	defer db.Close()

	var schemas []SchemaMetadata
	var routines []RoutineMetadata

	// ── 1. Tables: columns with full type info ────────────────────────────────
	type oraCol struct {
		name       string
		colType    string
		isNullable bool
		defaultVal sql.NullString
	}
	tableColsMap := make(map[string][]oraCol)

	colRows, err := db.QueryContext(ctx, `
		SELECT
			table_name,
			column_name,
			CASE
				WHEN data_type IN ('VARCHAR2','NVARCHAR2','CHAR','NCHAR','RAW')
					THEN data_type || '(' || data_length || ')'
				WHEN data_type = 'NUMBER' AND data_precision IS NOT NULL
					THEN 'NUMBER(' || data_precision || ',' || NVL(data_scale, 0) || ')'
				WHEN data_type = 'FLOAT' AND data_precision IS NOT NULL
					THEN 'FLOAT(' || data_precision || ')'
				ELSE data_type
			END AS col_type,
			nullable,
			data_default
		FROM user_tab_columns
		ORDER BY table_name, column_id
	`)
	if err == nil {
		defer colRows.Close()
		for colRows.Next() {
			var tbl, colName, colType, nullable string
			var dataDefault sql.NullString
			if err := colRows.Scan(&tbl, &colName, &colType, &nullable, &dataDefault); err == nil {
				tableColsMap[tbl] = append(tableColsMap[tbl], oraCol{
					name:       colName,
					colType:    colType,
					isNullable: nullable == "Y",
					defaultVal: dataDefault,
				})
			}
		}
	}

	// ── 2. Tables: PRIMARY KEY constraints ────────────────────────────────────
	oraPKMap := make(map[string][]string)
	pkRows, err := db.QueryContext(ctx, `
		SELECT cc.table_name, cc.column_name
		FROM user_constraints c
		JOIN user_cons_columns cc ON c.constraint_name = cc.constraint_name
		WHERE c.constraint_type = 'P'
		ORDER BY cc.table_name, cc.position
	`)
	if err == nil {
		defer pkRows.Close()
		for pkRows.Next() {
			var tbl, col string
			if err := pkRows.Scan(&tbl, &col); err == nil {
				oraPKMap[tbl] = append(oraPKMap[tbl], col)
			}
		}
	}

	// Build CREATE TABLE DDL
	for tbl, cols := range tableColsMap {
		var parts []string
		for _, col := range cols {
			line := fmt.Sprintf("  %s %s", col.name, col.colType)
			if !col.isNullable {
				line += " NOT NULL"
			}
			if col.defaultVal.Valid {
				line += " DEFAULT " + strings.TrimSpace(col.defaultVal.String)
			}
			parts = append(parts, line)
		}
		if pks, ok := oraPKMap[tbl]; ok && len(pks) > 0 {
			parts = append(parts, fmt.Sprintf("  CONSTRAINT PK_%s PRIMARY KEY (%s)",
				tbl, strings.Join(pks, ", ")))
		}
		script := fmt.Sprintf("CREATE TABLE %s (\n%s\n);", tbl, strings.Join(parts, ",\n"))
		schemas = append(schemas, SchemaMetadata{Name: tbl, Type: "table", SchemaText: script})
	}

	// ── 3. Views: actual view body via USER_VIEWS ─────────────────────────────
	viewRows, err := db.QueryContext(ctx, `
		SELECT view_name, text
		FROM user_views
		ORDER BY view_name
	`)
	if err == nil {
		defer viewRows.Close()
		for viewRows.Next() {
			var name, text string
			if err := viewRows.Scan(&name, &text); err == nil {
				script := fmt.Sprintf("CREATE VIEW %s AS\n%s", name, strings.TrimSpace(text))
				schemas = append(schemas, SchemaMetadata{Name: name, Type: "view", SchemaText: script})
			}
		}
	}

	// ── 4. Routines: full source via USER_SOURCE (assembled line by line) ──────
	routineRows, err := db.QueryContext(ctx, `
		SELECT name, type, text
		FROM user_source
		WHERE type IN ('PROCEDURE', 'FUNCTION')
		ORDER BY name, line
	`)
	if err == nil {
		defer routineRows.Close()
		routineMap := make(map[string][]string)
		typeMap := make(map[string]string)
		for routineRows.Next() {
			var name, rtType, text string
			if err := routineRows.Scan(&name, &rtType, &text); err == nil {
				routineMap[name] = append(routineMap[name], text)
				typeMap[name] = strings.ToLower(rtType)
			}
		}
		for name, lines := range routineMap {
			routines = append(routines, RoutineMetadata{
				Name:        name,
				Type:        typeMap[name],
				RoutineText: strings.Join(lines, ""),
			})
		}
	}

	return schemas, routines, nil
}

// ==============
// MongoDB Extractor
// ==============
// No DDL concept — records collection names with minimal schema text.
type MongoDBExtractor struct{}

func (e *MongoDBExtractor) Extract(ctx context.Context, connString string) ([]SchemaMetadata, []RoutineMetadata, error) {
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
			continue
		}
		for _, colName := range collections {
			schemas = append(schemas, SchemaMetadata{
				Name:       colName,
				Type:       "collection",
				SchemaText: fmt.Sprintf("Collection: %s\nDatabase: %s\nType: NoSQL Document Store", colName, dbName),
			})
		}
	}

	return schemas, nil, nil
}

func prepareConnectionString(cs string, driver string) string {
	cs = strings.TrimSpace(cs)
	if driver == "postgres" && (!strings.HasPrefix(cs, "postgres://") && !strings.HasPrefix(cs, "postgresql://")) {
		// pgx handles standard DSN formats; return as-is.
	}
	return cs
}
