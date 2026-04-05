package services

import (
	"context"
	"database/sql"
	"fmt"
	"os/exec"
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

	// 1. Tables/Views
	tableRows, err := db.QueryContext(ctx, `
		SELECT table_schema, table_name, table_type
		FROM information_schema.tables
		WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
		  AND table_type IN ('BASE TABLE', 'VIEW')
	`)
	if err == nil {
		defer tableRows.Close()
		for tableRows.Next() {
			var tableSchema, tableName, tableType string
			if err := tableRows.Scan(&tableSchema, &tableName, &tableType); err == nil {
				t := "table"
				if tableType == "VIEW" {
					t = "view"
				}

				// Format the target name. e.g. "public.users"
				targetObj := fmt.Sprintf("%s.%s", tableSchema, tableName)
				
				// Use pg_dump to extract the exact DDL
				out, execErr := exec.CommandContext(ctx, "pg_dump", "-s", "-t", targetObj, "--no-owner", "--no-privileges", connString).Output()
				if execErr == nil {
					ddl := strings.TrimSpace(string(out))
					if ddl != "" {
						schemas = append(schemas, SchemaMetadata{Name: tableName, Type: t, SchemaText: ddl})
					}
				}
			}
		}
	}

	// 2. Routines (Functions/Procedures)
	routineRows, err := db.QueryContext(ctx, `
		SELECT proname as name,
			CASE WHEN prokind = 'p' THEN 'procedure' ELSE 'function' END as type,
			pg_get_functiondef(oid) as definition
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

	// 1a. Tables (Columns only since OBJECT_DEFINITION doesn't support basic tables)
	tableRows, err := db.QueryContext(ctx, `
		SELECT t.name, 'table', c.name, type_name(c.user_type_id) 
		FROM sys.tables t JOIN sys.columns c ON t.object_id = c.object_id
	`)
	if err == nil {
		defer tableRows.Close()
		tableMap := make(map[string][]string)
		for tableRows.Next() {
			var tableName, tableType, colName, dataType string
			if err := tableRows.Scan(&tableName, &tableType, &colName, &dataType); err == nil {
				tableMap[tableName] = append(tableMap[tableName], fmt.Sprintf("[%s] %s", colName, dataType))
			}
		}
		for name, cols := range tableMap {
			schemaText := fmt.Sprintf("CREATE TABLE [%s] (\n  %s\n);", name, strings.Join(cols, ",\n  "))
			schemas = append(schemas, SchemaMetadata{Name: name, Type: "table", SchemaText: schemaText})
		}
	}

	// 1b. Views (Using OBJECT_DEFINITION)
	viewRows, err := db.QueryContext(ctx, `
		SELECT name, OBJECT_DEFINITION(object_id)
		FROM sys.views
	`)
	if err == nil {
		defer viewRows.Close()
		for viewRows.Next() {
			var viewName string
			var definition sql.NullString
			if err := viewRows.Scan(&viewName, &definition); err == nil && definition.Valid {
				schemas = append(schemas, SchemaMetadata{Name: viewName, Type: "view", SchemaText: definition.String})
			}
		}
	}

	// 2. Routines (Using OBJECT_DEFINITION)
	routineRows, err := db.QueryContext(ctx, `
		SELECT name, 
			CASE WHEN type = 'P' THEN 'procedure' ELSE 'function' END,
			OBJECT_DEFINITION(object_id)
		FROM sys.objects 
		WHERE type IN ('P', 'FN', 'IF', 'TF')
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

	// 1. Tables/Views
	tableRows, err := db.QueryContext(ctx, "SHOW FULL TABLES WHERE Table_type = 'BASE TABLE' OR Table_type = 'VIEW'")
	if err == nil {
		defer tableRows.Close()
		for tableRows.Next() {
			var tableName, tableType string
			if err := tableRows.Scan(&tableName, &tableType); err == nil {
				t := "table"
				query := fmt.Sprintf("SHOW CREATE TABLE `%s`", tableName)
				if tableType == "VIEW" {
					t = "view"
					query = fmt.Sprintf("SHOW CREATE VIEW `%s`", tableName)
				}
				
				ddl, err := getMySQLDDL(ctx, db, query)
				if err == nil && ddl != "" {
					schemas = append(schemas, SchemaMetadata{Name: tableName, Type: t, SchemaText: ddl})
				}
			}
		}
	}

	// 2. Routines (Functions/Procedures)
	routineRows, err := db.QueryContext(ctx, `
		SELECT routine_name, routine_type 
		FROM information_schema.routines 
		WHERE routine_schema = DATABASE()
	`)
	if err == nil {
		defer routineRows.Close()
		for routineRows.Next() {
			var name, rtType string
			if err := routineRows.Scan(&name, &rtType); err == nil {
				rtTypeLower := strings.ToLower(rtType)
				query := fmt.Sprintf("SHOW CREATE %s `%s`", strings.ToUpper(rtType), name)
				
				ddl, err := getMySQLDDL(ctx, db, query)
				if err == nil && ddl != "" {
					routines = append(routines, RoutineMetadata{Name: name, Type: rtTypeLower, RoutineText: ddl})
				}
			}
		}
	}

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

func (e *OracleExtractor) Extract(ctx context.Context, connString string) ([]SchemaMetadata, []RoutineMetadata, error) {
	connString = prepareConnectionString(connString, "oracle")
	db, err := sql.Open("oracle", connString)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open oracle connection: %w", err)
	}
	defer db.Close()

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
	if err == nil {
		defer tableRows.Close()
		for tableRows.Next() {
			var objName, objType string
			if err := tableRows.Scan(&objName, &objType); err == nil {
				var ddl sql.NullString
				ddlRow := db.QueryRowContext(ctx, "SELECT DBMS_METADATA.GET_DDL(:1, :2) FROM DUAL", objType, objName)
				if err := ddlRow.Scan(&ddl); err == nil && ddl.Valid && ddl.String != "" {
					schemas = append(schemas, SchemaMetadata{Name: objName, Type: strings.ToLower(objType), SchemaText: ddl.String})
				}
			}
		}
	}

	// 2. Routines (Functions/Procedures)
	routineRows, err := db.QueryContext(ctx, `
		SELECT object_name, object_type 
		FROM user_objects
		WHERE object_type IN ('PROCEDURE', 'FUNCTION')
	`)
	if err == nil {
		defer routineRows.Close()
		for routineRows.Next() {
			var objName, objType string
			if err := routineRows.Scan(&objName, &objType); err == nil {
				var ddl sql.NullString
				ddlRow := db.QueryRowContext(ctx, "SELECT DBMS_METADATA.GET_DDL(:1, :2) FROM DUAL", objType, objName)
				if err := ddlRow.Scan(&ddl); err == nil && ddl.Valid && ddl.String != "" {
					routines = append(routines, RoutineMetadata{Name: objName, Type: strings.ToLower(objType), RoutineText: ddl.String})
				}
			}
		}
	}

	return schemas, routines, nil
}

// ==============
// MongoDB Extractor
// ==============
type MongoDBExtractor struct{}

func (e *MongoDBExtractor) Extract(ctx context.Context, connString string) ([]SchemaMetadata, []RoutineMetadata, error) {
	connString = prepareConnectionString(connString, "mongodb")
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(connString))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open mongodb connection: %w", err)
	}
	defer client.Disconnect(ctx)

	// In MongoDB, connection string usually contains db name or we list user databases
	var schemas []SchemaMetadata
	
	// Assume connection string specifies the database
	// If it doesn't, this will fail or we could iterate over list databases.
	dbNames, err := client.ListDatabaseNames(ctx, bson.M{})
	if err == nil {
		for _, dbName := range dbNames {
			if dbName == "admin" || dbName == "local" || dbName == "config" {
				continue
			}
			collections, err := client.Database(dbName).ListCollectionNames(ctx, bson.M{})
			if err == nil {
				for _, colName := range collections {
					// We only record the name of the collection, as MongoDB has no rigid schema text.
					schemas = append(schemas, SchemaMetadata{
						Name:       colName,
						Type:       "collection",
						SchemaText: fmt.Sprintf("Collection: %s\nDatabase: %s\nType: NoSQL Document Store", colName, dbName),
					})
				}
			}
		}
	} else {
		return nil, nil, fmt.Errorf("failed to list collections: %w", err)
	}

	// No routines for MongoDB in traditional sense
	return schemas, nil, nil
}

func prepareConnectionString(cs string, driver string) string {
	cs = strings.TrimSpace(cs)
	if driver == "postgres" && (!strings.HasPrefix(cs, "postgres://") && !strings.HasPrefix(cs, "postgresql://")) {
		// Usually handled natively if properly formed, but pgx prefers specific formats
		// For simplicity, we just return the string since the exact DSN resolver logic 
		// handles fallback in standard db providers (see postgres_provider.go).
		// We'll assume the string stored in `databases` table is a valid URI or DSN for the driver.
	}
	return cs
}
