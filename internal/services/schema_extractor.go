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

	// 1. Tables/Views (Columns)
	tableRows, err := db.QueryContext(ctx, `
		SELECT table_name, table_type, column_name, data_type 
		FROM information_schema.columns 
		JOIN information_schema.tables USING (table_name, table_schema)
		WHERE table_schema NOT IN ('information_schema', 'pg_catalog') 
		ORDER BY table_name, ordinal_position
	`)
	if err == nil {
		defer tableRows.Close()
		tableMap := make(map[string][]string)
		typeMap := make(map[string]string)
		for tableRows.Next() {
			var tableName, tableType, colName, dataType string
			if err := tableRows.Scan(&tableName, &tableType, &colName, &dataType); err == nil {
				tableMap[tableName] = append(tableMap[tableName], fmt.Sprintf("%s %s", colName, dataType))
				t := "table"
				if tableType == "VIEW" {
					t = "view"
				}
				typeMap[tableName] = t
			}
		}
		for name, cols := range tableMap {
			schemaText := fmt.Sprintf("CREATE %s %s (\n  %s\n);", strings.ToUpper(typeMap[name]), name, strings.Join(cols, ",\n  "))
			schemas = append(schemas, SchemaMetadata{Name: name, Type: typeMap[name], SchemaText: schemaText})
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

	// 1. Tables/Views (Columns)
	tableRows, err := db.QueryContext(ctx, `
		SELECT t.name, 'table', c.name, type_name(c.user_type_id) 
		FROM sys.tables t JOIN sys.columns c ON t.object_id = c.object_id
		UNION ALL
		SELECT v.name, 'view', c.name, type_name(c.user_type_id)
		FROM sys.views v JOIN sys.columns c ON v.object_id = c.object_id
	`)
	if err == nil {
		defer tableRows.Close()
		tableMap := make(map[string][]string)
		typeMap := make(map[string]string)
		for tableRows.Next() {
			var tableName, tableType, colName, dataType string
			if err := tableRows.Scan(&tableName, &tableType, &colName, &dataType); err == nil {
				tableMap[tableName] = append(tableMap[tableName], fmt.Sprintf("%s %s", colName, dataType))
				typeMap[tableName] = tableType
			}
		}
		for name, cols := range tableMap {
			schemaText := fmt.Sprintf("CREATE %s %s (\n  %s\n);", strings.ToUpper(typeMap[name]), name, strings.Join(cols, ",\n  "))
			schemas = append(schemas, SchemaMetadata{Name: name, Type: typeMap[name], SchemaText: schemaText})
		}
	}

	// 2. Routines (Functions/Procedures)
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

	// 1. Tables/Views (Columns)
	tableRows, err := db.QueryContext(ctx, `
		SELECT table_name, table_type, column_name, column_type 
		FROM information_schema.columns 
		JOIN information_schema.tables USING (table_name, table_schema)
		WHERE table_schema = DATABASE()
		ORDER BY table_name, ordinal_position
	`)
	if err == nil {
		defer tableRows.Close()
		tableMap := make(map[string][]string)
		typeMap := make(map[string]string)
		for tableRows.Next() {
			var tableName, tableType, colName, dataType string
			if err := tableRows.Scan(&tableName, &tableType, &colName, &dataType); err == nil {
				tableMap[tableName] = append(tableMap[tableName], fmt.Sprintf("%s %s", colName, dataType))
				t := "table"
				if strings.Contains(strings.ToUpper(tableType), "VIEW") {
					t = "view"
				}
				typeMap[tableName] = t
			}
		}
		for name, cols := range tableMap {
			schemaText := fmt.Sprintf("CREATE %s %s (\n  %s\n);", strings.ToUpper(typeMap[name]), name, strings.Join(cols, ",\n  "))
			schemas = append(schemas, SchemaMetadata{Name: name, Type: typeMap[name], SchemaText: schemaText})
		}
	}

	// 2. Routines (Functions/Procedures)
	routineRows, err := db.QueryContext(ctx, `
		SELECT routine_name, routine_type, routine_definition 
		FROM information_schema.routines 
		WHERE routine_schema = DATABASE()
	`)
	if err == nil {
		defer routineRows.Close()
		for routineRows.Next() {
			var name, rtType string
			var definition sql.NullString
			if err := routineRows.Scan(&name, &rtType, &definition); err == nil && definition.Valid {
				routines = append(routines, RoutineMetadata{Name: name, Type: strings.ToLower(rtType), RoutineText: definition.String})
			}
		}
	}

	return schemas, routines, nil
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

	var schemas []SchemaMetadata
	var routines []RoutineMetadata

	// 1. Tables/Views (Columns)
	tableRows, err := db.QueryContext(ctx, `
		SELECT table_name, 'table', column_name, data_type 
		FROM user_tab_columns
		UNION ALL
		SELECT view_name, 'view', column_name, data_type 
		FROM user_updatable_columns
	`)
	if err == nil {
		defer tableRows.Close()
		tableMap := make(map[string][]string)
		typeMap := make(map[string]string)
		for tableRows.Next() {
			var tableName, tableType, colName, dataType string
			if err := tableRows.Scan(&tableName, &tableType, &colName, &dataType); err == nil {
				tableMap[tableName] = append(tableMap[tableName], fmt.Sprintf("%s %s", colName, dataType))
				typeMap[tableName] = tableType
			}
		}
		for name, cols := range tableMap {
			schemaText := fmt.Sprintf("CREATE %s %s (\n  %s\n);", strings.ToUpper(typeMap[name]), name, strings.Join(cols, ",\n  "))
			schemas = append(schemas, SchemaMetadata{Name: name, Type: typeMap[name], SchemaText: schemaText})
		}
	}

	// 2. Routines (Functions/Procedures)
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
			routines = append(routines, RoutineMetadata{Name: name, Type: typeMap[name], RoutineText: strings.Join(lines, "")})
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
