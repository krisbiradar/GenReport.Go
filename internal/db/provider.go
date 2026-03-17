package db

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"genreport/internal/models"
)

type ProviderKind string

const (
	ProviderPostgreSQL ProviderKind = "postgresql"
	ProviderSQLServer  ProviderKind = "sqlserver"
	ProviderMySQL      ProviderKind = "mysql"
	ProviderOracle     ProviderKind = "oracle"
	ProviderMongoDB    ProviderKind = "mongodb"
)

type DBProvider interface {
	TestConnection(ctx context.Context, req models.TestConnectionRequest) error
}

func ResolveProviderKind(req models.TestConnectionRequest) (ProviderKind, error) {
	if req.Provider.IntValue != nil {
		switch *req.Provider.IntValue {
		case 1:
			return ProviderPostgreSQL, nil
		case 2:
			return ProviderSQLServer, nil
		case 3:
			return ProviderMySQL, nil
		case 4:
			return ProviderOracle, nil
		case 5:
			return ProviderMongoDB, nil
		default:
			return "", fmt.Errorf("unsupported provider value")
		}
	}

	if strings.TrimSpace(req.Provider.StringValue) != "" {
		if kind, ok := mapProviderName(req.Provider.StringValue); ok {
			return kind, nil
		}
	}

	if strings.TrimSpace(req.DatabaseType) != "" {
		if kind, ok := mapProviderName(req.DatabaseType); ok {
			return kind, nil
		}
	}

	return "", errors.New("unsupported provider")
}

func mapProviderName(raw string) (ProviderKind, bool) {
	normalized := strings.ToLower(strings.TrimSpace(raw))
	normalized = strings.ReplaceAll(normalized, "_", "")
	normalized = strings.ReplaceAll(normalized, "-", "")
	normalized = strings.ReplaceAll(normalized, " ", "")

	switch normalized {
	case "1", "npgsql", "postgres", "postgresql":
		return ProviderPostgreSQL, true
	case "2", "sqlclient", "sqlserver", "mssql":
		return ProviderSQLServer, true
	case "3", "mysqlconnector", "mysql":
		return ProviderMySQL, true
	case "4", "oracle":
		return ProviderOracle, true
	case "5", "mongoclient", "mongo", "mongodb":
		return ProviderMongoDB, true
	default:
		return "", false
	}
}
