package db

import (
	"context"
	"database/sql"
	"net/url"
	"strconv"
	"strings"

	"genreport/internal/config"
	"genreport/internal/models"

	_ "github.com/microsoft/go-mssqldb"
)

type SQLServerProvider struct {
	cache  *SQLCache
	config config.Config
}

func NewSQLServerProvider(cfg config.Config) *SQLServerProvider {
	return &SQLServerProvider{
		cache:  NewSQLCache(),
		config: cfg,
	}
}

func (p *SQLServerProvider) TestConnection(ctx context.Context, req models.TestConnectionRequest) error {
	connectionString := strings.TrimSpace(req.ConnectionString)
	if connectionString == "" {
		port := req.Port
		if port <= 0 {
			port = 1433
		}
		connectionString = buildSQLServerConnectionString(req.HostName, port, req.UserName, req.Password, req.DatabaseName)
	}

	dbConn, err := p.cache.GetOrCreate(connectionString, func() (*sql.DB, error) {
		dbConn, openErr := sql.Open("sqlserver", connectionString)
		if openErr != nil {
			return nil, openErr
		}
		dbConn.SetMaxOpenConns(p.config.SQLMaxOpenConns)
		dbConn.SetMaxIdleConns(p.config.SQLMaxIdleConns)
		dbConn.SetConnMaxLifetime(p.config.SQLConnMaxLifetime)
		return dbConn, nil
	})
	if err != nil {
		return err
	}

	return dbConn.PingContext(ctx)
}

func buildSQLServerConnectionString(host string, port int, user string, password string, database string) string {
	connURL := url.URL{
		Scheme: "sqlserver",
		Host:   host + ":" + strconv.Itoa(port),
	}
	if strings.TrimSpace(user) != "" {
		connURL.User = url.UserPassword(user, password)
	}

	query := url.Values{}
	if strings.TrimSpace(database) != "" {
		query.Set("database", database)
	}
	query.Set("encrypt", "disable")
	connURL.RawQuery = query.Encode()
	return connURL.String()
}
