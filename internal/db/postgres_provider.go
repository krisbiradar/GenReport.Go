package db

import (
	"context"
	"database/sql"
	"net/url"
	"strconv"
	"strings"

	"genreport/internal/config"
	"genreport/internal/models"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type PostgresProvider struct {
	cache  *SQLCache
	config config.Config
}

func NewPostgresProvider(cfg config.Config) *PostgresProvider {
	return &PostgresProvider{
		cache:  NewSQLCache(),
		config: cfg,
	}
}

func (p *PostgresProvider) TestConnection(ctx context.Context, req models.TestConnectionRequest) error {
	connectionString := strings.TrimSpace(req.ConnectionString)
	if connectionString == "" {
		port := req.Port
		if port <= 0 {
			port = 5432
		}
		connectionString = buildPostgresConnectionString(req.HostName, port, req.UserName, req.Password, req.DatabaseName)
	}

	dbConn, err := p.cache.GetOrCreate(connectionString, func() (*sql.DB, error) {
		dbConn, openErr := sql.Open("pgx", connectionString)
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

func buildPostgresConnectionString(host string, port int, user string, password string, dbName string) string {
	query := url.Values{}
	query.Set("sslmode", "disable")
	endpoint := host + ":" + strconv.Itoa(port)
	connURL := url.URL{
		Scheme:   "postgres",
		Host:     endpoint,
		Path:     "/" + dbName,
		RawQuery: query.Encode(),
	}

	if strings.TrimSpace(user) != "" {
		connURL.User = url.UserPassword(user, password)
	}

	return connURL.String()
}
