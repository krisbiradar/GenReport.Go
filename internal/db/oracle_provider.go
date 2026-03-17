package db

import (
	"context"
	"database/sql"
	"net/url"
	"strconv"
	"strings"

	"genreport/internal/config"
	"genreport/internal/models"

	_ "github.com/sijms/go-ora/v2"
)

type OracleProvider struct {
	cache  *SQLCache
	config config.Config
}

func NewOracleProvider(cfg config.Config) *OracleProvider {
	return &OracleProvider{
		cache:  NewSQLCache(),
		config: cfg,
	}
}

func (p *OracleProvider) TestConnection(ctx context.Context, req models.TestConnectionRequest) error {
	connectionString := strings.TrimSpace(req.ConnectionString)
	if connectionString == "" {
		port := req.Port
		if port <= 0 {
			port = 1521
		}
		connectionString = buildOracleConnectionString(req.HostName, port, req.UserName, req.Password, req.DatabaseName)
	}

	dbConn, err := p.cache.GetOrCreate(connectionString, func() (*sql.DB, error) {
		dbConn, openErr := sql.Open("oracle", connectionString)
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

func buildOracleConnectionString(host string, port int, user string, password string, serviceName string) string {
	connURL := url.URL{
		Scheme: "oracle",
		Host:   host + ":" + strconv.Itoa(port),
	}

	if strings.TrimSpace(user) != "" {
		connURL.User = url.UserPassword(user, password)
	}

	if strings.TrimSpace(serviceName) != "" {
		connURL.Path = "/" + serviceName
	}

	return connURL.String()
}
