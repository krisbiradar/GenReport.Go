package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"genreport/internal/config"
	"genreport/internal/models"

	_ "github.com/go-sql-driver/mysql"
)

type MySQLProvider struct {
	cache  *SQLCache
	config config.Config
}

func NewMySQLProvider(cfg config.Config) *MySQLProvider {
	return &MySQLProvider{
		cache:  NewSQLCache(),
		config: cfg,
	}
}

func (p *MySQLProvider) TestConnection(ctx context.Context, req models.TestConnectionRequest) error {
	connectionString := strings.TrimSpace(req.ConnectionString)
	if connectionString == "" {
		port := req.Port
		if port <= 0 {
			port = 3306
		}
		connectionString = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?timeout=5s&readTimeout=5s&writeTimeout=5s&parseTime=true",
			req.UserName,
			req.Password,
			req.HostName,
			port,
			req.DatabaseName,
		)
	}

	dbConn, err := p.cache.GetOrCreate(connectionString, func() (*sql.DB, error) {
		dbConn, openErr := sql.Open("mysql", connectionString)
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
