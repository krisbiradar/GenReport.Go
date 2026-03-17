package services

import (
	"context"
	"errors"
	"strings"
	"time"

	"genreport/internal/db"
	"genreport/internal/models"

	"github.com/rs/zerolog"
)

type ServiceError struct {
	StatusCode int
	Message    string
}

func (e *ServiceError) Error() string {
	return e.Message
}

type ConnectionTestService struct {
	providerFactory *db.ProviderFactory
	timeout         time.Duration
	logger          zerolog.Logger
}

func NewConnectionTestService(factory *db.ProviderFactory, timeout time.Duration, logger zerolog.Logger) *ConnectionTestService {
	return &ConnectionTestService{
		providerFactory: factory,
		timeout:         timeout,
		logger:          logger,
	}
}

func (s *ConnectionTestService) TestConnection(ctx context.Context, req models.TestConnectionRequest) error {
	if err := req.Validate(); err != nil {
		return &ServiceError{StatusCode: 400, Message: err.Error()}
	}

	kind, err := db.ResolveProviderKind(req)
	if err != nil {
		return &ServiceError{StatusCode: 400, Message: "Unsupported provider"}
	}

	provider, ok := s.providerFactory.Resolve(kind)
	if !ok {
		return &ServiceError{StatusCode: 400, Message: "Unsupported provider"}
	}

	testCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	s.logger.Info().
		Str("provider", string(kind)).
		Str("hostName", req.HostName).
		Str("databaseName", req.DatabaseName).
		Int("port", req.Port).
		Msg("testing database connection")

	err = provider.TestConnection(testCtx, req)
	if err != nil {
		message := mapConnectionError(kind, err)
		s.logger.Warn().
			Str("provider", string(kind)).
			Str("message", message).
			Msg("database connection test failed")
		return &ServiceError{StatusCode: 500, Message: message}
	}

	s.logger.Info().
		Str("provider", string(kind)).
		Msg("database connection test succeeded")

	return nil
}

func mapConnectionError(kind db.ProviderKind, err error) string {
	raw := strings.ToLower(strings.TrimSpace(err.Error()))
	if errors.Is(err, context.DeadlineExceeded) || strings.Contains(raw, "timeout") {
		return providerLabel(kind) + " connection timeout"
	}
	if strings.Contains(raw, "not implemented") {
		return "Oracle provider is not implemented"
	}
	if strings.Contains(raw, "authentication") || strings.Contains(raw, "login failed") || strings.Contains(raw, "access denied") {
		return providerLabel(kind) + " authentication failed"
	}
	if strings.Contains(raw, "no such host") || strings.Contains(raw, "name or service not known") {
		return providerLabel(kind) + " host unreachable"
	}
	if strings.Contains(raw, "connection refused") {
		return providerLabel(kind) + " connection refused"
	}
	return providerLabel(kind) + " connection failed"
}

func providerLabel(kind db.ProviderKind) string {
	switch kind {
	case db.ProviderPostgreSQL:
		return "PostgreSQL"
	case db.ProviderMongoDB:
		return "Mongo"
	case db.ProviderMySQL:
		return "MySQL"
	case db.ProviderSQLServer:
		return "SQL Server"
	case db.ProviderOracle:
		return "Oracle"
	default:
		return "Database"
	}
}
