package config

import (
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

type Config struct {
	Port                 string
	DBTestTimeout        time.Duration
	LogLevel             zerolog.Level
	SQLMaxOpenConns      int
	SQLMaxIdleConns      int
	SQLConnMaxLifetime   time.Duration
}

func Load() Config {
	return Config{
		Port:               readString("GO_SERVICE_PORT", "12334"),
		DBTestTimeout:      time.Duration(readInt("DB_TEST_TIMEOUT_SECONDS", 5)) * time.Second,
		LogLevel:           parseLogLevel(readString("LOG_LEVEL", "info")),
		SQLMaxOpenConns:    readInt("DB_TEST_SQL_MAX_OPEN_CONNS", 5),
		SQLMaxIdleConns:    readInt("DB_TEST_SQL_MAX_IDLE_CONNS", 2),
		SQLConnMaxLifetime: time.Duration(readInt("DB_TEST_SQL_CONN_MAX_LIFETIME_SECONDS", 300)) * time.Second,
	}
}

func readString(key string, defaultValue string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return defaultValue
	}
	return value
}

func readInt(key string, defaultValue int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return defaultValue
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return defaultValue
	}
	return value
}

func parseLogLevel(level string) zerolog.Level {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return zerolog.DebugLevel
	case "warn":
		return zerolog.WarnLevel
	case "error":
		return zerolog.ErrorLevel
	default:
		return zerolog.InfoLevel
	}
}
