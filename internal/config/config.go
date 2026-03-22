package config

import (
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

type JobSettings struct {
	Enabled  bool
	Interval time.Duration
}

type Config struct {
	Port               string
	DatabaseURL        string
	RabbitMQURL        string
	DBTestTimeout      time.Duration
	LogLevel           zerolog.Level
	SQLMaxOpenConns    int
	SQLMaxIdleConns    int
	SQLConnMaxLifetime time.Duration
	Jobs               map[string]JobSettings
}

func Load() Config {
	return Config{
		Port:               readString("GO_SERVICE_PORT", "12334"),
		DatabaseURL:        readString("DATABASE_URL", ""),
		RabbitMQURL:        readString("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
		DBTestTimeout:      time.Duration(readInt("DB_TEST_TIMEOUT_SECONDS", 5)) * time.Second,
		LogLevel:           parseLogLevel(readString("LOG_LEVEL", "info")),
		SQLMaxOpenConns:    readInt("DB_TEST_SQL_MAX_OPEN_CONNS", 5),
		SQLMaxIdleConns:    readInt("DB_TEST_SQL_MAX_IDLE_CONNS", 2),
		SQLConnMaxLifetime: time.Duration(readInt("DB_TEST_SQL_CONN_MAX_LIFETIME_SECONDS", 300)) * time.Second,
		Jobs:               loadJobSettings(),
	}
}

func loadJobSettings() map[string]JobSettings {
	// Define each job with its env-var prefix and default interval.
	type jobDefault struct {
		Key             string
		DefaultInterval int // seconds
	}

	defaults := []jobDefault{
		{Key: "HEALTH_CHECK", DefaultInterval: 60},
		{Key: "CLEANUP", DefaultInterval: 300},
	}

	jobs := make(map[string]JobSettings, len(defaults))
	for _, d := range defaults {
		jobs[d.Key] = JobSettings{
			Enabled:  readBool("JOB_"+d.Key+"_ENABLED", true),
			Interval: time.Duration(readInt("JOB_"+d.Key+"_INTERVAL_SECONDS", d.DefaultInterval)) * time.Second,
		}
	}
	return jobs
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

func readBool(key string, defaultValue bool) bool {
	raw := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	if raw == "" {
		return defaultValue
	}
	return raw == "true" || raw == "1" || raw == "yes"
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
