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

type R2Config struct {
	AccountID       string
	AccessKeyID     string
	SecretAccessKey string
	Bucket          string
	PublicURL       string // e.g. https://pub-<hash>.r2.dev or custom domain
}

type SMTPConfig struct {
	Host     string
	Port     int
	Username string
	Password string
	From     string
}

type OllamaConfig struct {
	BaseURL        string // OLLAMA_BASE_URL, default: http://localhost:11434
	EmbeddingModel string // OLLAMA_EMBEDDING_MODEL, default: nomic-embed-text
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
	R2                 R2Config
	SMTP               SMTPConfig
	Ollama             OllamaConfig
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
		R2: R2Config{
			AccountID:       readString("R2_ACCOUNT_ID", ""),
			AccessKeyID:     readString("R2_ACCESS_KEY_ID", ""),
			SecretAccessKey: readString("R2_SECRET_ACCESS_KEY", ""),
			Bucket:          readString("R2_BUCKET", ""),
			PublicURL:       readString("R2_PUBLIC_URL", ""),
		},
		SMTP: SMTPConfig{
			Host:     readString("SMTP_HOST", ""),
			Port:     readInt("SMTP_PORT", 587),
			Username: readString("SMTP_USERNAME", ""),
			Password: readString("SMTP_PASSWORD", ""),
			From:     readString("SMTP_FROM", ""),
		},
		Ollama: OllamaConfig{
			BaseURL:        readString("OLLAMA_BASE_URL", "http://localhost:11434"),
			EmbeddingModel: readString("OLLAMA_EMBEDDING_MODEL", "nomic-embed-text"),
		},
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
		{Key: "SCHEMA_SYNC", DefaultInterval: 86400},      // runs daily by default
		{Key: "GENERATE_EMBEDDINGS", DefaultInterval: 3600}, // runs hourly by default
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
