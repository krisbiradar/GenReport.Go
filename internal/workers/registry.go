package workers

import (
	"github.com/rs/zerolog"

	"genreport/internal/broker"
	"genreport/internal/config"
	"gorm.io/gorm"
)

// WorkerEntry maps a RabbitMQ topic to its handler function.
type WorkerEntry struct {
	Topic   string
	Handler broker.JobHandler
}

// All returns every registered worker.
// To add a new worker: create a handler file, then add an entry here.
func All(cfg config.Config, logger zerolog.Logger, db *gorm.DB) []WorkerEntry {
	entries := []WorkerEntry{
		{Topic: "health_check", Handler: HandleHealthCheck(logger)},
		{Topic: "cleanup", Handler: HandleCleanup(logger)},
	}

	if db != nil {
		entries = append(entries, WorkerEntry{Topic: "schema_copy", Handler: HandleSchemaCopy(cfg, logger, db)})
	}

	return entries
}
