package workers

import (
	"github.com/rs/zerolog"

	"genreport/internal/broker"
)

// WorkerEntry maps a RabbitMQ topic to its handler function.
type WorkerEntry struct {
	Topic   string
	Handler broker.JobHandler
}

// All returns every registered worker.
// To add a new worker: create a handler file, then add an entry here.
func All(logger zerolog.Logger) []WorkerEntry {
	return []WorkerEntry{
		{Topic: "health_check", Handler: HandleHealthCheck(logger)},
		{Topic: "cleanup", Handler: HandleCleanup(logger)},
	}
}
