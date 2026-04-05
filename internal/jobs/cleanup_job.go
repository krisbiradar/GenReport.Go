package jobs

import (
	"encoding/json"
	"genreport/internal/broker"
	"time"

	"github.com/rs/zerolog"
)

// CleanupJob publishes a cleanup message to RabbitMQ.
func CleanupJob(producer *broker.Producer, logger zerolog.Logger) {
	payload, _ := json.Marshal(map[string]interface{}{
		"type":      "cleanup",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})

	if err := producer.Publish("cleanup", payload); err != nil {
		logger.Error().Err(err).Msg("failed to publish cleanup job")
	}
}
