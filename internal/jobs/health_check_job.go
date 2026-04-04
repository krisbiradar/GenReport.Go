package jobs

import (
	"encoding/json"
	"time"

	"genreport/internal/broker"

	"github.com/rs/zerolog"
)

// HealthCheckJob publishes a health check message to RabbitMQ.
func HealthCheckJob(producer *broker.Producer, logger zerolog.Logger) error {
	payload, _ := json.Marshal(map[string]interface{}{
		"type":      "health_check",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})

	if err := producer.Publish("health_check", payload); err != nil {
		logger.Error().Err(err).Msg("failed to publish health check job")
		return err
	}
	return nil
}
