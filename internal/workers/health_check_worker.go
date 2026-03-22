package workers

import (
	"github.com/rs/zerolog"

	"genreport/internal/broker"
)

// HandleHealthCheck returns a JobHandler that processes health check messages.
func HandleHealthCheck(logger zerolog.Logger) broker.JobHandler {
	return func(payload []byte) error {
		logger.Info().
			Str("payload", string(payload)).
			Msg("worker: processing health check job")
		return nil
	}
}
