package workers

import (
	"github.com/rs/zerolog"

	"genreport/internal/broker"
)

// HandleCleanup returns a JobHandler that processes cleanup messages.
func HandleCleanup(logger zerolog.Logger) broker.JobHandler {
	return func(payload []byte) error {
		logger.Info().
			Str("payload", string(payload)).
			Msg("worker: processing cleanup job")
		return nil
	}
}
