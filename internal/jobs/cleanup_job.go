package jobs

import "github.com/rs/zerolog"

func CleanupJob(logger zerolog.Logger) {
	logger.Info().Msg("background job: running cleanup cycle")
}
