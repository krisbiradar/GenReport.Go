package jobs

import "github.com/rs/zerolog"

func HealthCheckJob(logger zerolog.Logger) {
	logger.Info().Msg("background job: health check heartbeat")
}
