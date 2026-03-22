package jobs

import (
	"genreport/internal/config"

	"github.com/go-co-op/gocron/v2"
	"github.com/rs/zerolog"
)

// jobEntry maps a config key to its task function.
type jobEntry struct {
	ConfigKey string
	NewTask   func(logger zerolog.Logger) gocron.Task
}

// registry lists all available background jobs.
// To add a new job, append an entry here and add its config in config.Load().
var registry = []jobEntry{
	{
		ConfigKey: "HEALTH_CHECK",
		NewTask: func(logger zerolog.Logger) gocron.Task {
			return gocron.NewTask(HealthCheckJob, logger)
		},
	},
	{
		ConfigKey: "CLEANUP",
		NewTask: func(logger zerolog.Logger) gocron.Task {
			return gocron.NewTask(CleanupJob, logger)
		},
	},
}

// RegisterAll registers all enabled background jobs with the scheduler.
func RegisterAll(s gocron.Scheduler, cfg config.Config, logger zerolog.Logger) {
	for _, entry := range registry {
		settings, ok := cfg.Jobs[entry.ConfigKey]
		if !ok || !settings.Enabled {
			logger.Info().
				Str("job", entry.ConfigKey).
				Msg("background job is disabled, skipping")
			continue
		}

		_, err := s.NewJob(
			gocron.DurationJob(settings.Interval),
			entry.NewTask(logger),
		)
		if err != nil {
			logger.Error().
				Err(err).
				Str("job", entry.ConfigKey).
				Msg("failed to register background job")
			continue
		}

		logger.Info().
			Str("job", entry.ConfigKey).
			Dur("interval", settings.Interval).
			Msg("registered background job")
	}
}
