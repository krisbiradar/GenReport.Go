package jobs

import (
	"genreport/internal/broker"
	"genreport/internal/config"
	"genreport/internal/services"

	"github.com/go-co-op/gocron/v2"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

// jobEntry maps a config key to its task function.
type jobEntry struct {
	ConfigKey string
	NewTask   func(producer *broker.Producer, logger zerolog.Logger) gocron.Task
}

// registry lists all available background jobs (producers).
// To add a new job, append an entry here and add its config in config.Load().
var registry = []jobEntry{
	{
		ConfigKey: "HEALTH_CHECK",
		NewTask: func(producer *broker.Producer, logger zerolog.Logger) gocron.Task {
			return gocron.NewTask(HealthCheckJob, producer, logger)
		},
	},
	{
		ConfigKey: "CLEANUP",
		NewTask: func(producer *broker.Producer, logger zerolog.Logger) gocron.Task {
			return gocron.NewTask(CleanupJob, producer, logger)
		},
	},
	{
		ConfigKey: "SCHEMA_SYNC",
		NewTask: func(producer *broker.Producer, logger zerolog.Logger) gocron.Task {
			return gocron.NewTask(SchemaSyncJob, producer, logger)
		},
	},
}

// RegisterAll registers all enabled background jobs with the scheduler.
// Jobs now act as producers — they publish messages to RabbitMQ topics.
func RegisterAll(s gocron.Scheduler, cfg config.Config, producer *broker.Producer, logger zerolog.Logger, emailService *services.EmailService) {
	for _, entry := range registry {
		settings, ok := cfg.Jobs[entry.ConfigKey]
		if !ok || !settings.Enabled {
			logger.Info().
				Str("job", entry.ConfigKey).
				Msg("background job is disabled, skipping")
			continue
		}

		// Capture loop variable for use inside the closure.
		jobConfigKey := entry.ConfigKey

		_, err := s.NewJob(
			gocron.DurationJob(settings.Interval),
			entry.NewTask(producer, logger),
			gocron.WithEventListeners(
				gocron.AfterJobRunsWithError(func(jobID uuid.UUID, _ string, jobErr error) {
					logger.Error().Err(jobErr).Str("job", jobConfigKey).Msg("job failed — disabling and sending alert")

					// RemoveJob must not be called directly here: gocron holds its internal
					// mutex while invoking this callback, so calling RemoveJob on the same
					// goroutine would deadlock. Dispatch to a new goroutine instead.
					go s.RemoveJob(jobID)

					if emailService != nil {
						go emailService.SendJobFailureAlert(jobConfigKey, jobErr)
					}
				}),
			),
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
			Msg("registered background job (producer)")
	}
}
