package jobs

import (
	"genreport/internal/broker"
	"genreport/internal/config"
	"genreport/internal/services"

	"github.com/go-co-op/gocron/v2"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

// jobEntry maps a config key to its task constructor.
// The constructor receives cfg so jobs can read runtime configuration.
type jobEntry struct {
	ConfigKey string
	NewTask   func(cfg config.Config, producer *broker.Producer, logger zerolog.Logger) gocron.Task
}

// buildRegistry returns the full list of background job entries.
// Constructed at registration time so cfg is available to all closures.
func buildRegistry(cfg config.Config) []jobEntry {
	return []jobEntry{
		{
			ConfigKey: "HEALTH_CHECK",
			NewTask: func(cfg config.Config, producer *broker.Producer, logger zerolog.Logger) gocron.Task {
				return gocron.NewTask(HealthCheckJob, producer, logger)
			},
		},
		{
			ConfigKey: "CLEANUP",
			NewTask: func(cfg config.Config, producer *broker.Producer, logger zerolog.Logger) gocron.Task {
				return gocron.NewTask(CleanupJob, producer, logger)
			},
		},
		{
			ConfigKey: "SCHEMA_SYNC",
			NewTask: func(cfg config.Config, producer *broker.Producer, logger zerolog.Logger) gocron.Task {
				return gocron.NewTask(SchemaSyncJob, producer, logger, cfg)
			},
		},
	}
}

// RegisterAll registers all enabled background jobs with the scheduler.
// Jobs now act as producers — they publish messages to RabbitMQ topics.
func RegisterAll(s gocron.Scheduler, cfg config.Config, producer *broker.Producer, logger zerolog.Logger, emailService *services.EmailService) {
	registry := buildRegistry(cfg)

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
			entry.NewTask(cfg, producer, logger),
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
