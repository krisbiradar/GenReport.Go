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
// StartNow controls whether the job fires immediately at startup.
type jobEntry struct {
	ConfigKey string
	StartNow  bool
	NewTask   func(cfg config.Config, producer *broker.Producer, logger zerolog.Logger) gocron.Task
}

// buildRegistry returns the full list of background job entries.
// Constructed at registration time so cfg is available to all closures.
func buildRegistry() []jobEntry {
	return []jobEntry{
		{
			ConfigKey: "HEALTH_CHECK",
			StartNow:  true,
			NewTask: func(cfg config.Config, producer *broker.Producer, logger zerolog.Logger) gocron.Task {
				return gocron.NewTask(HealthCheckJob, producer, logger)
			},
		},
		{
			ConfigKey: "CLEANUP",
			StartNow:  true,
			NewTask: func(cfg config.Config, producer *broker.Producer, logger zerolog.Logger) gocron.Task {
				return gocron.NewTask(CleanupJob, producer, logger)
			},
		},
		{
			ConfigKey: "SCHEMA_SYNC",
			StartNow:  true,
			NewTask: func(cfg config.Config, producer *broker.Producer, logger zerolog.Logger) gocron.Task {
				return gocron.NewTask(SchemaSyncJob, cfg, producer, logger)
			},
		},
		{
			// GENERATE_EMBEDDINGS is NOT started immediately — SchemaSyncJob
			// fires it inline after sync completes. This job still runs on its
			// own interval to refresh embeddings when schema hasn't changed.
			ConfigKey: "GENERATE_EMBEDDINGS",
			StartNow:  false,
			NewTask: func(cfg config.Config, producer *broker.Producer, logger zerolog.Logger) gocron.Task {
				return gocron.NewTask(GenerateEmbeddingsJob, producer, logger)
			},
		},
	}
}

// RegisterAll registers all enabled background jobs with the scheduler.
// Jobs now act as producers — they publish messages to RabbitMQ topics.
func RegisterAll(s gocron.Scheduler, cfg config.Config, producer *broker.Producer, logger zerolog.Logger, emailService *services.EmailService) {
	registry := buildRegistry()

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

		jobOpts := []gocron.JobOption{
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
		}
		if entry.StartNow {
			jobOpts = append(jobOpts, gocron.WithStartAt(gocron.WithStartImmediately()))
		}

		_, err := s.NewJob(
			gocron.DurationJob(settings.Interval),
			entry.NewTask(cfg, producer, logger),
			jobOpts...,
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
