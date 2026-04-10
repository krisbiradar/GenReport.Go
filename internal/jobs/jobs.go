package jobs

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"genreport/internal/broker"
	"genreport/internal/config"
	"genreport/internal/services"

	"github.com/go-co-op/gocron/v2"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

// envMu serialises all concurrent read-modify-write operations on the .env
// file. Without this, two jobs failing simultaneously would both read the
// same original file, each modify a different key, and the last os.Rename
// would silently drop the other job's disable.
var envMu sync.Mutex

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
					go func() {
						s.RemoveJob(jobID)
						// Persist the disable to .env so it survives a server restart.
						if err := disableJobInEnv(jobConfigKey, logger); err != nil {
							logger.Warn().Err(err).Str("job", jobConfigKey).Msg("could not persist job disable to .env")
						}
					}()

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

// disableJobInEnv sets JOB_<key>_ENABLED=false in the project's .env file.
//
// The entire read-modify-write is serialised by envMu. Without the mutex, two
// goroutines running concurrently would each read the same original file,
// modify a different key, and the last os.Rename would silently drop the other
// job's change — even though the rename itself is atomic.
// It walks up from the current working directory to find the file.
func disableJobInEnv(jobKey string, logger zerolog.Logger) error {
	envMu.Lock()
	defer envMu.Unlock()

	cwd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("could not determine working directory: %w", err)
	}
	envPath := ""
	for dir := cwd; ; dir = filepath.Dir(dir) {
		candidate := filepath.Join(dir, ".env")
		if _, statErr := os.Stat(candidate); statErr == nil {
			envPath = candidate
			break
		}
		if dir == filepath.Dir(dir) {
			break // reached filesystem root
		}
	}
	if envPath == "" {
		return fmt.Errorf(".env file not found (searched from %s)", cwd)
	}

	targetKey := "JOB_" + jobKey + "_ENABLED"

	// Read all lines.
	f, err := os.Open(envPath)
	if err != nil {
		return fmt.Errorf("could not open .env: %w", err)
	}
	var lines []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	f.Close()
	if scanErr := scanner.Err(); scanErr != nil {
		return fmt.Errorf("error reading .env: %w", scanErr)
	}

	// Rewrite the matching line; append it if not found.
	found := false
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, targetKey+"=") || trimmed == targetKey {
			lines[i] = targetKey + "=false"
			found = true
			break
		}
	}
	if !found {
		lines = append(lines, targetKey+"=false")
	}

	// Write atomically via a temp file + rename.
	tmp, err := os.CreateTemp(filepath.Dir(envPath), ".env.tmp.*")
	if err != nil {
		return fmt.Errorf("could not create temp file: %w", err)
	}
	writer := bufio.NewWriter(tmp)
	for _, line := range lines {
		if _, werr := fmt.Fprintln(writer, line); werr != nil {
			tmp.Close()
			os.Remove(tmp.Name())
			return fmt.Errorf("error writing temp file: %w", werr)
		}
	}
	if err := writer.Flush(); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return fmt.Errorf("error flushing temp file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return fmt.Errorf("error syncing temp file: %w", err)
	}
	tmp.Close()

	if err := os.Rename(tmp.Name(), envPath); err != nil {
		os.Remove(tmp.Name())
		return fmt.Errorf("could not update .env: %w", err)
	}

	logger.Info().Str("job", jobKey).Str("env", envPath).Msg("persisted job disable to .env")
	return nil
}
