package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"genreport/internal/config"
	"genreport/internal/database"
	"genreport/internal/db"
	"genreport/internal/handlers"
	"genreport/internal/jobs"
	_ "genreport/internal/models"
	"genreport/internal/services"

	"github.com/go-co-op/gocron/v2"
	"github.com/joho/godotenv"
	"github.com/rs/zerolog"
)

func main() {
	if err := godotenv.Load(); err != nil {
		_ = godotenv.Load("../../.env")
	}

	cfg := config.Load()
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger().Level(cfg.LogLevel)

	// Shared database (GORM) — optional, only if DATABASE_URL is configured
	if cfg.DatabaseURL != "" {
		_, err := database.Connect(cfg.DatabaseURL, logger)
		if err != nil {
			logger.Warn().Err(err).Msg("shared database connection failed — continuing without it")
		}
	} else {
		logger.Info().Msg("DATABASE_URL not set — skipping shared database connection")
	}

	providerFactory := db.NewProviderFactory(cfg)
	connectionService := services.NewConnectionTestService(providerFactory, cfg.DBTestTimeout, logger)
	connectionHandler := handlers.NewConnectionHandler(connectionService, logger)
	router := handlers.NewRouter(connectionHandler)

	// Background jobs scheduler
	scheduler, err := gocron.NewScheduler()
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to create job scheduler")
	}
	jobs.RegisterAll(scheduler, cfg, logger)
	scheduler.Start()
	logger.Info().Msg("background job scheduler started")

	server := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		logger.Info().
			Str("addr", server.Addr).
			Dur("dbTestTimeout", cfg.DBTestTimeout).
			Msg("starting go connection test server")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal().Err(err).Msg("server failed")
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	// Graceful shutdown: stop jobs first, then HTTP server
	if err := scheduler.Shutdown(); err != nil {
		logger.Error().Err(err).Msg("job scheduler shutdown failed")
	}
	logger.Info().Msg("background job scheduler stopped")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error().Err(err).Msg("graceful shutdown failed")
		return
	}

	logger.Info().Msg("server stopped")
}
