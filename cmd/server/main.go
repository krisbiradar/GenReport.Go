package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"genreport/internal/broker"
	"genreport/internal/config"
	"genreport/internal/database"
	"genreport/internal/db"
	"genreport/internal/handlers"
	"genreport/internal/jobs"
	_ "genreport/internal/models"
	"genreport/internal/services"
	"genreport/internal/workers"

	"github.com/go-co-op/gocron/v2"
	"github.com/joho/godotenv"
	"github.com/rs/zerolog"
	"gorm.io/gorm"
)

func main() {
	if err := godotenv.Load(); err != nil {
		_ = godotenv.Load("../../.env")
	}

	cfg := config.Load()
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger().Level(cfg.LogLevel)

	// Shared database (GORM) — optional, only if DATABASE_URL is configured
	var gormDB *gorm.DB
	if cfg.DatabaseURL != "" {
		var err error
		gormDB, err = database.Connect(cfg.DatabaseURL, logger)
		if err != nil {
			logger.Warn().Err(err).Msg("shared database connection failed — continuing without it")
		}
	} else {
		logger.Info().Msg("DATABASE_URL not set — skipping shared database connection")
	}

	// RabbitMQ broker
	rmqBroker, err := broker.Connect(cfg.RabbitMQURL, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to connect to RabbitMQ")
	}
	defer rmqBroker.Close()

	producer := broker.NewProducer(rmqBroker)
	consumer := broker.NewConsumer(rmqBroker)

	// Start one worker per topic
	done := make(chan struct{})
	for _, w := range workers.All(logger, gormDB) {
		if err := consumer.StartWorker(w.Topic, w.Handler, done); err != nil {
			logger.Fatal().Err(err).Str("topic", w.Topic).Msg("failed to start worker")
		}
	}

	// Background jobs scheduler (gocron) — jobs are now producers
	scheduler, err := gocron.NewScheduler()
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to create job scheduler")
	}
	jobs.RegisterAll(scheduler, cfg, producer, logger)
	scheduler.Start()
	logger.Info().Msg("background job scheduler started")

	// HTTP server
	providerFactory := db.NewProviderFactory(cfg)
	connectionService := services.NewConnectionTestService(providerFactory, cfg.DBTestTimeout, logger)
	connectionHandler := handlers.NewConnectionHandler(connectionService, logger)

	r2Service, err := services.NewR2UploadService(cfg.R2, logger)
	if err != nil {
		logger.Warn().Err(err).Msg("R2 upload service unavailable — /storage/upload will return errors")
	}
	uploadHandler := handlers.NewUploadHandler(r2Service, logger)

	router := handlers.NewRouter(connectionHandler, uploadHandler)


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

	// Graceful shutdown: stop scheduler → stop workers → stop HTTP → close RabbitMQ
	if err := scheduler.Shutdown(); err != nil {
		logger.Error().Err(err).Msg("job scheduler shutdown failed")
	}
	logger.Info().Msg("background job scheduler stopped")

	close(done) // signal all workers to stop
	logger.Info().Msg("workers stopped")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error().Err(err).Msg("graceful shutdown failed")
		return
	}

	logger.Info().Msg("server stopped")
}
