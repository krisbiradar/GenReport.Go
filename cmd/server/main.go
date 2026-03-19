package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"genreport/internal/config"
	"genreport/internal/db"
	"genreport/internal/handlers"
	"genreport/internal/services"

	"github.com/joho/godotenv"
	"github.com/rs/zerolog"
)

func main() {
	if err := godotenv.Load(); err != nil {
		_ = godotenv.Load("../../.env")
	}

	cfg := config.Load()
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger().Level(cfg.LogLevel)

	providerFactory := db.NewProviderFactory(cfg)
	connectionService := services.NewConnectionTestService(providerFactory, cfg.DBTestTimeout, logger)
	connectionHandler := handlers.NewConnectionHandler(connectionService, logger)
	router := handlers.NewRouter(connectionHandler)

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

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error().Err(err).Msg("graceful shutdown failed")
		return
	}

	logger.Info().Msg("server stopped")
}
