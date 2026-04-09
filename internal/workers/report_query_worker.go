package workers

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/rs/zerolog"
	"gorm.io/gorm"

	"genreport/internal/broker"
	"genreport/internal/config"
	"genreport/internal/models"
	"genreport/internal/services"
)

// HandleReportQuery returns a broker.JobHandler that processes messages from
// the "report_query" topic. Each message carries a ReportQueryMessage JSON
// payload. The handler:
//
//  1. Deserialises the payload.
//  2. Delegates to ReportQueryService.Execute which:
//     - Runs the SQL query against the target database (cursor-batched).
//     - Writes results to a temp SQLite file.
//     - POSTs the file + request JSON to the configured callback URL.
//
// On success the message is Acked; on any error it is Nacked (no requeue).
func HandleReportQuery(cfg config.Config, logger zerolog.Logger, db *gorm.DB) broker.JobHandler {
	svc := services.NewReportQueryService(
		db,
		cfg.EncryptionMasterKey,
		cfg.ReportCallbackURL,
		cfg.ReportQueryBatchSize,
		logger,
	)

	return func(payload []byte) error {
		var msg models.ReportQueryMessage
		if err := json.Unmarshal(payload, &msg); err != nil {
			return fmt.Errorf("report_query: failed to parse message payload: %w", err)
		}

		if msg.DatabaseID == "" {
			return fmt.Errorf("report_query: databaseId is required")
		}
		if msg.Query == "" {
			return fmt.Errorf("report_query: query is required")
		}
		if msg.UserID == "" {
			return fmt.Errorf("report_query: userId is required")
		}

		logger.Info().
			Str("userId", msg.UserID).
			Str("databaseId", msg.DatabaseID).
			Str("sessionTitle", msg.SessionTitle).
			Msg("report_query: processing message")

		if err := svc.Execute(context.Background(), msg); err != nil {
			logger.Error().Err(err).
				Str("userId", msg.UserID).
				Str("databaseId", msg.DatabaseID).
				Msg("report_query: execution failed")
			return err
		}

		logger.Info().
			Str("userId", msg.UserID).
			Str("databaseId", msg.DatabaseID).
			Msg("report_query: completed successfully")
		return nil
	}
}
