package workers

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/rs/zerolog"

	"genreport/internal/broker"
	"genreport/internal/models"
	"genreport/internal/services"
)

const (
	topicReportGenerate = "report_generate"
	topicReportSuccess  = "report_success"
	topicReportError    = "report_error"
)

// HandleReportGenerate returns a JobHandler that:
//  1. Deserialises the ReportJobRequest from the "report_generate" queue.
//  2. Executes the query and materialises results into a SQLite file.
//  3. Publishes a ReportJobResult to "report_success" (file path included) or
//     "report_error" (error message included).
//
// The message is always ACKed — errors are propagated via the result queues
// rather than nacking (which could cause infinite retry loops).
func HandleReportGenerate(svc *services.ReportQueryService, producer *broker.Producer, logger zerolog.Logger) broker.JobHandler {
	return func(payload []byte) error {
		var req models.ReportJobRequest
		if err := json.Unmarshal(payload, &req); err != nil {
			logger.Error().Err(err).Msg("report worker: failed to deserialise job request")
			// Publish to error queue so the caller knows something went wrong.
			_ = publishResult(producer, logger, topicReportError, models.ReportJobResult{
				Error: fmt.Sprintf("invalid job payload: %s", err.Error()),
			})
			return nil // ACK — bad payload should not be requeued
		}

		logger.Info().
			Str("sessionId", req.SessionID).
			Str("databaseConnectionId", req.DatabaseConnectionID).
			Str("format", req.Format).
			Msg("report worker: processing report job")

		sqlitePath, err := svc.Execute(context.Background(), req)
		if err != nil {
			logger.Error().
				Err(err).
				Str("sessionId", req.SessionID).
				Str("databaseConnectionId", req.DatabaseConnectionID).
				Msg("report worker: job failed")

			_ = publishResult(producer, logger, topicReportError, models.ReportJobResult{
				DatabaseConnectionID: req.DatabaseConnectionID,
				Format:               req.Format,
				Query:                req.Query,
				SessionID:            req.SessionID,
				Error:                err.Error(),
			})
			return nil // ACK — error forwarded via report_error queue
		}

		logger.Info().
			Str("sessionId", req.SessionID).
			Str("sqlitePath", sqlitePath).
			Msg("report worker: job succeeded")

		_ = publishResult(producer, logger, topicReportSuccess, models.ReportJobResult{
			DatabaseConnectionID: req.DatabaseConnectionID,
			Format:               req.Format,
			Query:                req.Query,
			SessionID:            req.SessionID,
			SQLiteFilePath:       sqlitePath,
		})
		return nil
	}
}

func publishResult(producer *broker.Producer, logger zerolog.Logger, topic string, result models.ReportJobResult) error {
	data, err := json.Marshal(result)
	if err != nil {
		logger.Error().Err(err).Str("topic", topic).Msg("report worker: failed to marshal result")
		return err
	}
	if err := producer.Publish(topic, data); err != nil {
		logger.Error().Err(err).Str("topic", topic).Msg("report worker: failed to publish result")
		return err
	}
	return nil
}
