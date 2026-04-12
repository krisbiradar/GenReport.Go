package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/rs/zerolog"

	"genreport/internal/broker"
	"genreport/internal/models"
)

// ReportHandler exposes the report generation endpoint.
type ReportHandler struct {
	producer *broker.Producer
	logger   zerolog.Logger
}

func NewReportHandler(producer *broker.Producer, logger zerolog.Logger) *ReportHandler {
	return &ReportHandler{
		producer: producer,
		logger:   logger,
	}
}

// GenerateReport handles POST /reports/generate.
// It validates the incoming JSON, then publishes it to the "report_generate" queue
// and immediately returns 202 Accepted.
func (h *ReportHandler) GenerateReport(w http.ResponseWriter, r *http.Request) {
	var req models.ReportJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request payload"})
		return
	}

	// Basic validation
	if req.DatabaseConnectionID == "" {
		h.writeJSON(w, http.StatusBadRequest, map[string]string{"error": "databaseConnectionId is required"})
		return
	}
	if req.Query == "" {
		h.writeJSON(w, http.StatusBadRequest, map[string]string{"error": "query is required"})
		return
	}
	if req.SessionID == "" {
		h.writeJSON(w, http.StatusBadRequest, map[string]string{"error": "sessionId is required"})
		return
	}

	payload, err := json.Marshal(req)
	if err != nil {
		h.logger.Error().Err(err).Msg("report handler: failed to marshal job request")
		h.writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}

	if err := h.producer.Publish("report_generate", payload); err != nil {
		h.logger.Error().Err(err).Msg("report handler: failed to publish job to RabbitMQ")
		h.writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to queue report job"})
		return
	}

	h.logger.Info().
		Str("databaseConnectionId", req.DatabaseConnectionID).
		Str("sessionId", req.SessionID).
		Str("format", req.Format).
		Msg("report job queued")

	h.writeJSON(w, http.StatusAccepted, map[string]string{"message": "report job queued"})
}

func (h *ReportHandler) writeJSON(w http.ResponseWriter, statusCode int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(body)
}
