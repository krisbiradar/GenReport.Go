package handlers

import (
	"encoding/json"
	"net/http"
	"strings"

	"genreport/internal/models"
	"genreport/internal/services"

	"github.com/rs/zerolog"
)

// QueryValidationHandler exposes POST /queries/validate.
type QueryValidationHandler struct {
	service *services.QueryValidationService
	logger  zerolog.Logger
}

func NewQueryValidationHandler(service *services.QueryValidationService, logger zerolog.Logger) *QueryValidationHandler {
	return &QueryValidationHandler{service: service, logger: logger}
}

// ValidateQuery handles POST /queries/validate.
// It decodes a QueryValidationRequest JSON body, runs the two-phase validation
// (static read-only check + dry-run), and returns a QueryValidationResult JSON.
func (h *QueryValidationHandler) ValidateQuery(w http.ResponseWriter, r *http.Request) {
	var req models.QueryValidationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeJSON(w, http.StatusBadRequest, models.QueryValidationResult{
			Status:      models.QueryValidationStatusParseError,
			Description: "invalid request payload: " + err.Error(),
		})
		return
	}

	if strings.TrimSpace(req.DatabaseID) == "" {
		h.writeJSON(w, http.StatusBadRequest, models.QueryValidationResult{
			Status:      models.QueryValidationStatusParseError,
			Description: "databaseId is required",
		})
		return
	}

	if strings.TrimSpace(req.SQL) == "" {
		h.writeJSON(w, http.StatusBadRequest, models.QueryValidationResult{
			Status:      models.QueryValidationStatusParseError,
			Description: "sql is required",
		})
		return
	}

	result := h.service.Validate(r.Context(), req)

	// Return HTTP 200 in all cases — the caller inspects `status` in the body.
	h.writeJSON(w, http.StatusOK, result)
}

func (h *QueryValidationHandler) writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		h.logger.Error().Err(err).Msg("query validation handler: failed to write JSON response")
	}
}
