package handlers

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"genreport/internal/models"
	"genreport/internal/services"

	"github.com/rs/zerolog"
)

type ConnectionHandler struct {
	service *services.ConnectionTestService
	logger  zerolog.Logger
}

func NewConnectionHandler(service *services.ConnectionTestService, logger zerolog.Logger) *ConnectionHandler {
	return &ConnectionHandler{
		service: service,
		logger:  logger,
	}
}

func (h *ConnectionHandler) TestConnection(w http.ResponseWriter, r *http.Request) {
	var req models.TestConnectionRequest
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&req); err != nil {
		h.writePlainText(w, http.StatusBadRequest, "Invalid request payload")
		return
	}

	if err := h.service.TestConnection(r.Context(), req); err != nil {
		var serviceErr *services.ServiceError
		if errors.As(err, &serviceErr) {
			h.writePlainText(w, serviceErr.StatusCode, strings.TrimSpace(serviceErr.Message))
			return
		}
		h.logger.Error().Err(err).Msg("unexpected error in connection test handler")
		h.writePlainText(w, http.StatusInternalServerError, "Connection test failed")
		return
	}

	h.writePlainText(w, http.StatusOK, "Connection successful")
}

func (h *ConnectionHandler) writePlainText(w http.ResponseWriter, statusCode int, message string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(statusCode)
	_, _ = w.Write([]byte(message))
}
