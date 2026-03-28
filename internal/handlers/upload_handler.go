package handlers

import (
	"encoding/json"
	"net/http"

	"genreport/internal/models"
	"genreport/internal/services"

	"github.com/rs/zerolog"
)

type UploadHandler struct {
	service *services.R2UploadService
	logger  zerolog.Logger
}

func NewUploadHandler(service *services.R2UploadService, logger zerolog.Logger) *UploadHandler {
	return &UploadHandler{
		service: service,
		logger:  logger,
	}
}

// UploadFile handles POST /storage/upload.
// Body: { "fileName": "...", "content": "<base64>", "mimeType": "..." }
// Returns: { "url": "https://..." } on success, { "url": "" } on failure.
func (h *UploadHandler) UploadFile(w http.ResponseWriter, r *http.Request) {
	if h.service == nil {
		h.logger.Error().Msg("upload: R2 service not configured")
		h.writeJSON(w, http.StatusServiceUnavailable, models.UploadFileResponse{URL: ""})
		return
	}

	var req models.UploadFileRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeJSON(w, http.StatusBadRequest, models.UploadFileResponse{URL: ""})
		return
	}

	if err := req.Validate(); err != nil {
		h.logger.Warn().Err(err).Msg("upload: invalid request")
		h.writeJSON(w, http.StatusBadRequest, models.UploadFileResponse{URL: ""})
		return
	}

	url := h.service.Upload(r.Context(), req)

	statusCode := http.StatusOK
	if url == "" {
		statusCode = http.StatusInternalServerError
	}

	h.writeJSON(w, statusCode, models.UploadFileResponse{URL: url})
}

func (h *UploadHandler) writeJSON(w http.ResponseWriter, statusCode int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(v)
}
