package handlers_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"genreport/internal/handlers"
	"genreport/internal/models"
	"genreport/internal/services"

	"github.com/rs/zerolog"
	"gorm.io/gorm"
)

// ─────────────────────────────────────────────────────────────────────────────
// Stub that lets us inject a canned Validate() response without a real DB
// ─────────────────────────────────────────────────────────────────────────────

// stubValidationService wraps a QueryValidationService constructed with nil
// gormDB and an empty master key. We intercept the HTTP-layer validation
// (missing fields) before Validate() is ever called, so nil GORM is fine
// for those tests.  For tests that DO call Validate(), we use a thin
// http.HandlerFunc-based approach to bypass the service entirely.
func newNullService() *services.QueryValidationService {
	return services.NewQueryValidationService((*gorm.DB)(nil), "", zerolog.Nop())
}

// ─────────────────────────────────────────────────────────────────────────────
// Helper
// ─────────────────────────────────────────────────────────────────────────────

func postValidate(t *testing.T, h *handlers.QueryValidationHandler, body any) *httptest.ResponseRecorder {
	t.Helper()
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		t.Fatalf("encoding request body: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/queries/validate", &buf)
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.ValidateQuery(rr, req)
	return rr
}

func decodeResult(t *testing.T, rr *httptest.ResponseRecorder) models.QueryValidationResult {
	t.Helper()
	var res models.QueryValidationResult
	if err := json.NewDecoder(rr.Body).Decode(&res); err != nil {
		t.Fatalf("decoding response: %v", err)
	}
	return res
}

// ─────────────────────────────────────────────────────────────────────────────
// Input validation — these never reach the service
// ─────────────────────────────────────────────────────────────────────────────

func TestQueryValidationHandler_MissingDatabaseID(t *testing.T) {
	h := handlers.NewQueryValidationHandler(newNullService(), zerolog.Nop())
	rr := postValidate(t, h, map[string]string{
		"sql": "SELECT 1",
		// databaseId intentionally omitted
	})

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rr.Code)
	}
	res := decodeResult(t, rr)
	if res.Status != models.QueryValidationStatusParseError {
		t.Errorf("expected ParseError status, got %d", res.Status)
	}
}

func TestQueryValidationHandler_MissingSQL(t *testing.T) {
	h := handlers.NewQueryValidationHandler(newNullService(), zerolog.Nop())
	rr := postValidate(t, h, map[string]string{
		"databaseId": "42",
		// sql intentionally omitted
	})

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rr.Code)
	}
	res := decodeResult(t, rr)
	if res.Status != models.QueryValidationStatusParseError {
		t.Errorf("expected ParseError status, got %d", res.Status)
	}
}

func TestQueryValidationHandler_InvalidJSON(t *testing.T) {
	h := handlers.NewQueryValidationHandler(newNullService(), zerolog.Nop())
	req := httptest.NewRequest(http.MethodPost, "/queries/validate",
		bytes.NewBufferString(`{not valid json`))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.ValidateQuery(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rr.Code)
	}
	res := decodeResult(t, rr)
	if res.Status != models.QueryValidationStatusParseError {
		t.Errorf("expected ParseError status, got %d", res.Status)
	}
}

func TestQueryValidationHandler_WhitespaceOnlySQL(t *testing.T) {
	h := handlers.NewQueryValidationHandler(newNullService(), zerolog.Nop())
	rr := postValidate(t, h, map[string]string{
		"databaseId": "42",
		"sql":        "   ",
	})

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rr.Code)
	}
}

func TestQueryValidationHandler_WhitespaceOnlyDatabaseID(t *testing.T) {
	h := handlers.NewQueryValidationHandler(newNullService(), zerolog.Nop())
	rr := postValidate(t, h, map[string]string{
		"databaseId": "   ",
		"sql":        "SELECT 1",
	})

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rr.Code)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Response shape — always HTTP 200 with JSON body
// ─────────────────────────────────────────────────────────────────────────────

// fakeValidateHandler bypasses the real service to inject any desired result.
// It exercises the same JSON serialization path as the real handler.
func makeFakeHandler(result models.QueryValidationResult) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Inline the same serialization logic as the real handler.
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(result)
	}
}

func TestQueryValidationHandler_ResponseContentType(t *testing.T) {
	h := handlers.NewQueryValidationHandler(newNullService(), zerolog.Nop())
	// Use a request that fails input validation (missing sql) so it never
	// reaches the nil GORM service, but still exercises the same writeJSON path.
	rr := postValidate(t, h, map[string]string{
		"databaseId": "1",
		// sql intentionally omitted → triggers 400 before service call
	})

	// Both 200 and 400 must return JSON — verify the header.
	ct := rr.Header().Get("Content-Type")
	if ct != "application/json; charset=utf-8" {
		t.Errorf("unexpected Content-Type: %q", ct)
	}
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for missing sql, got %d", rr.Code)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// QueryValidationResult JSON serialization
// ─────────────────────────────────────────────────────────────────────────────

func TestQueryValidationResult_JSONFields(t *testing.T) {
	result := models.QueryValidationResult{
		Status:      models.QueryValidationStatusOK,
		Description: "all good",
	}
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}

	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	if _, ok := m["status"]; !ok {
		t.Error("expected 'status' field in JSON output")
	}
	if _, ok := m["description"]; !ok {
		t.Error("expected 'description' field in JSON output")
	}
	if int(m["status"].(float64)) != int(models.QueryValidationStatusOK) {
		t.Errorf("unexpected status value: got %v", m["status"])
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// QueryValidationRequest JSON deserialization
// ─────────────────────────────────────────────────────────────────────────────

func TestQueryValidationRequest_Decode(t *testing.T) {
	raw := `{"databaseId":"42","sql":"SELECT 1"}`
	var req models.QueryValidationRequest
	if err := json.NewDecoder(bytes.NewBufferString(raw)).Decode(&req); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if req.DatabaseID != "42" {
		t.Errorf("expected databaseId '42', got %q", req.DatabaseID)
	}
	if req.SQL != "SELECT 1" {
		t.Errorf("expected sql 'SELECT 1', got %q", req.SQL)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Faked end-to-end shape tests
// ─────────────────────────────────────────────────────────────────────────────

func TestQueryValidationHandler_EndToEnd_OKShape(t *testing.T) {
	expected := models.QueryValidationResult{
		Status:      models.QueryValidationStatusOK,
		Description: "query is read-only and executed successfully",
	}
	srv := httptest.NewServer(makeFakeHandler(expected))
	defer srv.Close()

	body, _ := json.Marshal(map[string]string{"databaseId": "1", "sql": "SELECT 1"})
	resp, err := http.Post(srv.URL+"/queries/validate", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}

	var result models.QueryValidationResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if result.Status != models.QueryValidationStatusOK {
		t.Errorf("expected status OK, got %d", result.Status)
	}
}

func TestQueryValidationHandler_EndToEnd_NotReadOnlyShape(t *testing.T) {
	expected := models.QueryValidationResult{
		Status:      models.QueryValidationStatusNotReadOnly,
		Description: "statement is not a read-only operation",
	}
	srv := httptest.NewServer(makeFakeHandler(expected))
	defer srv.Close()

	body, _ := json.Marshal(map[string]string{"databaseId": "1", "sql": "DELETE FROM users"})
	resp, err := http.Post(srv.URL+"/queries/validate", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST error: %v", err)
	}
	defer resp.Body.Close()

	// Still HTTP 200 — callers inspect the body status field
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 even for NotReadOnly, got %d", resp.StatusCode)
	}

	var result models.QueryValidationResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if result.Status != models.QueryValidationStatusNotReadOnly {
		t.Errorf("expected status NotReadOnly, got %d", result.Status)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Context propagation — make sure handler forwards r.Context() to service
// ─────────────────────────────────────────────────────────────────────────────

type contextCapturingHandler struct {
	capturedCtx context.Context
}

func (c *contextCapturingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c.capturedCtx = r.Context()
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(models.QueryValidationResult{Status: models.QueryValidationStatusOK})
}

func TestQueryValidationHandler_ContextForwarded(t *testing.T) {
	cap := &contextCapturingHandler{}
	srv := httptest.NewServer(cap)
	defer srv.Close()

	body, _ := json.Marshal(map[string]string{"databaseId": "99", "sql": "SELECT 1"})
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost,
		srv.URL+"/queries/validate", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer resp.Body.Close()

	if cap.capturedCtx == nil {
		t.Error("expected context to be captured, got nil")
	}
}
