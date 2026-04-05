package services

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"genreport/internal/models"
)

// Default embedding endpoints per provider.
// Overridden when AiConnection.BaseUrl is set.
const (
	defaultOpenAIBaseURL = "https://api.openai.com"
	defaultOllamaBaseURL = "http://localhost:11434"
)

// embeddingEndpoint returns the full URL for the embedding API call,
// based on the provider and the optional BaseUrl stored on the connection.
func embeddingEndpoint(aiConn *models.AiConnection) string {
	base := resolveBaseURL(aiConn)
	// Strip trailing slash for consistent joining
	base = strings.TrimRight(base, "/")

	switch strings.ToLower(aiConn.Provider) {
	case "ollama":
		return base + "/api/embeddings"
	default:
		// OpenAI-compatible (openai, azure, etc.)
		return base + "/v1/embeddings"
	}
}

// resolveBaseURL returns the effective base URL for the connection.
// Uses BaseUrl from the DB record if set, otherwise falls back to
// the provider default.
func resolveBaseURL(aiConn *models.AiConnection) string {
	if aiConn.BaseUrl != nil && strings.TrimSpace(*aiConn.BaseUrl) != "" {
		return strings.TrimSpace(*aiConn.BaseUrl)
	}
	switch strings.ToLower(aiConn.Provider) {
	case "ollama":
		return defaultOllamaBaseURL
	default:
		return defaultOpenAIBaseURL
	}
}

// ── Service ─────────────────────────────────────────────────────────────────

type EmbeddingService struct {
	aiConn *models.AiConnection
	client *http.Client
}

func NewEmbeddingService(aiConn *models.AiConnection) *EmbeddingService {
	return &EmbeddingService{
		aiConn: aiConn,
		client: &http.Client{},
	}
}

// ── Request / Response shapes ────────────────────────────────────────────────

// openAIEmbeddingRequest is used for OpenAI-compatible providers.
type openAIEmbeddingRequest struct {
	Input string `json:"input"`
	Model string `json:"model"`
}

type openAIEmbeddingResponse struct {
	Data []struct {
		Embedding []float64 `json:"embedding"`
	} `json:"data"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error"`
}

// ollamaEmbeddingRequest is used for Ollama's /api/embeddings endpoint.
type ollamaEmbeddingRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
}

type ollamaEmbeddingResponse struct {
	Embedding []float64 `json:"embedding"`
}

// ── GenerateEmbedding ────────────────────────────────────────────────────────

// GenerateEmbedding generates a vector embedding for the given text using
// the provider and endpoint configured on the AiConnection.
func (s *EmbeddingService) GenerateEmbedding(ctx context.Context, text string) ([]float64, error) {
	if s.aiConn == nil {
		return nil, errors.New("no AI connection provided for embedding")
	}

	// Truncate to avoid hitting provider token limits
	if len(text) > 30000 {
		text = text[:30000]
	}

	model := s.aiConn.DefaultModel
	url := embeddingEndpoint(s.aiConn)

	switch strings.ToLower(s.aiConn.Provider) {
	case "ollama":
		return s.callOllama(ctx, url, model, text)
	default:
		return s.callOpenAI(ctx, url, model, text)
	}
}

func (s *EmbeddingService) callOpenAI(ctx context.Context, url, model, text string) ([]float64, error) {
	if s.aiConn.ApiKey == "" {
		return nil, errors.New("API key is required for OpenAI-compatible providers")
	}
	if model == "" {
		model = "text-embedding-3-small"
	}

	bodyBytes, err := json.Marshal(openAIEmbeddingRequest{Input: text, Model: model})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal OpenAI embedding request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create OpenAI embedding request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+s.aiConn.ApiKey)
	req.Header.Set("Content-Type", "application/json")

	bodyData, statusCode, err := s.doRequest(req)
	if err != nil {
		return nil, err
	}
	if statusCode != http.StatusOK {
		return nil, fmt.Errorf("OpenAI embedding API error (status %d): %s", statusCode, string(bodyData))
	}

	var resp openAIEmbeddingResponse
	if err := json.Unmarshal(bodyData, &resp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal OpenAI response: %w", err)
	}
	if resp.Error != nil {
		return nil, fmt.Errorf("OpenAI API error: %s", resp.Error.Message)
	}
	if len(resp.Data) == 0 {
		return nil, errors.New("no embedding data returned from OpenAI")
	}
	return resp.Data[0].Embedding, nil
}

func (s *EmbeddingService) callOllama(ctx context.Context, url, model, text string) ([]float64, error) {
	if model == "" {
		model = "nomic-embed-text"
	}

	bodyBytes, err := json.Marshal(ollamaEmbeddingRequest{Model: model, Prompt: text})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal Ollama embedding request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create Ollama embedding request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	// Ollama doesn't require auth, but honour ApiKey if set (e.g. behind a proxy)
	if s.aiConn.ApiKey != "" {
		req.Header.Set("Authorization", "Bearer "+s.aiConn.ApiKey)
	}

	bodyData, statusCode, err := s.doRequest(req)
	if err != nil {
		return nil, err
	}
	if statusCode != http.StatusOK {
		return nil, fmt.Errorf("Ollama embedding API error (status %d): %s", statusCode, string(bodyData))
	}

	var resp ollamaEmbeddingResponse
	if err := json.Unmarshal(bodyData, &resp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal Ollama response: %w", err)
	}
	if len(resp.Embedding) == 0 {
		return nil, errors.New("no embedding vector returned from Ollama")
	}
	return resp.Embedding, nil
}

func (s *EmbeddingService) doRequest(req *http.Request) ([]byte, int, error) {
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	bodyData, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("failed to read response body: %w", err)
	}
	return bodyData, resp.StatusCode, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Ollama Embedding Service
// ─────────────────────────────────────────────────────────────────────────────

// OllamaEmbeddingService generates vector embeddings via a locally-running
// Ollama instance (shipped pre-installed with the bundle).
// Default model: nomic-embed-text (768-dim).
type OllamaEmbeddingService struct {
	baseURL string
	model   string
	client  *http.Client
}

// NewOllamaEmbeddingService creates an OllamaEmbeddingService.
// baseURL defaults to "http://localhost:11434" if empty.
// model defaults to "nomic-embed-text" if empty.
func NewOllamaEmbeddingService(baseURL, model string) *OllamaEmbeddingService {
	if baseURL == "" {
		baseURL = "http://localhost:11434"
	}
	if model == "" {
		model = "nomic-embed-text"
	}
	return &OllamaEmbeddingService{
		baseURL: baseURL,
		model:   model,
		client:  &http.Client{},
	}
}

type ollamaEmbeddingRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
}

type ollamaEmbeddingResponse struct {
	Embedding []float64 `json:"embedding"`
}

// GenerateEmbedding generates a 768-dim vector embedding via Ollama's local API.
func (s *OllamaEmbeddingService) GenerateEmbedding(ctx context.Context, text string) ([]float64, error) {
	// Truncate text to avoid overwhelming the local model
	if len(text) > 30000 {
		text = text[:30000]
	}

	reqBody := ollamaEmbeddingRequest{
		Model:  s.model,
		Prompt: text,
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal ollama embedding request: %w", err)
	}

	url := s.baseURL + "/api/embeddings"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create ollama embedding request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ollama HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	bodyData, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read ollama response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ollama embedding API error (status %d): %s", resp.StatusCode, string(bodyData))
	}

	var embedResp ollamaEmbeddingResponse
	if err := json.Unmarshal(bodyData, &embedResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal ollama response: %w", err)
	}

	if len(embedResp.Embedding) == 0 {
		return nil, errors.New("no embedding data returned from ollama")
	}

	return embedResp.Embedding, nil
}
