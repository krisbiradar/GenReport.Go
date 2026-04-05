package services

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"genreport/internal/models"
)

// ─────────────────────────────────────────────────────────────────────────────
// OpenAI Embedding Service
// ─────────────────────────────────────────────────────────────────────────────

// EmbeddingService generates vector embeddings via the OpenAI API.
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

// GenerateEmbedding generates a 1536-dim vector embedding via OpenAI.
func (s *EmbeddingService) GenerateEmbedding(ctx context.Context, text string) ([]float64, error) {
	if s.aiConn == nil || s.aiConn.ApiKey == "" {
		return nil, errors.New("no valid AI connection provided for embedding")
	}

	// Truncate text if it's too large (OpenAI max is ~30k chars)
	if len(text) > 30000 {
		text = text[:30000]
	}

	model := s.aiConn.DefaultModel
	if model == "" {
		model = "text-embedding-3-small"
	}

	reqBody := openAIEmbeddingRequest{
		Input: text,
		Model: model,
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal embedding request: %w", err)
	}

	url := "https://api.openai.com/v1/embeddings"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create embedding request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+s.aiConn.ApiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	bodyData, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("embedding API error (status %d): %s", resp.StatusCode, string(bodyData))
	}

	var embedResp openAIEmbeddingResponse
	if err := json.Unmarshal(bodyData, &embedResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	if embedResp.Error != nil {
		return nil, fmt.Errorf("API error: %s", embedResp.Error.Message)
	}
	if len(embedResp.Data) == 0 {
		return nil, errors.New("no embedding data returned")
	}

	return embedResp.Data[0].Embedding, nil
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
