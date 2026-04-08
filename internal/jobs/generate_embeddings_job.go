package jobs

import (
	"context"
	"encoding/json"
	"fmt"

	"genreport/internal/broker"
	"genreport/internal/database"
	"genreport/internal/models"
	"genreport/internal/services"

	"github.com/rs/zerolog"
	"gorm.io/gorm"
)

// GenerateEmbeddingsJob reads all schema and routine objects and (re)generates their
// vector embeddings using the local Ollama nomic-embed-text model (768-dim).
// Embedding generation is always local — no OpenAI calls, no cost.
// Results are stored in the embedding_ollama column.
//
// This job is intentionally separate from SchemaSyncJob so that schema structure
// and embeddings can be scheduled and monitored independently.
func GenerateEmbeddingsJob(producer *broker.Producer, logger zerolog.Logger) error {
	logger.Info().Msg("Starting GenerateEmbeddingsJob")
	ctx := context.Background()

	// 1. Get database connection
	gormDB := database.GetDB()
	if gormDB == nil {
		err := fmt.Errorf("failed to get gorm connection")
		logger.Error().Err(err).Msg("Aborting GenerateEmbeddingsJob")
		return err
	}

	// 2. Always use local Ollama (nomic-embed-text, 768-dim) — zero cost, no API key needed.
	//    Results go into the embedding_ollama column.
	embedService := services.NewOllamaEmbeddingService("", "") // defaults: localhost:11434, nomic-embed-text
	const embeddingCol = "embedding_ollama"
	logger.Info().Str("model", "nomic-embed-text").Str("column", embeddingCol).Msg("Using local Ollama for embeddings")

	// 3. Generate embeddings for schema objects (all rows — overwrite existing)
	schemaUpdated, schemaFailed := generateSchemaEmbeddings(ctx, gormDB, embedService, embeddingCol, logger)

	// 4. Generate embeddings for routine objects (all rows — overwrite existing)
	routineUpdated, routineFailed := generateRoutineEmbeddings(ctx, gormDB, embedService, embeddingCol, logger)

	logger.Info().
		Int("schema_updated", schemaUpdated).
		Int("schema_failed", schemaFailed).
		Int("routine_updated", routineUpdated).
		Int("routine_failed", routineFailed).
		Msg("Completed GenerateEmbeddingsJob")

	if schemaFailed+routineFailed > 0 {
		return fmt.Errorf("%d schema and %d routine embedding(s) failed to generate", schemaFailed, routineFailed)
	}
	return nil
}

func generateSchemaEmbeddings(
	ctx context.Context,
	gormDB *gorm.DB,
	embedService *services.OllamaEmbeddingService,
	embeddingCol string,
	logger zerolog.Logger,
) (updated, failed int) {
	var objects []models.SchemaObject
	if err := gormDB.Find(&objects).Error; err != nil {
		logger.Error().Err(err).Msg("Failed to fetch schema objects for embedding")
		return 0, 1
	}

	for _, obj := range objects {
		if obj.EmbeddingText == nil || *obj.EmbeddingText == "" {
			logger.Warn().
				Int64("id", obj.ID).
				Str("name", obj.Name).
				Msg("Schema object has no embedding text, skipping")
			continue
		}

		vec, err := embedService.GenerateEmbedding(ctx, *obj.EmbeddingText)
		if err != nil {
			logger.Warn().Err(err).
				Int64("id", obj.ID).
				Str("name", obj.Name).
				Msg("Failed to generate embedding for schema object")
			failed++
			continue
		}

		vecBytes, err := json.Marshal(vec)
		if err != nil {
			logger.Warn().Err(err).Int64("id", obj.ID).Str("name", obj.Name).Msg("Failed to marshal embedding vector")
			failed++
			continue
		}
		vecStr := string(vecBytes)
		logger.Info().
			Int64("id", obj.ID).
			Str("name", obj.Name).
			Int("dims", len(vec)).
			Str("sample", vecStr[:min(80, len(vecStr))]).
			Msg("[DEBUG] schema object embedding generated")

		query := fmt.Sprintf(`UPDATE schema_objects SET %s = ? WHERE id = ?`, embeddingCol)
		if err := gormDB.Exec(query, vecStr, obj.ID).Error; err != nil {
			logger.Warn().Err(err).
				Int64("id", obj.ID).
				Str("name", obj.Name).
				Msg("Failed to save embedding for schema object")
			failed++
			continue
		}

		updated++
	}

	logger.Info().Int("updated", updated).Int("failed", failed).Msg("Schema object embeddings processed")
	return updated, failed
}

func generateRoutineEmbeddings(
	ctx context.Context,
	gormDB *gorm.DB,
	embedService *services.OllamaEmbeddingService,
	embeddingCol string,
	logger zerolog.Logger,
) (updated, failed int) {
	var objects []models.RoutineObject
	if err := gormDB.Find(&objects).Error; err != nil {
		logger.Error().Err(err).Msg("Failed to fetch routine objects for embedding")
		return 0, 1
	}

	for _, obj := range objects {
		if obj.EmbeddingText == nil || *obj.EmbeddingText == "" {
			logger.Warn().
				Int64("id", obj.ID).
				Str("name", obj.Name).
				Msg("Routine object has no embedding text, skipping")
			continue
		}

		vec, err := embedService.GenerateEmbedding(ctx, *obj.EmbeddingText)
		if err != nil {
			logger.Warn().Err(err).
				Int64("id", obj.ID).
				Str("name", obj.Name).
				Msg("Failed to generate embedding for routine object")
			failed++
			continue
		}

		vecBytes, err := json.Marshal(vec)
		if err != nil {
			logger.Warn().Err(err).Int64("id", obj.ID).Str("name", obj.Name).Msg("Failed to marshal embedding vector")
			failed++
			continue
		}
		vecStr := string(vecBytes)
		logger.Info().
			Int64("id", obj.ID).
			Str("name", obj.Name).
			Int("dims", len(vec)).
			Str("sample", vecStr[:min(80, len(vecStr))]).
			Msg("[DEBUG] routine object embedding generated")

		query := fmt.Sprintf(`UPDATE routine_objects SET %s = ? WHERE id = ?`, embeddingCol)
		if err := gormDB.Exec(query, vecStr, obj.ID).Error; err != nil {
			logger.Warn().Err(err).
				Int64("id", obj.ID).
				Str("name", obj.Name).
				Msg("Failed to save embedding for routine object")
			failed++
			continue
		}

		updated++
	}

	logger.Info().Int("updated", updated).Int("failed", failed).Msg("Routine object embeddings processed")
	return updated, failed
}
