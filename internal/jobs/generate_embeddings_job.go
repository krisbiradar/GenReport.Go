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
// vector embeddings using the active AI connection. This job is intentionally separate
// from SchemaSyncJob so that schema structure and embeddings can be scheduled and
// monitored independently.
//
// This job overwrites all existing embeddings on every run.
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

	// 2. Load active AI connection — required for this job
	var activeAiConn models.AiConnection
	if err := gormDB.Where("is_active = ?", true).First(&activeAiConn).Error; err != nil {
		err = fmt.Errorf("no active AI connection found: %w", err)
		logger.Error().Err(err).Msg("Cannot generate embeddings without an active AI connection")
		return err
	}

	embedService := services.NewEmbeddingService(&activeAiConn)
	logger.Info().Str("provider", activeAiConn.Provider).Msg("Loaded AI connection for embedding")

	// 3. Generate embeddings for schema objects (all rows — overwrite existing)
	schemaUpdated, schemaFailed := generateSchemaEmbeddings(ctx, gormDB, embedService, logger)

	// 4. Generate embeddings for routine objects (all rows — overwrite existing)
	routineUpdated, routineFailed := generateRoutineEmbeddings(ctx, gormDB, embedService, logger)

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
	embedService *services.EmbeddingService,
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

		if err := gormDB.Model(&models.SchemaObject{}).
			Where("id = ?", obj.ID).
			Update("embedding", vecStr).Error; err != nil {
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
	embedService *services.EmbeddingService,
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

		if err := gormDB.Model(&models.RoutineObject{}).
			Where("id = ?", obj.ID).
			Update("embedding", vecStr).Error; err != nil {
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
