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

// SchemaSyncJob iterates through all stored databases, extracts their schemas/routines,
// generates their vector embeddings, and persists them into the central database.
func SchemaSyncJob(producer *broker.Producer, logger zerolog.Logger) {
	logger.Info().Msg("Starting SchemaSyncJob")
	ctx := context.Background()

	// 1. Get the local database connection
	gormDB := database.GetDB()
	if gormDB == nil {
		logger.Error().Msg("Failed to get gorm connection; aborting SchemaSyncJob")
		return
	}

	// 2. Load the active AI connection for embeddings
	var activeAiConn models.AiConnection
	err := gormDB.Where("is_active = ?", true).First(&activeAiConn).Error
	var embeddingService *services.EmbeddingService
	if err == nil {
		embeddingService = services.NewEmbeddingService(&activeAiConn)
		logger.Info().Str("provider", activeAiConn.Provider).Msg("Loaded AI connection for embedding")
	} else {
		logger.Warn().Err(err).Msg("No active AI connection found; will skip vector embedding generation")
	}

	// 3. Fetch all shared databases
	var dbList []models.Database
	if err := gormDB.Find(&dbList).Error; err != nil {
		logger.Error().Err(err).Msg("Failed to fetch databases")
		return
	}

	// 4. Process each database
	for _, dbRecord := range dbList {
		processDatabase(ctx, gormDB, embeddingService, dbRecord, logger)
	}

	logger.Info().Msg("Completed SchemaSyncJob")
}

func processDatabase(
	ctx context.Context, 
	gormDB *gorm.DB, 
	embedService *services.EmbeddingService, 
	dbRecord models.Database, 
	logger zerolog.Logger,
) {
	log := logger.With().Str("db_name", dbRecord.Name).Str("provider", fmt.Sprintf("%d", dbRecord.Provider)).Logger()
	log.Info().Msg("Processing database schema sync")

	// Get extractor
	extractor, err := services.GetExtractorForProvider(dbRecord.Provider)
	if err != nil {
		log.Error().Err(err).Msg("Failed to get schema extractor")
		return
	}

	// Extract
	schemas, routines, err := extractor.Extract(ctx, dbRecord.ConnectionString)
	if err != nil {
		log.Error().Err(err).Msg("Failed to extract schema metadata")
		return
	}

	// Process Embeddings and Map to GORM objects
	var schemaObjects []models.SchemaObject
	for _, sm := range schemas {
		obj := models.SchemaObject{
			DatabaseID: dbRecord.ID,
			Name:       sm.Name,
			Type:       sm.Type,
		}
		
		text := sm.SchemaText
		obj.FullSchema = &text
		obj.EmbeddingText = &text

		if embedService != nil {
			vec, err := embedService.GenerateEmbedding(ctx, sm.SchemaText)
			if err != nil {
				log.Warn().Err(err).Str("schema", sm.Name).Msg("Failed to generate embedding")
			} else {
				// Convert to string format used by pgvector: [0.1, 0.2, ...]
				vecBytes, _ := json.Marshal(vec)
				vecStr := string(vecBytes)
				obj.Embedding = &vecStr
			}
		}
		schemaObjects = append(schemaObjects, obj)
	}

	var routineObjects []models.RoutineObject
	for _, rm := range routines {
		obj := models.RoutineObject{
			DatabaseID: dbRecord.ID,
			Name:       rm.Name,
			Type:       rm.Type,
		}

		text := rm.RoutineText
		obj.FullSchema = &text
		obj.EmbeddingText = &text

		if embedService != nil {
			vec, err := embedService.GenerateEmbedding(ctx, rm.RoutineText)
			if err != nil {
				log.Warn().Err(err).Str("routine", rm.Name).Msg("Failed to generate embedding")
			} else {
				vecBytes, _ := json.Marshal(vec)
				vecStr := string(vecBytes)
				obj.Embedding = &vecStr
			}
		}
		routineObjects = append(routineObjects, obj)
	}

	// Begin Transaction to replace schemas
	err = gormDB.Transaction(func(tx *gorm.DB) error {
		// 1. Delete existing records for this DB
		if err := tx.Where("database_id = ?", dbRecord.ID).Delete(&models.SchemaObject{}).Error; err != nil {
			return fmt.Errorf("failed to delete old schema objects: %w", err)
		}
		if err := tx.Where("database_id = ?", dbRecord.ID).Delete(&models.RoutineObject{}).Error; err != nil {
			return fmt.Errorf("failed to delete old routine objects: %w", err)
		}

		// 2. Insert new schema objects
		if len(schemaObjects) > 0 {
			if err := tx.CreateInBatches(schemaObjects, 100).Error; err != nil {
				return fmt.Errorf("failed to insert schema objects: %w", err)
			}
		}

		// 3. Insert new routine objects
		if len(routineObjects) > 0 {
			if err := tx.CreateInBatches(routineObjects, 100).Error; err != nil {
				return fmt.Errorf("failed to insert routine objects: %w", err)
			}
		}

		return nil
	})

	if err != nil {
		log.Error().Err(err).Msg("Transaction failed for database schema sync")
	} else {
		log.Info().
			Int("schemas", len(schemaObjects)).
			Int("routines", len(routineObjects)).
			Msg("Successfully synchronized schemas")
	}
}
