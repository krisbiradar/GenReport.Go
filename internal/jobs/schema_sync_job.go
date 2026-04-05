package jobs

import (
	"context"
	"fmt"

	"genreport/internal/broker"
	"genreport/internal/database"
	"genreport/internal/models"
	"genreport/internal/services"

	"github.com/rs/zerolog"
	"gorm.io/gorm"
)

// SchemaSyncJob iterates through all stored databases across all supported providers,
// extracts their schema text (tables, views, SPs, functions) and persists them into
// the central database. Embeddings are left nil — run GenerateEmbeddingsJob separately.
func SchemaSyncJob(producer *broker.Producer, logger zerolog.Logger) error {
	logger.Info().Msg("Starting SchemaSyncJob")
	ctx := context.Background()

	// 1. Get the local database connection
	gormDB := database.GetDB()
	if gormDB == nil {
		err := fmt.Errorf("failed to get gorm connection")
		logger.Error().Err(err).Msg("Aborting SchemaSyncJob")
		return err
	}

	// 2. Fetch all shared databases
	var dbList []models.Database
	if err := gormDB.Find(&dbList).Error; err != nil {
		logger.Error().Err(err).Msg("Failed to fetch databases")
		return fmt.Errorf("failed to fetch databases: %w", err)
	}

	// 3. Process each database
	var lastErr error
	for _, dbRecord := range dbList {
		if err := syncDatabaseSchema(ctx, gormDB, dbRecord, logger); err != nil {
			lastErr = err
		}
	}

	if lastErr != nil {
		logger.Error().Err(lastErr).Msg("SchemaSyncJob completed with errors")
		return lastErr
	}

	logger.Info().Msg("Completed SchemaSyncJob")
	return nil
}

func syncDatabaseSchema(
	ctx context.Context,
	gormDB *gorm.DB,
	dbRecord models.Database,
	logger zerolog.Logger,
) error {
	log := logger.With().Str("db_name", dbRecord.Name).Str("provider", fmt.Sprintf("%d", dbRecord.Provider)).Logger()
	log.Info().Msg("Syncing database schema")

	// Get extractor for this provider
	extractor, err := services.GetExtractorForProvider(dbRecord.Provider)
	if err != nil {
		log.Error().Err(err).Msg("Failed to get schema extractor")
		return err
	}

	// Extract schema metadata
	schemas, routines, err := extractor.Extract(ctx, dbRecord.ConnectionString)
	if err != nil {
		log.Error().Err(err).Msg("Failed to extract schema metadata")
		return err
	}

	// Map to GORM objects — embeddings are always nil here
	var schemaObjects []models.SchemaObject
	for _, sm := range schemas {
		text := sm.SchemaText
		schemaObjects = append(schemaObjects, models.SchemaObject{
			DatabaseID:    dbRecord.ID,
			Name:          sm.Name,
			Type:          sm.Type,
			FullSchema:    &text,
			EmbeddingText: &text,
			Embedding:     nil,
		})
	}

	var routineObjects []models.RoutineObject
	for _, rm := range routines {
		text := rm.RoutineText
		routineObjects = append(routineObjects, models.RoutineObject{
			DatabaseID:    dbRecord.ID,
			Name:          rm.Name,
			Type:          rm.Type,
			FullSchema:    &text,
			EmbeddingText: &text,
			Embedding:     nil,
		})
	}

	// Transactionally replace existing schema records for this database
	err = gormDB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("database_id = ?", dbRecord.ID).Delete(&models.SchemaObject{}).Error; err != nil {
			return fmt.Errorf("failed to delete old schema objects: %w", err)
		}
		if err := tx.Where("database_id = ?", dbRecord.ID).Delete(&models.RoutineObject{}).Error; err != nil {
			return fmt.Errorf("failed to delete old routine objects: %w", err)
		}

		if len(schemaObjects) > 0 {
			if err := tx.CreateInBatches(schemaObjects, 100).Error; err != nil {
				return fmt.Errorf("failed to insert schema objects: %w", err)
			}
		}
		if len(routineObjects) > 0 {
			if err := tx.CreateInBatches(routineObjects, 100).Error; err != nil {
				return fmt.Errorf("failed to insert routine objects: %w", err)
			}
		}

		return nil
	})

	if err != nil {
		log.Error().Err(err).Msg("Transaction failed for schema sync")
		return err
	}

	log.Info().
		Int("schemas", len(schemaObjects)).
		Int("routines", len(routineObjects)).
		Msg("Successfully synchronized schema (embeddings pending)")
	return nil
}
