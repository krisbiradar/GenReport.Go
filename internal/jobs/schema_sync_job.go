package jobs

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"genreport/internal/broker"
	"genreport/internal/config"
	"genreport/internal/database"
	"genreport/internal/models"
	"genreport/internal/security"
	"genreport/internal/services"

	"github.com/rs/zerolog"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// SchemaSyncJob iterates through all stored databases across all supported providers,
// extracts their schema text (tables, views, SPs, functions) and persists them into
// the central database. Embeddings are left nil — run GenerateEmbeddingsJob separately.
func SchemaSyncJob(cfg config.Config, producer *broker.Producer, logger zerolog.Logger) error {
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

	// 3. Process each database — collect ALL errors, not just the last one
	var errs []error
	for _, dbRecord := range dbList {
		if err := syncDatabaseSchema(ctx, gormDB, dbRecord, logger, cfg.EncryptionMasterKey); err != nil {
			errs = append(errs, fmt.Errorf("db %q (id=%d): %w", dbRecord.Name, dbRecord.ID, err))
		}
	}

	if len(errs) > 0 {
		combined := errors.Join(errs...)
		logger.Error().Err(combined).Int("failed", len(errs)).Int("total", len(dbList)).Msg("SchemaSyncJob completed with errors")
		return combined
	}

	logger.Info().Msg("Completed SchemaSyncJob")
	return nil
}

func syncDatabaseSchema(ctx context.Context, gormDB *gorm.DB, dbRecord models.Database, logger zerolog.Logger, masterKey string) error {
	log := logger.With().Str("db_name", dbRecord.Name).Int("provider", int(dbRecord.Provider)).Logger()
	log.Info().Msg("Syncing database schema")

	// Get extractor for this provider
	extractor, err := services.GetExtractorForProvider(dbRecord.Provider)
	if err != nil {
		log.Error().Err(err).Msg("Failed to get schema extractor")
		return err
	}

	connString := dbRecord.ConnectionString
	if masterKey != "" && len(connString) > 20 && !strings.Contains(connString, "host=") && !strings.Contains(connString, "://") && !strings.Contains(connString, "Server=") {
		// Attempt to decrypt. Try common credential types used in C#
		decrypted := false
		if dec, decErr := security.Decrypt(connString, "ConnectionString", masterKey); decErr == nil && dec != "" {
			connString = dec
			decrypted = true
		} else if dec, decErr := security.Decrypt(connString, "DatabaseConnectionString", masterKey); decErr == nil && dec != "" {
			connString = dec
			decrypted = true
		}
		if !decrypted {
			log.Warn().Msg("Failed to decrypt connection string — proceeding with raw value (may fail)")
		}
	}

	// Extract schema metadata
	schemas, routines, err := extractor.Extract(ctx, connString, log)
	if err != nil {
		log.Error().Err(err).Msg("Failed to extract schema metadata")
		return err
	}

	// Map to GORM objects — embeddings are always nil here.
	// Deduplicate by (name, type): if the same key appears more than once
	// (e.g. overloaded Postgres functions), append a counter suffix so every
	// row has a distinct unique-key and the batch upsert doesn't hit
	// "ON CONFLICT DO UPDATE command cannot affect row a second time" (21000).
	schemaKeyCount := make(map[string]int)
	var schemaObjects []models.SchemaObject
	for _, sm := range schemas {
		text := sm.SchemaText
		key := sm.Name + "\x00" + sm.Type
		schemaKeyCount[key]++
		name := sm.Name
		if schemaKeyCount[key] > 1 {
			name = fmt.Sprintf("%s_%d", sm.Name, schemaKeyCount[key])
		}
		schemaObjects = append(schemaObjects, models.SchemaObject{
			DatabaseID:    dbRecord.ID,
			Name:          name,
			Type:          sm.Type,
			FullSchema:    &text,
			EmbeddingText: &text,
			Embedding:     nil,
		})
	}

	// ── Build routine objects ─────────────────────────────────────────────────
	routineKeyCount := make(map[string]int)
	routineObjects := make([]models.RoutineObject, 0, len(routines))
	for _, rm := range routines {
		text := rm.RoutineText
		key := rm.Name + "\x00" + rm.Type
		routineKeyCount[key]++
		name := rm.Name
		if routineKeyCount[key] > 1 {
			name = fmt.Sprintf("%s_%d", rm.Name, routineKeyCount[key])
		}
		stripped := services.StripRoutineForEmbedding(text)
		routineObjects = append(routineObjects, models.RoutineObject{
			DatabaseID:    dbRecord.ID,
			Name:          name,
			Type:          rm.Type,
			FullSchema:    &text,
			EmbeddingText: &stripped,
			Embedding:     nil,
		})
	}


	// Upsert schema records — no DELETE so row IDs are stable across syncs.
	// The embedding job identifies rows by ID; deleting + re-inserting would
	// assign new IDs and cause UPDATE WHERE "Id" = <old> to silently miss.
	// Embedding columns are excluded from DoUpdates so vectors are preserved.
	err = gormDB.Transaction(func(tx *gorm.DB) error {
		if len(schemaObjects) > 0 {
			result := tx.Clauses(clause.OnConflict{
				Columns: []clause.Column{
					{Name: "database_id"},
					{Name: "name"},
					{Name: "type"},
				},
				DoUpdates: clause.AssignmentColumns([]string{
					"embedding_text",
					"full_schema",
					"updated_at",
				}),
			}).CreateInBatches(schemaObjects, 100)
			if result.Error != nil {
				return fmt.Errorf("failed to upsert schema objects: %w", result.Error)
			}
		}
		if len(routineObjects) > 0 {
			result := tx.Clauses(clause.OnConflict{
				Columns: []clause.Column{
					{Name: "database_id"},
					{Name: "name"},
					{Name: "type"},
				},
				DoUpdates: clause.AssignmentColumns([]string{
					"embedding_text",
					"full_schema",
					"updated_at",
				}),
			}).CreateInBatches(routineObjects, 100)
			if result.Error != nil {
				return fmt.Errorf("failed to upsert routine objects: %w", result.Error)
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
