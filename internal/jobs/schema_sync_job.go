package jobs

import (
	"context"
	"encoding/json"
	"fmt"

	"genreport/internal/broker"
	"genreport/internal/config"
	"genreport/internal/database"
	"genreport/internal/models"
	"genreport/internal/services"

	"github.com/rs/zerolog"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// SchemaSyncJob iterates through all stored databases, extracts their schemas/routines,
// generates vector embeddings using both OpenAI (if configured) and Ollama (local),
// and persists them into the central database via upsert.
func SchemaSyncJob(producer *broker.Producer, logger zerolog.Logger, cfg config.Config) error {
	logger.Info().Msg("Starting SchemaSyncJob")
	ctx := context.Background()

	// 1. Get the local database connection
	gormDB := database.GetDB()
	if gormDB == nil {
		return fmt.Errorf("failed to get gorm connection; aborting SchemaSyncJob")
	}

	// 2. Load the active AI connection for OpenAI embeddings (optional)
	var openAISvc *services.EmbeddingService
	var activeAiConn models.AiConnection
	if err := gormDB.Where("is_active = ?", true).First(&activeAiConn).Error; err == nil {
		openAISvc = services.NewEmbeddingService(&activeAiConn)
		logger.Info().Str("provider", activeAiConn.Provider).Msg("Loaded AI connection for OpenAI embedding")
	} else {
		logger.Warn().Err(err).Msg("No active AI connection found; OpenAI embeddings will be skipped")
	}

	// 3. Instantiate Ollama embedding service (always attempted — local instance)
	ollamaSvc := services.NewOllamaEmbeddingService(cfg.Ollama.BaseURL, cfg.Ollama.EmbeddingModel)
	logger.Info().
		Str("baseURL", cfg.Ollama.BaseURL).
		Str("model", cfg.Ollama.EmbeddingModel).
		Msg("Ollama embedding service initialized")

	// 4. Fetch all shared databases
	var dbList []models.Database
	if err := gormDB.Find(&dbList).Error; err != nil {
		return fmt.Errorf("failed to fetch databases: %w", err)
	}

	// 5. Process each database
	var lastErr error
	for _, dbRecord := range dbList {
		if err := processDatabase(ctx, gormDB, openAISvc, ollamaSvc, dbRecord, logger); err != nil {
			logger.Error().Err(err).Str("db_name", dbRecord.Name).Msg("Failed to process database")
			lastErr = err
		}
	}

	logger.Info().Msg("Completed SchemaSyncJob")
	return lastErr
}

func processDatabase(
	ctx context.Context,
	gormDB *gorm.DB,
	openAISvc *services.EmbeddingService,
	ollamaSvc *services.OllamaEmbeddingService,
	dbRecord models.Database,
	logger zerolog.Logger,
) error {
	log := logger.With().Str("db_name", dbRecord.Name).Str("provider", fmt.Sprintf("%d", dbRecord.Provider)).Logger()
	log.Info().Msg("Processing database schema sync")

	// Get extractor
	extractor, err := services.GetExtractorForProvider(dbRecord.Provider)
	if err != nil {
		return fmt.Errorf("failed to get schema extractor: %w", err)
	}

	// Extract schema metadata
	schemas, routines, err := extractor.Extract(ctx, dbRecord.ConnectionString)
	if err != nil {
		return fmt.Errorf("failed to extract schema metadata: %w", err)
	}

	// ── Build schema objects ──────────────────────────────────────────────────
	schemaObjects := make([]models.SchemaObject, 0, len(schemas))
	for _, sm := range schemas {
		text := sm.SchemaText
		obj := models.SchemaObject{
			DatabaseID:    dbRecord.ID,
			Name:          sm.Name,
			Type:          sm.Type,
			FullSchema:    &text,
			EmbeddingText: &text,
		}

		// OpenAI embedding (vector 1536)
		if openAISvc != nil {
			vec, err := openAISvc.GenerateEmbedding(ctx, sm.SchemaText)
			if err != nil {
				log.Warn().Err(err).Str("schema", sm.Name).Msg("OpenAI embedding failed for schema object")
			} else {
				vecStr := vecToString(vec)
				obj.Embedding = &vecStr
			}
		}

		// Ollama embedding (vector 768)
		vec, err := ollamaSvc.GenerateEmbedding(ctx, sm.SchemaText)
		if err != nil {
			log.Warn().Err(err).Str("schema", sm.Name).Msg("Ollama embedding failed for schema object")
		} else {
			vecStr := vecToString(vec)
			obj.EmbeddingOllama = &vecStr
		}

		schemaObjects = append(schemaObjects, obj)
	}

	// ── Build routine objects ─────────────────────────────────────────────────
	routineObjects := make([]models.RoutineObject, 0, len(routines))
	for _, rm := range routines {
		text := rm.RoutineText
		obj := models.RoutineObject{
			DatabaseID:    dbRecord.ID,
			Name:          rm.Name,
			Type:          rm.Type,
			FullSchema:    &text,
			EmbeddingText: &text,
		}

		// OpenAI embedding (vector 1536)
		if openAISvc != nil {
			vec, err := openAISvc.GenerateEmbedding(ctx, rm.RoutineText)
			if err != nil {
				log.Warn().Err(err).Str("routine", rm.Name).Msg("OpenAI embedding failed for routine object")
			} else {
				vecStr := vecToString(vec)
				obj.Embedding = &vecStr
			}
		}

		// Ollama embedding (vector 768)
		vec, err := ollamaSvc.GenerateEmbedding(ctx, rm.RoutineText)
		if err != nil {
			log.Warn().Err(err).Str("routine", rm.Name).Msg("Ollama embedding failed for routine object")
		} else {
			vecStr := vecToString(vec)
			obj.EmbeddingOllama = &vecStr
		}

		routineObjects = append(routineObjects, obj)
	}

	// ── Upsert in a single transaction ───────────────────────────────────────
	// ON CONFLICT (database_id, name, type) → update embedding columns + text
	err = gormDB.Transaction(func(tx *gorm.DB) error {
		if len(schemaObjects) > 0 {
			result := tx.Clauses(clause.OnConflict{
				Columns: []clause.Column{
					{Name: "database_id"},
					{Name: "name"},
					{Name: "type"},
				},
				DoUpdates: clause.AssignmentColumns([]string{
					"embedding",
					"embedding_ollama",
					"embedding_text",
					"full_schema",
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
					"embedding",
					"embedding_ollama",
					"embedding_text",
					"full_schema",
				}),
			}).CreateInBatches(routineObjects, 100)
			if result.Error != nil {
				return fmt.Errorf("failed to upsert routine objects: %w", result.Error)
			}
		}

		return nil
	})

	if err != nil {
		log.Error().Err(err).Msg("Transaction failed for database schema sync")
		return err
	}

	log.Info().
		Int("schemas", len(schemaObjects)).
		Int("routines", len(routineObjects)).
		Msg("Successfully synchronized schemas")

	return nil
}

// vecToString converts a float64 slice to a pgvector-compatible string: [0.1,0.2,...]
func vecToString(vec []float64) string {
	b, _ := json.Marshal(vec)
	return string(b)
}
