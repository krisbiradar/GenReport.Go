package database

import (
	"fmt"

	"github.com/rs/zerolog"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var globalDB *gorm.DB

// GetDB returns the configured global GORM database connection, or nil if not connected.
func GetDB() *gorm.DB {
	return globalDB
}

// Connect opens a GORM connection to the shared PostgreSQL database.
// It does NOT run AutoMigrate — schema ownership belongs to the C# project.
func Connect(dsn string, log zerolog.Logger) (*gorm.DB, error) {
	if dsn == "" {
		return nil, fmt.Errorf("DATABASE_URL is empty; cannot connect to shared database")
	}

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger:                                   logger.Default.LogMode(logger.Silent),
		DisableAutomaticPing:                     false,
		SkipDefaultTransaction:                   true,
		DisableForeignKeyConstraintWhenMigrating: true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to shared database: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to get underlying sql.DB: %w", err)
	}

	if err := sqlDB.Ping(); err != nil {
		return nil, fmt.Errorf("shared database ping failed: %w", err)
	}

	log.Info().Msg("connected to shared PostgreSQL database via GORM (read-only schema)")
	globalDB = db
	return db, nil
}
