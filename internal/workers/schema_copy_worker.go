package workers

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/rs/zerolog"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"genreport/internal/broker"
	"genreport/internal/config"
	"genreport/internal/models"
	"genreport/internal/security"
)

// HandleSchemaCopy creates a handler for duplicating database schemas into [DbName]_blank schemas.
func HandleSchemaCopy(cfg config.Config, logger zerolog.Logger, db *gorm.DB) broker.JobHandler {
	return func(payload []byte) error {
		ctx := context.Background()
		logger.Info().Msg("starting schema copy job")

		masterKey := cfg.EncryptionMasterKey
		if masterKey == "" {
			logger.Error().Msg("EncryptionMasterKey is not configured. Cannot run schema copy.")
			return fmt.Errorf("EncryptionMasterKey configuration is not set")
		}

		var databases []models.Database
		if err := db.Find(&databases).Error; err != nil {
			logger.Error().Err(err).Msg("failed to query databases table")
			return err
		}

		for _, database := range databases {
			if err := processDatabase(ctx, logger, &database, masterKey); err != nil {
				logger.Error().Err(err).Int64("database_id", database.ID).Str("db_name", database.Name).Msg("failed to process database schema copy")
			}
		}

		logger.Info().Msg("finished schema copy job")
		return nil
	}
}

func processDatabase(ctx context.Context, logger zerolog.Logger, db *models.Database, masterKey string) error {
	logger.Info().Str("db_name", db.Name).Int("provider", int(db.Provider)).Msg("processing schema copy")

	// Decrypt the password
	// Assuming "Password" is the string variant from C# CredentialType.Password
	decryptedPassword, err := security.Decrypt(db.Password, "Password", masterKey)
	if err != nil {
		return fmt.Errorf("failed to decrypt database password: %w", err)
	}

	blankDbName := db.Name + "_blank"

	switch db.Provider {
	case models.DbProviderNpgSql:
		return copyPostgresSchema(logger, db, decryptedPassword, blankDbName)

	case models.DbProviderMySqlConnector:
		return copyMysqlSchema(logger, db, decryptedPassword, blankDbName)

	case models.DbProviderSqlClient, models.DbProviderOracle, models.DbProviderMongoClient:
		logger.Warn().
			Int("provider", int(db.Provider)).
			Str("db_name", db.Name).
			Msg("schema copying for this provider is currently unsupported or requires complex utility wrappers. Skipping.")
		return nil

	default:
		logger.Warn().Int("provider", int(db.Provider)).Msg("unknown database provider. Skipping.")
		return nil
	}
}

// copyPostgresSchema creates a blank PG database and uses pg_dump/psql to pipe the schema into it.
func copyPostgresSchema(logger zerolog.Logger, db *models.Database, password, blankDbName string) error {
	if _, err := exec.LookPath("pg_dump"); err != nil {
		return fmt.Errorf("pg_dump is not installed on the system, cannot copy schema")
	}
	if _, err := exec.LookPath("psql"); err != nil {
		return fmt.Errorf("psql is not installed on the system, cannot copy schema")
	}

	// Connect to default ("postgres") database to execute CREATE DATABASE
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s sslmode=disable",
		db.ServerAddress, db.Port, db.Username, password)

	gormDb, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return fmt.Errorf("failed to open connection to target postgres server: %w", err)
	}

	// Create new database. GORM doesn't support parameterized db names here nicely so we just concat (assuming safe internal names)
	err = gormDb.Exec(fmt.Sprintf(`CREATE DATABASE "%s";`, blankDbName)).Error
	
	// If it fails, it might already exist. We could log and continue, but let's assume if it exists we might want to drop or ignore it.
	// For now, if we fail to create (e.g., exists), we just log it and try applying schema anyway (it might fail if tables exist, but safe).
	if err != nil {
		logger.Warn().Err(err).Msgf("CREATE DATABASE %s encountered an error or already exists", blankDbName)
	} else {
		logger.Info().Msgf("created blank database %s", blankDbName)
	}

	// Pipe pg_dump -s to psql
	cmdDump := exec.Command("pg_dump", "-h", db.ServerAddress, "-p", strconv.Itoa(db.Port), "-U", db.Username, "-s", "-w", db.Name)
	cmdDump.Env = append(os.Environ(), "PGPASSWORD="+password)

	cmdRestore := exec.Command("psql", "-h", db.ServerAddress, "-p", strconv.Itoa(db.Port), "-U", db.Username, "-w", "-d", blankDbName)
	cmdRestore.Env = append(os.Environ(), "PGPASSWORD="+password)

	pipeOut, err := cmdDump.StdoutPipe()
	if err != nil {
		return err
	}
	cmdRestore.Stdin = pipeOut

	if err := cmdDump.Start(); err != nil {
		return fmt.Errorf("failed to start pg_dump: %w", err)
	}
	if err := cmdRestore.Start(); err != nil {
		return fmt.Errorf("failed to start psql restore: %w", err)
	}

	if err := cmdDump.Wait(); err != nil {
		logger.Warn().Err(err).Msg("pg_dump completed with warning/error")
	}
	if err := cmdRestore.Wait(); err != nil {
		return fmt.Errorf("psql restore failed: %w", err)
	}

	logger.Info().Msgf("successfully copied schema from %s to %s via pg_dump", db.Name, blankDbName)
	return nil
}

// copyMysqlSchema creates a blank MySQL database and uses mysqldump/mysql to pipe the schema into it.
func copyMysqlSchema(logger zerolog.Logger, db *models.Database, password, blankDbName string) error {
	if _, err := exec.LookPath("mysqldump"); err != nil {
		return fmt.Errorf("mysqldump is not installed on the system, cannot copy schema")
	}
	if _, err := exec.LookPath("mysql"); err != nil {
		return fmt.Errorf("mysql cli is not installed on the system, cannot copy schema")
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?parseTime=true", db.Username, password, db.ServerAddress, db.Port)
	sqlDb, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to open connection to target mysql server: %w", err)
	}
	defer sqlDb.Close()

	// Setting short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = sqlDb.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", blankDbName))
	if err != nil {
		return fmt.Errorf("failed to create blank mysqldb %s: %w", blankDbName, err)
	}

	logger.Info().Msgf("created/verified blank database %s", blankDbName)

	passArg := fmt.Sprintf("-p%s", password)
	cmdDump := exec.Command("mysqldump", "-h", db.ServerAddress, "-P", strconv.Itoa(db.Port), "-u", db.Username, passArg, "--no-data", db.Name, "--routines", "--triggers")
	cmdRestore := exec.Command("mysql", "-h", db.ServerAddress, "-P", strconv.Itoa(db.Port), "-u", db.Username, passArg, blankDbName)

	pipeOut, err := cmdDump.StdoutPipe()
	if err != nil {
		return err
	}
	cmdRestore.Stdin = pipeOut

	if err := cmdDump.Start(); err != nil {
		return fmt.Errorf("failed to start mysqldump: %w", err)
	}
	if err := cmdRestore.Start(); err != nil {
		return fmt.Errorf("failed to start mysql restore: %w", err)
	}

	if err := cmdDump.Wait(); err != nil {
		logger.Warn().Err(err).Msg("mysqldump completed with warning/error")
	}
	if err := cmdRestore.Wait(); err != nil {
		return fmt.Errorf("mysql restore failed: %w", err)
	}

	logger.Info().Msgf("successfully copied schema from %s to %s via mysqldump", db.Name, blankDbName)
	return nil
}
