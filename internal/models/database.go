package models

import "time"

// Database maps to the "databases" table.
type Database struct {
	ID                    int64      `gorm:"column:id;primaryKey"`
	Name                  string     `gorm:"column:name;size:255;not null"`
	DatabaseAlias         string     `gorm:"column:database_alias;size:255;not null"`
	Type                  string     `gorm:"column:type;size:50;not null"`
	ConnectionString      string     `gorm:"column:connection_string;not null"`
	ServerAddress         string     `gorm:"column:server_address;size:255"`
	Port                  int        `gorm:"column:port"`
	Username              string     `gorm:"column:username;size:255"`
	Password              string     `gorm:"column:password"`
	CreatedAt             time.Time  `gorm:"column:created_at;not null"`
	UpdatedAt             time.Time  `gorm:"column:updated_at;not null"`
	Status                string     `gorm:"column:status;size:50;not null"`
	Description           string     `gorm:"column:description"`
	SizeInBytes           int64      `gorm:"column:size_in_bytes"`
	BackupSchedule        int        `gorm:"column:backup_schedule"`
	BackupRetentionPolicy *string    `gorm:"column:backup_retention_policy"`
	EncryptionMethod      *string    `gorm:"column:encryption_method"`
	SecurityLevel         *string    `gorm:"column:security_level"`
	Provider              DbProvider `gorm:"column:provider;not null"`
}

func (Database) TableName() string { return "databases" }
