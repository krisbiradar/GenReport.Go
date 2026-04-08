package models

import (
	"time"

	"github.com/lib/pq"
)

// Query maps to the "queries" table.
type Query struct {
	ID              int64          `gorm:"column:id;primaryKey"`
	Rawtext         string         `gorm:"column:rawtext"`
	DatabaseID      int64          `gorm:"column:database_id"`
	CreatedByID     int64          `gorm:"column:created_by_id"`
	CollectionID    *int64         `gorm:"column:collection_id"`
	InvolvedColumns pq.StringArray `gorm:"column:involved_columns;type:text[]"`
	InvolvedTables  pq.StringArray `gorm:"column:involved_tables;type:text[]"`
	Comments        pq.StringArray `gorm:"column:comments;type:text[]"`
	CreatedAt       time.Time      `gorm:"column:created_at"`
	UpdatedAt       time.Time      `gorm:"column:updated_at"`
	Database        *Database      `gorm:"foreignKey:DatabaseID"`
	CreatedBy       *User          `gorm:"foreignKey:CreatedByID"`
}

func (Query) TableName() string { return "queries" }
