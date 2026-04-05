package models

import "time"

// Report maps to the "reports" table.
type Report struct {
	ID            int64      `gorm:"column:Id;primaryKey"`
	Name          string     `gorm:"column:name"`
	Description   *string    `gorm:"column:description"`
	QueryID       int64      `gorm:"column:query_id"`
	MediaFileID   int64      `gorm:"column:mediafile_id"`
	CreatedAt     time.Time  `gorm:"column:created_at"`
	UpdatedAt     time.Time  `gorm:"column:updated_at"`
	NoOfRows      int        `gorm:"column:no_of_rows"`
	NoOfColumns   int        `gorm:"column:no_of_columns"`
	TimeInSeconds int        `gorm:"column:time_in_seconds"`
	Query         *Query     `gorm:"foreignKey:QueryID"`
	MediaFile     *MediaFile `gorm:"foreignKey:MediaFileID"`
}

func (Report) TableName() string { return "reports" }
