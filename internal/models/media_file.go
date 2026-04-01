package models

import "time"

// MediaFile maps to the "mediafiles" table.
type MediaFile struct {
	ID         int64     `gorm:"column:id;primaryKey"`
	StorageURL *string   `gorm:"column:storage_url"`
	FileName   string    `gorm:"column:file_name;not null;uniqueIndex:idx_mediafiles_file_name"`
	MimeType   string    `gorm:"column:mime_type;not null"`
	Size       int64     `gorm:"column:size"`
	CreatedAt  time.Time `gorm:"column:created_at"`
	UpdatedAt  time.Time `gorm:"column:updated_at"`
}

func (MediaFile) TableName() string { return "mediafiles" }
