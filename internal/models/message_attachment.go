package models

import "time"

// MessageAttachment maps to the "message_attachments" table.
type MessageAttachment struct {
	ID          int64        `gorm:"column:Id;primaryKey"`
	MessageID   int64        `gorm:"column:message_id"`
	MediaFileID int64        `gorm:"column:media_file_id"`
	CreatedAt   time.Time    `gorm:"column:created_at"`
	Message     *ChatMessage `gorm:"foreignKey:MessageID"`
	MediaFile   *MediaFile   `gorm:"foreignKey:MediaFileID"`
}

func (MessageAttachment) TableName() string { return "message_attachments" }
