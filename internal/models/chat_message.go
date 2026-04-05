package models

import "time"

// ChatMessage maps to the "chat_messages" table.
type ChatMessage struct {
	ID        int64           `gorm:"column:Id;primaryKey"`
	SessionID int64           `gorm:"column:session_id"`
	Role        string              `gorm:"column:role;size:20;not null"`
	Content     string              `gorm:"column:content;type:text;not null"`
	Intent      *string             `gorm:"column:intent;size:50"`
	CreatedAt   time.Time           `gorm:"column:created_at"`
	Session     *ChatSession        `gorm:"foreignKey:SessionID"`
	Reports     []MessageReport     `gorm:"foreignKey:MessageID"`
	Attachments []MessageAttachment `gorm:"foreignKey:MessageID"`
}

func (ChatMessage) TableName() string { return "chat_messages" }
