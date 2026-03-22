package models

import "time"

// ChatSession maps to the "chat_sessions" table.
type ChatSession struct {
	ID        int64         `gorm:"column:id;primaryKey"`
	UserID    int64         `gorm:"column:user_id"`
	Title     *string       `gorm:"column:title;size:255"`
	CreatedAt time.Time     `gorm:"column:created_at"`
	UpdatedAt time.Time     `gorm:"column:updated_at"`
	User      *User         `gorm:"foreignKey:UserID"`
	Messages  []ChatMessage `gorm:"foreignKey:SessionID"`
}

func (ChatSession) TableName() string { return "chat_sessions" }
