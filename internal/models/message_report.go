package models

import "time"

// MessageReport maps to the "message_reports" table.
type MessageReport struct {
	ID        int64        `gorm:"column:Id;primaryKey"`
	MessageID int64        `gorm:"column:message_id"`
	ReportID  int64        `gorm:"column:report_id"`
	CreatedAt time.Time    `gorm:"column:created_at"`
	Message   *ChatMessage `gorm:"foreignKey:MessageID"`
	Report    *Report      `gorm:"foreignKey:ReportID"`
}

func (MessageReport) TableName() string { return "message_reports" }
