package models

import "time"

// AiConfig maps to the "ai_configs" table.
type AiConfig struct {
	ID             int64        `gorm:"column:Id;primaryKey"`
	Type           AiConfigType `gorm:"column:type;not null;index:idx_ai_configs_type"`
	Value          string       `gorm:"column:value;type:text;not null"`
	AiConnectionID int64        `gorm:"column:ai_connection_id;not null"`
	ModelID        *string      `gorm:"column:model_id;size:100"`
	IsActive       bool         `gorm:"column:is_active;default:true"`
	Version        int          `gorm:"column:version;default:1"`
	CreatedAt      time.Time    `gorm:"column:created_at;not null"`
	UpdatedAt      time.Time    `gorm:"column:updated_at"`
	AiConnection   *AiConnection `gorm:"foreignKey:AiConnectionID"`
}

func (AiConfig) TableName() string { return "ai_configs" }
