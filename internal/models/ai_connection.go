package models

import "time"

// AiConnection maps to the "ai_connections" table.
type AiConnection struct {
	ID                    int64     `gorm:"column:id;primaryKey"`
	Provider              string    `gorm:"column:provider;size:100;not null"`
	ApiKey                string    `gorm:"column:api_key;type:text;not null"`
	BaseUrl               *string   `gorm:"column:base_url;type:text"`
	DefaultModel          string    `gorm:"column:default_model;size:100;not null"`
	SystemPrompt          *string   `gorm:"column:system_prompt"`
	Temperature           *float64  `gorm:"column:temperature"`
	MaxTokens             *int      `gorm:"column:max_tokens"`
	RateLimitRpm          *int      `gorm:"column:rate_limit_rpm"`
	RateLimitTpm          *int      `gorm:"column:rate_limit_tpm"`
	CostPer1kInputTokens  *float64  `gorm:"column:cost_per_1k_input_tokens;type:decimal(18,8)"`
	CostPer1kOutputTokens *float64  `gorm:"column:cost_per_1k_output_tokens;type:decimal(18,8)"`
	IsActive              bool      `gorm:"column:is_active"`
	IsDefault             bool      `gorm:"column:is_default;default:false"`
	CreatedAt             time.Time `gorm:"column:created_at;not null"`
	UpdatedAt             time.Time `gorm:"column:updated_at;not null"`
	AiConfigs             []AiConfig `gorm:"foreignKey:AiConnectionID"`
}

func (AiConnection) TableName() string { return "ai_connections" }
