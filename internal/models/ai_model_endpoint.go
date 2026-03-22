package models

// AiModelEndpoint maps to the "ai_model_endpoints" table.
type AiModelEndpoint struct {
	ID              int64          `gorm:"column:id;primaryKey"`
	AiConnectionID  int64          `gorm:"column:ai_connection_id;not null"`
	EndpointType    AiEndpointType `gorm:"column:endpoint_type;not null"`
	Path            string         `gorm:"column:path;size:500;not null"`
	HttpMethod      string         `gorm:"column:http_method;size:10;not null"`
	IsEnabled       bool           `gorm:"column:is_enabled"`
	Notes           *string        `gorm:"column:notes;size:500"`
	AiConnection    *AiConnection  `gorm:"foreignKey:AiConnectionID"`
}

func (AiModelEndpoint) TableName() string { return "ai_model_endpoints" }
