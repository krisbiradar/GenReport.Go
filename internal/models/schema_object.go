package models

// SchemaObject maps to the "schema_objects" table.
// Represents a table or view from a database with its vector embedding.
type SchemaObject struct {
	ID            int64     `gorm:"column:Id;primaryKey"`
	DatabaseID    int64     `gorm:"column:database_id;not null"`
	Name          string    `gorm:"column:name;size:255;not null"`
	Type          string    `gorm:"column:type;size:10;not null"`
	EmbeddingText *string   `gorm:"column:embedding_text"`
	FullSchema    *string   `gorm:"column:full_schema"`
	Embedding     *string   `gorm:"column:embedding;type:vector(1536)"`
	EmbeddingOllama *string `gorm:"column:embedding_ollama;type:vector(768)"`
	Metadata      *string   `gorm:"column:metadata;type:jsonb"`
	Database      *Database `gorm:"foreignKey:DatabaseID"`
}

func (SchemaObject) TableName() string { return "schema_objects" }
