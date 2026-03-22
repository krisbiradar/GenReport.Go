package models

// RoutineObject maps to the "routine_objects" table.
// Represents a stored procedure or function from a database with its vector embedding.
type RoutineObject struct {
	ID            int64     `gorm:"column:id;primaryKey"`
	DatabaseID    int64     `gorm:"column:database_id;not null"`
	Name          string    `gorm:"column:name;size:255;not null"`
	Type          string    `gorm:"column:type;size:10;not null"`
	EmbeddingText *string   `gorm:"column:embedding_text"`
	FullSchema    *string   `gorm:"column:full_schema"`
	Embedding     *string   `gorm:"column:embedding;type:vector(1536)"`
	Metadata      *string   `gorm:"column:metadata;type:jsonb"`
	Database      *Database `gorm:"foreignKey:DatabaseID"`
}

func (RoutineObject) TableName() string { return "routine_objects" }
