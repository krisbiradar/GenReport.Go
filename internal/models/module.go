package models

import "time"

// Module maps to the "modules" table.
type Module struct {
	ID          int64     `gorm:"column:id;primaryKey"`
	Name        string    `gorm:"column:name;not null"`
	Description string    `gorm:"column:description;not null"`
	IconClass   string    `gorm:"column:icon_class"`
	CreatedAt   time.Time `gorm:"column:created_at"`
	UpdatedAt   time.Time `gorm:"column:updated_at"`
}

func (Module) TableName() string { return "modules" }
