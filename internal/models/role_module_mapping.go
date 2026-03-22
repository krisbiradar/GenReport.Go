package models

// RoleModuleMapping maps to the "rolemodulemappings" table.
type RoleModuleMapping struct {
	ID        int64   `gorm:"column:id;primaryKey"`
	RoleID    int64   `gorm:"column:role_id"`
	ModuleID  int64   `gorm:"column:module_id"`
	CreatedAt bool    `gorm:"column:created_at"`
	UpdatedAt bool    `gorm:"column:updated_at"`
	Module    *Module `gorm:"foreignKey:ModuleID"`
}

func (RoleModuleMapping) TableName() string { return "rolemodulemappings" }
