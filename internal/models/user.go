package models

import "time"

// User maps to the "users" table.
type User struct {
	ID         int64      `gorm:"column:Id;primaryKey"`
	Password   string     `gorm:"column:password"`
	Email      string     `gorm:"column:email;index:idx_users_email"`
	FirstName  string     `gorm:"column:first_name;index:idx_users_first_name"`
	LastName   string     `gorm:"column:last_name;index:idx_users_last_name"`
	MiddleName *string    `gorm:"column:middle_name"`
	ProfileURL *string    `gorm:"column:profile_url"`
	CreatedAt  time.Time  `gorm:"column:created_at"`
	UpdatedAt  time.Time  `gorm:"column:updated_at"`
	IsDeleted  bool       `gorm:"column:is_deleted"`
	RoleID     int        `gorm:"column:role_id"`
}

func (User) TableName() string { return "users" }
