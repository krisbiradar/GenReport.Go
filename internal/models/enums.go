package models

// AiEndpointType represents the functional category of an AI model endpoint.
type AiEndpointType int

const (
	AiEndpointTypeChat   AiEndpointType = 1
	AiEndpointTypeModels AiEndpointType = 2
	AiEndpointTypeQuota  AiEndpointType = 3
)

// DatabaseType represents different database types.
type DatabaseType int

const (
	DatabaseTypeMySQL      DatabaseType = 1
	DatabaseTypePostgreSQL DatabaseType = 2
	DatabaseTypeSQLServer  DatabaseType = 3
	DatabaseTypeOracle     DatabaseType = 4
	DatabaseTypeSQLite     DatabaseType = 5
)

// DbProvider represents the database provider (driver) used for connections.
type DbProvider int

const (
	DbProviderNpgSql        DbProvider = 1
	DbProviderSqlClient     DbProvider = 2
	DbProviderMySqlConnector DbProvider = 3
	DbProviderOracle        DbProvider = 4
	DbProviderMongoClient   DbProvider = 5
)

// Role represents user roles.
type Role int

const (
	RoleUser  Role = 1
	RoleAdmin Role = 2
)
