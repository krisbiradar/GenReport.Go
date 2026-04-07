package models

// QueryValidationRequest is the payload for POST /queries/validate.
// The caller provides the ID of a stored database record; the service
// resolves and decrypts the connection string internally.
type QueryValidationRequest struct {
	// DatabaseID is the primary key from the "databases" table.
	DatabaseID string `json:"databaseId"`
	// SQL is the raw query to validate.
	SQL string `json:"sql"`
}
