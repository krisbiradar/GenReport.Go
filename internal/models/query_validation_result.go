package models

// QueryValidationResult is the response returned by POST /queries/validate.
type QueryValidationResult struct {
	// Status is a typed enum describing the validation outcome.
	Status QueryValidationStatus `json:"status"`
	// Description is a human-readable explanation of the status.
	Description string `json:"description"`
}
