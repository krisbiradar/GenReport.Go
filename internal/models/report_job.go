package models

// ReportJobRequest is published to the "report_generate" queue by the HTTP handler.
type ReportJobRequest struct {
	DatabaseConnectionID string `json:"databaseConnectionId"`
	Format               string `json:"format"`    // e.g. "excel", "pdf"
	Query                string `json:"query"`
	SessionID            string `json:"sessionId"`
}

// ReportJobResult is published to "report_success" or "report_error" by the worker.
type ReportJobResult struct {
	DatabaseConnectionID string `json:"databaseConnectionId"`
	Format               string `json:"format"`
	Query                string `json:"query"`
	SessionID            string `json:"sessionId"`
	// SQLiteFilePath is the absolute path to the generated SQLite file (success only).
	SQLiteFilePath string `json:"sqliteFilePath,omitempty"`
	// Error contains the failure reason (error only).
	Error string `json:"error,omitempty"`
}
