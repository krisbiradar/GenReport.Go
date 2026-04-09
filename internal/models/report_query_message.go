package models

// ReportQueryMessage is the JSON payload published to the "report_query"
// RabbitMQ topic. The worker deserialises this, runs the SQL against the
// specified database, and delivers the results as a SQLite file to the
// callback API.
type ReportQueryMessage struct {
	UserID       string `json:"userId"`
	DatabaseID   string `json:"databaseId"`
	Query        string `json:"query"`
	SessionTitle string `json:"sessionTitle"`
}
