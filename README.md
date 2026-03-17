# GenReport.Go

## Connection test service

This service exposes a single endpoint for database connection testing:

- `POST /go/connections/test`

Request body accepts the forwarded payload shape from C# (`name`, `databaseType`, `provider`, `hostName`, `port`, `userName`, `databaseName`, `password`, `connectionString`, `description`).

Behavior:

- Returns `200` with plain text `Connection successful` on success
- Returns non-2xx with plain text error message on failure

### Run

```bash
go run ./cmd/server
```

### Environment variables

- `GO_SERVICE_PORT` (default: `12334`)
- `DB_TEST_TIMEOUT_SECONDS` (default: `5`)
- `LOG_LEVEL` (default: `info`)
- `DB_TEST_SQL_MAX_OPEN_CONNS` (default: `5`)
- `DB_TEST_SQL_MAX_IDLE_CONNS` (default: `2`)
- `DB_TEST_SQL_CONN_MAX_LIFETIME_SECONDS` (default: `300`)
