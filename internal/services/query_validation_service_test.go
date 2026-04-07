package services

import (
	"testing"

	"genreport/internal/models"
)

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

func assertReadOnly(t *testing.T, result models.QueryValidationResult, ok bool) {
	t.Helper()
	if !ok {
		t.Errorf("expected read-only=true but got false; status=%d description=%q", result.Status, result.Description)
	}
}

func assertNotReadOnly(t *testing.T, result models.QueryValidationResult, ok bool) {
	t.Helper()
	if ok {
		t.Errorf("expected read-only=false but got true")
	}
	if result.Status != models.QueryValidationStatusNotReadOnly {
		t.Errorf("expected status NotReadOnly (%d) but got %d; description=%q",
			models.QueryValidationStatusNotReadOnly, result.Status, result.Description)
	}
}

func assertParseError(t *testing.T, result models.QueryValidationResult, ok bool) {
	t.Helper()
	if ok {
		t.Errorf("expected parse error but got read-only=true")
	}
	if result.Status != models.QueryValidationStatusParseError {
		t.Errorf("expected status ParseError (%d) but got %d; description=%q",
			models.QueryValidationStatusParseError, result.Status, result.Description)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// PostgreSQL tests (pg_query_go AST)
// ─────────────────────────────────────────────────────────────────────────────

func TestCheckReadOnlyPostgres_SelectAllowed(t *testing.T) {
	queries := []string{
		"SELECT 1",
		"SELECT id, name FROM users",
		"SELECT * FROM orders WHERE created_at > NOW()",
		"SELECT u.id, o.total FROM users u JOIN orders o ON u.id = o.user_id",
		"select id from users", // lowercase
	}
	for _, q := range queries {
		t.Run(q, func(t *testing.T) {
			result, ok := checkReadOnlyPostgres(q)
			assertReadOnly(t, result, ok)
		})
	}
}

func TestCheckReadOnlyPostgres_CTEAllowed(t *testing.T) {
	queries := []string{
		"WITH cte AS (SELECT id FROM users) SELECT * FROM cte",
		"WITH a AS (SELECT 1), b AS (SELECT 2) SELECT * FROM a, b",
	}
	for _, q := range queries {
		t.Run(q, func(t *testing.T) {
			result, ok := checkReadOnlyPostgres(q)
			assertReadOnly(t, result, ok)
		})
	}
}

func TestCheckReadOnlyPostgres_ExplainAllowed(t *testing.T) {
	queries := []string{
		"EXPLAIN SELECT * FROM users",
		"EXPLAIN ANALYZE SELECT id FROM orders",
	}
	for _, q := range queries {
		t.Run(q, func(t *testing.T) {
			result, ok := checkReadOnlyPostgres(q)
			assertReadOnly(t, result, ok)
		})
	}
}

func TestCheckReadOnlyPostgres_MutatingRejected(t *testing.T) {
	cases := []struct {
		name string
		sql  string
	}{
		{"INSERT", "INSERT INTO users (name) VALUES ('test')"},
		{"UPDATE", "UPDATE users SET name = 'x' WHERE id = 1"},
		{"DELETE", "DELETE FROM users WHERE id = 1"},
		{"DROP", "DROP TABLE users"},
		{"CREATE", "CREATE TABLE foo (id INT)"},
		{"ALTER", "ALTER TABLE users ADD COLUMN age INT"},
		{"TRUNCATE", "TRUNCATE TABLE users"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result, ok := checkReadOnlyPostgres(tc.sql)
			assertNotReadOnly(t, result, ok)
		})
	}
}

func TestCheckReadOnlyPostgres_ParseError(t *testing.T) {
	sqls := []string{
		// Note: `SELECT ??? garbage` is actually valid PG syntax (??? is an operator).
		// Use SQL that is structurally invalid for the pg parser.
		"THIS IS NOT SQL AT ALL !!!",
		"SELECT FROM FROM FROM",
		"( (",
	}
	for _, q := range sqls {
		t.Run(q, func(t *testing.T) {
			result, ok := checkReadOnlyPostgres(q)
			assertParseError(t, result, ok)
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// MySQL tests (Vitess AST)
// ─────────────────────────────────────────────────────────────────────────────

func TestCheckReadOnlyMySQL_SelectAllowed(t *testing.T) {
	queries := []string{
		"SELECT 1",
		"SELECT id, name FROM users",
		"SELECT * FROM orders WHERE created_at > NOW()",
		"SELECT u.id, o.total FROM users u JOIN orders o ON u.id = o.user_id",
	}
	for _, q := range queries {
		t.Run(q, func(t *testing.T) {
			result, ok := checkReadOnlyMySQL(q)
			assertReadOnly(t, result, ok)
		})
	}
}

func TestCheckReadOnlyMySQL_UnionAllowed(t *testing.T) {
	queries := []string{
		"SELECT id FROM users UNION SELECT id FROM admins",
		"SELECT id FROM users UNION ALL SELECT id FROM guests",
	}
	for _, q := range queries {
		t.Run(q, func(t *testing.T) {
			result, ok := checkReadOnlyMySQL(q)
			assertReadOnly(t, result, ok)
		})
	}
}

func TestCheckReadOnlyMySQL_ShowAllowed(t *testing.T) {
	// Vitess classifies SHOW as StmtShow (IsReadStatement=true).
	// DESCRIBE maps to StmtExplain but Vitess does NOT consider Explain a read
	// statement — it's excluded here intentionally.
	queries := []string{
		"SHOW TABLES",
		"SHOW DATABASES",
		"SHOW CREATE TABLE users",
	}
	for _, q := range queries {
		t.Run(q, func(t *testing.T) {
			result, ok := checkReadOnlyMySQL(q)
			assertReadOnly(t, result, ok)
		})
	}
}

func TestCheckReadOnlyMySQL_MutatingRejected(t *testing.T) {
	cases := []struct {
		name string
		sql  string
	}{
		{"INSERT", "INSERT INTO users (name) VALUES ('test')"},
		{"UPDATE", "UPDATE users SET name = 'x' WHERE id = 1"},
		{"DELETE", "DELETE FROM users WHERE id = 1"},
		{"DROP", "DROP TABLE users"},
		{"CREATE", "CREATE TABLE foo (id INT)"},
		{"ALTER", "ALTER TABLE users ADD COLUMN age INT"},
		{"TRUNCATE", "TRUNCATE TABLE users"},
		{"REPLACE", "REPLACE INTO users (id, name) VALUES (1, 'test')"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result, ok := checkReadOnlyMySQL(tc.sql)
			assertNotReadOnly(t, result, ok)
		})
	}
}

func TestCheckReadOnlyMySQL_ParseError(t *testing.T) {
	sqls := []string{
		"SELECT ??? garbage",
		"THIS IS NOT SQL AT ALL !!!",
	}
	for _, q := range sqls {
		t.Run(q, func(t *testing.T) {
			result, ok := checkReadOnlyMySQL(q)
			assertParseError(t, result, ok)
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// SQL Server tests (keyword normaliser via checkReadOnlySQLServer)
// ─────────────────────────────────────────────────────────────────────────────

func TestCheckReadOnlySQLServer_SelectAllowed(t *testing.T) {
	queries := []string{
		"SELECT 1",
		"SELECT id, name FROM dbo.Users",
		"SELECT TOP 10 * FROM Orders",
		"select id from dbo.users", // lowercase
	}
	for _, q := range queries {
		t.Run(q, func(t *testing.T) {
			result, ok := checkReadOnlySQLServer(q)
			assertReadOnly(t, result, ok)
		})
	}
}

func TestCheckReadOnlySQLServer_CTEAllowed(t *testing.T) {
	query := "WITH cte AS (SELECT id FROM dbo.Users) SELECT * FROM cte"
	result, ok := checkReadOnlySQLServer(query)
	assertReadOnly(t, result, ok)
}

func TestCheckReadOnlySQLServer_CTEWithMutatingRejected(t *testing.T) {
	query := "WITH cte AS (SELECT id FROM Users) DELETE FROM Users WHERE id IN (SELECT id FROM cte)"
	result, ok := checkReadOnlySQLServer(query)
	assertNotReadOnly(t, result, ok)
}

func TestCheckReadOnlySQLServer_MutatingRejected(t *testing.T) {
	cases := []struct {
		name string
		sql  string
	}{
		{"INSERT", "INSERT INTO dbo.Users (Name) VALUES ('test')"},
		{"UPDATE", "UPDATE dbo.Users SET Name = 'x' WHERE Id = 1"},
		{"DELETE", "DELETE FROM dbo.Users WHERE Id = 1"},
		{"DROP", "DROP TABLE dbo.Users"},
		{"CREATE", "CREATE TABLE dbo.Foo (Id INT)"},
		{"ALTER", "ALTER TABLE dbo.Users ADD Age INT"},
		{"TRUNCATE", "TRUNCATE TABLE dbo.Users"},
		{"MERGE", "MERGE dbo.Target AS t USING dbo.Source AS s ON t.Id = s.Id WHEN MATCHED THEN UPDATE SET t.Name = s.Name"},
		{"EXEC stored proc", "EXEC dbo.MyStoredProc"},
		{"GRANT", "GRANT SELECT ON dbo.Users TO ReadUser"},
		{"REVOKE", "REVOKE SELECT ON dbo.Users FROM ReadUser"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result, ok := checkReadOnlySQLServer(tc.sql)
			assertNotReadOnly(t, result, ok)
		})
	}
}

func TestCheckReadOnlySQLServer_CommentStripping(t *testing.T) {
	// Mutating keyword inside a comment should NOT trigger rejection.
	safeSQL := `
		-- This would DELETE everything but it's just a comment
		SELECT id FROM dbo.Users
	`
	result, ok := checkReadOnlySQLServer(safeSQL)
	assertReadOnly(t, result, ok)

	// Mutating keyword inside a block comment should also be stripped.
	safeSQL2 := `
		/* DROP TABLE Users */
		SELECT * FROM dbo.Users
	`
	result2, ok2 := checkReadOnlySQLServer(safeSQL2)
	assertReadOnly(t, result2, ok2)
}

func TestCheckReadOnlySQLServer_StringLiteralStripping(t *testing.T) {
	// A mutating keyword inside a string literal should not trigger rejection.
	safeSQL := "SELECT 'DROP TABLE users' AS harmless_string FROM dbo.Config"
	result, ok := checkReadOnlySQLServer(safeSQL)
	assertReadOnly(t, result, ok)
}

func TestCheckReadOnlySQLServer_EmptySQL(t *testing.T) {
	cases := []string{
		"",
		"   ",
		"-- just a comment",
		"/* just a block comment */",
	}
	for _, q := range cases {
		t.Run(q, func(t *testing.T) {
			result, ok := checkReadOnlySQLServer(q)
			assertParseError(t, result, ok)
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Oracle tests (keyword normaliser via checkReadOnlyOracle)
// ─────────────────────────────────────────────────────────────────────────────

func TestCheckReadOnlyOracle_SelectAllowed(t *testing.T) {
	queries := []string{
		"SELECT 1 FROM DUAL",
		"SELECT id, name FROM EMPLOYEES",
		"SELECT * FROM ALL_TABLES",
	}
	for _, q := range queries {
		t.Run(q, func(t *testing.T) {
			result, ok := checkReadOnlyOracle(q)
			assertReadOnly(t, result, ok)
		})
	}
}

func TestCheckReadOnlyOracle_MutatingRejected(t *testing.T) {
	cases := []struct {
		name string
		sql  string
	}{
		{"INSERT", "INSERT INTO employees (name) VALUES ('test')"},
		{"UPDATE", "UPDATE employees SET name = 'x' WHERE id = 1"},
		{"DELETE", "DELETE FROM employees WHERE id = 1"},
		{"DROP", "DROP TABLE employees"},
		{"MERGE", "MERGE INTO target t USING source s ON (t.id = s.id) WHEN MATCHED THEN UPDATE SET t.name = s.name"},
		{"GRANT", "GRANT SELECT ON employees TO readonly_user"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result, ok := checkReadOnlyOracle(tc.sql)
			assertNotReadOnly(t, result, ok)
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// checkReadOnly dispatcher tests
// ─────────────────────────────────────────────────────────────────────────────

func TestCheckReadOnly_UnsupportedProvider(t *testing.T) {
	// MongoDB (DbProviderMongoClient = 5) should be handled by the caller,
	// but an unknown provider enum value should return Unsupported.
	result, ok := checkReadOnly("SELECT 1", models.DbProvider(99))
	if ok {
		t.Error("expected ok=false for unsupported provider")
	}
	if result.Status != models.QueryValidationStatusUnsupported {
		t.Errorf("expected Unsupported status, got %d", result.Status)
	}
}

func TestCheckReadOnly_RoutesCorrectly(t *testing.T) {
	// Mutating SQL rejected by each provider's specific checker.
	mutating := "DELETE FROM users WHERE id = 1"
	providers := []models.DbProvider{
		models.DbProviderNpgSql,
		models.DbProviderMySqlConnector,
		models.DbProviderSqlClient,
		models.DbProviderOracle,
	}
	for _, p := range providers {
		t.Run(string(rune(int(p)+'0')), func(t *testing.T) {
			result, ok := checkReadOnly(mutating, p)
			if ok {
				t.Errorf("provider %d: expected rejection for DELETE, got ok=true", p)
			}
			if result.Status != models.QueryValidationStatusNotReadOnly &&
				result.Status != models.QueryValidationStatusParseError {
				t.Errorf("provider %d: unexpected status %d for DELETE", p, result.Status)
			}
		})
	}
}
