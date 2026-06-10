package dbos

import "fmt"

// renderSQL is a test-only helper preserving the pre-sqlc query-formatting shape
// used by assertion queries in tests. Table references are unqualified; the
// connection's search_path selects the configured schema, so the former schema
// prefix argument is now an empty string.
func (k *Kernel) renderSQL(format string, args ...any) string {
	return fmt.Sprintf(format, args...)
}
