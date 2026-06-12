package dbos

import "fmt"

func (k *Kernel) renderSql(format string, args ...any) string {
	return fmt.Sprintf(format, args...)
}
