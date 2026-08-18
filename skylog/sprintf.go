package skylog

import "fmt"

// sprintf keeps the formatting call in one place so callers that pass no
// arguments skip the vararg formatting machinery entirely.
func sprintf(format string, args []any) string {
	if len(args) == 0 {
		return format
	}
	return fmt.Sprintf(format, args...)
}
