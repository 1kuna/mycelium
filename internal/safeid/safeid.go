package safeid

import (
	"fmt"
	"path/filepath"
	"strings"
)

func Validate(kind, value string) error {
	if value == "" {
		return fmt.Errorf("%s is required", kind)
	}
	if strings.TrimSpace(value) != value {
		return fmt.Errorf("%s %q must not contain leading or trailing whitespace", kind, value)
	}
	if value == "." || filepath.IsAbs(value) || strings.ContainsAny(value, `/\`) || strings.Contains(value, "..") {
		return fmt.Errorf("%s %q is not a safe path component", kind, value)
	}
	return nil
}
