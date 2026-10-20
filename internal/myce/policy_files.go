package myce

import (
	"errors"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

// Keep the strict decoder's key and line diagnostics and identify known retired
// settings. This context is shared by startup and install preflight.
func policyFileError(path string, err error) error {
	if err == nil {
		return nil
	}
	var typed *yaml.TypeError
	if errors.As(err, &typed) {
		details := make([]string, 0, len(typed.Errors))
		for _, detail := range typed.Errors {
			if strings.Contains(detail, " not found in type ") {
				kind := "unknown"
				if strings.Contains(detail, "field production_idle not found in type myce.HostPolicy") {
					kind = "retired"
				}
				detail += " (" + kind + " key)"
			}
			details = append(details, detail)
		}
		err = &yaml.TypeError{Errors: details}
	}
	return fmt.Errorf("%s: %w", path, err)
}

// ValidateInstallPolicies loads only explicitly configured files, using the
// startup readers, before the installer writes files or invokes a manager.
func (c ServiceConfig) ValidateInstallPolicies() error {
	if _, err := ReadHostPolicies(c.HostConfig); err != nil {
		return err
	}
	_, err := ReadProjectPolicies(c.ProjectConfig)
	return err
}
