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

// ValidateInstallPolicies checks files read by the role, using startup decoders
// and semantic validators before installer writes or service-manager calls.
// Comparison with saved database policy belongs to queue startup only.
func (c ServiceConfig) ValidateInstallPolicies() error {
	if c.Role != "queue" {
		return nil
	}
	hosts, err := ReadHostPolicies(c.HostConfig)
	if err != nil {
		return err
	}
	if err := validateHostPolicies(hosts); err != nil {
		return policyFileError(c.HostConfig, err)
	}
	projects, err := ReadProjectPolicies(c.ProjectConfig)
	if err != nil {
		return err
	}
	return policyFileError(c.ProjectConfig, validateProjectPolicies(projects))
}
