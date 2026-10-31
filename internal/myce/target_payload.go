package myce

import "errors"

// ValidateTargetPortability checks the author's declaration, never payload bytes
// or input locations. Explicit-arch targets retain their existing behavior.
func ValidateTargetPortability(s Spec) error {
	if !s.Portable {
		for _, target := range s.Targets {
			if target.Arch == "" {
				return rejectFieldRule("job_placement_invalid", errors.New("OS-only targets require a portability declaration: declare portable: true if this runs on any arch, or name the arch"))
			}
		}
	}
	return nil
}
