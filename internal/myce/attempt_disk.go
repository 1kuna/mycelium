package myce

const diskLimitExceeded = "disk_limit_exceeded"

// Only a typed runtime cause establishes a disk quota failure. Command exit
// codes, stderr and an uncontained ENOSPC are insufficient. This classification
// does not qualify a backend or prove that every writable path is confined.
// Native backend integration must supply this cause after observing denial.
func attemptDiskLimitFailure(r Result) bool {
	cause := safeRuntimeCause(r.ExecutionCause)
	return !r.Success && cause != nil && cause.Class == diskLimitExceeded
}
