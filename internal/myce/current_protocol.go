package myce

import (
	"strings"
)

// Version zero is a valid hardware-facts profile. Missing facts are not capacity.
func validateCurrentCheckin(c Checkin) error {
	m := &c.Machine
	if m.ID == "" || m.Session == "" {
		return currentProtocolError("current worker protocol requires machine and session identity")
	}
	p := m.CapabilityProfile
	if p == nil || p.Version != 0 || (p.MemoryLayout != "separate" && p.MemoryLayout != "unified") {
		return currentProtocolError("current worker protocol requires a valid capability profile (version 0)")
	}
	if (m.HostMemoryBudgetGB > 0 || strings.HasPrefix(m.MemoryEnforcement, "memory ceilings enforced")) && (!m.HostMemoryMeasured || m.HostMemoryError != "") {
		return currentProtocolError("enforced host requires measured system RAM")
	}
	for _, s := range m.ModelApprovalStates {
		if s != approvalVerified && s != approvalInvalid && s != approvalUnknown {
			return currentProtocolError("local-source state must be verified, invalid or unknown")
		}
	}
	for source := range m.ApprovedModels {
		if m.ModelApprovalStates[source] == "" {
			return currentProtocolError("local-source approval requires explicit verified, invalid or unknown state")
		}
	}
	for _, r := range c.ModelReports {
		if r.Name == "" || r.Attempt == "" {
			return currentProtocolError("engine report requires model and attempt identity")
		}
	}
	return nil
}

func currentProtocolError(message string) error {
	return &HTTPError{Status: 426, Code: "worker_protocol_invalid", Message: message}
}
