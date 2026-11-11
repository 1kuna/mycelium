package myce

import "errors"

// Host roles (MY-45). A node is a managed on-demand machine with nothing
// persistent on it; the services host runs every project's always-on services
// and keeps its accelerators schedulable; the interactive host keeps the
// conservative owner-protecting rules. An unset role means node.
const (
	hostRoleNode        = "node"
	hostRoleServices    = "services"
	hostRoleInteractive = "interactive"
)

func validHostRole(role string) error {
	switch role {
	case hostRoleNode, hostRoleServices, hostRoleInteractive:
		return nil
	}
	return errors.New("role must be node, services or interactive")
}

func (p HostMaintenance) role() string {
	if p.SharedHost != nil {
		return hostRoleInteractive
	}
	if p.Role == "" {
		return hostRoleNode
	}
	return p.Role
}

// ownerGated reports a worker that declared owner use (--daily-use) or is a
// laptop. Those explicit worker facts keep the conservative rule whatever the
// queue-assigned role, as does a shared-interactive profile (#389).
func ownerGated(m *Machine) bool {
	if m == nil || m.SharedHost != nil {
		return true
	}
	if m.CapabilityProfile == nil || m.CapabilityProfile.Availability == nil {
		return false
	}
	a := m.CapabilityProfile.Availability
	return a.Mode == "daily" || a.Signals.Laptop
}
