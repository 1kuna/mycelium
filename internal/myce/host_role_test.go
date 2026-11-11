package myce

import "testing"

// MY-45 roles: unset means node; the shared-interactive profile always means
// interactive; owner-use and laptop worker facts keep the conservative rule.
func TestHostRoleDefaultsAndOwnerGating(t *testing.T) {
	shared := &SharedHostProfile{InteractiveReserveGB: 1}
	for _, tc := range []struct {
		policy HostMaintenance
		want   string
	}{
		{HostMaintenance{}, hostRoleNode},
		{HostMaintenance{Role: hostRoleServices}, hostRoleServices},
		{HostMaintenance{SharedHost: shared}, hostRoleInteractive},
	} {
		if got := tc.policy.role(); got != tc.want {
			t.Fatal(tc.policy, got)
		}
	}
	if validHostRole("gpu") == nil || validHostRole(hostRoleServices) != nil {
		t.Fatal("role validation")
	}
	m := machine("node", "cpu", "host")
	if ownerGated(&m) {
		t.Fatal("worker without participation facts gated")
	}
	for _, a := range []Availability{{Mode: "daily"}, {Mode: "dedicated", Signals: OwnerSignals{Laptop: true}}} {
		m.CapabilityProfile.Availability = &a
		if !ownerGated(&m) {
			t.Fatal("owner fact ignored", a)
		}
	}
	m.CapabilityProfile.Availability, m.SharedHost = &Availability{Mode: "dedicated"}, shared
	if !ownerGated(&m) {
		t.Fatal("shared profile ignored")
	}
}
