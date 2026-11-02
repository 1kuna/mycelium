package myce

import (
	"strings"
	"testing"
	"time"
)

func TestQueuedReasonIgnoresIneligibleReservation(t *testing.T) {
	q, clock := fixture(t)
	wrong := m3Machine()
	wrong.ID, wrong.HostID, wrong.Session = "other-platform", "other-platform", "other-session"
	wrong.CapabilityProfile.OS = "darwin"
	machineCheck(t, q, wrong)
	bound := submit(t, q, exclusionSpec(wrong.ID, "0"))
	request := reservationRequest("irrelevant", "host=other-platform;devices=0", bound.ID)
	request.Reason = "unrelated validation"
	if _, err := q.CreateReservation(request); err != nil {
		t.Fatal(err)
	}
	m := m3Machine()
	m.HostAvailableMemoryGB = 0.5
	machineCheck(t, q, m)
	s := exclusionSpec("", "0")
	s.AllowedMachines = nil
	s.Needs.HostMemoryGB = 4
	s.Targets = []ExecutionTarget{{OS: "linux", Arch: "amd64", Platform: "fake"}}
	j := submit(t, q, s)
	clock.Advance(time.Second)
	machineCheck(t, q, m)
	for _, reason := range []string{job(t, q, j.ID).Reason, q.statusWaitingReason(job(t, q, j.ID))} {
		if strings.Contains(reason, "unrelated validation") || strings.Contains(reason, wrong.ID) || !strings.Contains(reason, "4.0 GiB needed") || !strings.Contains(reason, "0.5 GiB available") {
			t.Fatal("reason did not name the job's eligible RAM blocker", reason)
		}
	}
}

func TestQueuedReasonIgnoresReservationOnUndersizedHost(t *testing.T) {
	q, clock := fixture(t)
	small := cpuMachine("undersized", "undersized", "cpu", 4, 32)
	check(t, q, small, nil)
	boundSpec := cpuSpec(1, 1)
	boundSpec.AllowedMachines = []string{small.ID}
	bound := submit(t, q, boundSpec)
	request := reservationRequest("irrelevant", "host=undersized", bound.ID)
	request.Reason = "unrelated small-host validation"
	if _, err := q.CreateReservation(request); err != nil {
		t.Fatal(err)
	}
	eligible := cpuMachine("eligible", "eligible", "cpu", 8, 0.5)
	check(t, q, eligible, nil)
	j := submit(t, q, cpuSpec(8, 4))
	clock.Advance(time.Second)
	check(t, q, eligible, nil)
	for _, reason := range []string{job(t, q, j.ID).Reason, q.statusWaitingReason(job(t, q, j.ID))} {
		if strings.Contains(reason, small.ID) || strings.Contains(reason, request.Reason) || !strings.Contains(reason, "eligible: 4.0 GiB needed, 0.5 GiB available") {
			t.Fatal("incompatible CPU capacity contributed a reservation reason", reason)
		}
	}
}

func TestQueuedReasonNetworkPreparationAndMemory(t *testing.T) {
	q, clock := fixture(t)
	recipe := binaryRecipe(t, "cpu")
	if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"download-tools": {"cpu": recipe}}}); err != nil {
		t.Fatal(err)
	}
	partial := cpuMachine("partial", "partial", "cpu", 8, 32)
	partial.EnvironmentBuilder = true
	partial.Capabilities = []string{"native_isolation_landlock", "native_network_partial"}
	check(t, q, partial, nil)
	shared := sharedHostFixture(t, q)
	shared.Capabilities = []string{"native_isolation_seatbelt", "native_network"}
	shared.SharedHost = &SharedHostProfile{InteractiveReserveGB: 4}
	if _, err := q.ChangeHostMaintenance(shared.HostID, HostMaintenanceRequest{Action: "resume"}); err != nil {
		t.Fatal(err)
	}
	check(t, q, shared, nil)
	full := cpuMachine("full", "full", "cpu", 8, 0.5)
	full.EnvironmentBuilder = true
	full.Capabilities = shared.Capabilities
	check(t, q, full, nil)
	s := cpuSpec(1, 0)
	s.Needs.Memory, s.Needs.Network, s.Isolation = "auto", true, "sandboxed"
	s.Environment.Name = "download-tools"
	j := submit(t, q, s)
	clock.Advance(time.Second)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	for _, reason := range []string{job(t, q, j.ID).Reason, q.statusWaitingReason(job(t, q, j.ID))} {
		for _, want := range []string{"partial: needs.network: true requires native_network", "interactive: environment not prepared on this shared host", "full: RAM envelope:", "GiB needed, 0.5 GiB available", "exploratory"} {
			if !strings.Contains(reason, want) {
				t.Errorf("missing %q: %s", want, reason)
			}
		}
		if strings.Contains(reason, "available for work") {
			t.Fatal("host availability hid job blockers", reason)
		}
	}
}
