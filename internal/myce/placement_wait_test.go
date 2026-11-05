package myce

import (
	"context"
	"net/http/httptest"
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
	// A cold probe now fits a small host's measured headroom. State an actual
	// lower requirement so this fixture still has a RAM shortage to explain.
	s.Needs.HostMemoryGB = 1
	j := submit(t, q, s)
	clock.Advance(time.Second)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	for _, reason := range []string{job(t, q, j.ID).Reason, q.statusWaitingReason(job(t, q, j.ID))} {
		for _, want := range []string{"partial: needs.network: true requires native_network", "interactive: environment not prepared on this shared host", "full: RAM envelope:", "GiB needed, 0.5 GiB available", "hint"} {
			if !strings.Contains(reason, want) {
				t.Errorf("missing %q: %s", want, reason)
			}
		}
		if strings.Contains(reason, "available for work") {
			t.Fatal("host availability hid job blockers", reason)
		}
	}
}

// A RAM shortage on another eligible host must not hide the occupied card on
// the host that has enough RAM (#537). Cover both shared and exclusive slots.
func TestQueuedReasonCardContentionWithOtherHostRAMShortage(t *testing.T) {
	for _, exclusive := range []bool{false, true} {
		t.Run(map[bool]string{false: "shared", true: "exclusive"}[exclusive], func(t *testing.T) {
			q, clock := fixture(t)
			m := sharedUnifiedMachine()
			var running []*Job
			if exclusive {
				s := sharedSpec("experiment", 17)
				s.Needs.Model = "served"
				running = []*Job{assignGPU(t, q, m, submit(t, q, s))}
			} else {
				a, b := sharedPair(t, q, m)
				running = []*Job{a, b}
			}
			other := sharedUnifiedMachine()
			other.ID, other.HostID, other.Session = "ram-short", "ram-short", "ram-short-session"
			other.HostAvailableMemoryGB = 0.5
			machineCheck(t, q, other)
			j := submit(t, q, sharedSpec("experiment", 17))
			clock.Advance(time.Second)
			if got := assignedJob(machineCheck(t, q, m, running...)); got != nil {
				t.Fatal("contended card admitted another attempt", got.ID)
			}
			server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
			defer server.Close()
			receipt, err := (Client{URL: server.URL, Token: "test"}).GetJob(context.Background(), j.ID)
			if err != nil {
				t.Fatal(err)
			}
			for _, reason := range []string{q.waitReason(q.s.Jobs[j.ID]), job(t, q, j.ID).Reason, receipt.Reason} {
				want := "device 0 shared with 2 attempts"
				if exclusive {
					want = "card busy"
				}
				if !strings.Contains(reason, want) {
					t.Errorf("missing %q: %s", want, reason)
				}
				for _, holder := range running {
					if !strings.Contains(reason, holder.ID+" (experiment)") {
						t.Errorf("missing holding job and tier: %s", reason)
					}
				}
			}
			for _, reason := range []string{job(t, q, j.ID).Reason, receipt.Reason} {
				for _, want := range []string{"cores reserved; utilization unknown", "RAM live 101.0 GiB available", "GiB admission headroom"} {
					if !strings.Contains(reason, want) {
						t.Errorf("card contention hid capacity context %q: %s", want, reason)
					}
				}
			}
			other.HostAvailableMemoryGB = 101
			if got := assignedJob(machineCheck(t, q, other)); got == nil || got.ID != j.ID {
				t.Fatal("busy card hid a now-runnable target", got)
			}
		})
	}
}
