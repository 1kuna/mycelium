package myce

import (
	"context"
	"fmt"
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
	for _, reason := range []string{job(t, q, j.ID).Reason, queuedStatusReason(t, q, j.ID)} {
		if strings.Contains(reason, "unrelated validation") || strings.Contains(reason, wrong.ID) || !strings.Contains(reason, "4.0 GiB needed") || !strings.Contains(reason, "0.5 GiB available") {
			t.Fatal("reason did not name the job's eligible RAM blocker", reason)
		}
	}
}

// Mixed shortages must describe both eligible hosts, regardless of whether
// the CPU-busy host also lacks RAM. The RAM fallback used to skip busy CPUs.
func TestQueuedReasonMixedCPUAndRAMTargets(t *testing.T) {
	for _, busyRAM := range []float64{32, 2} {
		t.Run(fmt.Sprint(busyRAM), func(t *testing.T) {
			q, clock := fixture(t)
			busy := cpuMachine("arm-host", "arm-host", "cpu", 8, busyRAM)
			busy.CapabilityProfile.Arch = "arm64"
			check(t, q, busy, nil)
			holderSpec := cpuSpec(8, 1)
			holderSpec.AllowedMachines = []string{busy.ID}
			holder := submit(t, q, holderSpec)
			if got := check(t, q, busy, nil).Job; got == nil || got.ID != holder.ID {
				t.Fatal("fixture did not reserve all cores", got)
			} else {
				holder = got
			}
			ram := cpuMachine("amd-host", "amd-host", "cpu", 8, 0.5)
			check(t, q, ram, nil)
			wrong := cpuMachine("unrelated", "unrelated", "cpu", 8, 0.5)
			wrong.CapabilityProfile.OS = "darwin"
			check(t, q, wrong, nil)
			s := cpuSpec(2, 4)
			s.Targets = []ExecutionTarget{{OS: "linux", Arch: "amd64", Platform: "cpu"}, {OS: "linux", Arch: "arm64", Platform: "cpu"}}
			j := submit(t, q, s)
			clock.Advance(time.Second)
			check(t, q, busy, holder)
			server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
			defer server.Close()
			receipt, err := (Client{URL: server.URL, Token: "test"}).GetJob(context.Background(), j.ID)
			if err != nil {
				t.Fatal(err)
			}
			q.mu.Lock()
			waitReason := q.waitReason(q.s.Jobs[j.ID])
			q.mu.Unlock()
			for _, reason := range []string{waitReason, job(t, q, j.ID).Reason, receipt.Reason} {
				for _, want := range []string{"arm-host: CPU capacity: 2 cores needed; 0 cores unreserved of 8 admitted", "amd-host: RAM envelope: 4.0 GiB needed, 0.5 GiB available"} {
					if !strings.Contains(reason, want) {
						t.Errorf("missing %q: %s", want, reason)
					}
				}
				if strings.Contains(reason, wrong.ID) {
					t.Errorf("unrelated target in reason: %s", reason)
				}
				if busyRAM == 2 && !strings.Contains(reason, "RAM envelope: 4.0 GiB needed, 1.0 GiB available") {
					t.Errorf("CPU shortage hid same-host RAM shortage: %s", reason)
				}
			}
			// A newly fitting target restores normal admission; diagnostics do
			// not turn another host's shortage into a scheduling restriction.
			ram.HostAvailableMemoryGB = 32
			if got := check(t, q, ram, nil).Job; got == nil || got.ID != j.ID {
				t.Fatal("fitting peer did not admit queued work", got)
			}
		})
	}
}

func TestQueuedReasonUnavailablePeerAndRAMShortage(t *testing.T) {
	q, clock := fixture(t)
	owner := cpuMachine("owner-host", "owner-host", "cpu", 8, 32)
	owner.CapabilityProfile.Availability = &Availability{Mode: "daily", Available: false, Reason: "on battery", CheckedAt: clock.Now(), Signals: idleSignals()}
	check(t, q, owner, nil)
	ram := cpuMachine("ram-host", "ram-host", "cpu", 8, 0.5)
	check(t, q, ram, nil)
	j := submit(t, q, cpuSpec(2, 4))
	clock.Advance(time.Second)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	q.mu.Lock()
	waitReason := q.waitReason(q.s.Jobs[j.ID])
	q.mu.Unlock()
	for _, reason := range []string{waitReason, job(t, q, j.ID).Reason, queuedStatusReason(t, q, j.ID)} {
		for _, want := range []string{"owner-host: hypha on battery", "ram-host: RAM envelope: 4.0 GiB needed, 0.5 GiB available"} {
			if !strings.Contains(reason, want) {
				t.Errorf("missing %q: %s", want, reason)
			}
		}
	}
	owner.CapabilityProfile.Availability.Available = true
	if got := check(t, q, owner, nil).Job; got == nil || got.ID != j.ID {
		t.Fatal("available peer did not admit queued work", got)
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
	for _, reason := range []string{job(t, q, j.ID).Reason, queuedStatusReason(t, q, j.ID)} {
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
	for _, reason := range []string{job(t, q, j.ID).Reason, queuedStatusReason(t, q, j.ID)} {
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
			q.mu.Lock()
			waitReason := q.waitReason(q.s.Jobs[j.ID])
			q.mu.Unlock()
			for _, reason := range []string{waitReason, job(t, q, j.ID).Reason, receipt.Reason} {
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

// Like production status projection, evaluate the internal helper under q.mu
// on a detached job; admission evidence may publish between fixture calls.
func queuedStatusReason(t *testing.T, q *Queue, id string) string {
	t.Helper()
	copy := job(t, q, id)
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.statusWaitingReason(copy)
}
