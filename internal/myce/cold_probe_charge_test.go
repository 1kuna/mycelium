package myce

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Replay a machine heartbeat whose inventory work outlasts its original actor
// snapshot, while the command actor continues sampling a small CPU footprint.
func TestColdCPUProbeChargeAfterSlowHeartbeatInventory(t *testing.T) {
	oldRSS := workerMemoryRSS
	workerMemoryRSS = func(context.Context) (float64, error) { return 0, nil }
	t.Cleanup(func() { workerMemoryRSS = oldRSS })
	for _, evidence := range []string{"fresh", "missing", "stale", "wrong attempt"} {
		t.Run(evidence, func(t *testing.T) {
			q, clock := fixture(t)
			m := unifiedCUDAAdmissionMachine()
			m.CapabilityProfile.Arch = "arm64"
			m.HostAvailableMemoryGB = 99
			m.MemoryObservation = &HostMemoryObservation{TotalGB: 121, UsedGB: 6}
			m.Devices = []MachineDevice{{ID: "0", Platform: "cuda", Identity: "GPU-test", Fingerprint: "cuda-test", Selector: "0", MemoryMode: "unified", UnifiedMemory: true, TotalMemoryGB: 121, BudgetGB: 121, FreeMemoryGB: 115, MemoryMeasured: true}}
			machineCheck(t, q, m)
			submit(t, q, cpuSpec(2, 0))
			probe := assignedJob(machineCheck(t, q, m))
			if probe == nil || probe.MemoryEnvelope.ProbeFloorGB <= 0 || probe.MemoryEnvelope.HostGB < 80 {
				t.Fatal("missing large cold probe")
			}
			ceiling := probe.Spec.Needs.HostMemoryGB
			dir := t.TempDir()
			write := func(name, value string) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(dir, name), []byte(value), 0600); err != nil {
					t.Fatal(err)
				}
			}
			write("memory.current", "1073741824")
			write("memory.stat", "anon 1073741824\nkernel 0\nshmem 0\nunevictable 0\nfile_dirty 0\nfile_writeback 0\n")
			memory := &memoryAttempt{fs: osMemoryFS{}, path: dir}
			report := AttemptReport{Execution: Execution{ID: probe.ID, Attempt: probe.Attempt}, Session: probe.Session, Started: true, CurrentHostMemory: memory.currentHostMemory(clock)}
			actor := &attemptSupervisor{job: *probe, report: report}
			reports := []AttemptReport{report}
			actors := map[attemptKey]*attemptSupervisor{jobKey(*probe): actor}
			// The inventory reader holds only a copied report. The actor observes
			// a second timed sample independently before that heartbeat is sent.
			clock.Advance(hostMemorySampleFreshFor + time.Second)
			actor.report.CurrentHostMemory = memory.currentHostMemory(clock)
			switch evidence {
			case "missing":
				actor.report.CurrentHostMemory = nil
			case "stale":
				clock.Advance(hostMemorySampleFreshFor)
			case "wrong attempt":
				actor.report.Attempt++
			}
			pending := submit(t, q, cpuSpec(2, 2))
			w := Worker{Machine: m, Clock: clock, HostMemoryReserveGB: 16, HostMemorySource: func(context.Context) (float64, error) { return 99, nil }}
			w.sampleHeartbeatMemory(context.Background(), &m, []Job{*probe}, reports, actors)
			reply, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: reports})
			if err != nil {
				t.Fatal(err)
			}
			q.mu.RLock()
			charge := q.hostJobCharge(q.s.Jobs[probe.ID])
			q.mu.RUnlock()
			got := assignedJob(reply)
			if evidence == "fresh" {
				if got == nil || got.ID != pending.ID {
					t.Fatalf("fresh small footprint kept large ceiling charged: %s", job(t, q, pending.ID).Reason)
				}
				if charge >= 2 {
					t.Fatalf("small probe charged %g GiB", charge)
				}
			} else {
				if got != nil || charge != probe.MemoryEnvelope.HostGB {
					t.Fatal("unavailable evidence reduced the containment charge", fmt.Sprint(got))
				}
			}
			if storedTestJob(t, q, probe.ID).Spec.Needs.HostMemoryGB != ceiling {
				t.Fatal("changed execution containment ceiling")
			}
			w.Machine, w.hostMemorySamples = m, currentHostMemorySamples(reports, clock.Now())
			if err := localAttemptAdmission(w, []Job{storedTestJob(t, q, probe.ID)}, storedTestJob(t, q, pending.ID), nil); (err == nil) != (evidence == "fresh") {
				t.Fatalf("worker admission disagrees with queue: %v", err)
			}
		})
	}
}

func TestHeartbeatMemoryGrowthRefreshesPhysicalHeadroom(t *testing.T) {
	q, clock := fixture(t)
	m := cpuMachine("worker", "host", "cpu", 16, 24)
	m.Devices = []MachineDevice{}
	m.HostMemoryBudgetGB = 24
	submit(t, q, cpuSpec(2, 16))
	holder := assignedJob(machineCheck(t, q, m))
	if holder == nil {
		t.Fatal("holder not assigned")
	}
	m.HostAvailableMemoryGB = 14
	report := AttemptReport{Execution: Execution{ID: holder.ID, Attempt: holder.Attempt}, Session: holder.Session, Started: true, CurrentHostMemory: &CurrentHostMemory{NonreclaimableGB: 2, sampledAt: clock.Now()}}
	reports := []AttemptReport{report}
	actor := &attemptSupervisor{job: *holder, report: report}
	// During inventory the holder consumes eight more GiB. A newer resident
	// counter must not cancel growth against the old 14 GiB availability.
	clock.Advance(hostMemorySampleFreshFor + time.Second)
	actor.report.CurrentHostMemory = &CurrentHostMemory{NonreclaimableGB: 10, sampledAt: clock.Now()}
	w := Worker{Machine: m, Clock: clock, HostMemorySource: func(context.Context) (float64, error) { return 6, nil }}
	w.sampleHeartbeatMemory(context.Background(), &m, []Job{*holder}, reports, map[attemptKey]*attemptSupervisor{jobKey(*holder): actor})
	pending := submit(t, q, cpuSpec(2, 2))
	reply, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: reports})
	if err != nil {
		t.Fatal(err)
	}
	if assignedJob(reply) != nil || m.HostAvailableMemoryGB != 6 {
		t.Fatal("new resident usage spent old physical headroom")
	}
	w.Machine, w.hostMemorySamples = m, currentHostMemorySamples(reports, clock.Now())
	if err := localAttemptAdmission(w, []Job{storedTestJob(t, q, holder.ID)}, storedTestJob(t, q, pending.ID), nil); err == nil {
		t.Fatal("worker admitted without physical growth room")
	}
}
