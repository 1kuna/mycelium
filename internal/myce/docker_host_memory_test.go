package myce

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

type dockerHostMemoryFS struct {
	osMemoryFS
	files map[string]string
}

func (f dockerHostMemoryFS) ReadFile(path string) ([]byte, error) {
	if b, ok := f.files[path]; ok {
		return []byte(b), nil
	}
	return nil, os.ErrNotExist
}

func dockerHostMemoryFixture(t *testing.T, clock Clock) (*dockerAttempt, dockerHostMemoryFS) {
	t.Helper()
	id := strings.Repeat("a", 64)
	cs := []dockerInspection{{ID: id}}
	cs[0].Config.Labels = map[string]string{"myce.attempt": "exact-attempt"}
	live := true
	cs[0].State.Running, cs[0].State.Status, cs[0].State.Pid = &live, "running", 42
	d := &dockerAttempt{clock: clock, timeout: time.Second, state: dockerExecution{Endpoint: "unix:///synthetic/docker.sock", Identity: "exact-attempt"}}
	d.command = func(_ context.Context, args ...string) ([]byte, error) {
		if args[0] == "--host" {
			args = args[2:]
		}
		switch args[0] {
		case "ps":
			return []byte(id), nil
		case "inspect":
			return json.Marshal(cs)
		}
		return nil, fmt.Errorf("unexpected command %v", args)
	}
	path := "/sys/fs/cgroup/system.slice/docker-" + id + ".scope"
	fs := dockerHostMemoryFS{files: map[string]string{
		"/proc/42/cgroup":        "0::/system.slice/docker-" + id + ".scope\n",
		path + "/memory.stat":    "anon 42949672960\nfile 8589934592\n",
		path + "/memory.current": "51539607552",
	}}
	return d, fs
}

func dockerHostCreditFixture(t *testing.T) (*Queue, *fakeClock, Machine, *Job, *CurrentHostMemory) {
	t.Helper()
	q, clock := fixture(t)
	m := unifiedCUDAAdmissionMachine()
	m.Capabilities = append(m.Capabilities, "docker")
	m.CapabilityProfile.OS = "linux"
	s := dockerJob().Spec
	s.Needs.MemoryGB, s.Needs.HostMemoryGB = 16, 40
	s.Run.DockerMemoryGB = 40
	first := submit(t, q, s)
	holder := check(t, q, m, nil).Job
	if holder == nil || holder.ID != first.ID {
		t.Fatal("Docker holder not assigned", holder, job(t, q, first.ID).Reason)
	}
	d, fs := dockerHostMemoryFixture(t, clock)
	if d.envelope() != nil {
		t.Fatal("fixture unexpectedly has a shared parent")
	}
	sample := d.currentHostMemory(clock, fs)
	if sample == nil || sample.NonreclaimableGB != 40 {
		t.Fatal("missing member-only cgroup sample", sample)
	}
	m.HostAvailableMemoryGB = 50
	m.MemoryObservation = &HostMemoryObservation{TotalGB: 121, UsedGB: 46}
	return q, clock, m, holder, sample
}

func TestDockerHostMemoryFreshCredit(t *testing.T) {
	q, clock, m, holder, sample := dockerHostCreditFixture(t)
	r := AttemptReport{Execution: Execution{ID: holder.ID, Attempt: holder.Attempt}, Session: holder.Session, Started: true, CurrentHostMemory: sample}
	if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{r}}); err != nil {
		t.Fatal(err)
	}
	// 56.125 reserved, 40 resident => 16.125 growth, not 56.125.
	if got := q.s.Machines[m.ID].FreeHostMemoryGB; got != 33.875 {
		t.Fatalf("headroom %g, want 50 - (56.125 - 40) = 33.875", got)
	}
	w := Worker{Machine: m, Clock: clock, HostMemoryReserveGB: 16, HostMemorySource: func(context.Context) (float64, error) { return 59, nil }, hostMemorySamples: currentHostMemorySamples([]AttemptReport{r}, clock.Now())}
	if err := localAttemptAdmission(w, []Job{*holder}, Job{Spec: cpuSpec(1, 20)}, nil); err != nil {
		t.Fatal("worker did not credit current Docker use", err)
	}
	// Even a larger host cgroup cannot consume the full device-growth floor.
	sample.NonreclaimableGB = 55
	if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{r}}); err != nil {
		t.Fatal(err)
	}
	if got := q.s.Machines[m.ID].FreeHostMemoryGB; got != 34 {
		t.Fatal("unified CUDA device floor lost", got)
	}
}

func TestDockerHostMemoryStaleOrMissingKeepsReservation(t *testing.T) {
	for _, mode := range []string{"stale", "missing", "wrong-attempt", "wrong-session", "unconfirmed-membership"} {
		t.Run(mode, func(t *testing.T) {
			q, clock, m, holder, sample := dockerHostCreditFixture(t)
			r := AttemptReport{Execution: Execution{ID: holder.ID, Attempt: holder.Attempt}, Session: holder.Session, Started: true, CurrentHostMemory: sample}
			switch mode {
			case "stale":
				clock.Advance(hostMemorySampleFreshFor)
				sample.AgeSeconds = clock.Now().Sub(sample.sampledAt).Seconds()
			case "missing":
				r.CurrentHostMemory = nil
			case "wrong-attempt":
				r.Attempt++
			case "wrong-session":
				r.Session = "old-session"
			case "unconfirmed-membership":
				d, fs := dockerHostMemoryFixture(t, clock)
				fs.files["/proc/42/cgroup"] = "0::/unrelated\n"
				r.CurrentHostMemory = d.currentHostMemory(clock, fs)
				if r.CurrentHostMemory != nil {
					t.Fatal("unrelated local PID granted credit")
				}
			}
			if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{r}}); err != nil {
				t.Fatal(err)
			}
			if got := q.s.Machines[m.ID].FreeHostMemoryGB; got != 0 {
				t.Fatal("full reservation was not retained", got)
			}
			w := Worker{Machine: m, Clock: clock, HostMemoryReserveGB: 16, HostMemorySource: func(context.Context) (float64, error) { return 59, nil }, hostMemorySamples: currentHostMemorySamples([]AttemptReport{r}, clock.Now())}
			if err := localAttemptAdmission(w, []Job{*holder}, Job{Spec: cpuSpec(1, 1)}, nil); err == nil {
				t.Fatal("worker credited unavailable/stale/wrong attempt memory")
			}
		})
	}
}

func TestDockerHostMemoryExternalAttribution(t *testing.T) {
	q, clock, m, holder, sample := dockerHostCreditFixture(t)
	r := AttemptReport{Execution: Execution{ID: holder.ID, Attempt: holder.Attempt}, Session: holder.Session, Started: true, CurrentHostMemory: sample}
	old := workerMemoryRSS
	t.Cleanup(func() { workerMemoryRSS = old })
	workerMemoryRSS = func(context.Context) (float64, error) { return 1, nil }
	w := Worker{Clock: clock}
	m.MemoryObservation = creditDockerObservation(w.hostMemoryObservation(context.Background(), m, 75, nil), []Job{*holder}, []AttemptReport{r}, clock.Now())
	if m.MemoryObservation == nil || m.MemoryObservation.MyceliumGB != 41 {
		t.Fatal("managed container memory labelled external", m.MemoryObservation)
	}
	// Isolate this observation from the initial idle inventory check-in.
	q.s.ExternalMemory = nil
	q.recordExternalMemory(&m)
	if got := q.externalMemoryStatus(m.HostID).Week.MaxGB; got != 5 {
		t.Fatal("external memory includes Docker charge", got)
	}
	clock.Advance(hostMemorySampleFreshFor)
	if o := creditDockerObservation(w.hostMemoryObservation(context.Background(), m, 75, nil), []Job{*holder}, []AttemptReport{r}, clock.Now()); o == nil || !o.AttributionUnknown || o.TotalGB != 121 || o.UsedGB != 46 {
		t.Fatal("stale container sample lost physical memory evidence", o)
	}
}

// A failed attribution probe must preserve raw physical evidence for admission.
func TestDockerHostMemoryUnknownAttributionKeepsPhysicalHeadroom(t *testing.T) {
	q, clock, m, holder, _ := dockerHostCreditFixture(t)
	m.HostAvailableMemoryGB = 80
	before := q.externalMemoryStatus(m.HostID).Week.Samples
	r := AttemptReport{Execution: Execution{ID: holder.ID, Attempt: holder.Attempt}, Session: holder.Session, Started: true}
	// An earlier fresh sample exceeds current physical usage after a drop.
	// A later missing sample must not publish partial, invalid attribution.
	fresh := *holder
	fresh.ID = "other-docker"
	freshReport := r
	freshReport.ID = fresh.ID
	freshReport.CurrentHostMemory = &CurrentHostMemory{NonreclaimableGB: 40, sampledAt: clock.Now()}
	m.MemoryObservation = creditDockerObservation(&HostMemoryObservation{TotalGB: 121, UsedGB: 16, MyceliumGB: 1}, []Job{fresh, *holder}, []AttemptReport{freshReport, r}, clock.Now())
	if !validHostMemoryObservation(m.MemoryObservation) || !m.MemoryObservation.AttributionUnknown {
		t.Fatal("missing attribution lost physical observation", m.MemoryObservation)
	}
	if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{r}}); err != nil {
		t.Fatal(err)
	}
	if got := q.s.Machines[m.ID].FreeHostMemoryGB; got != 23.875 {
		t.Fatalf("unknown attribution blocked full-reservation headroom: %g, want 80 - 56.125", got)
	}
	if got := q.externalMemoryStatus(m.HostID).Week.Samples; got != before {
		t.Fatal("unknown attribution polluted external history", got)
	}
}
