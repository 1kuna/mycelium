package myce

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMemoryReviewLegacyObservationKey(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	j := startObservation(t, q, m, s)
	key, why := q.workObservationKey(&j)
	if key == nil {
		t.Fatal(why)
	}
	// This is the persisted pre-146 field order, including its OOM fence.
	var legacy struct {
		Allowance       string          `json:"allowance,omitempty"`
		Work            string          `json:"work"`
		ExtensionDigest string          `json:"extension_digest"`
		Target          ExecutionTarget `json:"target"`
		Reference       WorkReference   `json:"reference"`
		Host            string          `json:"host"`
		Component       string          `json:"component"`
		Hardware        string          `json:"hardware"`
		Fingerprint     string          `json:"fingerprint"`
		Environment     string          `json:"environment"`
		Configuration   string          `json:"configuration"`
		Concurrency     int             `json:"concurrency"`
		BatchSize       int             `json:"batch_size"`
	}
	b, _ := json.Marshal(key)
	if err := json.Unmarshal(b, &legacy); err != nil {
		t.Fatal(err)
	}
	b, _ = json.Marshal(legacy)
	var restored ObservationKey
	if err := json.Unmarshal(b, &restored); err != nil {
		t.Fatal(err)
	}
	if err := q.update(func() error {
		q.s.WorkObservations = map[string]WorkObservation{identityHash(legacy): {Key: restored, Forbidden: true, RecordedAt: clock.Now()}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !q.s.WorkObservations[identityHash(key)].Forbidden {
		t.Fatal("persisted pre-146 Forbidden observation no longer resolves")
	}
}

func TestMemoryReviewDockerMissingParentReleases(t *testing.T) {
	_, clock := fixture(t)
	parent, err := fakeCeilings(t).create("attempt-stopped-unit", 4<<30, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := parent.cleanup(clock); err != nil {
		t.Fatal(err)
	}
	unavailable := true
	d := &dockerAttempt{clock: clock, dir: t.TempDir(), memory: parent,
		state: dockerExecution{Identity: "stopped-unit", CgroupParent: "/worker/attempt-stopped-unit", Stopped: true, MemoryLimitBytes: 4 << 30},
		command: func(_ context.Context, args ...string) ([]byte, error) {
			if args[0] != "ps" && !(args[0] == "network" && args[1] == "ls") {
				t.Fatalf("unexpected daemon operation %v", args)
			}
			if unavailable {
				return nil, errors.New("daemon unavailable")
			}
			return nil, nil // Successful inventory proves no labelled members remain.
		}}
	if done, err := d.reconcile(); err == nil || done || d.state.Released {
		t.Fatal("missing parent released without daemon proof")
	}
	unavailable = false
	done, err := d.reconcile()
	if err != nil || !done || !d.state.Released {
		t.Fatalf("stopped unit held custody: done=%v err=%v", done, err)
	}
	r := dockerOutcome(d.state, Result{}, errDockerRecovery)
	if r.MemoryPeak != nil || r.Success || !r.Interrupted {
		t.Fatalf("lost counters were learned or called successful: %+v", r)
	}
}

func TestMemoryReviewNativeMemberOOM(t *testing.T) {
	_, clock := fixture(t)
	parent, err := fakeCeilings(t).create("attempt-capped-group", 4<<30, 0)
	if err != nil {
		t.Fatal(err)
	}
	j := Job{Spec: Spec{Needs: ResourceNeeds{HostMemoryGB: 4}, Run: RunSpec{MemoryGB: 2, Sidecars: []NativeSidecar{{Name: "helper", MemoryGB: 1}}}}}
	g := nativeGroup{memory: parent, p: &process{job: j}, worker: Worker{Clock: clock}}
	if err := g.setupMemory(); err != nil {
		t.Fatal(err)
	}
	mode, _ := os.ReadFile(filepath.Join(parent.path, "memory.oom.group"))
	if strings.TrimSpace(string(mode)) != "0" {
		t.Error("legacy capped native group gained whole-group OOM kill")
	}
	member := g.memberMemory["helper"]
	g.members = []*nativeMember{{Name: "helper", memory: member}}
	for p, value := range map[string]string{
		filepath.Join(parent.path, "memory.events"):       "oom 1\noom_kill 1\n",
		filepath.Join(parent.path, "memory.events.local"): "oom 0\noom_kill 0\n",
		filepath.Join(member.path, "memory.events"):       "oom 1\noom_kill 1\n",
	} {
		if err := os.WriteFile(p, []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.memoryFailure(); err == nil || !strings.Contains(err.Error(), "native member helper") {
		t.Fatalf("member OOM attributed to parent: %v", err)
	}
	// A parent limit may kill a descendant without that descendant hitting
	// its own limit. Local OOM triggers distinguish the two causes.
	for p, value := range map[string]string{
		filepath.Join(parent.path, "memory.events.local"): "oom 1\noom_kill 0\n",
		filepath.Join(member.path, "memory.events"):       "oom 0\noom_kill 1\n",
	} {
		if err := os.WriteFile(p, []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.memoryFailure(); err == nil || strings.Contains(err.Error(), "native member") {
		t.Fatalf("parent OOM attributed to member victim: %v", err)
	}
	s := &workerStore{dir: t.TempDir(), state: workerState{Identity: "capped-group", Job: &j}}
	if err := saveWorkerJSON(s.dir, "attempt-capped-group.json", workerState{Identity: s.state.Identity, Cgroup: parent.path}); err != nil {
		t.Fatal(err)
	}
	if err := saveWorkerJSON(s.dir, "native-capped-group.json", struct {
		Identity string
		Members  []nativeMember
	}{s.state.Identity, []nativeMember{{Name: "helper", Cgroup: member.path}}}); err != nil {
		t.Fatal(err)
	}
	if err := s.recoverNativeCause("worker", parent.fs); err != nil {
		t.Fatal(err)
	}
	if s.state.Result == nil || !s.state.Result.OutOfMemory || strings.Contains(s.state.Result.Error, "native member") {
		t.Fatalf("recovery lost parent OOM attribution: %+v", s.state.Result)
	}
	t.Cleanup(func() { _ = parent.cleanup(clock) })
}

func TestMemoryReviewReclaimableCache(t *testing.T) {
	a, err := fakeCeilings(t).create("attempt-cache", 8<<30, 0)
	if err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string]string{
		"memory.peak":   "8589934592",
		"memory.stat":   "anon 1073741824\nkernel 536870912\nshmem 536870912\nfile 6442450944\npgscan_file 100\n",
		"memory.events": "max 100\noom 0\noom_kill 0\n",
	} {
		if err := os.WriteFile(filepath.Join(a.path, name), []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	p := a.peak()
	if p == nil || p.HostGB == nil || *p.HostGB != 2 || p.Constrained {
		t.Fatalf("reclaimable cache inflated or invalidated resident peak: %+v", p)
	}
	if err := os.WriteFile(filepath.Join(a.path, "memory.stat"), []byte("anon 1073741824\nkernel 0\nshmem 0\npgscan_anon 1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if p := a.peak(); p == nil || !p.Constrained {
		t.Fatal("anonymous reclaim was learned as unconstrained")
	}
}

func TestMemoryReviewResumedEstimateAndOOMFloor(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	first := startObservation(t, q, m, s)
	clock.Advance(time.Second)
	r := result(&first, true)
	host := 2.0
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	// A retry may use complete-input history even though it cannot record it.
	probe := *q.s.Jobs[first.ID]
	probe.State, probe.Attempt, probe.Spec.Resumable, probe.Spec.Needs.Memory = "queued", 2, true, "auto"
	for id, point := range q.s.WorkObservations {
		point.Forbidden = true
		q.s.WorkObservations[id] = point
	}
	estimate := q.memoryCandidate(&probe, q.s.Machines[m.ID])
	if estimate.Spec.Needs.HostMemoryGB != 2.5 {
		t.Fatal("resumed or Forbidden performance point lost its accepted memory peak")
	}
	s.Needs.Memory, s.Resumable = "auto", true
	second := startObservation(t, q, m, s)
	clock.Advance(time.Second)
	r = result(&second, false)
	r.OutOfMemory, r.Error = true, "exceeded its 2.5 GiB memory ceiling"
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	queued := job(t, q, second.ID)
	if queued.State != "queued" || queued.OOMCapacity != nil || queued.MemoryEnvelope != nil {
		t.Fatalf("auto OOM requires a larger hypha: %+v", queued)
	}
	next := check(t, q, m, nil).Job
	if next == nil || next.Spec.Needs.HostMemoryGB <= 2.5 {
		t.Fatalf("same-host retry lost OOM lower bound: %+v; %s", next, job(t, q, second.ID).Reason)
	}
	if next != nil {
		if err := q.Started(result(next, false)); err != nil {
			t.Fatal(err)
		}
		clock.Advance(time.Second)
		finish(t, q, next, true)
		for _, point := range q.s.WorkObservations {
			if point.Job == second.ID && point.Attempt > 1 {
				t.Fatal("resumed attempt trained a full-input observation")
			}
		}
	}
}
