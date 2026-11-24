package myce

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

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
