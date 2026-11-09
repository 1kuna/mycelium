package myce

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// I2 (#585 step 2): an explicit figure with accepted evidence shows both
// values on job GET, and status counts each host's admissions by basis.
func TestResourceBasisDeclaredBesideLearned(t *testing.T) {
	q, clock := fixture(t)
	if err := q.ConfigureFileStore(t.TempDir(), 1<<20, true); err != nil {
		t.Fatal(err)
	}
	m := m3Machine()
	m.CPUExtensionFingerprint = "profile-cpu-v1"
	m.MemoryEnforcement = "memory ceilings enforced (cgroup v2)"
	m.HostMemoryBudgetGB, m.MemoryCeilingSlackGB = 16, 0.1
	machineCheck(t, q, m)
	s := cpuSpec(1, 0)
	s.Name, s.Project, s.RequireTarget, s.Needs.Memory = "suite", "synthetic", true, "auto"
	s.Targets = []ExecutionTarget{{OS: "linux", Arch: "amd64", Platform: "cpu"}}
	s.Run.Command = []string{"sh", "-ec", "tar -xf .myce-source.tar\nbash scripts/suite.sh"}
	path := filepath.Join(t.TempDir(), "source")
	if err := os.WriteFile(path, []byte("commit"), 0600); err != nil {
		t.Fatal(err)
	}
	ref, err := IdentifyFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(q.files.blob(ref), []byte("commit"), 0600); err != nil {
		t.Fatal(err)
	}
	s.Files = FileManifest{".myce-source.tar": ref}
	start := func() *Job {
		t.Helper()
		queued := submit(t, q, s)
		j := assignedJob(machineCheck(t, q, m))
		if j == nil || j.ID != queued.ID {
			t.Fatalf("not assigned: %+v", job(t, q, queued.ID))
		}
		if err := q.Started(result(j, false)); err != nil {
			t.Fatal(err)
		}
		return j
	}
	first := start()
	clock.Advance(time.Second)
	peak := 2.0
	r := result(first, true)
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &peak}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	s.Needs.Memory, s.Needs.HostMemoryGB = "", 6
	second := start()
	got, err := q.jobSnapshot(second.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Resources) != 3 {
		t.Fatalf("want host memory, CPU and storage for a CPU job: %+v", got.Resources)
	}
	mem, cpu := got.Resources[0], got.Resources[1]
	if mem.Dimension != "host_memory_gb" || mem.Basis != "declared" || mem.Declared == nil || *mem.Declared != 6 || mem.Learned == nil || *mem.Learned != peak+minimumHostMemoryGB || mem.Evidence != 1 || mem.Used == nil {
		t.Fatalf("explicit figure without its learned comparison: %+v", mem)
	}
	if cpu.Dimension != "cpu_cores" || cpu.Used == nil || *cpu.Used != 1 || cpu.Basis != "declared" || cpu.Declared == nil || cpu.Learned != nil {
		t.Fatalf("CPU dimension: %+v", cpu)
	}
}
