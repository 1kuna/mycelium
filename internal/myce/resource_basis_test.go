package myce

import (
	"maps"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Job GET retains the submitted hint alongside the measured pool estimate,
// and status counts every retained admission even with a paged job list.
func TestResourceBasisWithMemoryPools(t *testing.T) {
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
	if mem.Dimension != "host_memory_gb" || mem.Basis != pooledMemoryCapability || mem.Declared == nil || *mem.Declared != 6 || mem.Used == nil || *mem.Used != 6 {
		t.Fatalf("submitted memory hint and charge: %+v", mem)
	}
	if got.Memory == nil || got.Memory.RAM.TypicalGB != peak || got.Memory.RAM.ChargeGB != 6 || got.Memory.RAM.Evidence.Runs != 1 {
		t.Fatalf("missing estimate and its measured evidence: %+v", got.Memory)
	}
	if cpu.Dimension != "cpu_cores" || cpu.Used == nil || *cpu.Used != 1 || cpu.Basis != "declared" || cpu.Declared == nil || cpu.Learned != nil {
		t.Fatalf("CPU dimension: %+v", cpu)
	}
	snap, err := q.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	host := hostID(&m)
	if len(snap.Admissions) != 1 || snap.Admissions[0].Host != host || !maps.Equal(snap.Admissions[0].Current, map[string]int{pooledMemoryCapability: 1}) || !maps.Equal(snap.Admissions[0].Recent, map[string]int{pooledMemoryCapability: 2}) {
		t.Fatalf("admissions summary for %s: %+v", host, snap.Admissions)
	}
	// A one-job status page still counts every retained admission.
	paged, err := q.statusSnapshotOptions(&statusOptions{jobs: "recent", limit: 1, full: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(paged.Admissions) != 1 || !maps.Equal(paged.Admissions[0].Recent, snap.Admissions[0].Recent) || !maps.Equal(paged.Admissions[0].Current, snap.Admissions[0].Current) {
		t.Fatalf("paged status admissions: %+v", paged.Admissions)
	}
}
