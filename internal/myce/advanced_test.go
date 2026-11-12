package myce

import (
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// MY-54: GPU work never runs on CPU unless it opts in under advanced.
func TestGPUWorkRunsOnCPUOnlyWithAdvancedFallback(t *testing.T) {
	t.Parallel()
	q, _ := fixture(t)
	cpu := cpuMachine("cpu", "host", "cpu", 8, 16)
	gpu := spec("experiment")
	gpu.Needs.Platform = Platform{Required: []string{"cuda"}}
	waiting := submit(t, q, gpu)
	if got := check(t, q, cpu, nil).Job; got != nil {
		t.Fatal("GPU work ran on a CPU target by default", got.ID)
	}
	if j := job(t, q, waiting.ID); j.State != "queued" {
		t.Fatal("GPU work stopped waiting", j.State)
	}
	if err := q.Cancel(waiting.ID); err != nil {
		t.Fatal(err)
	}

	opted := spec("experiment")
	opted.Needs.Platform = Platform{Required: []string{"cuda"}}
	opted.Advanced = &Advanced{AllowCPUFallback: true}
	fallback := submit(t, q, opted)
	if !fallback.Spec.AllowCPUFallback || fallback.Spec.Advanced != nil {
		t.Fatalf("advanced not stored flat: %+v", fallback.Spec)
	}
	run := check(t, q, cpu, nil).Job
	if run == nil || run.ID != fallback.ID {
		t.Fatal("opted-in GPU work did not run on CPU", job(t, q, fallback.ID).Reason)
	}
	stored := q.s.Jobs[fallback.ID]
	if !stored.Spec.Needs.cpuOnly() || stored.Spec.Needs.MemoryGB != 0 || stored.Spec.Needs.HostMemoryGB != 8 || stored.CPUFallbackFrom == nil {
		t.Fatalf("CPU attempt reservation: needs=%+v from=%+v", stored.Spec.Needs, stored.CPUFallbackFrom)
	}
	q.requeue(stored, "machine quiet for 1m", false)
	if stored.CPUFallbackFrom != nil || !slices.Equal(stored.Spec.Needs.Platform.Required, []string{"cuda"}) || stored.Spec.Needs.MemoryGB != 8 {
		t.Fatalf("requeue kept the CPU reservation: %+v", stored.Spec.Needs)
	}
}

// With a GPU target free, opted-in work still goes to the GPU.
func TestCPUFallbackPrefersAFreeGPU(t *testing.T) {
	t.Parallel()
	q, _ := fixture(t)
	cpu, gpu := cpuMachine("cpu", "host", "cpu", 8, 16), cpuMachine("gpu", "host2", "cuda", 8, 16)
	gpu.FreeMemoryGB = 16
	check(t, q, gpu, nil)
	s := spec("experiment")
	s.Needs.Platform = Platform{Required: []string{"cuda"}}
	s.Advanced = &Advanced{AllowCPUFallback: true}
	j := submit(t, q, s)
	if got := check(t, q, cpu, nil).Job; got != nil {
		t.Fatal("fallback took the CPU while a GPU fit")
	}
	if got := check(t, q, gpu, nil).Job; got == nil || got.ID != j.ID || got.Spec.Needs.cpuOnly() {
		t.Fatal("GPU target did not take the work", got)
	}
}

func TestAdvancedSectionParsingDefaultsAndDeprecation(t *testing.T) {
	t.Parallel()
	decode := func(text string) (Spec, error) {
		var s Spec
		d := yaml.NewDecoder(strings.NewReader(text))
		d.KnownFields(true)
		return s, d.Decode(&s)
	}
	base := "name: j\nproject: p\npriority: experiment\nrun: {command: [echo]}\nneeds: {memory_gb: 1, platform: cuda}\n"

	// Defaults when advanced is absent.
	s, err := decode(base)
	if err != nil {
		t.Fatal(err)
	}
	if deprecated, err := s.FoldAdvanced(); err != nil || len(deprecated) != 0 || s.AllowCPUFallback || s.Resumable || s.Isolation != "" || AdvancedStatus(s) != nil {
		t.Fatalf("defaults: %v %v %+v", deprecated, err, s)
	}

	// The advanced section folds onto the stored fields.
	s, err = decode(base + "advanced: {allow_cpu_fallback: true, resumable: true, wall_time: 2h, isolation: sandboxed}\n")
	if err != nil {
		t.Fatal(err)
	}
	if deprecated, err := s.FoldAdvanced(); err != nil || len(deprecated) != 0 {
		t.Fatal(deprecated, err)
	}
	if !s.AllowCPUFallback || !s.Resumable || s.WallTime != "2h" || s.Isolation != "sandboxed" || s.Advanced != nil || s.Validate() != nil {
		t.Fatalf("folded: %+v", s)
	}
	if a := AdvancedStatus(s); a == nil || !a.AllowCPUFallback || a.WallTime != "2h" || a.Preemptible {
		t.Fatalf("status view: %+v", a)
	}

	// Old top-level placement still parses, and is reported as deprecated.
	s, err = decode(base + "resumable: true\nwall_time: 1h\n")
	if err != nil {
		t.Fatal(err)
	}
	deprecated, err := s.FoldAdvanced()
	if err != nil || !slices.Equal(deprecated, []string{"resumable", "wall_time"}) || !s.Resumable || s.Validate() != nil {
		t.Fatalf("legacy: %v %v %+v", deprecated, err, s)
	}

	// Conflicting placements are refused; the new opt-in is advanced-only.
	s, _ = decode(base + "wall_time: 1h\nadvanced: {wall_time: 2h}\n")
	if _, err := s.FoldAdvanced(); err == nil || !strings.Contains(err.Error(), "advanced.wall_time") {
		t.Fatal("conflict accepted", err)
	}
	if _, err := decode(base + "allow_cpu_fallback: true\n"); err == nil {
		t.Fatal("top-level allow_cpu_fallback accepted")
	}
	s, _ = decode("name: j\nproject: p\npriority: experiment\nrun: {command: [echo]}\nneeds: {memory_gb: 1, platform: cuda, model: m}\nadvanced: {allow_cpu_fallback: true}\n")
	if err := s.Validate(); err == nil {
		t.Fatal("CPU fallback accepted for a GPU model server")
	}
}
