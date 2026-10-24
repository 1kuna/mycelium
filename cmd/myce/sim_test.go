package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestSimCommandNoTokenAndJSON(t *testing.T) {
	t.Setenv("MYCE_TOKEN", "")
	path := filepath.Join(t.TempDir(), "result.json")
	var out bytes.Buffer
	if e := simCommand([]string{"--fleet", "../../examples/sim/fleet.yaml", "--workload", "../../examples/sim/workload.yaml", "--days", "0.01", "--what-if", "split-batch", "--json", path}, &out); e != nil {
		t.Fatal(e)
	}
	data, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	var result map[string]json.RawMessage
	if e = json.Unmarshal(data, &result); e != nil {
		t.Fatal(e)
	}
	if result["baseline"] == nil || result["split"] == nil || out.Len() == 0 {
		t.Fatal("missing comparison output")
	}
}
func TestSimCommandRejectsInputs(t *testing.T) {
	for _, args := range [][]string{{"--what-if", "unknown"}, {"extra"}, {"--days", "0", "--fleet", "../../examples/sim/fleet.yaml", "--workload", "../../examples/sim/workload.yaml"}} {
		if simCommand(args, &bytes.Buffer{}) == nil {
			t.Fatalf("accepted %v", args)
		}
	}
}

func TestSimDeclaredCommandGate(t *testing.T) {
	dir := t.TempDir()
	fleet := filepath.Join(dir, "fleet.yaml")
	if err := os.WriteFile(fleet, []byte(`machines:
  - {name: a, platform: fake, memory_gb: 8, host_memory_gb: 32, speed: 1}
  - {name: b, platform: fake, memory_gb: 8, host_memory_gb: 32, speed: 1}
  - {name: c, platform: cuda, memory_gb: 8, host_memory_gb: 32, speed: 1, departure: loss, windows: [{days: [0], start_hour: 0, end_hour: 0.000277777777777778}]}
`), 0600); err != nil {
		t.Fatal(err)
	}
	for _, arrival := range []string{"0", "0.001388888888888889"} { // simultaneous, then preemption at 5 s
		workload := filepath.Join(dir, "workload.yaml")
		data := `duration_jitter: 0
models:
  - {name: interrupted, memory_gb: 1, host_memory_gb: 1, load_seconds: 0, platforms: [cuda]}
streams:
  - {name: background, project: other, priority: experiment, model: interrupted, call: true, duration_seconds: 100, every_hours: 24, count: 1}
  - name: batch
    project: research
    priority: experiment
    platforms: [fake]
    memory_gb: 1
    host_memory_gb: 1
    duration_seconds: 16
    checkpoint_seconds: 1
    every_hours: 24
    count: 1
    batch_files: 40
    curves:
      a: [{concurrency: 1, rate: 1}, {concurrency: 2, rate: 1.76}, {concurrency: 4, rate: 2.88}, {concurrency: 8, rate: 3.84}]
      b: [{concurrency: 1, rate: 1}, {concurrency: 2, rate: 1.76}, {concurrency: 4, rate: 2.88}, {concurrency: 8, rate: 3.84}]
  - {name: prod, project: prod, priority: production, platforms: [fake], memory_gb: 8, duration_seconds: 5, every_hours: 24, count: 1, start_hour: ` + arrival + `}
`
		if err := os.WriteFile(workload, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, "result.json")
		err := simCommand([]string{"--fleet", fleet, "--workload", workload, "--days", "0.012", "--what-if", "declared-batch", "--json", path}, &bytes.Buffer{})
		passed := arrival == "0"
		if (err == nil) != passed {
			t.Fatalf("arrival=%s gate error=%v", arrival, err)
		}
		result, readErr := os.ReadFile(path)
		if readErr != nil {
			t.Fatal(readErr)
		}
		var report struct {
			Passed             bool `json:"gate_passed"`
			Baseline, Declared struct{ Failed int }
		}
		if err := json.Unmarshal(result, &report); err != nil || report.Passed != passed || report.Baseline.Failed != 1 || report.Declared.Failed != 1 {
			t.Fatalf("gate/background failures: %+v (%v)", report, err)
		}
	}
}
