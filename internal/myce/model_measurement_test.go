package myce

import (
	"fmt"
	"testing"
	"time"
)

func boundModelFixture(t *testing.T, n int) (*Queue, *Machine, Model, *ModelCall) {
	t.Helper()
	q, clock := fixture(t)
	model := Model{Name: "bound", Engine: "fake", Path: "synthetic", MemoryGB: 16, HostMemoryGB: 8, MaxInFlight: 8, Platforms: []string{"fake"}}
	digest := ModelMeasurementDigest(model, q.environments)
	e := compositionExtension(t, false)
	e.Variants[0].Target = ExecutionTarget{OS: "linux", Arch: "amd64", Platform: "fake"}
	e.Variants[0].References = map[string]WorkReference{"extension:sample": {InputShape: "bounded-v1", Unit: "work", Units: 400}}
	e.Variants[0].Sweeps = map[string]ReferenceSweep{"extension:sample": {Command: []string{"synthetic-server-reference"}, Counts: []int{1, 2, 4, 8}, MaxConcurrency: 8, ServerDigest: digest}}
	e = freezeComposition(t, e)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	model.Measurement = &ModelMeasurementBinding{ExtensionDigest: e.Digest, Work: "extension:sample", ServerDigest: digest}
	if err := q.ConfigureModels(ModelConfig{Idle: time.Hour, Wait: time.Minute, Models: []Model{model}}); err != nil {
		t.Fatal(err)
	}
	m := cpuMachine("bound-host", "bound-host", "fake", 8, 64)
	m.ModelURL = "http://synthetic.invalid"
	m.ModelConfigHash = modelConfigHash(q.models)
	m.Devices = []MachineDevice{{ID: "0", Platform: "fake", Identity: "synthetic-card", Fingerprint: "synthetic-v1", ExtensionFingerprint: "synthetic-v1", Selector: "0", MemoryMode: "fixed", TotalMemoryGB: 32, BudgetGB: 32, FreeMemoryGB: 32, MemoryMeasured: true}}
	target := m.Devices[0].machine(m)
	r := newExtensionReport(e, &target, "extension:sample")
	r.Setup, r.Qualified, r.CheckID = true, true, "server-reference"
	m.Devices[0].ExtensionReports = []ExtensionReport{r}
	check(t, q, m, nil)
	q.s.ModelSlots[targetSlotKey(&target, model.Name)] = &ModelSlot{Machine: m.ID, Device: "0", Session: m.Session, Attempt: "synthetic-engine", State: "ready", Model: model.Name, Priority: "experiment", LastUsed: clock.Now()}
	q.refreshHostMemory()
	var actual *Machine
	for _, candidate := range q.machineTargets(q.s.Machines[m.ID]) {
		if candidate.Device == "0" {
			actual = candidate
		}
	}
	key, why := curveKey(e, actual, "extension:sample")
	if key == nil {
		t.Fatal(why)
	}
	q.s.WorkObservations = map[string]WorkObservation{}
	for c, rate := range map[int]float64{1: 25, 2: 44, 4: 72, 8: 96} {
		k := *key
		k.Concurrency = c
		q.s.WorkObservations[identityHash(k)] = WorkObservation{Key: k, Rate: rate, Samples: 1, HostMemoryGB: 4, DeviceMemoryGB: 8, RecordedAt: clock.Now()}
	}
	var first *ModelCall
	for i := range n {
		c := &ModelCall{ID: fmt.Sprintf("piece-%02d", i), Model: model.Name, Project: "synthetic", Priority: "experiment", Class: "experiment", FanOut: "group", Workload: &Workload{InputShape: "bounded-v1", Units: 400}, Queued: clock.Now(), WaitingSince: clock.Now()}
		q.calls[c.ID] = c
		if first == nil {
			first = c
		}
	}
	return q, actual, model, first
}

func TestModelMeasurementBindingAndMeasuredAdmission(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(fmt.Sprint(changed), func(t *testing.T) {
			q, m, _, _ := boundModelFixture(t, 8)
			want := 8
			if changed {
				// Altered server cannot reuse the command's bound measurement.
				q.models.Models[0].Args = []string{"changed-context"}
				q.s.Machines[m.ID].ModelConfigHash = modelConfigHash(q.models)
				want = 1
			}
			q.maintain()
			if got := q.s.ModelSlots[targetSlotKey(m, "bound")].InFlight; got != want {
				t.Fatalf("actual admission=%d want=%d", got, want)
			}
		})
	}
}
