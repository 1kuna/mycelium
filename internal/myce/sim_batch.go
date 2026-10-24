package myce

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
)

// Explicit synthetic assumptions, in work-units/second. These are execution
// timers, not another scheduling policy. Real fan-out admission selects counts.
type SimCurvePoint struct {
	Concurrency int     `yaml:"concurrency"`
	Rate        float64 `yaml:"rate"`
}

func simBatchExtensions(w SimWorkload) (map[string]Extension, error) {
	out := map[string]Extension{}
	for _, s := range w.Streams {
		if len(s.Curves) == 0 {
			continue
		}
		if s.Call || s.Batch < 2 || s.Batch > 256 {
			return nil, errors.New("synthetic curves require a declared command batch of 2..256 pieces")
		}
		for machine, points := range s.Curves {
			if machine == "" || len(points) == 0 || points[0].Concurrency != 1 {
				return nil, errors.New("synthetic curves require machine names and a serial point")
			}
			last := 0
			for _, p := range points {
				if p.Concurrency <= last || p.Concurrency > 64 || p.Concurrency&(p.Concurrency-1) != 0 || !validRate(p.Rate) {
					return nil, errors.New("invalid synthetic curve")
				}
				last = p.Concurrency
			}
		}
		e := Extension{Name: "sim-" + s.Name, Version: "1", Purpose: "Synthetic simulator execution; no hardware qualification", Resumable: s.Checkpoint > 0, Files: map[string][]byte{"baseline": []byte("synthetic\n")}}
		for _, platform := range s.Platforms {
			counts := []int{1, 2, 4, 8, 16, 32, 64}
			e.Variants = append(e.Variants, ExtensionVariant{Target: ExecutionTarget{OS: "linux", Arch: "amd64", Platform: platform}, Needs: ResourceNeeds{CPUCores: 1, MemoryGB: s.MemoryGB, HostMemoryGB: s.HostGB}, Execution: ExecutionSpec{Main: ExecutionMember{Command: []string{"simulated-timer"}}}, Check: []string{"simulated-check"}, Baselines: map[string]ExtensionBaseline{"extension:" + e.Name: {Input: "baseline", Output: "baseline", Native: "synthetic", Variation: "synthetic", Measure: "exact"}}, References: map[string]WorkReference{"extension:" + e.Name: {InputShape: "synthetic-v1", Unit: "work", Units: 1}}, Sweeps: map[string]ReferenceSweep{"extension:" + e.Name: {Command: []string{"simulated-sweep"}, Run: []string{"simulated-wave"}, Counts: counts, MaxConcurrency: 64}}})
		}
		data, err := yaml.Marshal(e)
		if err != nil {
			return nil, err
		}
		e.Files["extension.yaml"] = data
		e, err = extensionFromFiles(e.Files)
		if err != nil {
			return nil, err
		}
		out[s.Name] = e
	}
	return out, nil
}

func simBatchInventory(m *Machine, extensions map[string]Extension) {
	m.CapabilityProfile.Arch = "amd64"
	m.Device = "0"
	m.ExtensionFingerprint = "synthetic-v1"
	d := MachineDevice{ID: "0", Platform: m.GPU, Identity: "synthetic-" + m.ID, Fingerprint: "synthetic-v1", ExtensionFingerprint: "synthetic-v1", Selector: "0", MemoryMode: "fixed", MemoryMeasured: true, TotalMemoryGB: m.TotalMemoryGB, BudgetGB: m.TotalMemoryGB, FreeMemoryGB: m.FreeMemoryGB, UnifiedMemory: m.UnifiedMemory}
	m.Devices = []MachineDevice{d}
	for _, e := range extensions {
		if !e.matchesTarget(m) {
			continue
		}
		r := newExtensionReport(e, m, "extension:"+e.Name)
		r.Setup, r.Qualified, r.CheckID = true, true, "synthetic-reference"
		m.Devices[0].ExtensionReports = append(m.Devices[0].ExtensionReports, r)
	}
	// Virtual workers use the legacy single-assignment transport. Mirror their
	// one component's qualification for that path, just as device views do.
	m.ExtensionReports = slices.Clone(m.Devices[0].ExtensionReports)
}

func seedSimBatchCurves(q *Queue, w SimWorkload, extensions map[string]Extension) {
	if q.s.WorkObservations == nil {
		q.s.WorkObservations = map[string]WorkObservation{}
	}
	for _, s := range w.Streams {
		e, ok := extensions[s.Name]
		if !ok {
			continue
		}
		for _, m := range q.plannerMachines() {
			key, _ := curveKey(e, m, "extension:"+e.Name)
			if key == nil {
				continue
			}
			for _, p := range s.Curves[m.ID] {
				k := *key
				k.Concurrency = p.Concurrency
				id := identityHash(k)
				if _, exists := q.s.WorkObservations[id]; exists {
					continue
				}
				q.s.WorkObservations[id] = WorkObservation{Key: k, Source: "synthetic simulator", Rate: p.Rate, HostMemoryGB: s.HostGB * float64(p.Concurrency), DeviceMemoryGB: s.MemoryGB * float64(p.Concurrency), Samples: 1, RecordedAt: q.clock.Now()}
			}
		}
	}
}

func simDeclaredSpec(t *simTask, epoch time.Time, e Extension) Spec {
	s := simSpec(t, epoch)
	s.Run = RunSpec{}
	s.Needs = ResourceNeeds{}
	s.Extension = extensionKey(e)
	s.Workload = &Workload{InputShape: "synthetic-v1", Units: t.work / float64(len(t.pieceWork))}
	s.FanOut = &FanOut{MaxConcurrency: 64}
	for i := range t.pieceWork {
		s.FanOut.Pieces = append(s.FanOut.Pieces, FanOutPiece{Key: strconv.Itoa(i)})
	}
	return s
}

func simWaveWork(t *simTask, j *Job, m SimMachine) error {
	units := 0.0
	for _, p := range j.Batch.Pieces {
		index, err := strconv.Atoi(p.Key)
		if err != nil || index < 0 || index >= len(t.parent.pieceWork) {
			return errors.New("unknown simulated piece")
		}
		units += t.parent.pieceWork[index]
	}
	rate := m.Speed // serial fallback is still one piece, never inferred concurrency
	if j.Batch.Run {
		index := slices.IndexFunc(t.stream.Curves[m.Name], func(p SimCurvePoint) bool { return p.Concurrency == j.Batch.Concurrency })
		if index < 0 {
			return fmt.Errorf("missing execution curve for %s at %d", m.Name, j.Batch.Concurrency)
		}
		rate = t.stream.Curves[m.Name][index].Rate
	}
	t.work = units / rate * m.Speed
	t.reservation = j.Spec.Needs.MemoryGB
	return nil
}

func simModelSlot(q *Queue, machine, model string) *ModelSlot {
	for _, slot := range q.s.ModelSlots {
		if slot.Machine == machine && slot.Model == model {
			return slot
		}
	}
	return nil
}
