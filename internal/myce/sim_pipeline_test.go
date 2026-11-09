package myce

import (
	"testing"
)

func pipelineSimFixture() (SimFleet, SimWorkload) {
	f := simTestFleet(3)
	f.Machines[0].Platform = "xpu"
	f.Machines[1].Platform = "xpu"
	f.Machines[2].Platform = "cuda"
	f.Machines[1].Speed = 2
	s := simTestStream("pieces", "experiment", 0, 20, 1)
	s.Platforms = []string{"xpu"}
	s.MemoryGB, s.HostGB, s.Batch = 2, 1, 7
	s.Curves = map[string][]SimCurvePoint{"a": {{1, 1}, {2, 1.7}}, "b": {{1, 2}, {2, 3.4}}}
	s.Downstream = &SimDownstream{Duration: 8, Platforms: []string{"cuda"}, MemoryGB: 1, HostGB: 1, PublicationSeconds: 2, TransferSeconds: 1, PreparationSeconds: 1, IntermediateBytes: 1024}
	return f, simTestWork(s)
}
func TestSimPipelineOverlapAndCoverage(t *testing.T) {
	f, w := pipelineSimFixture()
	r, err := ComparePieceDependencies(f, w, SimOptions{Days: 300.0 / 86400, Seed: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !r.GatePassed {
		for k, m := range r.Baseline.Pipelines {
			t.Logf("baseline %s %+v", k, m)
			for k, p := range m.Pieces {
				t.Logf("piece %s %+v", k, p)
			}
		}
		for k, m := range r.PerPiece.Pipelines {
			t.Logf("candidate %s %+v", k, m)
			for k, p := range m.Pieces {
				t.Logf("piece %s %+v", k, p)
			}
		}
		t.Fatalf("gate: coverage=%t overlap=%t gain=%v failures=%d/%d unfinished=%d/%d baseline=%+v per-piece=%+v", r.Coverage, r.Overlap, r.MeanGain, r.Baseline.Failed, r.PerPiece.Failed, r.Baseline.Unfinished, r.PerPiece.Unfinished, r.Baseline.Pipelines, r.PerPiece.Pipelines)
	}
	for _, m := range r.PerPiece.Pipelines {
		for _, p := range m.Pieces {
			if p.Accepted == nil || p.Ready == nil || p.Started == nil || *p.AcceptanceToReady != 0 || *p.AcceptanceToStart < 2 || !p.Output {
				t.Fatalf("incorrect acceptance/staging: %+v", p)
			}
		}
	}
}
