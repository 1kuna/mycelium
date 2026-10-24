package myce

import "testing"

func TestSimDeclaredBatchGainAndProductionP95(t *testing.T) {
	f := simTestFleet(2)
	s := simTestStream("batch", "experiment", 0, 16, 1)
	s.MemoryGB, s.HostGB, s.Batch, s.Checkpoint = 1, 1, 40, 1
	s.Curves = map[string][]SimCurvePoint{}
	for _, m := range f.Machines {
		s.Curves[m.Name] = []SimCurvePoint{{1, 1}, {2, 1.76}, {4, 2.88}, {8, 3.84}}
	}
	// Production arrives with the batch and must retain first admission. The
	// mixed-load CLI comparison separately measures arrivals during running work.
	w := simTestWork(s, simTestStream("prod", "production", 0, 5, 1))
	baseline, err := Simulate(f, w, SimOptions{Days: 1000.0 / 86400, Seed: 1})
	if err != nil {
		t.Fatal(err)
	}
	declared, err := Simulate(f, w, SimOptions{Days: 1000.0 / 86400, Seed: 1, Declared: true})
	if err != nil {
		t.Fatal(err)
	}
	if declared.BatchCompleted != 1 || declared.BatchUnfinished != 0 || declared.BatchMeanSeconds >= baseline.BatchMeanSeconds || declared.Production.P95 != baseline.Production.P95 {
		t.Fatalf("baseline batch/p95=%.3f/%.3f declared=%.3f/%.3f completed=%d unfinished=%d", baseline.BatchMeanSeconds, baseline.Production.P95, declared.BatchMeanSeconds, declared.Production.P95, declared.BatchCompleted, declared.BatchUnfinished)
	}
}
