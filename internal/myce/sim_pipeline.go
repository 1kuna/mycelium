package myce

import (
	"fmt"
	"math"
	"strconv"
	"time"
)

// Synthetic transport/publication assumptions, separately charged in both modes.
type SimDownstream struct {
	Duration           float64  `yaml:"duration_seconds"`
	Platforms          []string `yaml:"platforms"`
	MemoryGB           float64  `yaml:"memory_gb"`
	HostGB             float64  `yaml:"host_memory_gb"`
	PublicationSeconds float64  `yaml:"publication_seconds"`
	TransferSeconds    float64  `yaml:"transfer_seconds"`
	PreparationSeconds float64  `yaml:"preparation_seconds"`
	IntermediateBytes  int64    `yaml:"intermediate_bytes"`
}
type SimPieceMetric struct {
	Accepted          *float64 `json:"accepted_seconds"`
	Ready             *float64 `json:"ready_seconds"`
	Started           *float64 `json:"started_seconds"`
	Finished          *float64 `json:"finished_seconds"`
	AcceptanceToReady *float64 `json:"acceptance_to_ready_seconds"`
	AcceptanceToStart *float64 `json:"acceptance_to_start_seconds"`
	Output            bool     `json:"output"`
}
type SimPipelineMetric struct {
	Submitted                    float64                    `json:"submitted_seconds"`
	Gather                       *float64                   `json:"producer_gather_seconds"`
	FirstStart                   *float64                   `json:"first_downstream_start_seconds"`
	Finished                     *float64                   `json:"pipeline_finished_seconds"`
	Makespan                     *float64                   `json:"makespan_seconds"`
	Pieces                       map[string]*SimPieceMetric `json:"pieces"`
	OutputCount                  int                        `json:"output_count"`
	PeakIntermediateBytes        int64                      `json:"peak_intermediate_bytes"`
	PeakPendingIntermediateBytes int64                      `json:"peak_pending_intermediate_bytes"`
	PublicationSeconds           float64                    `json:"publication_seconds_per_piece"`
	TransferSeconds              float64                    `json:"input_transfer_seconds_per_piece"`
	PreparationSeconds           float64                    `json:"input_preparation_seconds_per_piece"`
}

func simSubmitConsumers(q *Queue, t *simTask, epoch time.Time, barrier bool) ([]*simTask, *SimPipelineMetric, error) {
	d := t.stream.Downstream
	if !validRate(d.Duration) || len(d.Platforms) == 0 || !validRate(d.MemoryGB) || !validMemoryValue(d.HostGB) || !validMemoryValue(d.PublicationSeconds) || !validMemoryValue(d.TransferSeconds) || !validMemoryValue(d.PreparationSeconds) || d.IntermediateBytes < 0 {
		return nil, nil, fmt.Errorf("invalid downstream synthetic assumptions")
	}
	metric := &SimPipelineMetric{Submitted: t.at, Pieces: map[string]*SimPieceMetric{}, PublicationSeconds: d.PublicationSeconds, TransferSeconds: d.TransferSeconds, PreparationSeconds: d.PreparationSeconds}
	consumers := []*simTask{}
	for i := range t.pieceWork {
		key := strconv.Itoa(i)
		stream := t.stream
		stream.Name += "-consume-" + key
		stream.Model = ""
		stream.Batch = 0
		stream.Curves = nil
		stream.Downstream = nil
		stream.Duration = d.Duration
		stream.Platforms = d.Platforms
		stream.MemoryGB = d.MemoryGB
		stream.HostGB = d.HostGB
		stream.Checkpoint = 0
		c := &simTask{downstreamOf: t, piece: key, stream: stream, at: t.at, work: d.Duration, stopAt: math.Inf(1)}
		spec := simSpec(c, epoch)
		spec.After = &PieceDependency{Job: t.id, Piece: key, Outputs: map[string]string{"input": "intermediate"}}
		spec.Outputs = map[string]string{"final": "results"}
		j, err := q.Submit(spec)
		if err != nil {
			return nil, nil, err
		}
		c.id = j.ID
		q.s.Jobs[j.ID].simGatherWait = barrier
		metric.Pieces[key] = &SimPieceMetric{}
		consumers = append(consumers, c)
	}
	return consumers, metric, nil
}
func simPipelineTransitions(q *Queue, tasks []*simTask, metrics map[string]*SimPipelineMetric, now float64, epoch time.Time, o SimOptions) bool {
	changed := false
	for _, t := range tasks {
		if len(t.pieceWork) == 0 || t.id == "" || t.stream.Downstream == nil || !o.Pipeline {
			continue
		}
		p := q.s.Jobs[t.id]
		metric := metrics[t.batch]
		if p == nil || metric == nil {
			continue
		}
		if o.CancelAfter > 0 && !t.cancelSent && now >= t.at+o.CancelAfter {
			_ = q.Cancel(t.id)
			t.cancelSent = true
			changed = true
		}
		if p.State == "succeeded" && metric.Gather == nil {
			at := now
			metric.Gather = &at
			changed = true
		}
		live, retained := int64(0), int64(0)
		finished := 0
		last := 0.0
		for _, c := range tasks {
			if c.downstreamOf != t {
				continue
			}
			piece := metric.Pieces[c.piece]
			j := q.s.Jobs[c.id]
			if _, ok := p.FanOut.Results[c.piece]; ok && piece.Accepted == nil {
				at := now
				piece.Accepted = &at
			}
			if j.simGatherWait && terminal(p.State) {
				j.simGatherWait = false
				j.WaitingSince = q.clock.Now()
				if j.Spec.Priority == "production" {
					deadline := q.clock.Now().Add(ProductionStartTimeout)
					j.Spec.StartBy = &deadline
					j.StartWindow = ProductionStartTimeout
				}
				changed = true
			}
			if dependencyReady(j) && !cancelled(j) && piece.Ready == nil {
				at := now
				piece.Ready = &at
				if piece.Accepted != nil {
					dt := now - *piece.Accepted
					piece.AcceptanceToReady = &dt
				}
			}
			if c.started && c.timeStarted+c.loadUntil-c.segmentAt <= now && piece.Started == nil {
				// start() records assignment; command begins after staged input preparation.
				at := c.timeStarted + c.loadUntil - c.segmentAt
				piece.Started = &at
				if metric.FirstStart == nil || at < *metric.FirstStart {
					metric.FirstStart = &at
				}
				if piece.Accepted != nil {
					dt := at - *piece.Accepted
					piece.AcceptanceToStart = &dt
				}
			}
			if c.finished && piece.Finished == nil {
				at := c.timeFinished
				piece.Finished = &at
				piece.Output = j.State == "succeeded" && j.OutputFiles["final"] != ""
				if piece.Output {
					metric.OutputCount++
				}
			}
			if piece.Accepted != nil {
				retained += t.stream.Downstream.IntermediateBytes
			}
			if piece.Accepted != nil && piece.Finished == nil {
				live += t.stream.Downstream.IntermediateBytes
			}
			if piece.Finished != nil {
				finished++
				last = max(last, *piece.Finished)
			}
		}
		metric.PeakIntermediateBytes = max(metric.PeakIntermediateBytes, retained)
		metric.PeakPendingIntermediateBytes = max(metric.PeakPendingIntermediateBytes, live)
		if finished == len(metric.Pieces) && metric.Finished == nil {
			metric.Finished = &last
			dt := last - t.at
			metric.Makespan = &dt
		}
	}
	return changed
}

type SimPipelineComparison struct {
	WorstProducerSlowdown float64   `json:"worst_producer_gather_slowdown_seconds"`
	Baseline              SimReport `json:"gather_then_stage"`
	PerPiece              SimReport `json:"per_piece"`
	GatePassed            bool      `json:"gate_passed"`
	Coverage              bool      `json:"complete_output_coverage"`
	Overlap               bool      `json:"earlier_downstream_start"`
	MeanGain              float64   `json:"mean_makespan_gain_seconds"`
	WorstRegression       float64   `json:"worst_pipeline_regression_seconds"`
	ProductionDelta       float64   `json:"production_p95_delta_seconds"`
}

func ComparePieceDependencies(f SimFleet, w SimWorkload, o SimOptions) (SimPipelineComparison, error) {
	r := SimPipelineComparison{}
	o.Pipeline, o.Declared, o.Split = true, true, false
	o.GatherBarrier = true
	var err error
	r.Baseline, err = Simulate(f, w, o)
	if err != nil {
		return r, err
	}
	o.GatherBarrier = false
	r.PerPiece, err = Simulate(f, w, o)
	if err != nil {
		return r, err
	}
	r.Coverage = len(r.Baseline.Pipelines) > 0 && len(r.Baseline.Pipelines) == len(r.PerPiece.Pipelines)
	paired := 0
	for key, a := range r.Baseline.Pipelines {
		b := r.PerPiece.Pipelines[key]
		if b == nil || a.Submitted != b.Submitted || a.Makespan == nil || b.Makespan == nil || a.OutputCount != len(a.Pieces) || b.OutputCount != len(b.Pieces) || len(a.Pieces) != len(b.Pieces) {
			r.Coverage = false
			continue
		}
		paired++
		if a.Gather != nil && b.Gather != nil {
			r.WorstProducerSlowdown = max(r.WorstProducerSlowdown, *b.Gather-*a.Gather)
		}
		gain := *a.Makespan - *b.Makespan
		r.MeanGain += gain
		r.WorstRegression = max(r.WorstRegression, -gain)
		if a.FirstStart != nil && b.FirstStart != nil && *b.FirstStart < *a.FirstStart {
			r.Overlap = true
		}
	}
	if paired > 0 {
		r.MeanGain /= float64(paired)
	}
	a, b := r.Baseline, r.PerPiece
	r.ProductionDelta = b.Production.P95 - a.Production.P95
	r.GatePassed = r.Coverage && r.Overlap && r.MeanGain > 0 && r.ProductionDelta == 0 && a.Production.Started == b.Production.Started && a.Production.Unstarted == b.Production.Unstarted && b.Failed == a.Failed && b.Cancelled == a.Cancelled && b.Unfinished == a.Unfinished
	return r, nil
}

// Fixed qualification scenarios. These are synthetic assumptions, never fleet
// measurements; both modes receive this exact same value and RNG seed.
func PipelineScenario(name string) (SimFleet, SimWorkload, SimOptions, error) {
	o := SimOptions{Days: 600.0 / 86400}
	f := SimFleet{Machines: []SimMachine{
		{Name: "producer-a", Host: "host-a", Platform: "xpu", MemoryGB: 8, HostGB: 32, Speed: 1},
		{Name: "producer-b", Host: "host-b", Platform: "xpu", MemoryGB: 8, HostGB: 32, Speed: 2},
		{Name: "consumer", Host: "host-c", Platform: "cuda", MemoryGB: 8, HostGB: 32, Speed: 1},
	}}
	zero := 0.0
	s := SimStream{Name: "pieces", Project: "pipeline", Priority: "experiment", Platforms: []string{"xpu"}, MemoryGB: 2, HostGB: 1, Duration: 20, Every: 24, Count: 1, Batch: 7,
		Curves:     map[string][]SimCurvePoint{"producer-a": {{1, 1}, {2, 1.7}}, "producer-b": {{1, 2}, {2, 3.4}}},
		Downstream: &SimDownstream{Duration: 8, Platforms: []string{"cuda"}, MemoryGB: 1, HostGB: 1, PublicationSeconds: 2, TransferSeconds: 1, PreparationSeconds: 1, IntermediateBytes: 1 << 20}}
	w := SimWorkload{Jitter: &zero, Streams: []SimStream{s}}
	switch name {
	case "homogeneous":
		w.Streams[0].Batch = 4
		f.Machines[1].Speed = 1
		w.Streams[0].Curves["producer-b"] = []SimCurvePoint{{1, 1}, {2, 1.7}}
	case "heterogeneous-tail", "separate-stage":
		if name == "heterogeneous-tail" {
			w.Streams[0].Downstream.Platforms = []string{"xpu", "cuda"}
		}
	case "shared-stage":
		f.Machines = f.Machines[:2]
		w.Streams[0].Downstream.Platforms = []string{"xpu"}
	case "mixed-production":
		f = SimFleet{}
		w = SimWorkload{}
		if err := ReadSimYAML("examples/sim/fleet.yaml", &f); err != nil {
			return f, w, o, err
		}
		if err := ReadSimYAML("examples/sim/declared-fan-out/workload.yaml", &w); err != nil {
			return f, w, o, err
		}
		for i := range w.Streams {
			if w.Streams[i].Batch > 1 {
				w.Streams[i].Downstream = &SimDownstream{Duration: 60, Platforms: []string{"xpu", "cuda"}, MemoryGB: 1, HostGB: 1, PublicationSeconds: 2, TransferSeconds: 1, PreparationSeconds: 1, IntermediateBytes: 1 << 20}
			}
		}
		o.Days = 7
	default:
		return f, w, o, fmt.Errorf("unknown pipeline scenario %q", name)
	}
	return f, w, o, nil
}
