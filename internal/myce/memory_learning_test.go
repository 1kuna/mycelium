package myce

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Production construction belongs to the gate file. Tests below deliberately
// forge tokens to check the runtime guard; this is not a Go package boundary.
func TestMemoryLearningTokenConstruction(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	for _, name := range files {
		if name == "memory_learning.go" || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			if id, ok := n.(*ast.Ident); ok && id.Name == "gateBuilt" {
				t.Errorf("%s: token validity belongs to memory_learning.go", fset.Position(n.Pos()))
			}
			var typ ast.Expr
			switch n := n.(type) {
			case *ast.CompositeLit:
				typ = n.Type
			case *ast.CallExpr:
				if id, ok := n.Fun.(*ast.Ident); ok && id.Name == "new" && len(n.Args) == 1 {
					typ = n.Args[0]
				}
			}
			if typ == nil {
				return true
			}
			ast.Inspect(typ, func(n ast.Node) bool {
				if id, ok := n.(*ast.Ident); ok && id.Name == "eligibleMemorySample" {
					t.Errorf("%s: construct memory tokens only in memory_learning.go", fset.Position(n.Pos()))
				}
				return true
			})
			return true
		})
	}
}

func TestMemoryLearningWritersRejectUngatedToken(t *testing.T) {
	q, clock := fixture(t)
	e := fakeExtension(t)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	m := placementMachine("synthetic", "separate", 128, 128, e)
	check(t, q, m, nil)
	spec := placementSpec(true)
	start := func() *Job {
		t.Helper()
		queued := submit(t, q, spec)
		run := check(t, q, m, nil).Job
		if run == nil || run.ID != queued.ID {
			t.Fatal("not assigned")
		}
		if err := q.Started(result(run, false)); err != nil {
			t.Fatal(err)
		}
		return run
	}
	seed := start()
	host := 4.0
	r := result(seed, true)
	r.Usage = &SizeHint{MemoryGB: 2, HostMemoryGB: host}
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	key := programNeedsKey(seed, e, "fake")
	before := q.s.ProgramNeeds[key]
	if before.MemoryGB != 2.5 || before.HostMemoryGB != 5 {
		t.Fatal("accepted control did not learn", before)
	}
	run := start()
	r = result(run, true)
	r.Usage = &SizeHint{MemoryGB: 40, HostMemoryGB: 20}
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host, Constrained: true}
	workBefore := WorkObservation{Key: ObservationKey{Work: "synthetic-probe"}, HostMemoryGB: 4, DeviceMemoryGB: 2, MemoryPeak: &AttemptMemoryPeak{HostGB: &host}, MemoryRecordedAt: clock.Now()}
	workID := identityHash(workBefore.Key)
	if err := q.update(func() error {
		j := q.touchJob(run.ID)
		q.s.WorkObservations = map[string]WorkObservation{workID: workBefore}
		if sample, reason := q.memoryLearningSample(j, r); sample != nil || reason != "constrained" {
			t.Fatal("probe was not rejected by gate", reason)
		}
		// The reviewer's field-built token and the zero value must both fail.
		for _, sample := range []eligibleMemorySample{{}, {job: j, result: r, work: true, programKey: key}} {
			q.recordProgramNeeds(sample)
			point := workBefore
			q.recordWorkMemory(sample, workBefore, &point)
			if !reflect.DeepEqual(point, workBefore) {
				t.Error("ungated work writer changed accepted memory", point)
			}
			q.s.WorkObservations[workID] = point
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	q = memoryFactsRestart(t, q, clock)
	if q.s.ProgramNeeds[key] != before || job(t, q, run.ID).MeasuredNeeds != nil {
		t.Fatal("ungated program writer persisted rejected Usage", q.s.ProgramNeeds[key])
	}
	workAfter := q.s.WorkObservations[workID]
	// Restart separately marks absent inventory stale; memory must stay exact.
	workAfter.StaleReason = workBefore.StaleReason
	if !reflect.DeepEqual(workAfter, workBefore) {
		t.Fatal("ungated work writer persisted rejected memory", workAfter)
	}
}

// #486 review reproduction: rejected Usage used to turn accepted 2.5/5 GiB
// program reservations into 50/25 GiB, surviving restart and the next submit.
func TestMemoryLearningRejectedProgramUsage(t *testing.T) {
	for _, reason := range []string{"constrained", "pressure_unavailable"} {
		t.Run(reason, func(t *testing.T) {
			for _, seed := range []bool{false, true} {
				name := "cannot seed"
				if seed {
					name = "cannot overwrite"
				}
				t.Run(name, func(t *testing.T) {
					q, clock := fixture(t)
					e := fakeExtension(t)
					if err := q.AddExtension(e); err != nil {
						t.Fatal(err)
					}
					m := placementMachine("synthetic", "separate", 128, 128, e)
					check(t, q, m, nil)
					spec := placementSpec(true)
					complete := func(usage SizeHint, reject bool) (*Job, *AttemptMemoryPeak) {
						t.Helper()
						queued := submit(t, q, spec)
						run := check(t, q, m, nil).Job
						if run == nil || run.ID != queued.ID {
							t.Fatal("not assigned")
						}
						if err := q.Started(result(run, false)); err != nil {
							t.Fatal(err)
						}
						clock.Advance(time.Second)
						r := result(run, true)
						host := usage.HostMemoryGB
						r.Usage = &usage
						r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host, Constrained: reject && reason == "constrained", PressureUnavailable: reject && reason == "pressure_unavailable"}
						body, err := json.Marshal(r)
						if err != nil {
							t.Fatal(err)
						}
						var wire Result
						if err := json.Unmarshal(body, &wire); err != nil {
							t.Fatal(err)
						}
						if err := q.Complete(wire); err != nil {
							t.Fatal(err)
						}
						return run, wire.MemoryPeak
					}
					if seed {
						accepted, _ := complete(SizeHint{MemoryGB: 2, HostMemoryGB: 4}, false)
						key := programNeedsKey(accepted, e, "fake")
						if got := q.s.ProgramNeeds[key]; got.MemoryGB != 2.5 || got.HostMemoryGB != 5 {
							t.Fatal("eligible program sample did not establish control", got)
						}
						if facts := job(t, q, accepted.ID).MemoryAttempts; len(facts) != 1 || facts[0].LearningReason != "eligible" {
							t.Fatal("program learning gate disagrees with exported reason", facts)
						}
					}
					before := make(map[string]SizeHint, len(q.s.ProgramNeeds))
					for key, value := range q.s.ProgramNeeds {
						before[key] = value
					}
					rejected, peak := complete(SizeHint{MemoryGB: 40, HostMemoryGB: 20}, true)
					key := programNeedsKey(rejected, e, "fake")
					if len(q.s.ProgramNeeds) != len(before) || q.s.ProgramNeeds[key] != before[key] {
						t.Errorf("rejected sample changed program needs: before=%+v after=%+v", before, q.s.ProgramNeeds)
					}
					if got := job(t, q, rejected.ID); got.MeasuredNeeds != nil || got.State != "succeeded" {
						t.Fatal("rejected sample published measured needs or changed outcome", got.MeasuredNeeds, got.State)
					}
					q = memoryFactsRestart(t, q, clock)
					memoryFactsGET(t, q, *rejected, peak, false, reason)
					next := submit(t, q, spec)
					wantDevice, wantHost := rejected.Spec.Needs.MemoryGB, rejected.Spec.Needs.HostMemoryGB
					if seed {
						wantDevice, wantHost = 2.5, 5
					}
					if next.Spec.Needs.MemoryGB != wantDevice || next.Spec.Needs.HostMemoryGB != wantHost {
						t.Errorf("rejected sample changed future reservations: want=%v/%v got=%+v", wantDevice, wantHost, next.Spec.Needs)
					}
					if len(q.s.ProgramNeeds) != len(before) || q.s.ProgramNeeds[key] != before[key] {
						t.Fatal("restart changed accepted program needs", q.s.ProgramNeeds)
					}
				})
			}
		})
	}
}
