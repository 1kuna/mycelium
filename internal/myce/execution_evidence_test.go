package myce

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestExecutionEvidenceDisabledHost(t *testing.T) {
	q, _ := fixture(t)
	m := machine("worker", "fake")
	check(t, q, m, nil)
	if err := setMaintenance(t, q, m.ID, "disable"); err != nil {
		t.Fatal(err)
	}
	j := submit(t, q, spec("production"))
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	client := Client{URL: server.URL, Token: "test"}
	assert := func(j *Job) {
		t.Helper()
		c := j.ExecutionCause
		if j.State != "failed" || j.DiagnosticCode != "host_unavailable" || c == nil || c.Operation != "launch" || c.Class != "host_unavailable" || c.Predicate != "no_eligible_host_enabled" || !reflect.DeepEqual(j.ExecutionEvidence, unstartedExecution(0)) {
			t.Fatalf("missing no-host evidence: %+v cause=%+v evidence=%+v", j, c, j.ExecutionEvidence)
		}
		if j.Reason != "production cannot start: no eligible host is enabled" {
			t.Fatal("changed reason", j.Reason)
		}
	}
	got, err := client.GetJob(context.Background(), j.ID)
	if err != nil {
		t.Fatal(err)
	}
	assert(got)
	var snapshot Snapshot
	if err := client.Call(context.Background(), "GET", "/api/status", nil, &snapshot); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, j := range snapshot.Jobs {
		if j.ID == got.ID {
			assert(j)
			found = true
		}
	}
	if !found {
		t.Fatal("job omitted from status")
	}
}

// Real helper and main commands leave distinct markers. Refuse before any
// helper, after helper execution, or after an authored environment build.
func TestExecutionEvidenceMemoryRefusal(t *testing.T) {
	for _, phase := range []string{"before_helpers", "after_helper", "after_environment"} {
		t.Run(phase, func(t *testing.T) {
			q, _ := fixture(t)
			markers := t.TempDir()
			helper, main, preparation := filepath.Join(markers, "helper"), filepath.Join(markers, "main"), filepath.Join(markers, "preparation")
			s := spec("experiment")
			s.Resumable = false
			s.Needs.HostMemoryGB = 3
			s.Run.Command = []string{"sh", "-c", `printf main > "$1"`, "_", main}
			s.Run.MemoryGB = 1
			s.Run.Sidecars = []NativeSidecar{{Name: "helper", Command: []string{"sh", "-c", `printf helper > "$1"; exec sleep 60`, "_", helper}, MemoryGB: 1}}
			m := machine("worker", "fake")
			m.HostMemoryMeasured, m.FreeHostMemoryGB, m.HostAvailableMemoryGB = true, 64, 64
			if phase == "after_environment" {
				recipe, err := snapshotEnvironment(EnvironmentRecipe{Build: []string{"sh", "-c", `printf preparation > "$1"`, "_", preparation}}, "fake")
				if err != nil {
					t.Fatal(err)
				}
				if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"build": {"fake": recipe}}}); err != nil {
					t.Fatal(err)
				}
				s.Environment.Name = "build"
				m.Environments = []string{recipe.Key}
			}
			j := submit(t, q, s)
			offer := check(t, q, m, nil).Job
			if offer == nil {
				t.Fatal("missing assignment", q.waitReason(q.s.Jobs[j.ID]))
			}
			server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
			defer server.Close()
			w := recoveryWorker(t)
			w.Machine, w.Client = m, Client{URL: server.URL, Token: "test"}
			w.HostMemorySource = func(context.Context) (float64, error) { return 64, nil }
			w.environmentCache = &EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20, Clock: RealClock{}}
			calls := 0
			proceed := make(chan struct{})
			defer func() {
				select {
				case <-proceed:
				default:
					close(proceed)
				}
			}()
			w.batchAdmission = func(context.Context, HostLaunch) error {
				calls++
				if phase == "after_environment" {
					if _, err := os.Stat(preparation); err == nil {
						return errWorkerMemory
					}
				} else if phase == "before_helpers" && calls == 2 {
					return errWorkerMemory
				} else if phase == "after_helper" && calls == 4 {
					<-proceed
					return errWorkerMemory
				}
				return nil
			}
			store, err := w.openStore()
			if err != nil {
				t.Fatal(err)
			}
			defer store.close()
			store.state.Session = m.Session
			p, startErr := startAfterFixtureInputs(t, w, store, *offer)
			var r Result
			if p != nil {
				defer func() { p.stop(w.Clock.Now(), 0, true) }()
				if phase == "after_helper" {
					await(t, func() bool { _, err := os.Stat(helper); return err == nil })
					close(proceed)
				}
				select {
				case f := <-p.done:
					if f.err != nil {
						t.Fatal(f.err)
					}
					r = f.result
				case <-time.After(testWaitTimeout):
					t.Fatal("refusal did not settle")
				}
			} else {
				if startErr == nil {
					t.Fatal("missing refusal")
				}
				r = startFailureOutcome(result(offer, false), startErr)
			}
			e := r.ExecutionEvidence
			if e == nil || e.Attempt != offer.Attempt || e.Main != executionNotStarted {
				t.Fatalf("main proof lost: %+v", e)
			}
			wantHelper, wantPreparation := executionNotStarted, executionNotStarted
			if phase == "after_helper" {
				wantHelper = executionMayHaveRun
			}
			if phase == "after_environment" {
				wantPreparation = executionMayHaveRun
			}
			if e.Sidecars != wantHelper || e.EnvironmentPreparation != wantPreparation {
				t.Fatalf("overclaimed evidence: %+v", e)
			}
			if r.ExecutionCause == nil || r.ExecutionCause.Predicate != "memory_capacity" || !r.Interrupted {
				t.Fatalf("refusal classification changed: %+v", r)
			}
			if phase != "after_environment" && (r.ExecutionCause.Operation != "launch" || r.ExecutionCause.Class != "resource_limit") {
				t.Fatalf("group cause changed: %+v", r.ExecutionCause)
			}
			if _, err := os.Stat(main); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("main executed: %v", err)
			}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			got := job(t, q, j.ID)
			if got.StartedAt != nil || !reflect.DeepEqual(got.ExecutionEvidence, e) || len(got.AttemptHistory) != 1 || !reflect.DeepEqual(got.AttemptHistory[0].ExecutionEvidence, e) {
				t.Fatalf("queue lost evidence: %+v", got)
			}
			// Unchanged capacity must wait; a changed measurement enables retry.
			if next := check(t, q, m, nil).Job; next != nil {
				t.Fatal("unchanged capacity re-offered", next)
			}
			m.HostAvailableMemoryGB += 1
			// Retrying must discard current evidence; history retains the exact old attempt.
			next := check(t, q, m, nil).Job
			if next == nil || next.ExecutionEvidence != nil || !reflect.DeepEqual(next.AttemptHistory[0].ExecutionEvidence, e) {
				t.Fatal("retry inherited current proof", next)
			}
			stale := r
			stale.ExecutionEvidence = unstartedExecution(next.Attempt)
			if err := q.Complete(stale); !errors.Is(err, ErrStale) {
				t.Fatal("stale execution accepted", err)
			}
		})
	}
}

func TestExecutionEvidenceMemberProjection(t *testing.T) {
	q, _ := fixture(t)
	parent := &Job{ID: "parent", State: "failed", Spec: Spec{FanOut: &FanOut{}}, FanOut: &FanOutProgress{Children: []string{"child", "legacy"}}, ExecutionEvidence: unstartedExecution(0)}
	child := &Job{ID: "child", State: "failed", Attempt: 1, ExecutionEvidence: &ExecutionEvidence{1, executionNotStarted, executionMayHaveRun, executionMayHaveRun}}
	legacy := &Job{ID: "legacy", State: "failed", Attempt: 1}
	q.s.Jobs[parent.ID], q.s.Jobs[child.ID], q.s.Jobs[legacy.ID] = parent, child, legacy
	projected := cloneJob(parent)
	q.projectCancellation(&projected, parent)
	if len(projected.CancellationScope.Members) != 2 {
		t.Fatal("members omitted")
	}
	for _, m := range projected.CancellationScope.Members {
		if m.JobID == child.ID && !reflect.DeepEqual(m.ExecutionEvidence, child.ExecutionEvidence) {
			t.Fatal("child inherited parent proof", m)
		}
		if m.JobID == legacy.ID && m.ExecutionEvidence != nil {
			t.Fatal("legacy inferred proof", m)
		}
	}
}

func TestExecutionEvidenceProtocolCompatibility(t *testing.T) {
	wire := &resourceWire{}
	old := false
	sent := []bool{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		_, has := body["execution_evidence"]
		sent = append(sent, has)
		if old && has {
			w.WriteHeader(400)
			w.Write([]byte(`{"code":"request_invalid","error":"unknown field"}`))
			return
		}
		if !old {
			w.Header().Set(executionEvidenceProtocolHeader, executionEvidenceProtocol)
		}
		w.Write([]byte(`{}`))
	}))
	defer server.Close()
	client := Client{URL: server.URL, resourceWire: wire}
	result := Result{Execution: Execution{Attempt: 1}, ExecutionEvidence: unstartedExecution(1)}
	for i := 0; i < 4; i++ {
		if i == 2 {
			old = true
		}
		if err := client.Call(context.Background(), "POST", "/api/workers/complete", &result, nil); err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(sent, []bool{false, true, true, false, false}) || result.ExecutionEvidence == nil {
		t.Fatal("wire/durable evidence changed", sent)
	}
	bad := safeExecutionEvidence(&ExecutionEvidence{1, "private-secret", executionNotStarted, ""}, 1)
	data, _ := json.Marshal(bad)
	if strings.Contains(string(data), "private-secret") || bad.Main != executionUnknown || bad.EnvironmentPreparation != executionUnknown {
		t.Fatal("untrusted state escaped", bad)
	}
	if safeExecutionEvidence(result.ExecutionEvidence, 2) != nil {
		t.Fatal("wrong attempt proof accepted")
	}
}
