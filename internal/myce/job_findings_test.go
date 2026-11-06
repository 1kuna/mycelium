package myce

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func findingCode(t *testing.T, j *Job, code string) JobFinding {
	t.Helper()
	for _, f := range j.Findings {
		if f.Code == code {
			return f
		}
	}
	t.Fatalf("missing %s: %+v", code, j.Findings)
	return JobFinding{}
}

func TestJobFindingsProcessOverageReplay(t *testing.T) {
	q, clock := fixture(t)
	s := cpuSpec(2, 2)
	j := submit(t, q, s)
	m := cpuMachine("private-worker-identity", "private-worker-identity", "cpu", 8, 16)
	run := check(t, q, m, nil).Job
	if run == nil {
		t.Fatal("no assignment")
	}
	if err := q.Started(result(run, false)); err != nil {
		t.Fatal(err)
	}
	// Replay measurements, never create the incident's actual processes or RAM.
	initial, peak := uint64(2), uint64(2000)
	cpu := 120.0
	clock.Advance(10 * time.Second)
	r := result(run, true)
	r.Resources = &AttemptResources{ProcessInitial: &initial, ProcessPeak: &peak, ElapsedSeconds: 10, CPU: AttemptCPUTime{UsageSeconds: &cpu, Source: "sampled_process_tree", LowerBound: true}}
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: findingValue(64), Source: processGroupMemorySource, Constrained: true, Pressure: &MemoryPressureEvidence{Reason: "host_pressure", HostPressureLevel: 4}}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(API{q, "fixture"}.Handler())
	defer server.Close()
	client := Client{URL: server.URL, Token: "fixture"}
	got, err := client.GetJob(context.Background(), j.ID)
	if err != nil {
		t.Fatal(err)
	}
	f := findingCode(t, got, "memory_over_declaration")
	if f.Cause != "unknown" || *f.Evidence[0].Value != 64 || !strings.Contains(f.Evidence[0].Limitation, "not physical RAM") {
		t.Fatal(f)
	}
	if f = findingCode(t, got, "process_fanout"); *f.Evidence[0].Value != 2000 {
		t.Fatal(f)
	}
	findingCode(t, got, "cpu_over_declaration")
	if len(got.AttemptHistory) != 1 || len(got.AttemptHistory[0].Findings) != 3 {
		t.Fatal(got.AttemptHistory)
	}
	if len(q.s.WorkObservations) != 0 {
		t.Fatal("constrained incident taught a baseline")
	}
	// New findings are job-local even when another project has an incident.
	healthy := submit(t, q, Spec{Name: "healthy", Project: "unrelated-project-canary", Run: RunSpec{Command: []string{"true"}}, Needs: ResourceNeeds{CPUCores: 1, MemoryGB: 1}})
	second := check(t, q, m, nil).Job
	if second == nil || second.ID != healthy.ID {
		t.Fatal(second)
	}
	finish(t, q, second, true)
	clean, err := client.GetJob(context.Background(), healthy.ID)
	if err != nil || len(clean.Findings) != 0 {
		t.Fatalf("unrelated findings: %+v %v", clean, err)
	}
	data, _ := json.Marshal(got.Findings)
	for _, private := range []string{"private-worker-identity", "unrelated-project-canary", "host_pressure"} {
		if strings.Contains(string(data), private) {
			t.Fatalf("private context %s: %s", private, data)
		}
	}
	// The public read still requires installed authority's existing auth boundary.
	rec := httptest.NewRecorder()
	API{q, "fixture"}.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/api/jobs/"+j.ID, nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatal(rec.Code)
	}
}

func TestJobFindingsInfrastructureAndGuardReplay(t *testing.T) {
	q, clock := fixture(t)
	submit(t, q, cpuSpec(1, 2))
	m := cpuMachine("private-worker-identity", "private-worker-identity", "cpu", 8, 16)
	first := check(t, q, m, nil).Job
	if first == nil {
		t.Fatal("no assignment")
	}
	if err := q.Started(result(first, false)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(10 * time.Second)
	r := result(first, false)
	r.Interrupted = true
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	got, err := q.jobSnapshot(first.ID)
	if err != nil {
		t.Fatal(err)
	}
	f := findingCode(t, got, "infrastructure_interruption")
	if f.Classification != "infrastructure" || !strings.Contains(f.Message, "not your job's fault") || !strings.Contains(f.Action, "requeued") || f.Cause != "unknown" {
		t.Fatal(f)
	}
	// A measured guard cause is independent of the unknown reason for overuse.
	j := &Job{AttemptHistory: []AttemptOutcome{{Attempt: 2, ExecutionCause: &RuntimeCause{Operation: "command", Class: rssLimitExceeded, RSSLimit: &RSSLimitEvidence{ObservedBytes: 64 << 30, LimitBytes: 2 << 30, Quality: "owned_session_rss"}}}}}
	q.projectJobFindings(j)
	if f = findingCode(t, j, "guard_stop"); f.Evidence[0].Declared == nil || f.Cause == "unknown" || !strings.Contains(f.NextStep, "unknown") {
		t.Fatal(f)
	}
	for _, class := range []string{wallTimeLimit, diskLimitExceeded} {
		j = &Job{AttemptHistory: []AttemptOutcome{{Attempt: 1, ExecutionCause: &RuntimeCause{Operation: "execution", Class: class}}}}
		// Use each guard's actual allowed runtime operation.
		if class == diskLimitExceeded {
			j.AttemptHistory[0].ExecutionCause.Operation = "outputs"
		}
		q.projectJobFindings(j)
		findingCode(t, j, "guard_stop")
	}
}

func TestJobFindingsWaitAndUnknownBuildCause(t *testing.T) {
	q, clock := fixture(t)
	j := submit(t, q, cpuSpec(1, 2))
	clock.Advance(21 * time.Hour)
	got, err := q.jobSnapshot(j.ID)
	if err != nil {
		t.Fatal(err)
	}
	f := findingCode(t, got, "waiting")
	if f.Classification != "waiting" || *f.Evidence[0].Value != 21*3600 || f.NextStep == "" || f.Cause != "unknown" {
		t.Fatal(f)
	}
	// Build output is evidence for the job log, never guessed cause or callback text.
	broken := &Job{State: "failed", Reason: "environment failed: private-host /private/path Authorization: hidden", Spec: Spec{Logs: "none"}}
	q.projectJobFindings(broken)
	f = findingCode(t, broken, "environment_build_failed")
	b, _ := json.Marshal(f)
	if strings.Contains(string(b), "private") || strings.Contains(string(b), "hidden") || f.Cause != "unknown" || !strings.Contains(f.NextStep, "logs") {
		t.Fatal(string(b))
	}
}

func TestJobFindingsLearnedBehaviorAndHealthy(t *testing.T) {
	q, clock := fixture(t)
	initial, peak := uint64(2), uint64(20)
	resources := &AttemptResources{ProcessInitial: &initial, ProcessPeak: &peak, ElapsedSeconds: 10, CPU: AttemptCPUTime{UsageSeconds: findingValue(30)}}
	baseline := &FindingBehavior{CPU: &FindingRange{Min: 2, Max: 3, Samples: 3}, Processes: &FindingRange{Min: 10, Max: 20, Samples: 3}, RecordedAt: clock.Now()}
	j := &Job{State: "succeeded", ResourceAttempts: []AttemptResourceFacts{{Attempt: 1, CPUCores: 1, Baseline: baseline, Resources: resources}}}
	q.projectJobFindings(j)
	if len(j.Findings) != 0 {
		t.Fatal(j.Findings)
	}
	resources.CPU.UsageSeconds = findingValue(60)
	peak = 2000
	q.projectJobFindings(j)
	if f := findingCode(t, j, "cpu_over_declaration"); f.Evidence[0].Samples != 3 || *f.Evidence[0].Learned != 4 {
		t.Fatal(f)
	}
	findingCode(t, j, "process_fanout")
	// Learned comparisons use the existing accepted-work identity and expiry.
	key := ObservationKey{Work: "frozen-work"}
	job := &Job{Timing: &WorkTiming{Key: &key}}
	q.s.WorkObservations = map[string]WorkObservation{identityHash(&key): {FindingBehavior: baseline}}
	if q.findingBehaviorBaseline(job) != baseline {
		t.Fatal("missing frozen history")
	}
	clock.Advance(workCurveMaxAge + time.Second)
	if q.findingBehaviorBaseline(job) != nil {
		t.Fatal("stale history reused")
	}
	point := WorkObservation{}
	q.recordFindingBehavior(job, Result{Success: true, Resources: resources}, WorkObservation{}, &point, true)
	if point.FindingBehavior.CPU == nil || point.FindingBehavior.CPU.Samples != 1 {
		t.Fatal(point)
	}
	// Partial samples are useful diagnostic lower bounds but never normal history.
	resources.CPU.LowerBound = true
	point = WorkObservation{}
	q.recordFindingBehavior(job, Result{Success: true, Resources: resources}, WorkObservation{}, &point, true)
	if point.FindingBehavior.CPU != nil {
		t.Fatal("lower bound taught upper range")
	}
}

func TestJobFindingsProcessCollector(t *testing.T) {
	a := resourceCgroupFixture(t)
	if err := os.WriteFile(filepath.Join(a.path, "pids.current"), []byte("2\n"), 0600); err != nil {
		t.Fatal(err)
	}
	q, clock := fixture(t)
	_ = q
	o := &resourceObserver{clock: clock, start: clock.Now(), read: func() *AttemptResources { return cgroupResources(a, clock.Now()) }}
	o.sample()
	clock.Advance(time.Second)
	if err := os.WriteFile(filepath.Join(a.path, "pids.current"), []byte("2000\n"), 0600); err != nil {
		t.Fatal(err)
	}
	o.sample()
	if err := os.Remove(filepath.Join(a.path, "pids.current")); err != nil {
		t.Fatal(err)
	}
	o.sample()
	r := o.snapshot()
	if r.ProcessPeak == nil || *r.ProcessPeak != 2000 || *r.ProcessInitial != 2 {
		t.Fatal(r)
	}
}

func TestJobFindingsTypedEnvironmentCause(t *testing.T) {
	q, _ := fixture(t)
	report := EnvironmentReport{BuildSeconds: 3, PathDiagnostic: &PathDiagnostic{Field: "environment.binary", Side: "worker", Path: "binary", ResolutionBase: "worker-environment-library", Cause: "missing"}}
	j := &Job{State: "failed", EnvironmentBuildFailure: environmentBuildFailure(report)}
	q.projectJobFindings(j)
	f := findingCode(t, j, "environment_build_failed")
	if f.Confidence != "measured" || !strings.Contains(f.Cause, "no file") || *f.Evidence[1].Value != 3 {
		t.Fatal(f)
	}
	report.PathDiagnostic.Path = "/private/untrusted"
	j.EnvironmentBuildFailure = environmentBuildFailure(report)
	q.projectJobFindings(j)
	if f = findingCode(t, j, "environment_build_failed"); f.Cause != "unknown" {
		t.Fatal(f)
	}
}
