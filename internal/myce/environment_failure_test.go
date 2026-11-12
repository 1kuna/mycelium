package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func failedEnvironmentDemand(t *testing.T, q *Queue, clock *fakeClock) (*Job, Machine) {
	t.Helper()
	lock := filepath.Join(t.TempDir(), "requirements.txt")
	if err := os.WriteFile(lock, []byte("example==1.0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	s := cpuSpec(1, 1)
	s.Priority = "deadline"
	due := clock.Now().Add(time.Hour)
	s.Deadline = &due
	s.Environment = Environment{LockFile: lock, Python: "3.12", Platforms: []string{"cpu"}}
	m := m3Machine()
	m.EnvironmentBuilder = true
	return submit(t, q, s), m
}

func TestEnvironmentFailureCPUOnAcceleratorDoesNotBlockOtherRecipes(t *testing.T) {
	q, clock := fixture(t)
	j, m := failedEnvironmentDemand(t, q, clock)
	first := machineCheck(t, q, m).EnvironmentPlan[0]
	// Reproduce #717: a retained terminal receipt and an unstarted replacement
	// offer differ in attempt. Never pretend the old receipt released a builder.
	delete(q.s.PreparationPlans, first.Launch.Attempt)
	replacement := machineCheck(t, q, m).EnvironmentPlan[0]
	if replacement.Launch.Attempt == first.Launch.Attempt {
		t.Fatal("no replacement offer")
	}
	const excerpt = "No solution found: example==1.0 unavailable on first index"
	m.EnvironmentReports = []EnvironmentReport{{Key: first.Key, Attempt: first.Launch.Attempt, State: "failed", Error: "build exit 1", Log: excerpt}}
	other := cpuSpec(1, 1)
	r := binaryRecipe(t, "cpu")
	if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"other": {"cpu": r}}}); err != nil {
		t.Fatal(err)
	}
	other.Environment.Name = "other"
	low := submit(t, q, other)
	clock.Advance(time.Second)
	reply := machineCheck(t, q, m)
	got, err := q.jobSnapshot(j.ID)
	if err != nil || got.State != "failed" || got.FinishedAt == nil || !strings.Contains(got.Reason, excerpt) || !strings.Contains(got.Reason, "fix the recipe and resubmit") {
		t.Fatalf("failure hidden: %+v %v", got, err)
	}
	if len(reply.EnvironmentPlan) != 1 || reply.EnvironmentPlan[0].Key != r.Key || reply.EnvironmentPlan[0].Launch.Demand != low.ID {
		t.Fatalf("failed recipe blocked new preparation: %+v", reply.EnvironmentPlan)
	}
	if _, exists := q.s.PreparationPlans[replacement.Launch.Attempt]; exists {
		t.Fatal("unstarted replacement was reoffered")
	}
	if err := q.AuthorizeLaunch(*replacement.Launch); !errors.Is(err, ErrStale) {
		t.Fatal("withdrawn offer authorized", err)
	}
	snap, err := q.publicSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(snap)
	if err != nil || !strings.Contains(string(encoded), excerpt) {
		t.Fatal("status omitted excerpt", err)
	}
	logs, err := q.Logs(j.ID, 0)
	if err != nil || len(logs) == 0 || !strings.Contains(strings.TrimSpace(logs[0].Text), excerpt) {
		t.Fatal("build log hidden", logs, err)
	}
	// A new submission key alone cannot make unchanged dependency inputs build.
	again := submit(t, q, j.Spec)
	clock.Advance(time.Minute)
	reply = machineCheck(t, q, m)
	if got := job(t, q, again.ID); got.State != "failed" || !strings.Contains(got.Reason, excerpt) {
		t.Fatal(got.State, got.Reason)
	}
	if len(reply.EnvironmentPlan) != 1 || reply.EnvironmentPlan[0].Key != r.Key {
		t.Fatal("unchanged recipe reoffered", reply.EnvironmentPlan)
	}
}

func TestEnvironmentTransientFailureBackoffAndOtherPreparation(t *testing.T) {
	q, clock := fixture(t)
	j, m := failedEnvironmentDemand(t, q, clock)
	first := machineCheck(t, q, m).EnvironmentPlan[0]
	if err := q.AuthorizeLaunch(*first.Launch); err != nil {
		t.Fatal(err)
	}
	m.EnvironmentReports = []EnvironmentReport{{Key: first.Key, Attempt: first.Launch.Attempt, State: "failed", Error: "connection reset by peer", Log: "download interrupted"}}
	r := binaryRecipe(t, "cpu")
	if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"other": {"cpu": r}}}); err != nil {
		t.Fatal(err)
	}
	s := cpuSpec(1, 1)
	s.Environment.Name = "other"
	submit(t, q, s)
	reply := machineCheck(t, q, m)
	if got, err := q.jobSnapshot(j.ID); err != nil || got.State != "queued" || !strings.Contains(got.Reason, "environment retry pending") || !strings.Contains(got.Reason, "download interrupted") {
		t.Fatal("transient failure hidden", got, err)
	}
	if len(reply.EnvironmentPlan) != 1 || reply.EnvironmentPlan[0].Key != r.Key {
		t.Fatal("backoff blocked unrelated environment", reply.EnvironmentPlan)
	}
	clock.Advance(29 * time.Second)
	if reply = machineCheck(t, q, m); len(reply.EnvironmentPlan) != 1 || reply.EnvironmentPlan[0].Key != r.Key {
		t.Fatal("retried before backoff", reply.EnvironmentPlan)
	}
	clock.Advance(time.Second)
	reply = machineCheck(t, q, m)
	if len(reply.EnvironmentPlan) != 1 || reply.EnvironmentPlan[0].Key != first.Key || reply.EnvironmentPlan[0].Launch.Attempt == first.Launch.Attempt {
		t.Fatal("no fresh retry after backoff", reply.EnvironmentPlan)
	}
	// Retry the real worker runtime, including persisted negative-cache restore.
	dir := t.TempDir()
	if err := saveWorkerJSON(dir, "environment-reports.json", map[string]EnvironmentReport{first.Key: m.EnvironmentReports[0]}); err != nil {
		t.Fatal(err)
	}
	rt := &environmentRuntime{dir: dir, cache: &EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20, Clock: clock}, reports: map[string]EnvironmentReport{}}
	if err := rt.restore(); err != nil {
		t.Fatal(err)
	}
	called := false
	rt.prepare = func(context.Context, EnvironmentRecipe) error {
		called = true
		return errors.New("temporary failure in name resolution")
	}
	retry := reply.EnvironmentPlan[0]
	retry.Preparation = &ExtensionSelection{}
	rt.sync([]EnvironmentRecipe{retry}, false)
	<-rt.done
	if !called || rt.reports[first.Key].FailureKind != "transient" {
		t.Fatal("runtime retained failed key", rt.reports)
	}
	m.EnvironmentReports = []EnvironmentReport{rt.reports[first.Key]}
	if reply = machineCheck(t, q, m); len(reply.EnvironmentPlan) != 1 || reply.EnvironmentPlan[0].Key != r.Key {
		t.Fatal(reply.EnvironmentPlan)
	}
	clock.Advance(59 * time.Second)
	if reply = machineCheck(t, q, m); len(reply.EnvironmentPlan) != 1 || reply.EnvironmentPlan[0].Key != r.Key {
		t.Fatal("second retry not backed off", reply.EnvironmentPlan)
	}
	clock.Advance(time.Second)
	if reply = machineCheck(t, q, m); len(reply.EnvironmentPlan) != 1 || reply.EnvironmentPlan[0].Key != first.Key {
		t.Fatal("second retry lost", reply.EnvironmentPlan)
	}
}

func TestEnvironmentFailedUploadPreservesFullLogAndOtherPreparation(t *testing.T) {
	q, clock := fixture(t)
	j, m := failedEnvironmentDemand(t, q, clock)
	first := machineCheck(t, q, m).EnvironmentPlan[0]
	if err := q.AuthorizeLaunch(*first.Launch); err != nil {
		t.Fatal(err)
	}
	r := binaryRecipe(t, "cpu")
	if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"other": {"cpu": r}}}); err != nil {
		t.Fatal(err)
	}
	s := cpuSpec(1, 1)
	s.Environment.Name = "other"
	submit(t, q, s)
	const excerpt = "No solution found: fix the package index"
	m.EnvironmentReports = []EnvironmentReport{{Key: first.Key, Attempt: first.Launch.Attempt, State: "uploading", FailureKind: "deterministic", Log: excerpt}}
	reply := machineCheck(t, q, m)
	if got, err := q.jobSnapshot(j.ID); err != nil || got.State != "queued" || !strings.Contains(got.Reason, excerpt) || !strings.Contains(got.Reason, "environment failed:") {
		t.Fatal("upload hid failure", got, err)
	}
	if len(reply.EnvironmentPlan) != 1 || reply.EnvironmentPlan[0].Key != r.Key {
		t.Fatal("upload blocked another recipe", reply.EnvironmentPlan)
	}
	if _, held := q.s.PreparationPlans[first.Launch.Attempt]; held {
		t.Fatal("completed build retained custody during log upload")
	}
	const full = "compiler output preceding the final excerpt\nNo solution found: fix the package index\n"
	chunk := EnvironmentLog{Machine: m.ID, Session: m.Session, Key: first.Key, Offset: 1, Text: full}
	if err := q.AppendEnvironmentLog(chunk); err != nil {
		t.Fatal(err)
	}
	m.EnvironmentReports[0].State = "failed"
	clock.Advance(time.Second)
	machineCheck(t, q, m)
	if got := job(t, q, j.ID); got.State != "failed" {
		t.Fatal("upload completion did not fail job", got.State)
	}
	logs, err := q.Logs(j.ID, 0)
	if err != nil || len(logs) == 0 || !strings.HasPrefix(logs[0].Text, full) {
		t.Fatal("trailing log lost", logs, err)
	}
}

func TestEnvironmentFailureClassificationAndPersistedExcerpt(t *testing.T) {
	_, clock := fixture(t)
	for _, tc := range []struct{ output, kind string }{
		{"No solution found: example==1.0 unavailable on first index", "deterministic"},
		{"failed to download: connection reset by peer", "transient"},
		{"No space left on device", "transient"},
	} {
		t.Run(tc.kind+tc.output[:5], func(t *testing.T) {
			r := binaryRecipe(t, "cpu")
			r.Build = []string{"sh", "-c", "exit 1"}
			cache := &EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20, Clock: clock}
			cache.Run = func(ctx context.Context, argv []string, dir string, env []string, stdout, stderr io.Writer) error {
				fmt.Fprintln(stdout, tc.output)
				return exec.CommandContext(ctx, "sh", "-c", "exit 1").Run()
			}
			if err := cache.init(); err != nil {
				t.Fatal(err)
			}
			// Old infrastructure output must not reclassify this attempt.
			if err := os.WriteFile(filepath.Join(cache.Root, r.Key+".build.log"), []byte("old connection reset by peer\n"), 0600); err != nil {
				t.Fatal(err)
			}
			r.Launch = &HostLaunch{Attempt: "first"}
			rt := &environmentRuntime{dir: t.TempDir(), cache: cache, reports: map[string]EnvironmentReport{}}
			rt.sync([]EnvironmentRecipe{r}, false)
			<-rt.done
			_, reports, _, _ := rt.report()
			if len(reports) != 1 || reports[0].FailureKind != tc.kind || !strings.Contains(reports[0].Log, tc.output) {
				t.Fatal(reports)
			}
			restored := &environmentRuntime{dir: rt.dir, cache: cache, reports: map[string]EnvironmentReport{}}
			if err := restored.restore(); err != nil {
				t.Fatal(err)
			}
			_, reports, _, _ = restored.report()
			if len(reports) != 1 || reports[0].FailureKind != tc.kind || !strings.Contains(reports[0].Log, tc.output) {
				t.Fatal("lost persisted excerpt", reports)
			}
			if tc.kind == "deterministic" {
				r.Launch = &HostLaunch{Attempt: "replacement"}
				restored.sync([]EnvironmentRecipe{r}, false)
				if restored.done != nil {
					t.Fatal("unchanged deterministic inputs rebuilt")
				}
			}
		})
	}
}

// A recipe hash is not evidence of an HTTP status. #718 independent review
// reproduced an unchanged deterministic recipe retrying because its hash had 504.
func TestEnvironmentFailureHashIsNotHTTPStatus(t *testing.T) {
	const key = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789504503ab"
	err := exec.Command("sh", "-c", "exit 1").Run()
	wrapped := fmt.Errorf("environment %s build failed: %w (log %s.build.log)", key, err, key)
	const output = "No solution found: example==1.0 unavailable on first index"
	if got := classifyEnvironmentFailure(wrapped, output); got != "deterministic" {
		t.Fatal("hash converted a recipe failure to a retry", got)
	}
	if got := environmentFailureKind(EnvironmentReport{Error: wrapped.Error(), Log: output}); got != "deterministic" {
		t.Fatal("legacy hash converted a recipe failure to a retry", got)
	}
	for _, diagnostic := range []string{"HTTP 503", "HTTP/1.1 504 Gateway Timeout", "status code: 502", "HTTP error 504"} {
		if got := classifyEnvironmentFailure(wrapped, diagnostic); got != "transient" {
			t.Fatal("HTTP failure did not retry", diagnostic, got)
		}
	}
}
