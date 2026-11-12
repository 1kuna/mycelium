package myce

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/url"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Synthetic retained queue: eight current jobs, sixteen workers and independent
// per-execution storage, work, alert and efficiency evidence. No fleet data.
func retainedStatusFixture(tb testing.TB, retained int) *Queue {
	tb.Helper()
	c := &fakeClock{now: time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)}
	q, err := Open(filepath.Join(tb.TempDir(), "queue.db"), tb.TempDir(), c)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { q.Close() })
	q.s.WorkObservations = map[string]WorkObservation{}
	q.s.StorageObservations = map[string]StorageObservation{}
	q.efficiencyView = &EfficiencyReport{At: c.Now(), Coverage: "measured"}
	for i := 0; i < 16; i++ {
		id := fmt.Sprintf("worker-%02d", i)
		q.s.Machines[id] = &Machine{ID: id, HostID: id, GPU: "fake", LastSeen: c.Now(), TotalMemoryGB: 24, FreeMemoryGB: 16, HostAvailableMemoryGB: 32, FreeHostMemoryGB: 24}
	}
	for i := 0; i < retained; i++ {
		id := fmt.Sprintf("history-%05d", i)
		q.s.Jobs[id] = &Job{ID: id, State: "succeeded", ReleaseConfirmed: true, Queued: c.Now().Add(-time.Duration(i+1) * time.Second), Spec: Spec{Project: "example", Name: id}, Attempt: 1}
		q.s.WorkObservations[id] = WorkObservation{Job: id, Machine: "worker-00", Attempt: 1, Samples: 8, LastSeconds: 20, RecordedAt: c.Now(), Key: ObservationKey{Work: id, Hardware: strings.Repeat("h", 64), Fingerprint: strings.Repeat("f", 64), Environment: strings.Repeat("e", 64), Configuration: strings.Repeat("c", 64)}}
		q.s.StorageObservations[id] = StorageObservation{RecordedAt: c.Now(), Execution: Execution{ID: id, Attempt: 1}, LowerBoundReason: strings.Repeat("retained evidence ", 32)}
		q.s.Health.Events = append(q.s.Health.Events, HealthAlert{DeliveryID: id, Kind: "production_attempt_lost", JobID: id, At: c.Now(), Delivered: true})
		q.efficiencyView.Jobs = append(q.efficiencyView.Jobs, EfficiencyWait{JobID: id, Project: "example", Seconds: map[string]float64{"memory": 30}})
	}
	for i := 0; i < 8; i++ {
		id := fmt.Sprintf("current-%02d", i)
		q.s.Jobs[id] = &Job{ID: id, State: "running", Machine: fmt.Sprintf("worker-%02d", i), Queued: c.Now(), StartedAt: statusTestTime(c.Now()), Spec: Spec{Project: "example", Name: id}, Attempt: 1}
	}
	return q
}

func BenchmarkStatusRetained(b *testing.B) {
	q := retainedStatusFixture(b, 8000)
	for _, mode := range []string{"legacy_refresh", "refresh", "cache_hit"} {
		b.Run(mode, func(b *testing.B) {
			var samples []time.Duration
			var size int
			_, err := q.cachedStatusJSON()
			if err != nil {
				b.Fatal(err)
			}
			build := func() (string, error) {
				if mode != "legacy_refresh" {
					return q.cachedStatusJSON()
				}
				s, err := q.publicStatusSnapshot(&statusOptions{jobs: "recent", sections: []string{"jobs", "machines", "storage", "work", "health", "efficiency", "usage", "diagnostics"}})
				if err != nil {
					return "", err
				}
				s.Freshness = nil
				data, err := json.Marshal(s)
				if err != nil {
					return "", err
				}
				return q.statusJSONFreshness(string(data), s.Now, false)
			}
			cpu := func() int64 {
				var r syscall.Rusage
				syscall.Getrusage(syscall.RUSAGE_SELF, &r)
				return int64(r.Utime.Sec+r.Stime.Sec)*1e9 + int64(r.Utime.Usec+r.Stime.Usec)*1000
			}
			b.ReportAllocs()
			startCPU := cpu()
			b.ResetTimer()
			for range b.N {
				if mode == "refresh" {
					q.statusCache.data = ""
				}
				start := time.Now()
				data, err := build()
				if err != nil {
					b.Fatal(err)
				}
				size = len(data)
				samples = append(samples, time.Since(start))
			}
			b.StopTimer()
			b.ReportMetric(float64(cpu()-startCPU)/float64(b.N), "cpu-ns/op")
			sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
			b.ReportMetric(float64(size), "reply-bytes/op")
			b.ReportMetric(float64(samples[len(samples)/2].Nanoseconds()), "p50-ns/op")
			b.ReportMetric(float64(samples[(len(samples)-1)*99/100].Nanoseconds()), "p99-ns/op")
		})
	}
}

func statusTestTime(t time.Time) *time.Time { return &t }

func TestStatusCompactRetainedEvidence(t *testing.T) {
	small := retainedStatusFixture(t, 8)
	large := retainedStatusFixture(t, 8000)
	// Retained driver snapshots must stay out of the default fleet response.
	large.s.Machines["worker-00"].Devices = []MachineDevice{{ID: "0", FaultEvidence: []*FaultEvidence{{Event: "retained-driver-event", Host: map[string]string{"meminfo": strings.Repeat("0", 8192)}}}}}
	small.s.Machines["worker-00"].Devices = []MachineDevice{{ID: "0"}}
	smallJSON, err := small.cachedStatusJSON()
	if err != nil {
		t.Fatal(err)
	}
	largeJSON, err := large.cachedStatusJSON()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(largeJSON, "retained-driver-event") || len(large.s.Machines["worker-00"].Devices[0].FaultEvidence) != 1 {
		t.Fatal("compact projection leaked or mutated fault evidence")
	}
	// Only the retained count's decimal width changes. No retained collection
	// should enter the default capture, projection, or wire response.
	if len(largeJSON) > len(smallJSON)+16 {
		t.Fatalf("retention amplified default: %d -> %d", len(smallJSON), len(largeJSON))
	}
	refreshAllocs := func(q *Queue) float64 {
		// OS admission probes allocate and yield independently of retained
		// evidence. Admit once before measuring response construction.
		ctx, release, err := q.requestBudgets.projectionContext(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer release()
		return testing.AllocsPerRun(3, func() {
			allocation := projectionAllocation(ctx)
			mark := allocation.markProjection()
			defer allocation.rewindProjection(mark)
			q.statusCache.data = ""
			if _, err := q.cachedStatusJSON(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
	smallAllocs, largeAllocs := refreshAllocs(small), refreshAllocs(large)
	// Two times the small capture allows runtime noise while detecting a return
	// to per-retained-row copies (a 1000-fold fixture cardinality difference).
	if largeAllocs > 2*smallAllocs {
		t.Fatalf("retention amplified construction allocations: %.0f -> %.0f", smallAllocs, largeAllocs)
	}
	var s Snapshot
	if err = json.Unmarshal([]byte(largeJSON), &s); err != nil {
		t.Fatal(err)
	}
	if len(s.Jobs) != 8 || len(s.Machines) != 16 || s.Summary.RetainedJobs != 8008 || s.Efficiency != nil || len(s.WorkCurves) != 0 || len(s.StorageObservations) != 0 || len(s.HealthAlerts) != 0 || s.FileStore != nil {
		t.Fatal("default retained diagnostics or hid current work")
	}
	if len(large.s.WorkObservations) != 8000 || len(large.s.Health.Events) != 8000 {
		t.Fatal("projection changed retained evidence")
	}
	// Published inputs must stay cold: default status never materializes the
	// immutable retained generation just to omit it again at encoding time.
	q, c := fixture(t)
	j := submit(t, q, spec("experiment"))
	before := q.readGeneration
	_ = readStatusOptions(t, q, "")
	if before.state.Jobs != nil {
		t.Fatal("default materialized full retained generation")
	}
	q.statusCache.data = ""
	if err := q.update(func() error {
		q.s.Machines["ghost"] = &Machine{ID: "ghost", Gone: true, Replacement: &MachineReplacement{Machine: "worker", Device: "0"}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// Duplicate conditions fold into one group per kind (live: 18 work profiles
	// with one regression were 18 lines); recipe identities never leak.
	since := c.Now()
	// Status reads committed state; commit the job mutation.
	if err := q.update(func() error {
		q.touchJob(j.ID)
		q.s.Jobs[j.ID].Attempt = 1
		q.s.Jobs[j.ID].State = "failed" // Pending physical cleanup stays visible.
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// Commit maintenance clears managed conditions that have no live source,
	// so publish these grouping inputs directly as committed base state.
	q.mu.Lock()
	q.s.Health.Conditions = map[string]healthCondition{
		"recipe/worker/private/path":              {Active: true, Since: since},
		"recipe/peer/" + strings.Repeat("ab", 16): {Active: true, Since: since},
		"efficiency/work:profile:" + strings.Repeat("1", 64) + ":declared/memory_prediction_relative_error": {Active: true, Since: since.Add(time.Minute)},
		"efficiency/work:profile:" + strings.Repeat("2", 64) + ":learned/memory_prediction_relative_error":  {Active: true, Since: since},
		"efficiency/worker/wasted_attempts":       {Active: true, Since: since},
		"efficiency/project:p/wait_holds_seconds": {Active: false, Since: since},
	}
	q.publishReadRecords(map[string][]byte{"state": nil})
	q.publishReadRuntime()
	q.mu.Unlock()
	current := readStatusOptions(t, q, "")
	data, _ := json.Marshal(current.Summary)
	want := []StatusAlertGroup{
		{Kind: "efficiency/memory_prediction_relative_error", Count: 2, Since: since},
		{Kind: "recipe", Count: 2, Since: since, Scopes: map[string]int{"worker": 1, "peer": 1}},
		{Kind: "efficiency/wasted_attempts", Count: 1, Since: since, Scopes: map[string]int{"worker": 1}},
	}
	if len(current.Jobs) != 1 || strings.Contains(string(data), "private") || strings.Contains(string(data), "profile") || len(current.Summary.Alerts) != len(want) {
		t.Fatal("cleanup/alert projection", string(data))
	}
	for i, g := range current.Summary.Alerts {
		if g.Kind != want[i].Kind || g.Count != want[i].Count || !g.Since.Equal(want[i].Since) || !maps.Equal(g.Scopes, want[i].Scopes) {
			t.Fatalf("alert group %d: %+v want %+v", i, g, want[i])
		}
	}
	if slices.ContainsFunc(current.Machines, func(m *Machine) bool { return m.ID == "ghost" }) {
		t.Fatal("default listed a replaced worker identity")
	}
	if section := readStatusOptions(t, q, "?sections=machines"); slices.ContainsFunc(section.Machines, func(m *Machine) bool { return m.ID == "ghost" }) {
		t.Fatal("machines section listed a replaced worker identity")
	}
	if full := readStatusOptions(t, q, "?detail=full"); !slices.ContainsFunc(full.Machines, func(m *Machine) bool { return m.ID == "ghost" }) {
		t.Fatal("full status hid replaced worker history")
	}
}

func TestStatusCompactSectionsAndFull(t *testing.T) {
	q := retainedStatusFixture(t, 3)
	j := q.s.Jobs["current-00"]
	j.AttemptHistory = []AttemptOutcome{{Attempt: 1, State: "failed"}}
	m := q.s.Machines["worker-00"]
	m.HostHealth = &HostHealth{History: []HostHealthSample{{}, {}}, Baseline: []HostHealthSample{{}}}
	summary := readStatusOptions(t, q, "")
	if len(summary.Jobs[0].AttemptHistory) != 0 {
		t.Fatal("default copied attempt history")
	}
	for _, section := range []string{"storage", "work", "health", "efficiency", "usage", "jobs", "machines", "diagnostics"} {
		s := readStatusOptions(t, q, "?sections="+section)
		switch section {
		case "storage":
			if len(s.StorageObservations) != 3 {
				t.Fatal("storage omitted")
			}
		case "work":
			if len(s.WorkCurves) != 3 {
				t.Fatal("work omitted")
			}
		case "health":
			if len(s.HealthAlerts) != 3 {
				t.Fatal("health omitted")
			}
		case "efficiency":
			if s.Efficiency == nil || len(s.Efficiency.Jobs) != 3 {
				t.Fatal("efficiency omitted")
			}
		case "jobs":
			for _, job := range s.Jobs {
				if job.ID == j.ID && len(job.AttemptHistory) != 1 {
					t.Fatal("job detail omitted")
				}
			}
		case "machines":
			if len(s.Machines[0].HostHealth.History) != 2 {
				t.Fatal("machine detail omitted")
			}
		}
		if section != "work" && len(s.WorkCurves) != 0 {
			t.Fatal("unrequested work evidence returned", section)
		}
	}
	legacy, err := q.publicSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	options, _ := parseStatusOptions(url.Values{"detail": {"full"}})
	restored, err := q.publicStatusSnapshot(&options)
	if err != nil {
		t.Fatal(err)
	}
	restored.JobsPage = nil // Only pagination metadata is new in explicit full.
	if !reflect.DeepEqual(legacy, restored) {
		t.Fatal("full inspection differs from legacy projection")
	}
	full := readStatusOptions(t, q, "?detail=full")
	if len(full.Jobs) != 11 || len(full.StorageObservations) != 3 || len(full.WorkCurves) != 3 || len(full.HealthAlerts) != 3 || full.Efficiency == nil || len(full.Machines[0].HostHealth.Baseline) != 1 {
		t.Fatal("full inspection lost existing fields")
	}
}

func TestStatusCompactCallsAndFilteredCapacity(t *testing.T) {
	q := retainedStatusFixture(t, 0)
	q.calls = map[string]*ModelCall{
		"model-running": {ID: "model-running", Model: "sample", Machine: "worker-00", Project: "first", Priority: "experiment", Queued: q.clock.Now()},
		"model-queued":  {ID: "model-queued", Model: "sample", Project: "second", Priority: "experiment", Queued: q.clock.Now()},
	}
	q.s.Hosts = map[string]*HostState{"worker-00": {Active: true, Policy: HostPolicy{ProductionHold: true}}}
	q.s.Machines["worker-00"].MaxJobs = 1
	q.contacts = map[string]*workerContact{"worker-00": {allowance: time.Minute, arrivalAllowance: time.Minute, processed: q.clock.Now()}}
	s := readStatusOptions(t, q, "?project=second&view=snapshot")
	if len(s.Jobs) != 1 || s.Jobs[0].ID != "model-queued" || s.Jobs[0].State != "queued" {
		t.Fatal("filtered call identity/lifecycle lost", s.Jobs)
	}
	if len(s.Holds) == 0 || !strings.Contains(s.Holds[0].Detail, "stopping") {
		t.Fatal("call hold lost", s.Holds)
	}
	if s.Machines[0].AdmissionReason != "maximum simultaneous jobs reached" {
		t.Fatal("filter changed fleet capacity", s.Machines[0].AdmissionReason)
	}
	all := readStatusOptions(t, q, "")
	for _, j := range all.Jobs {
		if j.ID == "model-running" && (j.State != "running" || j.Machine != "worker-00") {
			t.Fatal("running call lifecycle lost")
		}
	}
}

func TestStatusCompactOutstandingCustody(t *testing.T) {
	q := retainedStatusFixture(t, 1)
	j := q.s.Jobs["current-00"]
	j.Spec.WallTime, j.Spec.Logs, j.Spec.AuthoringOrigin, j.Progress = "1m", "keep", OriginAdapter, 7
	q.clock.(*fakeClock).Advance(time.Second)
	j.Copies = []Copy{{Machine: "worker-00", Session: "previous", Attempt: 1}, {Machine: "worker-00", Session: "released", Attempt: 2, ReleaseConfirmed: true}}
	m := q.s.Machines["worker-00"]
	m.EnvironmentReports = []EnvironmentReport{{Key: "building", State: "building", Attempt: "prepare"}, {Key: "old", State: "ready"}}
	m.CopyReports = []CopyReport{{Model: "copying", State: "copying", Attempt: "copy"}, {Model: "old", State: "ready"}}
	options, _ := parseStatusOptions(url.Values{})
	v, _, _, err := q.compactStatusObservation(&options, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(v.s.Jobs[j.ID].Copies) != 1 || len(v.s.Machines[m.ID].EnvironmentReports) != 1 || len(v.s.Machines[m.ID].CopyReports) != 1 || len(v.hostObligations("worker-00")) != 2 || v.untrackedPreparations(v.s.Machines[m.ID]) != 2 {
		t.Fatal("compact capture lost outstanding attempt/preparation custody")
	}
	got := readStatusOptions(t, q, "")
	for _, current := range got.Jobs {
		if current.ID == j.ID && (len(current.Copies) != 1 || current.WallTime == nil || current.WallTime.RemainingSeconds == nil || *current.WallTime.RemainingSeconds != 59 || current.Progress != 7 || current.Spec.Logs != "keep" || current.Spec.AuthoringOrigin != OriginAdapter) {
			t.Fatal("current lifecycle facts omitted or stale")
		}
	}
}
