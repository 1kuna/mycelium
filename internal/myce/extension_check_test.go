package myce

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestExtensionCheckAutoMemory(t *testing.T) {
	t.Run("floor-free variant", func(t *testing.T) {
		e := compositionExtension(t, false)
		e.Variants[0].Needs = ResourceNeeds{Memory: "auto"}
		freezeComposition(t, e)
	})
	for _, mode := range []string{"", "auto"} {
		t.Run("submission/"+mode, func(t *testing.T) {
			e := compositionExtension(t, false)
			e.Kinds = nil
			e.Variants[0].Needs.Memory = mode
			if mode == "auto" {
				e.Variants[0].Needs = ResourceNeeds{Memory: mode}
			}
			delete(e.Variants[0].Baselines, "kind:sample")
			e = freezeComposition(t, e)
			root := t.TempDir()
			if err := InstallExtension(root, e); err != nil {
				t.Fatal(err)
			}
			fingerprint, err := ExtensionFingerprint(context.Background(), "fake", "0")
			if err != nil {
				t.Fatal(err)
			}
			m := machine("check-worker", "fake")
			m.HostID, m.Device, m.ExtensionFingerprint = "check-host", "0", fingerprint
			m.CapabilityProfile.OS, m.CapabilityProfile.Arch = e.Variants[0].Target.OS, e.Variants[0].Target.Arch
			response := func(v any) (*http.Response, error) {
				b, _ := json.Marshal(v)
				return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(b))}, nil
			}
			submitted := false
			client := Client{URL: "http://fixture", HTTP: &http.Client{Transport: clientTestTransport(func(req *http.Request) (*http.Response, error) {
				switch req.URL.Path {
				case "/api/inspection":
					return response(Snapshot{Machines: []*Machine{&m}})
				case "/api/extensions":
					return response(nil)
				case "/api/jobs":
					var s Spec
					if err := json.NewDecoder(req.Body).Decode(&s); err != nil {
						t.Fatal(err)
					}
					submitted = true
					if !s.RequireTarget {
						t.Error("runtime check omitted capacity-only target validation")
					}
					if s.Needs.Memory != mode || s.ExtensionCheck == nil {
						t.Errorf("check submission memory = %q, want %q", s.Needs.Memory, mode)
					}
					return response(Job{ID: "check", State: "failed", Reason: "fixture check failure"})
				case "/api/jobs/check":
					return response(Job{ID: "check", State: "failed"})
				case "/api/jobs/cancel":
					return response(map[string]any{"release": "released"})
				case "/api/jobs/check/logs":
					return response([]LogChunk{})
				}
				t.Fatalf("unexpected request: %s", req.URL.Path)
				return nil, os.ErrInvalid
			})}}
			cache := &EnvironmentCache{Root: t.TempDir(), Limit: 4 << 20}
			reports, err := CheckExtension(context.Background(), root, e, "fake", "0", m.HostID, io.Discard, cache, client)
			if !submitted || err == nil || len(reports) != 1 || reports[0].Qualified {
				t.Fatalf("failed check granted qualification: %+v, %v", reports, err)
			}
			saved := readExtensionReports(root, e, "fake")
			if len(saved) != 1 || saved[0].Qualified {
				t.Fatalf("failed check persisted qualification: %+v", saved)
			}
		})
	}
}

func TestExtensionCheckAutoPreservesThroughput(t *testing.T) {
	for _, checkAge := range []time.Duration{0, 29 * 24 * time.Hour} {
		t.Run(checkAge.String(), func(t *testing.T) {
			q, clock, m, s := observationFixture(t)
			e := registeredExtension(t, q, s.Extension)
			e.Version = "auto"
			e.Variants[0].Needs = ResourceNeeds{CPUCores: 1, Memory: "auto"}
			e = freezeComposition(t, e)
			if err := q.AddExtension(e); err != nil {
				t.Fatal(err)
			}
			report := newExtensionReport(e, &m, "extension:sample")
			report.Setup, report.Qualified, report.CheckID = true, true, "synthetic-check"
			m.ExtensionReports = []ExtensionReport{report}
			check(t, q, m, nil)
			s.Extension = extensionKey(e)
			work := startObservation(t, q, m, s)
			clock.Advance(2 * time.Second)
			finish(t, q, &work, true)
			key := identityHash(q.s.Jobs[work.ID].Timing.Key)
			before := q.s.WorkObservations[key]
			if before.Units != 10 || before.Seconds != 2 || before.Rate != 5 || before.LatencySeconds != 2 {
				t.Fatalf("missing throughput history: %+v", before)
			}
			clock.Advance(checkAge)
			probe := s
			probe.Workload = nil
			probe.ExtensionDigest = e.Digest
			probe.AllowedMachines = []string{m.ID}
			probe.Needs.Memory = "auto"
			probe.ExtensionCheck = &ExtensionCheckRequest{Work: "extension:sample", Directory: t.TempDir()}
			job := startObservation(t, q, m, probe)
			clock.Advance(time.Second)
			host := 2.0
			r := result(&job, true)
			r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			after := q.s.WorkObservations[key]
			if !after.RecordedAt.Equal(before.RecordedAt) {
				t.Errorf("auto check refreshed throughput age: before=%v after=%v", before.RecordedAt, after.RecordedAt)
			}
			if after.Job != before.Job || after.Machine != before.Machine || after.Session != before.Session || after.Attempt != before.Attempt || after.Source != before.Source || after.SampleID != before.SampleID || after.Forbidden != before.Forbidden || after.StaleReason != before.StaleReason || after.Key != before.Key {
				t.Errorf("auto check changed observation provenance or eligibility: before=%+v after=%+v", before, after)
			}
			if !after.MemoryRecordedAt.Equal(clock.Now()) {
				t.Errorf("auto check did not refresh memory age: %v", after.MemoryRecordedAt)
			}
			if after.Units != before.Units || after.Seconds != before.Seconds || after.Rate != before.Rate || after.LatencySeconds != before.LatencySeconds {
				t.Errorf("auto check changed throughput: before=%+v after=%+v", before, after)
			}
			if len(q.s.WorkObservations) != 1 || after.Samples != before.Samples+1 || after.MemoryPeak == nil || after.MemoryPeak.HostGB == nil || *after.MemoryPeak.HostGB != host {
				t.Errorf("auto check did not update the same observation's memory: %+v", q.s.WorkObservations)
			}
			timing := q.s.Jobs[job.ID].Timing
			if timing.ActualSeconds != nil || timing.PredictedSeconds != nil {
				t.Errorf("check learned timing: %+v", timing)
			}

			if checkAge > 0 {
				// Work was measured on day 0; the day-29 check must not keep its
				// throughput usable for placement or prediction on day 34.
				clock.Advance(5 * 24 * time.Hour)
				check(t, q, m, nil)
				next := submit(t, q, s)
				if estimate, ok := q.estimateCompletion(q.s.Jobs[next.ID], q.s.Machines[m.ID]); ok {
					t.Errorf("expired throughput used for placement: %+v", estimate)
				}
				reply := check(t, q, m, nil)
				if reply.Job == nil || reply.Job.ID != next.ID {
					t.Fatalf("day-34 work not assigned: %+v", reply)
				}
				if err := q.Started(result(reply.Job, true)); err != nil {
					t.Fatal(err)
				}
				if timing := q.s.Jobs[next.ID].Timing; timing == nil || timing.Key == nil || timing.PredictedSeconds != nil {
					t.Errorf("expired throughput used for prediction: %+v", timing)
				}
				// The day-29 memory evidence is fresh on its own clock.
				if got := q.s.Jobs[next.ID]; got.MemoryEnvelope == nil || got.MemoryEnvelope.Source != "accepted peak × 1.25" || got.Spec.Needs.HostMemoryGB != 2.5 {
					t.Errorf("fresh check peak not reused after throughput expired: %+v", got.MemoryEnvelope)
				}
			}
		})
	}
}

func TestExtensionCheckAutoMemoryOnBenchmarkPoint(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	e := registeredExtension(t, q, s.Extension)
	e.Version = "auto"
	e.Variants[0].Needs = ResourceNeeds{CPUCores: 1, Memory: "auto"}
	e = freezeComposition(t, e)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	report := newExtensionReport(e, &m, "extension:sample")
	report.Setup, report.Qualified, report.CheckID = true, true, "synthetic-check"
	m.ExtensionReports = []ExtensionReport{report}
	check(t, q, m, nil)
	k, why := curveKey(e, q.s.Machines[m.ID], "extension:sample")
	if k == nil {
		t.Fatal(why)
	}
	k.Concurrency = 1
	q.s.WorkObservations = map[string]WorkObservation{identityHash(k): {Key: *k, Source: "idle reference", SampleID: "benchmark", Samples: 1, Units: 10, Seconds: 2, Rate: 5, LatencySeconds: 2, RecordedAt: clock.Now()}}
	s.Extension = extensionKey(e)
	probe := s
	probe.Workload = nil
	probe.ExtensionDigest = e.Digest
	probe.AllowedMachines = []string{m.ID}
	probe.Needs.Memory = "auto"
	probe.ExtensionCheck = &ExtensionCheckRequest{Work: "extension:sample", Directory: t.TempDir()}
	job := startObservation(t, q, m, probe)
	clock.Advance(time.Second)
	host := 2.0
	r := result(&job, true)
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	if len(q.s.WorkObservations) != 1 {
		t.Fatalf("check did not share the benchmark point: %+v", q.s.WorkObservations)
	}
	queued := submit(t, q, s)
	next := check(t, q, m, nil).Job
	if next == nil || next.ID != queued.ID {
		t.Fatalf("comparable work not assigned: %+v", next)
	}
	if next.MemoryEnvelope == nil || next.MemoryEnvelope.Source != "accepted peak × 1.25" || next.Spec.Needs.HostMemoryGB != 2.5 {
		t.Fatalf("check peak on a benchmark point not reused: %+v", next.MemoryEnvelope)
	}
}

func TestExtensionCheckAutoMemorySurvivesOldBenchmarkReport(t *testing.T) {
	q, clock, m, s := observationFixture(t)
	e := registeredExtension(t, q, s.Extension)
	e.Version = "auto"
	e.Variants[0].Needs.Memory = "auto" // Keeps the host floor a reference point needs.
	e.Variants[0].Sweeps = map[string]ReferenceSweep{"extension:sample": {Command: []string{"sh", "curve.sh"}, Counts: []int{1}, MaxConcurrency: 1}}
	e.Files["curve.sh"] = []byte("exit 0\n")
	e = freezeComposition(t, e)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	report := newExtensionReport(e, &m, "extension:sample")
	report.Setup, report.Qualified, report.CheckID = true, true, "synthetic-check"
	m.ExtensionReports = []ExtensionReport{report}
	check(t, q, m, nil)
	s.Extension = extensionKey(e)
	probe := s
	probe.Workload = nil
	probe.ExtensionDigest = e.Digest
	probe.AllowedMachines = []string{m.ID}
	probe.Needs.Memory = "auto"
	probe.ExtensionCheck = &ExtensionCheckRequest{Work: "extension:sample", Directory: t.TempDir()}
	job := startObservation(t, q, m, probe)
	clock.Advance(time.Second)
	host := 2.0
	r := result(&job, true)
	r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host}
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	k, why := curveKey(e, q.s.Machines[m.ID], "extension:sample")
	if k == nil {
		t.Fatal(why)
	}
	k.Concurrency = 1
	// A distinct, otherwise valid benchmark report whose throughput is 31 days old.
	old := WorkObservation{Key: *k, Source: "idle reference", SampleID: "old-benchmark", Units: 10, Seconds: 2, Rate: 5, LatencySeconds: 2, RecordedAt: clock.Now().Add(-31 * 24 * time.Hour)}
	publishCurves(t, q, m, []WorkObservation{old})
	if p := q.s.WorkObservations[identityHash(k)]; len(q.s.WorkObservations) != 1 || p.SampleID != "old-benchmark" || p.MemoryRecordedAt.IsZero() {
		t.Fatalf("old benchmark report not accepted onto the check's point: %+v", q.s.WorkObservations)
	}
	queued := submit(t, q, s)
	next := check(t, q, m, nil).Job
	if next == nil || next.ID != queued.ID {
		t.Fatalf("comparable work not assigned: %+v", next)
	}
	if next.MemoryEnvelope == nil || next.MemoryEnvelope.Source != "accepted peak × 1.25" || next.Spec.Needs.HostMemoryGB != 2.5 {
		t.Fatalf("old benchmark report expired fresh memory: %+v", next.MemoryEnvelope)
	}
}

func TestExtensionCheckAutoLearnsMemoryForComparableWork(t *testing.T) {
	for _, outcome := range []string{"accepted", "constrained", "failed"} {
		t.Run(outcome, func(t *testing.T) {
			q, clock, m, s := observationFixture(t)
			e := registeredExtension(t, q, s.Extension)
			e.Version = "auto"
			e.Variants[0].Needs = ResourceNeeds{CPUCores: 1, Memory: "auto"}
			e = freezeComposition(t, e)
			if err := q.AddExtension(e); err != nil {
				t.Fatal(err)
			}
			prepared := newExtensionReport(e, &m, "extension:sample")
			prepared.Setup = true // Prepared but unqualified; only the check may run.
			m.ExtensionReports = []ExtensionReport{prepared}
			check(t, q, m, nil)
			s.Extension = extensionKey(e)
			probe := s
			probe.Workload = nil
			probe.ExtensionDigest = e.Digest
			probe.AllowedMachines = []string{m.ID}
			probe.Needs.Memory = "auto"
			probe.ExtensionCheck = &ExtensionCheckRequest{Work: "extension:sample", Directory: t.TempDir()}
			first := startObservation(t, q, m, probe)
			if first.MemoryEnvelope == nil || first.MemoryEnvelope.Source != "exploratory first run" {
				t.Fatalf("check did not explore: %+v", first.MemoryEnvelope)
			}
			clock.Advance(time.Second)
			host := 2.0
			r := result(&first, outcome != "failed")
			r.MemoryPeak = &AttemptMemoryPeak{HostGB: &host, Constrained: outcome == "constrained"}
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			if outcome == "accepted" && len(q.s.WorkObservations) != 1 {
				t.Fatalf("successful auto check did not learn memory: %+v", q.s.WorkObservations)
			}
			for _, point := range q.s.WorkObservations {
				if point.Rate != 0 || point.LatencySeconds != 0 || point.Units != 0 || point.Seconds != 0 {
					t.Fatalf("check learned throughput: %+v", point)
				}
			}
			if got := job(t, q, first.ID); got.Timing != nil && (got.Timing.ActualSeconds != nil || got.Timing.PredictedSeconds != nil) {
				t.Fatalf("check recorded timing: %+v", got.Timing)
			}
			queued := submit(t, q, s)
			if reply := check(t, q, m, nil); reply.Job != nil {
				t.Fatal("memory observation granted qualification")
			}
			report := newExtensionReport(e, &m, "extension:sample")
			report.Setup, report.Qualified, report.CheckID = true, true, "successful-check"
			m.ExtensionReports = []ExtensionReport{report}
			next := check(t, q, m, nil).Job // Caller omits memory; variant owns auto.
			if next == nil || next.ID != queued.ID {
				t.Fatalf("qualified comparable work not assigned: %+v", next)
			}
			if next.Spec.Needs.Memory != "auto" || next.MemoryEnvelope == nil {
				t.Fatalf("variant auto was lost: %+v", next.Spec.Needs)
			}
			if outcome == "accepted" {
				if next.MemoryEnvelope.Source != "accepted peak × 1.25" || next.Spec.Needs.HostMemoryGB != 2.5 {
					t.Fatalf("check peak not reused: %+v", next.MemoryEnvelope)
				}
			} else if !strings.HasPrefix(next.MemoryEnvelope.Source, "exploratory") {
				t.Fatalf("%s check seeded a peak: %+v", outcome, next.MemoryEnvelope)
			}
		})
	}
}

func TestExtensionQualificationSmokeFailureClearsPriorSuccess(t *testing.T) {
	e := compositionExtension(t, false)
	e.Kinds = nil
	asset := []byte("small checked asset")
	hash := sha256.Sum256(asset)
	e.Assets = FileManifest{"model": {SHA256: hex.EncodeToString(hash[:]), SizeBytes: int64(len(asset))}}
	e.Variants[0].Check = []string{"sh", "-c", `test -z "$MYCE_TOKEN" && test -f "$1" && test ! -e "$MYCE_EXTENSION_CACHE/fail" && sh "$MYCE_EXTENSION_DIR/check.sh"`, "check", "{assets.model}"}
	delete(e.Variants[0].Baselines, "kind:sample")
	e = freezeComposition(t, e)
	root := t.TempDir()
	if err := InstallExtension(root, e); err != nil {
		t.Fatal(err)
	}
	cache := &EnvironmentCache{Root: t.TempDir(), Limit: 4 << 20, Fetch: func(_ context.Context, _ FileRef, dest string) error {
		if err := os.MkdirAll(filepath.Dir(dest), 0700); err != nil {
			return err
		}
		return os.WriteFile(dest, asset, 0600)
	}}
	devRoot := t.TempDir()
	d, err := StartDev(DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(devRoot, "queue.db"), Checkpoints: filepath.Join(devRoot, "checkpoints"), FileStore: filepath.Join(devRoot, "files"), ExtensionRoot: root, HostID: "local-host", Token: "fixture", Workers: 1, MemoryGB: 8, Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	assetFile := filepath.Join(devRoot, "asset")
	if err = os.WriteFile(assetFile, asset, 0600); err != nil {
		t.Fatal(err)
	}
	if err = d.Client().UploadFile(context.Background(), assetFile, e.Assets["model"]); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { snap, _ := d.Queue.Snapshot(); return len(snap.Machines) > 0 })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	relative, err := filepath.Rel(cwd, root)
	if err != nil {
		t.Fatal(err)
	}
	client := d.Client()
	var submitted, failedRead atomic.Bool
	client.HTTP = &http.Client{Transport: clientTestTransport(func(r *http.Request) (*http.Response, error) {
		if r.Method == "POST" && r.URL.Path == "/api/jobs" {
			submitted.Store(true)
		}
		if r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/api/jobs/") && submitted.Load() && !failedRead.Swap(true) {
			return nil, context.DeadlineExceeded
		}
		return http.DefaultTransport.RoundTrip(r)
	})}
	reports, err := CheckExtension(ctx, relative, e, "fake", "0", "local-host", io.Discard, cache, client)
	if err != nil || len(reports) != 1 || !reports[0].Qualified || !reports[0].Setup || reports[0].CheckID == "" || !failedRead.Load() {
		t.Fatal(reports, err)
	}
	m := Machine{HostID: "local-host", GPU: "fake", ExtensionFingerprint: reports[0].Fingerprint, CapabilityProfile: &CapabilityProfile{OS: e.Variants[0].Target.OS, Arch: e.Variants[0].Target.Arch}}
	if extensionQualificationReason(e, reports[0], &m) != "" {
		t.Fatal("qualification does not bind placement")
	}
	m.HostID = "other-host"
	if extensionQualificationReason(e, reports[0], &m) == "" {
		t.Fatal("qualification crossed hosts")
	}
	before, err := os.ReadFile(extensionRecords(root, e, "fake"))
	if err != nil {
		t.Fatal(err)
	}
	for _, flags := range []struct {
		host, device string
		cache        *EnvironmentCache
	}{
		{"wrong-host", "0", cache}, {"", "0", cache}, {"local-host", "", cache}, {"local-host", "0", nil},
	} {
		if _, err := CheckExtension(ctx, root, e, "fake", flags.device, flags.host, io.Discard, flags.cache, d.Client()); err == nil {
			t.Fatal("invalid selection accepted")
		}
		after, err := os.ReadFile(extensionRecords(root, e, "fake"))
		if err != nil || !bytes.Equal(before, after) {
			t.Fatal("mistaken check changed qualification", err)
		}
	}
	if err = os.WriteFile(filepath.Join(extensionCache(root, e, "fake"), "fail"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	reports, err = CheckExtension(ctx, root, e, "fake", "0", "local-host", io.Discard, cache, d.Client())
	if err == nil || len(reports) != 1 || reports[0].Qualified {
		t.Fatal("failed check retained qualification", reports, err)
	}
	marker := "command log: "
	logPath := strings.SplitN(reports[0].Reason, marker, 2)
	if len(logPath) != 2 {
		t.Fatal("missing command log path", reports)
	}
	if _, err := os.Stat(logPath[1]); err != nil {
		t.Fatal("missing retained command log", err)
	}
	snapshot, err := d.Queue.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	for _, j := range snapshot.Jobs {
		if j.Spec.SubmissionKey == reports[0].CheckID && j.State != "failed" {
			t.Fatal("failed check displayed as success")
		}
	}
	saved := readExtensionReports(root, e, "fake")
	if len(saved) != 1 || saved[0].Qualified {
		t.Fatal("failed check not persisted", saved)
	}
}

// Same-host RAM evidence can fit a sibling even when cross-machine transfer
// would not. Truly impossible checks still reject before entering the queue.
func TestExtensionCheckSiblingMemoryCapacity(t *testing.T) {
	for _, peak := range []float64{13.08499526977539, 20} {
		t.Run(fmt.Sprint(peak), func(t *testing.T) {
			q, _, m, s := floorFreeFitFixture(t, 24)
			sibling := m.Devices[0]
			sibling.ID, sibling.Identity = "1", "sibling-card"
			sibling.ExtensionFingerprint = "sibling-backend-v1"
			m.Devices = append(m.Devices, sibling)
			m.HostAvailableMemoryGB = 1
			machineCheck(t, q, m)
			for id, p := range q.s.WorkObservations {
				delete(q.s.WorkObservations, id)
				p.Key.Component, p.Key.Hardware = "1", "sibling-card"
				p.Key.Fingerprint = sibling.ExtensionFingerprint
				p.MemoryPeak.HostGB, p.MemoryPeak.CgroupGB = &peak, &peak
				q.s.WorkObservations[identityHash(p.Key)] = p
			}
			// Physical capacity exceeds the operator's allowance, and live occupancy
			// is temporary. Qualification must compare against the whole allowance.
			s.Workload = nil
			s.ExtensionDigest = registeredExtension(t, q, s.Extension).Digest
			s.RequireTarget = true
			s.AllowedMachines = []string{m.ID}
			s.Needs = ResourceNeeds{Memory: "auto", GPU: &GPUSelection{Devices: []string{"0"}}}
			s.ExtensionCheck = &ExtensionCheckRequest{Work: "extension:sample", Directory: t.TempDir()}
			j, err := q.Submit(s)
			if peak*1.25 > 24 {
				var refusal *HTTPError
				if !errors.As(err, &refusal) || refusal.Code != "no_qualified_target" || !strings.Contains(refusal.Message, "exceeds allowance 24 GiB") {
					t.Fatalf("impossible sibling estimate was not rejected: job=%+v err=%v", j, err)
				}
				if len(q.s.Jobs) != 0 {
					t.Fatal("rejected runtime check entered the queue")
				}
			} else {
				if err != nil || j == nil || j.State != "queued" {
					t.Fatalf("temporary occupancy rejected a fitting check: job=%+v err=%v", j, err)
				}
				if got := assignedJob(machineCheck(t, q, m)); got != nil {
					t.Fatalf("busy-host check bypassed live admission: %+v", got)
				}
			}
		})
	}
}

func TestExtensionCheckProductionHoldStillQueues(t *testing.T) {
	q, clock, m, s := floorFreeFitFixture(t, 24)
	production := submit(t, q, m3Spec("production", "0", 1, 1))
	running := assignedJob(machineCheck(t, q, m))
	if running == nil || running.ID != production.ID {
		t.Fatal("production was not assigned")
	}
	clock.Advance(time.Second)
	s.Workload = nil
	s.ExtensionDigest = registeredExtension(t, q, s.Extension).Digest
	s.RequireTarget = true
	s.AllowedMachines = []string{m.ID}
	s.Needs = ResourceNeeds{Memory: "auto", GPU: &GPUSelection{Devices: []string{"0"}}}
	s.ExtensionCheck = &ExtensionCheckRequest{Work: "extension:sample", Directory: t.TempDir()}
	j, err := q.Submit(s)
	if err != nil || j == nil || j.State != "queued" {
		t.Fatalf("production hold rejected a fitting runtime check: job=%+v err=%v", j, err)
	}
	if got := assignedJob(machineCheck(t, q, m, running)); got != nil || job(t, q, running.ID).State != "running" {
		t.Fatalf("runtime check displaced production: %+v", got)
	}
	finish(t, q, running, true)
	if got := assignedJob(machineCheck(t, q, m)); got == nil || got.ID != j.ID {
		t.Fatalf("runtime check did not start after production released: %+v", got)
	}
}

// Drive the real check client and queue synchronously; only the command result
// is synthetic. Queue/admission/preparation and execution use the fake clock.
func TestExtensionCheckExecutionTimeout(t *testing.T) {
	for _, mode := range []string{"qualifies after occupancy", "never admitted", "never admitted cleanup fails", "preparation never starts", "execution exceeds timeout"} {
		t.Run(mode, func(t *testing.T) {
			q, clock := fixture(t)
			e := compositionExtension(t, false)
			e.Kinds = nil
			delete(e.Variants[0].Baselines, "kind:sample")
			e = freezeComposition(t, e)
			root := t.TempDir()
			if err := InstallExtension(root, e); err != nil {
				t.Fatal(err)
			}
			fingerprint, err := ExtensionFingerprint(context.Background(), "fake", "0")
			if err != nil {
				t.Fatal(err)
			}
			m := machine("check-worker", "fake", "check-host")
			m.Device, m.ExtensionFingerprint = "0", fingerprint
			m.CapabilityProfile.OS, m.CapabilityProfile.Arch = e.Variants[0].Target.OS, e.Variants[0].Target.Arch
			prepared := newExtensionReport(e, &m, "extension:sample")
			prepared.Setup = true
			m.ExtensionReports = []ExtensionReport{prepared}
			// The target fits when idle, but live host RAM is occupied.
			m.HostAvailableMemoryGB, m.HostMemoryMeasured = .01, true
			check(t, q, m, nil)
			var submitted *Job
			var scratch string
			response := func(v any) (*http.Response, error) {
				b, marshalErr := json.Marshal(v)
				if marshalErr != nil {
					t.Fatal(marshalErr)
				}
				return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(b))}, nil
			}
			client := Client{URL: "http://fixture", HTTP: &http.Client{Transport: clientTestTransport(func(req *http.Request) (*http.Response, error) {
				switch {
				case req.URL.Path == "/api/inspection":
					return response(Snapshot{Machines: []*Machine{&m}})
				case req.URL.Path == "/api/extensions":
					if err := q.AddExtension(e); err != nil {
						t.Fatal(err)
					}
					return response(nil)
				case req.URL.Path == "/api/jobs":
					var s Spec
					if err := json.NewDecoder(req.Body).Decode(&s); err != nil {
						t.Fatal(err)
					}
					scratch = s.ExtensionCheck.Directory
					submitted = submit(t, q, s)
					if submitted.Spec.Priority == "production" || submitted.Spec.ExtensionCheck.Timeout != "3m0s" {
						t.Fatalf("wrong check budget/priority: %+v", submitted.Spec)
					}
					if got := check(t, q, m, nil).Job; got != nil {
						t.Fatal("occupied RAM allowed check assignment", got)
					}
					return response(submitted)
				case req.URL.Path == "/api/jobs/cancel":
					if mode == "never admitted cleanup fails" {
						return nil, errors.New("release confirmation unavailable")
					}
					ack, err := q.CancelSubmission("extension-check", submitted.Spec.SubmissionKey)
					if err != nil {
						t.Fatal(err)
					}
					return response(ack)
				case strings.HasSuffix(req.URL.Path, "/logs"):
					return response([]LogChunk{})
				case req.URL.Path == "/api/jobs/"+submitted.ID:
					if strings.HasPrefix(mode, "never admitted") {
						clock.Advance(time.Hour)
						if err := q.Tick(); err != nil {
							t.Fatal(err)
						}
					} else {
						clock.Advance(4 * time.Minute) // longer than --timeout
						check(t, q, m, nil)
						if got := job(t, q, submitted.ID); got.State != "queued" {
							t.Fatal("admission wait consumed execution budget", got)
						}
						m.HostAvailableMemoryGB = 32
						running := check(t, q, m, nil).Job
						if running == nil || running.ID != submitted.ID {
							t.Fatalf("check not assigned after RAM released: running=%+v waiting=%+v", running, job(t, q, submitted.ID))
						}
						if mode == "preparation never starts" {
							clock.Advance(time.Hour)
							if err := q.Tick(); err != nil {
								t.Fatal(err)
							}
							if err := q.Started(result(running, true)); !errors.Is(err, ErrStale) {
								t.Fatal("expired preparation accepted Started", err)
							}
							finish(t, q, running, false) // exact release acknowledgement
						} else {
							clock.Advance(20 * time.Second) // preparation also excluded
							if err := q.Started(result(running, true)); err != nil {
								t.Fatal(err)
							}
							elapsed := 64 * time.Second
							if mode == "execution exceeds timeout" {
								elapsed = 3 * time.Minute
							}
							clock.Advance(elapsed)
							finish(t, q, running, true)
							if err := os.WriteFile(filepath.Join(scratch, "actual"), e.Files["baseline/output"], 0600); err != nil {
								t.Fatal(err)
							}
						}
					}
					reply, err := q.jobSnapshot(submitted.ID)
					if err != nil {
						t.Fatal(err)
					}
					return response(reply)
				}
				t.Fatalf("unexpected request: %s", req.URL.Path)
				return nil, os.ErrInvalid
			})}}
			cache := &EnvironmentCache{Root: t.TempDir(), Limit: 4 << 20}
			reports, err := CheckExtension(context.Background(), root, e, "fake", "0", m.HostID, io.Discard, cache, client, 3*time.Minute)
			if len(reports) != 1 {
				t.Fatalf("missing report: %+v, %v", reports, err)
			}
			if mode == "never admitted cleanup fails" && (err == nil || !strings.Contains(err.Error(), "release confirmation unavailable")) {
				t.Fatalf("lost cleanup error: %v", err)
			}
			got := job(t, q, submitted.ID)
			saved := readExtensionReports(root, e, "fake")
			if mode == "qualifies after occupancy" {
				if err != nil || !reports[0].Qualified || len(saved) != 1 || !saved[0].Qualified || got.State != "succeeded" {
					t.Fatalf("waiting check did not qualify: report=%+v job=%+v err=%v", reports, got, err)
				}
			} else if mode == "execution exceeds timeout" {
				if err == nil || reports[0].Qualified || got.Reason != "check execution timeout exceeded" || got.StartedAt == nil {
					t.Fatalf("execution deadline not enforced: report=%+v job=%+v err=%v", reports, got, err)
				}
			} else {
				if err == nil || !strings.HasPrefix(err.Error(), "never started: ") || got.StartedAt != nil || reports[0].Qualified || len(saved) != 1 || saved[0].Reason != "never started: "+extensionCheckAdmissionReason(strings.TrimPrefix(got.Reason, "never started: ")) || strings.Contains(saved[0].Reason, "check failed") {
					t.Fatalf("waiting expiry misreported: report=%+v job=%+v err=%v", reports, got, err)
				}
				want := "waiting for host RAM"
				if mode == "preparation never starts" {
					want = "waiting for environment or command preparation"
				}
				if !strings.Contains(saved[0].Reason, want) {
					t.Fatalf("missing admission reason %q: %s", want, saved[0].Reason)
				}
			}
		})
	}
}

func TestExtensionCheckTimeoutPrivacy(t *testing.T) {
	j := &Job{State: "failed", Spec: Spec{ExtensionCheck: &ExtensionCheckRequest{}}, Reason: "never started: environment preparation blocked: /private/fixture: worker-secret"}
	if got := statusJobValue(j); got.Reason != "never started: waiting for environment or command preparation" {
		t.Fatalf("unsafe or missing admission category: %s", got.Reason)
	} else if again := statusJobValue(&got); again.Reason != got.Reason {
		t.Fatalf("admission category lost on repeated projection: %s", again.Reason)
	}
	j.Spec.ExtensionCheck = nil
	if got := statusJobValue(j); got.Reason != "execution failed" {
		t.Fatalf("arbitrary job supplied a check timeout reason: %s", got.Reason)
	}
}
