package myce

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func statusViewResponse(t *testing.T, q *Queue, path string) string {
	t.Helper()
	r := httptest.NewRequest("GET", path, nil)
	r.SetBasicAuth("myce", "status-test")
	w := httptest.NewRecorder()
	API{Queue: q, Token: "status-test"}.Handler().ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatalf("status HTTP %d", w.Code)
	}
	return w.Body.String()
}

func TestStatusViewAvailabilityAndWaiting(t *testing.T) {
	for _, tc := range []struct{ reason, want string }{
		{"waiting for 5 idle minutes", "hypha in use by its owner; waiting for 5 idle minutes"},
		{"on battery", "hypha on battery; waiting for external power"},
		{"cooling down", "hypha hot; waiting to cool down"},
		{"owner signals stale", "hypha availability readings are out of date"},
	} {
		t.Run(tc.reason, func(t *testing.T) {
			q, c := fixture(t)
			m := machine("daily", "fake")
			m.TotalMemoryGB = 8
			m.CapabilityProfile = &CapabilityProfile{OS: "darwin", Arch: "arm64", LogicalCPUs: 8, Benchmark: &BenchmarkResult{State: "complete", Measurements: []Throughput{{Resource: "cpu", Work: "sha256", Rate: 123, Unit: "bytes/s"}}}, MemoryLayout: "unified", SystemMemoryBytes: 16 << 30, Accelerators: []AcceleratorProfile{{Type: "fake"}}, Availability: &Availability{Mode: "daily", Reason: tc.reason, CheckedAt: c.Now(), Signals: OwnerSignals{Power: "ac"}}}
			check(t, q, m, nil)
			submit(t, q, spec("experiment"))
			for _, path := range []string{"/", "/api/status"} {
				body := statusViewResponse(t, q, path)
				if path == "/api/status" {
					var snapshot Snapshot
					if err := json.Unmarshal([]byte(body), &snapshot); err != nil {
						t.Fatal(err)
					}
					if len(snapshot.Jobs) != 1 || !strings.Contains(snapshot.Jobs[0].Reason, tc.want) {
						t.Errorf("waiting job missing %q", tc.want)
					}
				}
				if path == "/" && !strings.Contains(body, "cpu sha256 123.00 bytes/s") {
					t.Errorf("%s missing measured benchmark rate", path)
				}
				if !strings.Contains(body, tc.want) {
					t.Errorf("%s missing %q", path, tc.want)
				}
			}
			// Display cannot change scheduling or the reported availability.
			if check(t, q, m, nil).Job != nil || q.s.Machines[m.ID].CapabilityProfile.Availability.Reason != tc.reason {
				t.Fatal("status changed admission")
			}
			c.Advance(time.Second)
		})
	}
}

func TestStatusViewPrivacyAndQualification(t *testing.T) {
	q, c := fixture(t)
	e := fakeExtension(t)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	m := placementMachine("test-hypha", "unified", 16, 16, e)
	m.ExtensionReports = []ExtensionReport{{Name: e.Name, Version: e.Version, Digest: e.Digest, Work: "kind:sample", Platform: "fake", Reason: "synthetic-private-secret /private/example"}}
	m.LastSeen = c.Now()
	if err := setMaintenance(t, q, hostID(&m), "disable"); err != nil {
		t.Fatal(err)
	}
	m.CapabilityProfile.Availability = &Availability{Reason: "synthetic-private-secret", CheckedAt: c.Now()}
	if err := q.update(func() error { q.s.Machines[m.ID] = &m; return nil }); err != nil {
		t.Fatal(err)
	}
	s := Spec{Name: "kind job", Project: "test", Priority: "experiment", Kind: "sample"}
	kind := submit(t, q, s)
	if err := q.update(func() error { q.s.Jobs[kind.ID].RequeuedByOperator = true; return nil }); err != nil {
		t.Fatal(err)
	}
	s = spec("experiment")
	s.Run.Command = []string{"echo", "synthetic-private-secret"}
	s.Run.Env = map[string]string{"PASSWORD": "synthetic-private-secret"}
	s.Run.Workdir = "/private/example"
	j := submit(t, q, s)
	if err := q.update(func() error {
		q.s.Jobs[j.ID].Spec.Environment = Environment{LockFile: "/private/example", Python: "3.11"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/", "/api/status"} {
		body := statusViewResponse(t, q, path)
		for _, bad := range []string{"synthetic-private-secret", "/private/example", "extension.yaml", "baseline/input"} {
			if strings.Contains(body, bad) {
				t.Errorf("%s exposed %s", path, bad)
			}
		}
		if !strings.Contains(body, "no hypha has a qualified extension for this kind of work") {
			t.Errorf("%s missing qualification explanation", path)
		}
	}
	var snap Snapshot
	if err := json.Unmarshal([]byte(statusViewResponse(t, q, "/api/status")), &snap); err != nil {
		t.Fatal(err)
	}
	if snap.Machines[0].Disabled == "" || snap.Machines[0].ExtensionReports[0].Qualified {
		t.Fatal("redaction lost state")
	}
	// Stored commands and extension contents remain intact for execution.
	for _, j := range q.s.Jobs {
		if j.Spec.Kind == "" && j.Spec.Run.Env["PASSWORD"] != "synthetic-private-secret" {
			t.Fatal("status mutated job")
		}
	}
}

func TestStatusViewReadOnlyAndTokenProtected(t *testing.T) {
	q, _ := fixture(t)
	h := API{Queue: q, Token: "status-test"}.Handler()
	for _, method := range []string{"GET", "POST", "PUT", "DELETE"} {
		for _, path := range []string{"/", "/api/status"} {
			for _, auth := range []bool{false, true} {
				r := httptest.NewRequest(method, path, nil)
				if auth {
					r.SetBasicAuth("myce", "status-test")
				}
				w := httptest.NewRecorder()
				h.ServeHTTP(w, r)
				want := 401
				if auth && method == "GET" {
					want = 200
				}
				// Basic auth is intentionally accepted for status reads only.
				if w.Code != want {
					t.Fatalf("%s %s auth=%t: %d", method, path, auth, w.Code)
				}
			}
			if method != "GET" {
				r := httptest.NewRequest(method, path, nil)
				r.Header.Set("Authorization", "Bearer status-test")
				w := httptest.NewRecorder()
				h.ServeHTTP(w, r)
				if w.Code != 405 {
					t.Fatalf("%s %s: %d", method, path, w.Code)
				}
			}
		}
	}
}

func TestStatusViewProfilesAndRunningWork(t *testing.T) {
	q, c := fixture(t)
	e := fakeExtension(t)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	m := placementMachine("qualified-hypha", "separate", 64, 24, e)
	m.CapabilityProfile.OS = "linux"
	m.CapabilityProfile.Arch = "arm64"
	for i := range m.ExtensionReports {
		m.ExtensionReports[i].Name = e.Name
		m.ExtensionReports[i].Version = e.Version
	}
	check(t, q, m, nil)
	s := spec("experiment")
	s.Resumable = false
	s.Name = "running sample"
	submit(t, q, s)
	running := check(t, q, m, nil).Job
	if running == nil {
		t.Fatal("no assignment")
	}
	if err := q.Started(result(running, true)); err != nil {
		t.Fatal(err)
	}
	m.FreeMemoryGB = 24
	check(t, q, m, running)
	s.Name = "needs more room"
	s.Needs.MemoryGB = 24
	submit(t, q, s)
	body := statusViewResponse(t, q, "/")
	for _, want := range []string{"Hyphae", "Capability profile", "separate system and accelerator memory", "64.0 GiB", "24.0 GiB", "sample@1", "kind of work sample", "qualified", "program sample-script", "running sample", "running · attempt 1", "waiting for a free hypha"} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q; jobs=%v", want, func() []string {
				snap, _ := q.publicSnapshot()
				var reasons []string
				for _, j := range snap.Jobs {
					reasons = append(reasons, j.Reason)
				}
				return reasons
			}())
		}
	}
	c.Advance(time.Second)
	unknown := CapabilityProfileLines(&Machine{CapabilityProfile: &CapabilityProfile{}})
	if strings.Contains(strings.Join(unknown, " "), "0.0 GiB") {
		t.Fatal("unknown measurement shown as zero")
	}
}

func TestStatusViewDisabledAndResourceConstraints(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Machine)
		want   string
	}{
		{"maintenance", func(m *Machine) {}, "planned host maintenance"},
		{"offline", func(m *Machine) { m.Gone = true }, "offline"},
		{"leaving", func(m *Machine) { m.Leaving = true }, "leaving; no new work"},
		{"memory", func(m *Machine) { m.TotalMemoryGB = 4; m.FreeMemoryGB = 4 }, "8.0 GiB free accelerator memory"},
		{"platform", func(m *Machine) { m.GPU = "metal" }, "NVIDIA (CUDA)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, c := fixture(t)
			m := machine("example", "cuda")
			m.TotalMemoryGB = 8
			m.LastSeen = c.Now()
			tc.change(&m)
			if err := q.update(func() error { q.s.Machines[m.ID] = &m; return nil }); err != nil {
				t.Fatal(err)
			}
			if tc.name == "maintenance" {
				if err := setMaintenance(t, q, hostID(&m), "disable"); err != nil {
					t.Fatal(err)
				}
			}
			s := spec("experiment")
			s.Needs.Platform = Platform{Required: []string{"cuda"}}
			submit(t, q, s)
			snap, err := q.publicSnapshot()
			if err != nil {
				t.Fatal(err)
			}
			if len(snap.Jobs) != 1 || !strings.Contains(snap.Jobs[0].Reason, tc.want) {
				t.Fatalf("wrong waiting explanation: %s", snap.Jobs[0].Reason)
			}
		})
	}
}

func TestStatusViewFloorFreeAutoMemory(t *testing.T) {
	q, clock, m, s := floorFreeFitFixture(t, 0)
	m.CapabilityProfile.Availability = &Availability{Mode: "daily", Reason: "on battery", CheckedAt: clock.Now()}
	machineCheck(t, q, m)
	j := submit(t, q, s)
	// The status fit must identify an otherwise fitting, unavailable host.
	body := statusViewResponse(t, q, "/api/status")
	if !strings.Contains(body, "hypha on battery") {
		t.Errorf("availability hidden: %s", body)
	}
	clock.Advance(time.Second)
	q, clock, m, s = floorFreeFitFixture(t, 0)
	j = submit(t, q, s)
	clock.Advance(time.Second)
	// Exercise the generic no-fit diagnostic with insufficient live host RAM.
	target := q.s.Machines[m.ID]
	target.FreeHostMemoryGB = 1
	probe := *q.s.Jobs[j.ID]
	probe.Reason = "no machine in the fleet can run this right now"
	body = q.statusWaitingReason(&probe)
	if !strings.Contains(body, "needs 3.8 GiB free accelerator memory and 2.5 GiB host RAM") || strings.Contains(body, "needs 0.0 GiB") {
		t.Errorf("sized need missing: %s", body)
	}
	stored := q.s.Jobs[j.ID]
	if stored.MemoryEnvelope != nil || stored.Spec.Needs.MemoryGB != 0 || stored.Spec.Needs.HostMemoryGB != 0 {
		t.Fatal("status resized the stored job")
	}
}

func TestStatusViewFloorFreeAutoMemoryWithoutHypha(t *testing.T) {
	for _, scenario := range []string{"empty fleet", "project excludes fleet", "auto variant"} {
		t.Run(scenario, func(t *testing.T) {
			q, _, _, s := floorFreeFitFixture(t, 0)
			s.Needs.Memory = "auto"
			j := &Job{Spec: s, State: "queued", Reason: "no machine in the fleet can run this right now"}
			if scenario == "project excludes fleet" {
				q.s.Policies = map[string]ProjectPolicy{s.Project: {Weight: 1, AllowedMachines: []string{"other-host"}}}
			} else {
				clear(q.s.Machines)
			}
			if scenario == "auto variant" {
				j.Spec.Needs.Memory = ""
				j.Extensions = []Extension{{Variants: []ExtensionVariant{{Needs: ResourceNeeds{Memory: "auto"}}}}}
			}
			reason := q.statusWaitingReason(j)
			if !strings.Contains(reason, "memory is sized per hypha") || !strings.Contains(reason, "hypha is available to size it against") || strings.Contains(reason, "0.0 GiB") {
				t.Fatalf("misleading unsized memory: %s", reason)
			}
		})
	}
}
