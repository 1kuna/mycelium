package myce

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A changed server may serve calls, but cannot spend the old server's measured
// concurrency. The matching configuration proves the reference is usable.
func TestModelDeclaredCallMeasurementAuthorityHTTP(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(fmt.Sprintf("changed-server=%v", changed), func(t *testing.T) {
			q, clock := fixture(t)
			ctx, cancel := context.WithTimeout(t.Context(), testWaitTimeout)
			defer cancel()
			model := Model{Name: "bound", Engine: "fake", Path: "synthetic", MemoryGB: 16, HostMemoryGB: 8, MaxInFlight: 2, Platforms: []string{"fake"}}
			digest := ModelMeasurementDigest(model, q.environments)
			e := compositionExtension(t, false)
			e.Variants[0].Target = ExecutionTarget{OS: "linux", Arch: "amd64", Platform: "fake"}
			e.Variants[0].References = map[string]WorkReference{"extension:sample": {InputShape: "bounded-v1", Unit: "work", Units: 10}}
			e.Variants[0].Sweeps = map[string]ReferenceSweep{"extension:sample": {Command: []string{"synthetic-server-reference"}, Counts: []int{1, 2}, MaxConcurrency: 2, ServerDigest: digest}}
			e = freezeComposition(t, e)
			if err := q.AddExtension(e); err != nil {
				t.Fatal(err)
			}
			model.Measurement = &ModelMeasurementBinding{ExtensionDigest: e.Digest, Work: "extension:sample", ServerDigest: digest}
			if changed {
				model.Args = []string{"changed-context"}
			}
			if err := q.ConfigureModels(ModelConfig{Idle: time.Hour, Wait: time.Second, Models: []Model{model}}); err != nil {
				t.Fatal(err)
			}
			m := cpuMachine("bound-host", "bound-host", "fake", 8, 64)
			m.ModelConfigHash = modelConfigHash(q.models)
			m.Devices = []MachineDevice{{ID: "0", Platform: "fake", Identity: "synthetic-card", Fingerprint: "synthetic-v1", ExtensionFingerprint: "synthetic-v1", Selector: "0", MemoryMode: "fixed", TotalMemoryGB: 32, BudgetGB: 32, FreeMemoryGB: 32, MemoryMeasured: true}}
			target := m.Devices[0].machine(m)
			report := newExtensionReport(e, &target, "extension:sample")
			report.Setup, report.Qualified, report.CheckID = true, true, "server-reference"
			m.Devices[0].ExtensionReports = []ExtensionReport{report}
			check(t, q, m, nil)
			target = m.Devices[0].machine(*q.s.Machines[m.ID])
			points := runReferenceSweep(ctx, BenchmarkConfig{Clock: clock, CurveMaxConcurrency: 2, MeasureCurve: func(_ context.Context, _ BenchmarkConfig, _ Extension, _ *Machine, _ string, count int) (CurveMeasurement, error) {
				return CurveMeasurement{Units: 10 * float64(count), Seconds: 1, LatencySeconds: 1, BaselineHostMemoryGB: 1, HostMemoryGB: 2, BaselineDeviceMemoryGB: 1, DeviceMemoryGB: 2}, nil
			}}, e, &target, "extension:sample", curveMonitor(clock, false))
			publishCurves(t, q, m, points)

			started, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			var calls atomic.Int32
			upstream := workerTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				if calls.Add(1) == 1 {
					close(started)
					select {
					case <-release:
					case <-r.Context().Done():
						return
					}
				}
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, `{"answer":"ok"}`)
			}))
			defer upstream.Close()
			defer unblock()
			m.ModelURL = upstream.URL
			check(t, q, m, nil)
			api := API{Queue: q, Token: "test"}.Handler()
			inspect := func() Snapshot {
				req := httptest.NewRequest("GET", "/api/inspection", nil)
				req.Header.Set("Authorization", "Bearer test")
				response := httptest.NewRecorder()
				api.ServeHTTP(response, req)
				var snapshot Snapshot
				if response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &snapshot) != nil {
					t.Fatalf("inspection: %d %s", response.Code, response.Body.String())
				}
				return snapshot
			}
			responses := make(chan *httptest.ResponseRecorder, 2)
			var requests sync.WaitGroup
			requests.Add(2)
			defer func() { cancel(); unblock(); requests.Wait() }()
			post := func() {
				defer requests.Done()
				req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"bound"}`)).WithContext(ctx)
				for k, v := range map[string]string{"Authorization": "Bearer test", "X-Myce-Project": "synthetic", "X-Myce-Priority": "experiment", "X-Myce-Fan-Out": "group", "X-Myce-Input-Shape": "bounded-v1", "X-Myce-Work-Units": "10"} {
					req.Header.Set(k, v)
				}
				response := httptest.NewRecorder()
				api.ServeHTTP(response, req)
				responses <- response
			}
			go post()
			go post()
			var loading ModelSlot
			await(t, func() bool {
				snapshot := inspect()
				if len(snapshot.Jobs) != 2 || len(snapshot.LoadedModels) != 1 {
					return false
				}
				loading = snapshot.LoadedModels[0]
				return snapshot.Jobs[0].State == "queued" && snapshot.Jobs[1].State == "queued" && loading.State == "loading"
			})
			if _, err := q.Checkin(Checkin{Machine: m, ModelReports: []ModelReport{{Name: "bound", Device: "0", Attempt: loading.Attempt, State: "ready"}}}); err != nil {
				t.Fatal(err)
			}
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal("no upstream call")
			}
			if changed {
				clock.Advance(time.Second)
			}
			var second *httptest.ResponseRecorder
			select {
			case second = <-responses:
			case <-ctx.Done():
				t.Fatal("second response stalled")
			}
			if changed {
				if second.Code != 504 || !strings.Contains(second.Body.String(), "maximum model wait exceeded") {
					t.Fatalf("changed server reused reference: %d %s", second.Code, second.Body.String())
				}
			} else if second.Code != 200 || second.Body.String() != `{"answer":"ok"}` {
				t.Fatalf("matching binding unusable: %d %s", second.Code, second.Body.String())
			}
			unblock()
			select {
			case first := <-responses:
				if first.Code != 200 || first.Body.String() != `{"answer":"ok"}` {
					t.Fatalf("active call failed: %d %s", first.Code, first.Body.String())
				}
			case <-ctx.Done():
				t.Fatal("active response stalled")
			}
		})
	}
}
