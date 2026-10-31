package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// #456: distinct real worker admission checks survive acknowledgement,
// replacement of the assignment envelope, success, and a durable queue reopen.
func TestAdmissionDiagnosticRetained(t *testing.T) {
	for _, subcheck := range []string{"live_host_ram", "aggregate_host_allowance", "live_host_growth", "live_gpu_memory"} {
		t.Run(subcheck, func(t *testing.T) {
			path, root := filepath.Join(t.TempDir(), "queue.db"), t.TempDir()
			clock := &fakeClock{now: time.Date(2026, 10, 31, 0, 0, 0, 0, time.UTC)}
			q, err := Open(path, root, clock)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { q.Close() })
			fakeCheckpointAvailability(t, q, true)
			m := machine("admission", "fake")
			m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 64
			s := spec("experiment")
			s.Needs.HostMemoryGB = 22
			submitted := submit(t, q, s)
			j := check(t, q, m, nil).Job
			if j == nil {
				t.Fatal("missing assignment")
			}
			w := recoveryWorker(t)
			w.Machine, w.Clock = m, clock
			w.HostMemorySource = func(context.Context) (float64, error) { return 21.9, nil }
			store, err := w.openStore()
			if err != nil {
				t.Fatal(err)
			}
			defer store.close()
			var refusal error
			if subcheck == "live_gpu_memory" {
				w.HostMemorySource = func(context.Context) (float64, error) { return 64, nil }
				w.MemorySource = func(context.Context) (float64, error) { return 7, nil }
				p, e := startAfterFixtureInputs(t, w, store, *j)
				if p != nil {
					t.Fatal("started without device memory")
				}
				refusal = e
			} else if subcheck == "live_host_ram" {
				// Exercise the first startContext RAM check: no process/custody.
				p, e := w.startContext(context.Background(), store, *j)
				if p != nil {
					t.Fatal("started without RAM")
				}
				refusal = e
			} else {
				if subcheck == "aggregate_host_allowance" {
					w.Machine.HostMemoryBudgetGB = 21.9
				}
				refusal = localAttemptAdmission(w, nil, *j, nil)
			}
			if !errors.Is(refusal, errWorkerMemory) {
				t.Fatal("no memory refusal", refusal)
			}
			startErr := store.startFailure(*j, launchRefused, refusal)
			live := startFailureOutcome(result(j, false), startErr)
			if live.ExecutionCause == nil || live.ExecutionCause.Admission == nil {
				t.Fatal("missing live detail", live)
			}
			data, err := os.ReadFile(filepath.Join(store.dir, store.state.LaunchOutcome))
			if err != nil {
				t.Fatal(err)
			}
			var facts memberOutcome
			if err := json.Unmarshal(data, &facts); err != nil {
				t.Fatal(err)
			}
			r := classifyAttemptOutcome(result(j, false), attemptCause{memberOutcome: facts})
			if r.ExecutionCause == nil || !r.ExecutionCause.PrelaunchRefused {
				t.Fatal("receipt lost refusal proof", r)
			}
			var logs bytes.Buffer
			old := log.Writer()
			log.SetOutput(&logs)
			defer log.SetOutput(old)
			if err := q.Complete(r); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(filepath.Join(store.dir, store.state.LaunchOutcome)); err != nil {
				t.Fatal(err)
			}
			clock.Advance(time.Second)
			next := check(t, q, m, nil).Job
			if next == nil || next.Attempt != 2 {
				t.Fatal("no retry", next)
			}
			ledgerCustody(t, q, next, "success", "reserved")
			ledgerCustody(t, q, next, "success", "started")
			if err := q.Started(result(next, true)); err != nil {
				t.Fatal(err)
			}
			finish(t, q, next, true)
			if err := q.Close(); err != nil {
				t.Fatal(err)
			}
			q, err = Open(path, root, clock)
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(API{Queue: q, Token: "private-token"}.Handler())
			defer server.Close()
			client := Client{URL: server.URL, Token: "private-token"}
			got, err := client.GetJob(context.Background(), submitted.ID)
			if err != nil {
				t.Fatal(err)
			}
			if got.State != "succeeded" || len(got.AttemptHistory) != 2 {
				t.Fatal("lost history", got)
			}
			d := got.AttemptHistory[0].ExecutionCause.Admission
			required, available := 22.0, 21.9
			if subcheck == "live_gpu_memory" {
				required, available = 8, 7
			}
			if d == nil || d.Subcheck != subcheck || d.Unit != "GiB" || d.Required != required || d.Available == nil || *d.Available != available || d.Unavailable || d.EnvelopeSource != j.MemoryEnvelope.Source || d.EnvelopeHostGB != j.MemoryEnvelope.HostGB || d.EnvelopeDeviceGB != j.MemoryEnvelope.DeviceGB {
				t.Fatalf("lost original comparison: %+v", d)
			}
			var status Snapshot
			if err := client.Call(context.Background(), "GET", "/api/status", nil, &status); err != nil {
				t.Fatal(err)
			}
			public, _ := json.Marshal(status)
			comparison := fmt.Sprintf("%s required=%g available=%g unit=GiB", subcheck, required, available)
			if !strings.Contains(string(public), `"subcheck":"`+subcheck+`"`) || !strings.Contains(logs.String(), comparison) || !strings.Contains(logs.String(), "envelope_source=") {
				t.Fatal("missing status/log detail", string(public), logs.String())
			}
			if strings.Contains(string(public)+logs.String(), "private-token") {
				t.Fatal("secret leaked")
			}
		})
	}
}

func TestAdmissionDiagnosticUnavailableAndPrivacy(t *testing.T) {
	w := Worker{Machine: machine("probe", "fake"), HostMemorySource: func(context.Context) (float64, error) { return 123, errors.New("private-secret/path") }}
	j := Job{Spec: spec("experiment"), MemoryEnvelope: &JobMemoryEnvelope{Source: "private-secret/path", HostGB: 22, DeviceGB: 8}}
	d := admissionDetail(w.checkHostRAM(8, 22), j)
	if d == nil || d.Subcheck != "live_host_ram" || !d.Unavailable || d.Available != nil || d.Required != 22 {
		t.Fatal("probe fabricated availability", d)
	}
	c := safeRuntimeCause(&RuntimeCause{Operation: "launch", Class: "resource_limit", Predicate: "memory_capacity", Admission: d, Reason: "private-secret/path"})
	public, _ := json.Marshal(c)
	if strings.Contains(string(public)+c.String(), "private-secret") {
		t.Fatal("untrusted strings leaked", string(public))
	}
	for _, mutate := range []func(*AdmissionDetail){
		func(d *AdmissionDetail) { d.Subcheck = "private-secret" },
		func(d *AdmissionDetail) { d.Unit = "private-secret" },
		func(d *AdmissionDetail) { d.Required = math.NaN() },
	} {
		bad := *d
		mutate(&bad)
		if safeAdmissionDetail(&bad) != nil {
			t.Fatal("invalid diagnostic accepted")
		}
	}
}
