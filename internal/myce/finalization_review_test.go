package myce

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestReviewNativeExitReceiptFailure(t *testing.T) {
	r := nativeStart(t, nativeSpec(t), "fake")
	nativeFile(t, r.dir, "main-ready")
	blocked := filepath.Join(r.s.dir, "native-"+r.s.state.Identity+".json.tmp")
	if err := os.Mkdir(blocked, 0700); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(blocked)
	nativeTouch(t, r.dir, "finish")
	var f workerFinish
	select {
	case f = <-r.p.done:
	case <-time.After(10 * time.Second):
		t.Fatal("no completion")
	}
	r.finished = true
	if f.err != nil {
		t.Fatal(f.err)
	}
	main := r.p.native.main
	if main.Exit == nil || main.Exit.Status != 0 {
		t.Fatalf("main did not exit zero: %+v", main.Exit)
	}
	if f.result.Success {
		t.Fatal("expected finalization failure")
	}
	if err := r.q.Complete(f.result); err != nil {
		t.Fatal(err)
	}
	got, getErr := r.w.Client.GetJob(context.Background(), r.j.ID)
	if getErr != nil {
		t.Fatal(getErr)
	}
	_, diagErr := DiagnosticFiles(got, got.Attempt)
	t.Logf("exit=%d state=%s reason=%q execution_cause=%+v cleanup_cause=%+v diagnostic_error=%v", main.Exit.Status, got.State, got.Reason, got.ExecutionCause, got.CleanupCause, diagErr)
	if got.ExecutionCause == nil || got.ExecutionCause.Finalization == nil || diagErr != nil {
		t.Error("BUG: exit-zero receipt persistence failure has no finalization detail/summary")
	}
	assertFinalization(t, got, "receipt_persistence", "wrong_type", "")
	if !f.result.ReleasePending || got.CleanupCause == nil {
		t.Fatal("receipt failure lost cleanup custody", got)
	}
	dest := filepath.Join(t.TempDir(), "summary.json")
	if err := r.w.Client.DownloadDiagnostic(context.Background(), got.ID, got.Attempt, "summary.json", dest); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(dest)
	if err != nil {
		t.Fatal(err)
	}
	var summary FailureSummary
	if err := json.Unmarshal(data, &summary); err != nil {
		t.Fatal(err)
	}
	if summary.ExecutionCause == nil || summary.ExecutionCause.Finalization == nil || summary.ExecutionCause.Finalization.Step != "receipt_persistence" || summary.ExecutionCause.Finalization.CommandExitCode == nil || *summary.ExecutionCause.Finalization.CommandExitCode != 0 {
		t.Fatalf("receipt diagnostics lost exit-zero cause: %s", data)
	}
	if strings.Contains(string(data), r.s.dir) || strings.Contains(got.Reason, r.s.dir) {
		t.Fatal("receipt failure exposed worker path")
	}
}

func TestReviewCheckDirectorySpecificCause(t *testing.T) {
	work, dest := t.TempDir(), t.TempDir()
	if err := os.MkdirAll(filepath.Join(work, ".myce-check", "actual"), 0700); err != nil {
		t.Fatal(err)
	}
	j := Job{Spec: Spec{Run: RunSpec{Env: map[string]string{"MYCE_CHECK_OUTPUT": "actual"}}, ExtensionCheck: &ExtensionCheckRequest{Directory: dest}}}
	err := (Worker{}).publishExtensionFiles(j, work)
	if err == nil {
		t.Fatal("directory accepted")
	}
	c := runtimeDiagnostic(err)
	t.Logf("cause=%+v detail=%+v", c, c.Finalization)
	if c.Finalization == nil || c.Finalization.Error != "not_regular" {
		t.Error("BUG: owned regular-file rejection collapsed to unknown I/O")
	}

}

func TestReviewOldQueueKeepsSupportedEvidence(t *testing.T) {
	wire := &resourceWire{}
	wire.supported.Store(true)
	wire.cpuPressure.Store(true)
	wire.executionEvidence.Store(true)
	calls := 0
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set(resourceProtocolHeader, resourceProtocol)
		w.Header().Set(cpuPressureProtocolHeader, cpuPressureProtocol)
		w.Header().Set(executionEvidenceProtocolHeader, executionEvidenceProtocol)
		var got Result
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatal(err)
		}
		if calls == 1 {
			if got.ExecutionCause == nil || got.ExecutionCause.Finalization == nil {
				t.Error("first request omitted finalization")
			}
			http.Error(w, `{"code":"request_invalid","error":"unknown finalization field"}`, 400)
			return
		}
		t.Logf("legacy retry resources_present=%t execution_evidence_present=%t", got.Resources != nil, got.ExecutionEvidence != nil)
		if got.Resources == nil || got.ExecutionEvidence == nil {
			t.Error("BUG: finalization downgrade discarded advertised supported telemetry")
		}
		if got.Resources != nil && got.Resources.CPU.Pressure == nil || got.ExecutionCause == nil || got.ExecutionCause.Finalization != nil {
			t.Error("retry changed supported CPU pressure or retained finalization")
		}
		fmt.Fprint(w, `{"ok":true}`)
	}))
	defer api.Close()
	zero := 0
	cause := &RuntimeCause{Operation: "outputs", Class: "not_regular", Finalization: &FinalizationDetail{Step: "output_validation", Error: "not_regular", CommandExitCode: &zero}}
	result := Result{ExecutionCause: cause, Resources: &AttemptResources{CPU: AttemptCPUTime{Pressure: &IOPressure{}}}, ExecutionEvidence: &ExecutionEvidence{Main: executionMayHaveRun}}
	if err := (Client{URL: api.URL, resourceWire: wire}).Call(context.Background(), "POST", "/api/workers/complete", result, nil); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("calls=%d", calls)
	}
	if !wire.supported.Load() || !wire.cpuPressure.Load() || !wire.executionEvidence.Load() || result.ExecutionCause.Finalization == nil {
		t.Fatal("downgrade changed supported protocols or durable result")
	}
}
