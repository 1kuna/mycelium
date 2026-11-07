package myce

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestResolveStoredInputBytesAndStaging(t *testing.T) {
	q, _ := fixture(t)
	if err := q.ConfigureFileStore(t.TempDir(), 1<<20, false); err != nil {
		t.Fatal(err)
	}
	client := Client{URL: testServerForEnvironments(t, q), Token: "test"}
	source := filepath.Join(t.TempDir(), "input")
	if err := os.WriteFile(source, []byte("verified input"), 0600); err != nil {
		t.Fatal(err)
	}
	ref, err := IdentifyFile(source)
	if err != nil {
		t.Fatal(err)
	}
	if err = client.UploadJobInput(context.Background(), source, ref); err != nil {
		t.Fatal(err)
	}
	m := cacheCapacityMachine("reader", 1<<20)
	m.GPU = "cpu"
	check(t, q, m, nil)
	req := ResolveRequest{Spec: cpuResolveSpec()}
	req.Spec.Files = FileManifest{"input.txt": ref}
	req.Spec.SharedInputs = map[string]SharedInput{"data": {SHA256: ref.SHA256, SizeBytes: ref.SizeBytes}}
	before := string(wireJSON(t, q.s))
	var report ResolveReport
	resolve := func(state string) {
		await(t, func() bool {
			report = ResolveReport{}
			if err := client.Call(context.Background(), "POST", "/api/resolve", req, &report); err != nil {
				t.Fatal(err)
			}
			if len(report.FileInputs) != 2 {
				return false
			}
			for _, input := range report.FileInputs {
				if input.State != state {
					return false
				}
			}
			return true
		})
	}
	resolve("ready")
	if len(report.Findings) != 0 {
		t.Fatal(report.Findings)
	}
	if after := string(wireJSON(t, q.s)); after != before {
		t.Fatal("stored-byte inspection mutated queue state")
	}
	// Same-sized corrupt bytes must not inherit readiness from a prior resolve.
	if err := os.WriteFile(q.files.blob(ref), []byte("corrupt! input"), 0600); err != nil {
		t.Fatal(err)
	}
	resolve("needs-staging")
	if resolveFinding(report, "files.input.txt") == nil || resolveFinding(report, "shared_inputs.data") == nil {
		t.Fatalf("bad bytes lack action: %+v", report)
	}
	// Submitter placeholders describe future uploads, never fake store identities.
	req.UnstagedInputs = []string{"files/input.txt", "shared/data"}
	resolve("needs-staging")
	if len(report.Findings) != 0 {
		t.Fatalf("local placeholders misreported as corrupt store bytes: %+v", report)
	}
	if err := os.Remove(q.files.blob(ref)); err != nil {
		t.Fatal(err)
	}
	req.UnstagedInputs = nil
	resolve("needs-staging")
	if resolveFinding(report, "files.input.txt") == nil {
		t.Fatal("missing stored file appeared ready")
	}
}

// Hold the real queue entry point beyond the former real-time observation
// deadline. Advancing the injected queue clock cannot exercise context timers.
func TestResolveStoredInputsWaitForVerification(t *testing.T) {
	q, _ := fixture(t)
	if err := q.ConfigureFileStore(t.TempDir(), 1<<20, false); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(t.TempDir(), "input")
	if err := os.WriteFile(source, []byte("small verified input"), 0600); err != nil {
		t.Fatal(err)
	}
	ref, err := IdentifyFile(source)
	if err != nil {
		t.Fatal(err)
	}
	client := Client{URL: testServerForEnvironments(t, q), Token: "test"}
	if err := client.UploadJobInput(context.Background(), source, ref); err != nil {
		t.Fatal(err)
	}
	q.mu.Lock()
	q.submitVerificationSlot = make(chan struct{}, 1)
	slot := q.submitVerificationSlot
	slot <- struct{}{}
	q.mu.Unlock()
	req := ResolveRequest{Spec: cpuResolveSpec()}
	req.Spec.Files = FileManifest{"input": ref}
	var report ResolveReport
	done := make(chan struct{})
	go func() {
		q.resolveFileInputs(context.Background(), req, &report)
		close(done)
	}()
	// Twice the removed budget distinguishes useful waiting from the old
	// silent timeout; this is regression timing, not a product deadline.
	timer := time.NewTimer(2 * localObservationBudget)
	defer timer.Stop()
	select {
	case <-done:
		<-slot
		t.Fatalf("verification returned before contention cleared: %+v", report)
	case <-timer.C:
	}
	// Taking q.mu while verification waits also proves the wait is outside it.
	q.mu.Lock()
	q.mu.Unlock()
	<-slot
	<-done
	if len(report.FileInputs) != 1 || report.FileInputs[0].State != "ready" {
		t.Fatalf("verified input not ready after contention: %+v", report)
	}
}

func TestResolveStoredInputsPreservePartialChecks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	slot := make(chan struct{}, 1)
	inputs := []ResolveFileInput{{State: "verifying"}, {State: "verifying"}, {State: "verifying"}}
	blocked := make(chan struct{})
	release := make(chan struct{})
	returned := make(chan struct{})
	calls := 0
	check := func(context.Context, FileRef) error {
		calls++
		if calls == 1 {
			return nil
		}
		close(blocked)
		<-release // model a syscall that does not obey cancellation
		close(returned)
		return ctx.Err()
	}
	done := make(chan struct{})
	go func() {
		resolveFileChecks(ctx, inputs, []int{0, 1, 2}, check, slot)
		close(done)
	}()
	<-blocked // first result has been published, second is still in I/O
	cancel()
	<-done
	if inputs[0].State != "ready" || inputs[1].State != "verifying" || inputs[2].State != "verifying" {
		t.Fatalf("partial observations lost: %+v", inputs)
	}
	// Cancellation must not free the slot while the fake syscall is blocked.
	select {
	case slot <- struct{}{}:
		t.Fatal("cancelled syscall lost its charged slot")
	default:
	}
	pending := []ResolveFileInput{{State: "verifying"}}
	resolveFileChecks(ctx, pending, []int{0}, check, slot)
	if pending[0].State != "verifying" {
		t.Fatal(pending)
	}
	close(release)
	<-returned
	// Joining via slot acquisition proves the verifier has released custody.
	slot <- struct{}{}
	<-slot
}
