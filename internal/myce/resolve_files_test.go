package myce

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
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

// #963: a submission holding the shared verification slot made every resolve
// report unknown after the old 25 ms budget. At the real HTTP entry point,
// resolve now waits for the slot and returns verified readiness.
func TestResolveStoredInputsWaitForVerificationSlot(t *testing.T) {
	q, _ := fixture(t)
	if err := q.ConfigureFileStore(t.TempDir(), 1<<20, false); err != nil {
		t.Fatal(err)
	}
	client := Client{URL: testServerForEnvironments(t, q), Token: "test"}
	source := filepath.Join(t.TempDir(), "input")
	if err := os.WriteFile(source, []byte("verified under contention"), 0600); err != nil {
		t.Fatal(err)
	}
	ref, err := IdentifyFile(source)
	if err != nil {
		t.Fatal(err)
	}
	if err = client.UploadJobInput(context.Background(), source, ref); err != nil {
		t.Fatal(err)
	}
	q.mu.Lock()
	if q.submitVerificationSlot == nil {
		q.submitVerificationSlot = make(chan struct{}, 1)
	}
	slot := q.submitVerificationSlot
	q.mu.Unlock()
	slot <- struct{}{} // a submission's verification owns the slot
	req := ResolveRequest{Spec: cpuResolveSpec()}
	req.Spec.Files = FileManifest{"input.txt": ref}
	var report ResolveReport
	done := make(chan error, 1)
	go func() { done <- client.Call(context.Background(), "POST", "/api/resolve", req, &report) }()
	// Four old budgets of real time: regression timing, not a product deadline.
	select {
	case err := <-done:
		t.Fatalf("resolve returned while the slot was held: %v %+v", err, report.FileInputs)
	case <-time.After(4 * localObservationBudget):
	}
	<-slot
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if len(report.FileInputs) != 1 || report.FileInputs[0].State != "ready" {
		t.Fatalf("contended input not verified: %+v", report.FileInputs)
	}
}

func TestResolveFileChecksKeepCompletedResultsAndSlotCustody(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		slot := make(chan struct{}, 1)
		inputs := []ResolveFileInput{{State: "unknown"}, {State: "unknown"}, {State: "unknown"}}
		blocked, release := make(chan struct{}), make(chan struct{})
		calls := 0
		check := func(context.Context, FileRef) error {
			calls++
			if calls == 1 {
				return nil
			}
			close(blocked)
			<-release // a syscall that ignores cancellation
			return nil
		}
		done := make(chan struct{})
		go func() { resolveFileChecks(ctx, inputs, []int{0, 1, 2}, check, slot); close(done) }()
		<-blocked
		cancel()
		<-done
		if inputs[0].State != "ready" || inputs[1].State != "unknown" || inputs[2].State != "unknown" {
			t.Fatalf("completed check lost or abandoned check reported: %+v", inputs)
		}
		select {
		case slot <- struct{}{}:
			t.Fatal("slot released while a read was still outstanding")
		default:
		}
		close(release)
		synctest.Wait()
		select {
		case slot <- struct{}{}:
		default:
			t.Fatal("slot not released after the read returned")
		}
	})
}
