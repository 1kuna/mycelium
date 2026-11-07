package myce

import (
	"context"
	"fmt"
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

// Both a busy submission slot and a slow first check can exceed the old
// observation budget. Neither should silently erase readiness evidence.
func TestResolveStoredInputsWaitForVerification(t *testing.T) {
	for _, contention := range []bool{true, false} {
		t.Run(fmt.Sprint("slot-contention=", contention), func(t *testing.T) {
			_, clock := fixture(t)
			slot := make(chan struct{}, 1)
			if contention {
				slot <- struct{}{}
			}
			inputs := []ResolveFileInput{{State: "verifying"}, {State: "verifying"}}
			started := make(chan struct{})
			release := make(chan struct{})
			done := make(chan struct{})
			calls := 0
			check := func(context.Context, FileRef) error {
				calls++
				if calls == 1 && !contention {
					close(started)
					<-release
				}
				return nil
			}
			go func() {
				if contention {
					close(started)
				}
				resolveFileChecks(context.Background(), inputs, []int{0, 1}, check, slot)
				close(done)
			}()
			<-started
			clock.Advance(time.Hour)
			if contention {
				<-slot
			} else {
				close(release)
			}
			<-done
			for _, input := range inputs {
				if input.State != "ready" {
					t.Fatalf("verified input = %s", input.State)
				}
			}
		})
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
