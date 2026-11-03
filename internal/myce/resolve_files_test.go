package myce

import (
	"context"
	"os"
	"path/filepath"
	"testing"
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
