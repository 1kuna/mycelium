package myce

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func cpuResolveSpec() Spec {
	return Spec{Name: "resolve", Project: "test", Priority: "experiment", Run: RunSpec{Command: []string{"sh", "run.sh"}}, Needs: ResourceNeeds{CPUCores: 1, MemoryGB: 1, Platform: Platform{Required: []string{"cpu"}}}}
}

func resolveFinding(r ResolveReport, field string) *ResolveFinding {
	for i := range r.Findings {
		if r.Findings[i].Field == field {
			return &r.Findings[i]
		}
	}
	return nil
}

// A locked CPU job with a staged input resolves on two dev workers whose
// local state roots differ, through the real API, without changing the queue.
// Resolve is not a permit: submission still verifies the staged bytes.
func TestResolveLockedCPUJobWithStagedInputOnTwoRoots(t *testing.T) {
	root := t.TempDir()
	key := strings.Repeat("c", 64)
	catalog := EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"tools": {"cpu": {Key: key, Backend: "uv", LockFile: "requirements.txt", Python: "3.12", Files: map[string][]byte{"requirements.txt": []byte("# locked fixture\n")}}}}}
	d, err := StartDev(DevConfig{EnvironmentCacheLimit: 1 << 30, Workers: 2, GPU: "cpu", Address: "127.0.0.1:0", Database: filepath.Join(root, "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"), FileStore: filepath.Join(root, "store"), Token: "dev-resolve-token", Interval: 10 * time.Millisecond, EnvironmentConfig: catalog, Environments: []string{key}})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	client := Client{URL: d.URL, Token: "dev-resolve-token"}
	s := cpuResolveSpec()
	s.Environment = Environment{Name: "tools"}
	s.Files = FileManifest{"run.sh": {SHA256: strings.Repeat("d", 64), SizeBytes: 12}, "data.txt": {SHA256: strings.Repeat("e", 64), SizeBytes: 5}}
	var r ResolveReport
	await(t, func() bool {
		r = ResolveReport{}
		if err := client.Call(context.Background(), "POST", "/api/resolve", ResolveRequest{Spec: s}, &r); err != nil {
			t.Fatal(err)
		}
		if len(r.Targets) != 2 || len(r.FileInputs) != 2 || r.FileInputs[0].State != "needs-staging" || r.FileInputs[1].State != "needs-staging" {
			return false
		}
		for _, target := range r.Targets {
			if !target.Eligible {
				return false
			}
		}
		return true
	})
	for _, target := range r.Targets {
		if !target.Eligible || !strings.Contains(target.Reason, "environment prepared") || target.Target.Platform != "cpu" {
			t.Fatalf("target not eligible with prepared environment: %+v", r)
		}
	}
	// These declared identities intentionally have no uploaded bytes. #548
	// requires needs-staging evidence rather than silently accepting the gap.
	if len(r.FileInputs) != 2 || r.FileInputs[0].State != "needs-staging" || r.FileInputs[1].State != "needs-staging" || resolveFinding(r, "files.run.sh") == nil || resolveFinding(r, "files.data.txt") == nil || !strings.Contains(strings.Join(r.Resolved, " "), "environment tools: queue library recipes for cpu") {
		t.Fatalf("staged locked job reported gaps: %+v", r)
	}
	d.Queue.mu.Lock()
	jobs, reservations := len(d.Queue.s.Jobs), len(d.Queue.s.Reservations)
	d.Queue.mu.Unlock()
	if jobs+reservations != 0 {
		t.Fatalf("resolve created queue state: jobs=%d reservations=%d", jobs, reservations)
	}
	var h *HTTPError
	if _, err := client.Submit(context.Background(), s); !errors.As(err, &h) || h.Status != 409 || h.Code != "input_blob_missing" || !strings.Contains(h.Message, "operation=submit_verify error=ENOENT") {
		t.Fatalf("submission accepted unstaged bytes after resolve: %v", err)
	}
}

// The field case: a shared interactive host refused an unprepared environment
// with only "shared host profile prevents execution".

// A pin that only exists because the data sits on one host is flagged with
// the staging alternative; an authored host requirement is respected.

// Review regression: resolve must resolve a registered extension as submit
// does, not report every target as lacking a qualified variant.
