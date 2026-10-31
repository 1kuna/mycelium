package myce

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func policyInput(t *testing.T, content string) (string, FileRef) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "input")
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	ref, err := IdentifyFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return path, ref
}
func assertFilePolicyRefusal(t *testing.T, err error, reason string) {
	t.Helper()
	var he *HTTPError
	if !errors.As(err, &he) || he.Status != http.StatusInsufficientStorage || he.Code != "file_store_capacity" || !strings.Contains(he.Message, reason) {
		t.Fatalf("wanted capacity reason %q; got %v", reason, err)
	}
}
func TestFileStoreProductionReserve(t *testing.T) {
	q, _ := fixture(t)
	c := retentionClient(t, q)
	q.files.limit = 12
	q.fileStoreProductionReserve, q.files.productionReserve = 6, 6
	experiment := WithFileStoreSubmission(context.Background(), Spec{Project: "research", Class: "experiment"})
	policy := q.PriorityPolicy()
	policy.Policy.Tiers[0].Selectors = []string{"service/customer"}
	if _, err := q.SetPriorityPolicy(policy.Revision, policy.Policy); err != nil {
		t.Fatal(err)
	}
	production := WithFileStoreSubmission(context.Background(), Spec{Project: "service", Class: "customer"})
	path, ref := policyInput(t, "123456")
	if err := c.UploadJobInput(experiment, path, ref); err != nil {
		t.Fatal(err)
	}
	path, ref = policyInput(t, "next")
	files := FileManifest{"input": ref}
	assertFilePolicyRefusal(t, c.PreflightFileStore(experiment, files), "production reserve=6 bytes")
	assertFilePolicyRefusal(t, c.UploadJobInput(experiment, path, ref), "production reserve=6 bytes")
	// Unscoped legacy/import uploads cannot claim the production reserve.
	assertFilePolicyRefusal(t, c.UploadFile(context.Background(), path, ref), "production reserve=6 bytes")
	if err := c.PreflightFileStore(production, files); err != nil {
		t.Fatal(err)
	}
	if err := c.UploadJobInput(production, path, ref); err != nil {
		t.Fatal(err)
	}
	if err := c.UploadJobInput(experiment, path, ref); err != nil {
		t.Fatal("deduplicated reuse must succeed", err)
	}
	status := q.fileStoreStatus(context.Background())
	if status.UsedBytes != 10 || status.ProductionReserveBytes != 6 {
		t.Fatalf("status: %+v", status)
	}
}
func TestFileStorePolicyDefaultsAndConcurrentUploads(t *testing.T) {
	q, _ := fixture(t)
	c := retentionClient(t, q)
	q.files.limit = 8
	path, ref := policyInput(t, "12345678")
	ctx := WithFileStoreSubmission(context.Background(), Spec{Project: "research"})
	if err := c.UploadJobInput(ctx, path, ref); err != nil {
		t.Fatal("unconfigured policy changed admission", err)
	}
	// Only one of two concurrent new uploads can consume the ordinary half.
	q.files.limit = 16
	q.fileStoreProductionReserve, q.files.productionReserve = 4, 4
	a, ar := policyInput(t, "abcd")
	b, br := policyInput(t, "efgh")
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, input := range []struct {
		path string
		ref  FileRef
	}{{a, ar}, {b, br}} {
		wg.Add(1)
		go func(path string, ref FileRef) { defer wg.Done(); errs <- c.UploadJobInput(ctx, path, ref) }(input.path, input.ref)
	}
	wg.Wait()
	close(errs)
	successes := 0
	for err := range errs {
		if err == nil {
			successes++
		} else {
			assertFilePolicyRefusal(t, err, "production reserve=4 bytes")
		}
	}
	if successes != 1 {
		t.Fatalf("admitted %d uploads", successes)
	}
}
func TestFileStoreProductionOutputReservation(t *testing.T) {
	q, _ := fixture(t)
	c := deliveryClient(t, q)
	q.files.limit = 16
	q.deliveryRoot = q.root
	q.fileStoreProductionReserve, q.files.productionReserve = 8, 8
	s := asyncDeliverySpec()
	s.Project = "service"
	s.Class = "production"
	s.Priority = "production"
	s.Disk.OutputBytes = 8
	m := machine("publisher", "fake")
	check(t, q, m, nil)
	submitted := submit(t, q, s)
	assigned := check(t, q, m, nil).Job
	if assigned == nil || assigned.ID != submitted.ID {
		t.Fatal("not assigned")
	}
	// Its production estimate already backs the reserve, leaving eight bytes for others.
	path, ref := policyInput(t, "12345678")
	if err := c.UploadJobInput(context.Background(), path, ref); err != nil {
		t.Fatal("reserve double charged", err)
	}
	assigned.Spec.Run.Workdir = t.TempDir()
	if err := os.WriteFile(filepath.Join(assigned.Spec.Run.Workdir, "out.txt"), []byte("result!!"), 0600); err != nil {
		t.Fatal(err)
	}
	result, err := (Worker{Client: c}).finishFileResult(*assigned, result(assigned, true), t.TempDir())
	if err != nil {
		t.Fatal("production result inside reserve", err)
	}
	if err := q.Complete(result); err != nil {
		t.Fatal(err)
	}
	if len(result.OutputRefs) != 1 {
		t.Fatal("missing result")
	}
	status := q.fileStoreStatus(context.Background())
	if status.FreeBytes != 0 || status.UsedBytes != 16 || status.ReservedOutputBytes != 0 {
		t.Fatalf("bad reservation accounting: %+v", status)
	}
}
