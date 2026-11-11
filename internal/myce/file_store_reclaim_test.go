package myce

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

// #1214: with no queue-wide retention, finished jobs held the store until the
// production reserve left non-production writers zero bytes. The refusal must
// name that cause and its fix; once the project's own retention has passed,
// admission collects the expired files instead of refusing, without a Tick.
func TestFileStoreReclaimsExpiredFilesBeforeRefusing(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	q.fileRetention = 0 // The default: unique bytes stay until release.
	q.files.limit = 24
	q.fileStoreProductionReserve, q.files.productionReserve = 8, 8
	old := quotaResult(t, q, c, "research", "experiment", "AAAAAAAA")
	other := quotaResult(t, q, c, "research", "experiment", "BBBBBBBB")
	_, next := policyInput(t, "CCCCCCCC")
	ctx := WithFileStoreSubmission(context.Background(), Spec{Project: "research", Class: "experiment"})
	files := FileManifest{"input": next}

	err := c.PreflightFileStore(ctx, files)
	for _, want := range []string{"non_production_free=0 bytes", "production reserve=8 bytes", "bound=store_allowance", "finished jobs hold 16 bytes until released", "queue file retention none", "project research file_retention none", "2 finished jobs", "myce file release plan --project research"} {
		assertFilePolicyRefusal(t, err, want)
	}
	clock.Advance(30 * 24 * time.Hour)
	assertFilePolicyRefusal(t, c.PreflightFileStore(ctx, files), "finished jobs hold 16 bytes")
	if _, err := os.Stat(q.files.blob(old.Spec.Files["input"])); err != nil {
		t.Fatal("unique bytes without a retention policy were deleted", err)
	}

	// The project's own retention applies to jobs that finished before it was set.
	setQuotaPolicy(t, q, "research", ProjectPolicy{FileRetention: "1h"})
	if err := c.PreflightFileStore(ctx, files); err != nil {
		t.Fatal("expired files were not reclaimed before refusing", err)
	}
	// Admission expires and frees only what it needs: one 8-byte job of two.
	// Ordinary maintenance expires the other later.
	expired := func() (n int) {
		t.Helper()
		for _, j := range []*Job{old, other} {
			got, err := c.GetJob(context.Background(), j.ID)
			if err != nil {
				t.Fatal(err)
			}
			if got.FilesExpired {
				n++
			}
		}
		return n
	}
	if n := expired(); n != 1 {
		t.Fatalf("admission expired %d jobs, want the 1 it needed", n)
	}
	if status := q.fileStoreStatus(context.Background()); status.UsedBytes != 8 {
		t.Fatalf("store holds %d bytes, want 8", status.UsedBytes)
	}
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if n := expired(); n != 2 {
		t.Fatalf("maintenance left %d of 2 jobs expired", n)
	}
}

// A project's file_retention can only shorten the queue-wide window, and the
// worker scratch and release plan read the same deadline.
func TestProjectFileRetentionShortensQueueWindow(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	j := quotaResult(t, q, c, "research", "experiment", "AAAA")
	setQuotaPolicy(t, q, "research", ProjectPolicy{FileRetention: "1000h"})
	q.mu.Lock()
	if got := q.jobFileRetention(q.s.Jobs[j.ID]); got != testFileRetention {
		t.Errorf("longer project retention widened the queue window: %s", got)
	}
	q.mu.Unlock()
	setQuotaPolicy(t, q, "research", ProjectPolicy{FileRetention: "2h"})
	clock.Advance(2*time.Hour + filePublicationGrace)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	q.mu.Lock()
	expired := q.s.Jobs[j.ID].FilesExpired
	q.mu.Unlock()
	if !expired {
		t.Fatal("project retention did not expire files")
	}
	record := q.PriorityPolicy()
	p := record.Policy.Projects["research"]
	p.FileRetention = "soon"
	record.Policy.Projects["research"] = p
	if _, err := q.SetPriorityPolicy(record.Revision, record.Policy); err == nil || !strings.Contains(err.Error(), "file_retention") {
		t.Fatal("invalid file_retention accepted", err)
	}
}

// Without anything finished to release, the refusal says retention cannot help.
func TestFileStoreRefusalWithNothingReleasable(t *testing.T) {
	q, _ := fixture(t)
	c := retentionClient(t, q)
	q.files.limit = 4
	_, ref := policyInput(t, "too large")
	err := c.PreflightFileStore(context.Background(), FileManifest{"input": ref})
	var he *HTTPError
	if !errors.As(err, &he) || !strings.Contains(he.Message, "finished jobs hold no releasable files") || !strings.Contains(he.Message, "shortfall=") {
		t.Fatal(err)
	}
}
