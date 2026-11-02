package myce

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func setQuotaPolicy(t *testing.T, q *Queue, project string, p ProjectPolicy) {
	t.Helper()
	record := q.PriorityPolicy()
	p.Weight = 1
	record.Policy.Projects[project] = p
	if _, err := q.SetPriorityPolicy(record.Revision, record.Policy); err != nil {
		t.Fatal(err)
	}
}

// A completed job whose input and result share one stored digest.
func quotaResult(t *testing.T, q *Queue, c Client, project, class, content string) *Job {
	t.Helper()
	_, ref := retentionJobInput(t, c, content)
	s := spec(class)
	s.Project = project
	s.Files = FileManifest{"input": ref}
	j := submit(t, q, s)
	if err := q.update(func() error {
		j := q.touchJob(j.ID)
		j.State = "succeeded"
		now := q.clock.Now()
		j.FinishedAt = &now
		j.OutputRefs = FileManifest{"result": ref}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return j
}

func projectUsage(t *testing.T, q *Queue, project string) ProjectFileStoreUsage {
	t.Helper()
	for _, u := range q.fileStoreStatus(context.Background()).Projects {
		if u.Project == project {
			return u
		}
	}
	return ProjectFileStoreUsage{Project: project}
}

func submitDeclaring(q *Queue, project, class string, outputBytes uint64) error {
	s := spec(class)
	s.Project = project
	s.Disk = &DiskNeeds{OutputBytes: outputBytes}
	_, err := q.Submit(s)
	return err
}

func assertQuotaRefusal(t *testing.T, err error, want ...string) {
	t.Helper()
	var he *HTTPError
	if !errors.As(err, &he) || he.Code != "project_file_store_quota" || he.Status != http.StatusInsufficientStorage {
		t.Fatalf("want project_file_store_quota, got %v", err)
	}
	for _, w := range want {
		if !strings.Contains(he.Message, w) {
			t.Fatalf("reason %q lacks %q", he.Message, w)
		}
	}
}

func TestProjectFileStoreUsageDeduplicatesDigests(t *testing.T) {
	q, _ := fixture(t)
	c := retentionClient(t, q)
	quotaResult(t, q, c, "owner", "experiment", "shared!")
	quotaResult(t, q, c, "owner", "experiment", "shared!")
	quotaResult(t, q, c, "owner", "experiment", "unique")
	quotaResult(t, q, c, "other", "experiment", "shared!")
	owner, other := projectUsage(t, q, "owner"), projectUsage(t, q, "other")
	if owner.RetainedBytes != 13 || owner.AddedBytes24h != 13 || other.RetainedBytes != 7 || owner.Refusal != "" {
		t.Fatalf("owner=%+v other=%+v", owner, other)
	}
	if used := q.fileStoreStatus(context.Background()).UsedBytes; used != 13 {
		t.Fatalf("shared digest charged twice globally: used=%d", used)
	}
}

func TestProjectFileStoreQuotaRefusesUntilRelease(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	setQuotaPolicy(t, q, "owner", ProjectPolicy{MaxRetainedFileBytes: 10})
	first := quotaResult(t, q, c, "owner", "experiment", "abcdef")
	assertQuotaRefusal(t, submitDeclaring(q, "owner", "experiment", 5), "retained=11", "max_retained_file_bytes=10", "myce file release plan --project owner", "own storage")
	if err := submitDeclaring(q, "other", "experiment", 5); err != nil {
		t.Fatal("unlimited project refused", err)
	}
	if err := submitDeclaring(q, "owner", "production", 5); err != nil {
		t.Fatal("production refused by project quota", err)
	}
	// An accepted result can overshoot; the project is refused afterwards.
	second := quotaResult(t, q, c, "owner", "production", "123456789")
	if u := projectUsage(t, q, "owner"); u.RetainedBytes != 15 || !strings.Contains(u.Refusal, "retained=15") {
		t.Fatalf("usage: %+v", u)
	}
	assertQuotaRefusal(t, submitDeclaring(q, "owner", "experiment", 0), "retained=15")

	root := q.files.root
	q = reopenTierQueue(t, q, clock)
	assertQuotaRefusal(t, submitDeclaring(q, "owner", "experiment", 0), "retained=15")
	if err := q.ConfigureFileStore(root, 1<<20, true); err != nil {
		t.Fatal(err)
	}
	clock.Advance(filePublicationGrace + time.Second)
	selection := FileReleaseSelection{Project: "owner", Jobs: []string{first.ID, second.ID}}
	if _, err := releaseApply(q, releasePlan(t, q, selection)); err != nil {
		t.Fatal(err)
	}
	if u := projectUsage(t, q, "owner"); u.RetainedBytes != 0 || u.Refusal != "" {
		t.Fatalf("release did not drop retained usage: %+v", u)
	}
	if err := submitDeclaring(q, "owner", "experiment", 5); err != nil {
		t.Fatal("released project still refused", err)
	}
}

func TestProjectFileStoreDailyQuotaRollingWindow(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	setQuotaPolicy(t, q, "owner", ProjectPolicy{MaxAddedFileBytes24h: 10})
	j := quotaResult(t, q, c, "owner", "experiment", "abcdefgh")
	clock.Advance(filePublicationGrace + time.Second)
	// Releasing results frees retained bytes but does not refund the allowance.
	if _, err := releaseApply(q, releasePlan(t, q, FileReleaseSelection{Project: "owner", Jobs: []string{j.ID}})); err != nil {
		t.Fatal(err)
	}
	assertQuotaRefusal(t, submitDeclaring(q, "owner", "experiment", 3), "added in the last 24h=11", "max_added_file_bytes_24h=10")
	clock.Advance(fileStoreQuotaWindow - filePublicationGrace - 2*time.Second)
	assertQuotaRefusal(t, submitDeclaring(q, "owner", "experiment", 3), "added in the last 24h=11")
	clock.Advance(time.Second)
	if err := submitDeclaring(q, "owner", "experiment", 3); err != nil {
		t.Fatal("addition older than 24h still counted", err)
	}
}

// Over HTTP, inputs named for an over-quota project are refused before they
// upload, at preflight and upload creation, and submit keeps the agent's reason.
// Production and unnamed uploads pass.
func TestProjectFileStoreQuotaRefusesInputUploads(t *testing.T) {
	q, _ := fixture(t)
	c := retentionClient(t, q)
	setQuotaPolicy(t, q, "owner", ProjectPolicy{MaxRetainedFileBytes: 10})
	quotaResult(t, q, c, "owner", "experiment", "abcdefgh")
	path := filepath.Join(t.TempDir(), "next")
	if err := os.WriteFile(path, []byte("0123"), 0600); err != nil {
		t.Fatal(err)
	}
	ref, err := IdentifyFile(path)
	if err != nil {
		t.Fatal(err)
	}
	files := FileManifest{"next": ref}
	owner := WithFileStoreSubmission(context.Background(), Spec{Project: "owner"})
	assertQuotaRefusal(t, c.PreflightFileStore(owner, files), "retained=12")
	assertQuotaRefusal(t, c.UploadJobInput(owner, path, ref), "retained=12")
	over := spec("experiment")
	over.Project, over.Disk = "owner", &DiskNeeds{OutputBytes: 5}
	_, err = c.Submit(context.Background(), over)
	assertQuotaRefusal(t, err, "retained=13", "myce file release plan")
	production := WithFileStoreSubmission(context.Background(), Spec{Project: "owner", Class: "production"})
	if err := c.PreflightFileStore(production, files); err != nil {
		t.Fatal("production preflight refused by project quota", err)
	}
	if err := c.UploadJobInput(context.Background(), path, ref); err != nil {
		t.Fatal("upload without a project refused by project quota", err)
	}
}

// An older CLI's get/set round-trip cannot carry the limit fields; it must not
// clear them. A current CLI marks its request, so its file can clear a limit.
func TestPolicySetFromOlderClientKeepsFileStoreLimits(t *testing.T) {
	q, _ := fixture(t)
	c := retentionClient(t, q)
	setQuotaPolicy(t, q, "owner", ProjectPolicy{MaxRetainedFileBytes: 10, MaxAddedFileBytes24h: 20})
	put := func(path string) {
		t.Helper()
		record := q.PriorityPolicy()
		owner := record.Policy.Projects["owner"]
		owner.MaxRetainedFileBytes, owner.MaxAddedFileBytes24h, owner.Weight = 0, 0, 2
		record.Policy.Projects["owner"] = owner
		request := map[string]any{"expected_revision": record.Revision, "policy": record.Policy}
		if err := c.Call(context.Background(), "PUT", path, request, nil); err != nil {
			t.Fatal(err)
		}
	}
	put("/api/policy")
	if p := q.s.Policies["owner"]; p.MaxRetainedFileBytes != 10 || p.MaxAddedFileBytes24h != 20 || p.Weight != 2 {
		t.Fatalf("older client round-trip changed limits or lost its edit: %+v", p)
	}
	put("/api/policy?file_store_limits=1")
	if p := q.s.Policies["owner"]; p.MaxRetainedFileBytes != 0 || p.MaxAddedFileBytes24h != 0 {
		t.Fatalf("current client could not clear limits: %+v", p)
	}
}

func benchDigest(s string) string { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:]) }

// About 10k jobs across 6 projects: inputs shared within a project, unique results.
func quotaBenchQueue(b *testing.B, n int) *Queue {
	b.Helper()
	clock := &fakeClock{now: time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)}
	q, err := Open(filepath.Join(b.TempDir(), "queue.db"), b.TempDir(), clock)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { q.Close() })
	q.s.Submissions = map[string]*submissionRecord{}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("job-%05d", i)
		project := fmt.Sprintf("p%d", i%6)
		finished := clock.Now().Add(-time.Duration(i) * time.Minute)
		j := &Job{ID: id, State: "succeeded", Queued: finished.Add(-time.Minute), FinishedAt: &finished,
			Spec: Spec{Name: "bench", Project: project, Needs: ResourceNeeds{Platform: Platform{Any: true}},
				Files: FileManifest{"a": {SHA256: benchDigest(project + fmt.Sprint(i%50)), SizeBytes: 1 << 20}, "b": {SHA256: benchDigest(id + "in"), SizeBytes: 4096}}},
			OutputRefs: FileManifest{"result": {SHA256: benchDigest(id + "out"), SizeBytes: 1 << 16}}}
		if i%5 == 0 {
			j.FilesExpired = true
		}
		if i%10 == 1 {
			j.State, j.FinishedAt = "queued", nil
		}
		q.s.Jobs[id] = j
		q.s.Submissions[id] = &submissionRecord{Kind: submissionBinding, JobID: id, Job: j}
	}
	return q
}

// Status holds q.mu for this whole capture (#80, #527); file-store accounting
// must not add a second reference walk to it.
func BenchmarkStatusCaptureTenK(b *testing.B) {
	q := quotaBenchQueue(b, 10000)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, _, err := q.captureStatusObservation("", nil); err != nil {
			b.Fatal(err)
		}
	}
}

// A limited project's submit check walks only that project's jobs under q.mu.
func BenchmarkQuotaSubmitCheckTenK(b *testing.B) {
	q := quotaBenchQueue(b, 10000)
	q.s.Policies = map[string]ProjectPolicy{"p0": {Weight: 1, MaxRetainedFileBytes: 1 << 50}}
	s := Spec{Project: "p0", Class: "experiment"}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		q.mu.Lock()
		if err := q.fileStoreQuotaRefusal(s, nil, 0); err != nil {
			b.Fatal(err)
		}
		q.mu.Unlock()
	}
}
