package myce

import (
	"context"
	"errors"
	"net/http"
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
