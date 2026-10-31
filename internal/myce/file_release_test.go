package myce

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"
)

func releaseTerminal(t *testing.T, q *Queue, c Client, content string) (*Job, Spec, FileRef) {
	t.Helper()
	_, ref := retentionJobInput(t, c, content)
	s := spec("experiment")
	s.Project = "owner"
	s.SubmissionKey = fmt.Sprintf("%s-%d", content, len(q.s.Jobs))
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
	return j, s, ref
}

func releaseSelection(jobs ...*Job) FileReleaseSelection {
	s := FileReleaseSelection{Project: "owner"}
	for _, j := range jobs {
		s.Jobs = append(s.Jobs, j.ID)
	}
	return s
}
func releasePlan(t *testing.T, q *Queue, s FileReleaseSelection) FileReleasePlan {
	t.Helper()
	p, e := q.PlanFileRelease(context.Background(), s)
	if e != nil {
		t.Fatal(e)
	}
	return p
}
func releaseApply(q *Queue, p FileReleasePlan) (FileReleaseResult, error) {
	return q.ApplyFileRelease(context.Background(), FileReleaseApply{Selection: p.Selection, PlanID: p.ID, AcceptExpiry: true})
}
func releaseCode(t *testing.T, err error, code string) {
	t.Helper()
	var he *HTTPError
	if !errors.As(err, &he) || he.Code != code {
		t.Fatalf("want %s, got %v", code, err)
	}
}

func TestFileReleasePlanReclaimReplayAndKeyedAnswer(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	j, s, ref := releaseTerminal(t, q, c, "unique")
	clock.Advance(filePublicationGrace + time.Second)
	before, _ := os.Stat(q.files.blob(ref))
	p := releasePlan(t, q, releaseSelection(j))
	if p.ReclaimableBytes != ref.SizeBytes || len(p.Blobs) != 1 || len(p.Jobs[0].Files) != 2 || p.Jobs[0].FilesExpired || p.Jobs[0].ExpiresAt == nil {
		t.Fatalf("bad plan: %+v", p)
	}
	if job(t, q, j.ID).FilesExpired || fileReleaseEntry(job(t, q, j.ID), p.ID) != nil {
		t.Fatal("plan mutated job")
	}
	after, _ := os.Stat(q.files.blob(ref))
	if !after.ModTime().Equal(before.ModTime()) {
		t.Fatal("plan refreshed content")
	}
	if again := releasePlan(t, q, p.Selection); again.ID != p.ID {
		t.Fatal("read-only replay changed plan")
	}
	_, err := q.ApplyFileRelease(context.Background(), FileReleaseApply{Selection: p.Selection, PlanID: p.ID})
	if err == nil {
		t.Fatal("implicit expiry accepted")
	}
	first, err := releaseApply(q, p)
	if err != nil {
		t.Fatal(err)
	}
	if first.Collection != "scheduled" || !first.AcceptedAt.Equal(clock.Now()) {
		t.Fatal(first)
	}
	// Expiry uses normal asynchronous maintenance; the payload remains until then.
	if _, err = os.Stat(q.files.blob(ref)); err != nil {
		t.Fatal(err)
	}
	if err = q.reclaimPublishedFiles(); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(q.files.blob(ref)); !os.IsNotExist(err) {
		t.Fatal("not reclaimed", err)
	}
	if err = c.DownloadFile(context.Background(), ref, filepath.Join(t.TempDir(), "gone")); !isResultExpired(err) {
		t.Fatal("typed expiry lost", err)
	}
	got := submit(t, q, s)
	if got.ID != j.ID || !got.FilesExpired || fileReleaseEntry(got, p.ID) == nil || len(got.OutputRefs) != 1 {
		t.Fatal("key identity/manifests lost", got)
	}
	clock.Advance(time.Hour)
	second, err := releaseApply(q, p)
	if err != nil || second != first {
		t.Fatal("apply replay changed receipt", second, err)
	}
	// The same durable receipt must work after closing/reopening SQLite.
	var dbSequence int
	var dbName, path string
	if err = q.db.QueryRow("PRAGMA database_list").Scan(&dbSequence, &dbName, &path); err != nil {
		t.Fatal(err)
	}
	root := q.root
	if err = q.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	second, err = releaseApply(reopened, p)
	if err != nil || second != first {
		t.Fatal("restart replay", second, err)
	}
}

func TestFileReleaseSharedReferencesAndWholeSelection(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	a, _, ref := releaseTerminal(t, q, c, "shared")
	b, _, _ := releaseTerminal(t, q, c, "shared")
	clock.Advance(filePublicationGrace + time.Second)
	p := releasePlan(t, q, releaseSelection(a))
	if p.ReclaimableBytes != 0 || len(p.Blobs[0].References) == 0 || p.Blobs[0].References[0].JobID != b.ID {
		t.Fatal("shared owner omitted", p)
	}
	all := releasePlan(t, q, releaseSelection(a, b))
	if all.ReclaimableBytes != ref.SizeBytes {
		t.Fatal("shared digest double charged", all)
	}
	// A different project's retained reference always wins, even for a terminal job.
	if err := q.update(func() error { q.touchJob(b.ID).Spec.Project = "other"; return nil }); err != nil {
		t.Fatal(err)
	}
	p = releasePlan(t, q, releaseSelection(a))
	if p.ReclaimableBytes != 0 || p.Blobs[0].References[0].Project != "other" {
		t.Fatal(p)
	}
	if _, err := releaseApply(q, p); err != nil {
		t.Fatal(err)
	}
	if err := q.reclaimPublishedFiles(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(q.files.blob(ref)); err != nil {
		t.Fatal("other project's bytes deleted", err)
	}
	if _, err := q.PlanFileRelease(context.Background(), releaseSelection(b)); err == nil {
		t.Fatal("wrong project selected")
	}
}

func TestFileReleaseProtectedCustodyAndDelivery(t *testing.T) {
	for _, kind := range []string{"live", "current_custody", "old_copy", "host_release", "pending_delivery", "missing_receipt", "invalid_receipt"} {
		t.Run(kind, func(t *testing.T) {
			q, clock := fixture(t)
			c := retentionClient(t, q)
			j, _, ref := releaseTerminal(t, q, c, kind)
			if err := q.update(func() error {
				j := q.touchJob(j.ID)
				switch kind {
				case "live":
					j.State = "running"
				case "current_custody":
					j.Attempt = 1
				case "old_copy":
					j.Copies = []Copy{{Attempt: 1}}
				case "host_release":
					q.s.HostReleases = map[string]HostRelease{"pending": {Job: j.ID}}
				case "pending_delivery":
					j.Delivery = &OutputDelivery{State: "pending"}
				case "missing_receipt":
					j.Spec.OutputDelivery = asyncNAS
				case "invalid_receipt":
					j.Delivery = &OutputDelivery{State: "verified"}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			clock.Advance(filePublicationGrace + time.Second)
			p := releasePlan(t, q, releaseSelection(j))
			if len(p.Jobs[0].Blockers) == 0 {
				t.Fatal("unprotected", p)
			}
			_, err := releaseApply(q, p)
			releaseCode(t, err, "file_release_protected")
			if err = q.reclaimPublishedFiles(); err != nil {
				t.Fatal(err)
			}
			if _, err = os.Stat(q.files.blob(ref)); err != nil {
				t.Fatal("protected bytes removed", err)
			}
		})
	}
}

func TestFileReleaseVerifiedDelivery(t *testing.T) {
	q, clock := fixture(t)
	j := deliveryAssignment(t, q)
	r := deliveryResult(t, q, j, "preserved output")
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	accepted := awaitDeliveryState(t, q, j.ID, "verified")
	clock.Advance(filePublicationGrace + time.Second)
	s := FileReleaseSelection{Project: accepted.Spec.Project, Jobs: []string{j.ID}}
	p := releasePlan(t, q, s)
	if len(p.Jobs[0].Blockers) > 0 || p.Jobs[0].DeliveryVerifiedAt == nil || p.ReclaimableBytes != r.OutputRefs["out.txt"].SizeBytes {
		t.Fatal(p)
	}
	if _, err := releaseApply(q, p); err != nil {
		t.Fatal(err)
	}
	if err := q.reclaimPublishedFiles(); err != nil {
		t.Fatal(err)
	}
	// Verify the supported destination receipt/content still exists after release.
	root, err := os.OpenRoot(q.deliveryRoot)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	ref := r.OutputRefs["out.txt"]
	receipt := deliveryReceipt{DestinationID: accepted.Delivery.DestinationID, Execution: accepted.Delivery.Execution, Machine: accepted.Machine, Session: accepted.Session, Policy: asyncNAS + "/1", ManifestSHA256: accepted.Delivery.ManifestSHA256, Files: FileManifest{"out.txt": ref}}
	if err = verifyDeliveryDirectory(root, filepath.Join("results", j.ID, "1"), receipt); err != nil {
		t.Fatal(err)
	}
}

func TestFileReleaseStalePlanAndPublicationGrace(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	j, _, ref := releaseTerminal(t, q, c, "stale")
	fresh := releasePlan(t, q, releaseSelection(j))
	if fresh.ReclaimableBytes != 0 || !fresh.Blobs[0].PublicationProtected {
		t.Fatal("grace omitted", fresh)
	}
	clock.Advance(filePublicationGrace + time.Second)
	_, err := releaseApply(q, fresh)
	releaseCode(t, err, "file_release_stale")
	p := releasePlan(t, q, fresh.Selection)
	other := spec("experiment")
	other.Project = "other"
	other.Files = FileManifest{"new": ref}
	submit(t, q, other)
	_, err = releaseApply(q, p)
	releaseCode(t, err, "file_release_stale")
	if job(t, q, j.ID).FilesExpired {
		t.Fatal("stale plan mutated expiry")
	}
	p = releasePlan(t, q, fresh.Selection)
	if err = c.HasFile(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	_, err = releaseApply(q, p)
	releaseCode(t, err, "file_release_stale")
	// Freshly reviewed zero-byte plans can expire selected identities while other
	// references/publication grace keep the shared bytes safe.
	p = releasePlan(t, q, fresh.Selection)
	if _, err = releaseApply(q, p); err != nil {
		t.Fatal(err)
	}
	if err = q.reclaimPublishedFiles(); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(q.files.blob(ref)); err != nil {
		t.Fatal(err)
	}
}

func TestFileReleaseRegistryCheckpointAndBundleProtection(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	j, _, ref := releaseTerminal(t, q, c, "protected-assets")
	if err := q.update(func() error {
		j := q.touchJob(j.ID)
		j.FileCheckpoints = map[int]FileCheckpoint{1: {Files: FileManifest{"save": ref}}}
		j.ResumeFiles = &FileCheckpoint{Files: FileManifest{"resume": ref}}
		q.s.Extensions = map[string]Extension{"registered": {Assets: FileManifest{"asset": ref}, FilesRef: &ref}}
		q.environments.Environments = map[string]map[string]EnvironmentRecipe{"registered": {"cpu": {Assets: FileManifest{"asset": ref}}}}
		q.models.Models = append(q.models.Models, Model{SHA256: ref.SHA256})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(filePublicationGrace + time.Second)
	p := releasePlan(t, q, releaseSelection(j))
	kinds := []string{}
	for _, r := range p.Blobs[0].References {
		kinds = append(kinds, r.Kind)
	}
	for _, kind := range []string{"checkpoint", "resume_checkpoint", "extension_asset", "extension_environment_bundle", "environment_asset", "model_catalog"} {
		if !slices.Contains(kinds, kind) {
			t.Fatal("missing protection", kind, p)
		}
	}
	if p.ReclaimableBytes != 0 {
		t.Fatal(p)
	}
	if _, err := releaseApply(q, p); err != nil {
		t.Fatal(err)
	}
	if err := q.reclaimPublishedFiles(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(q.files.blob(ref)); err != nil {
		t.Fatal(err)
	}
}

func TestFileReleaseAPI(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	j, _, _ := releaseTerminal(t, q, c, "api")
	clock.Advance(filePublicationGrace + time.Second)
	api := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer api.Close()
	c = Client{URL: api.URL, Token: "test"}
	var plan FileReleasePlan
	if err := c.Call(context.Background(), "POST", "/api/files/release/plan", releaseSelection(j), &plan); err != nil {
		t.Fatal(err)
	}
	var got FileReleaseResult
	req := FileReleaseApply{Selection: plan.Selection, PlanID: plan.ID, AcceptExpiry: true}
	if err := c.Call(context.Background(), "POST", "/api/files/release/apply", req, &got); err != nil {
		t.Fatal(err)
	}
	if got.PlanID != plan.ID {
		t.Fatal(got)
	}
}

func TestFileReleaseReferenceChangeDuringCommit(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	j, _, ref := releaseTerminal(t, q, c, "concurrent")
	clock.Advance(filePublicationGrace + time.Second)
	p := releasePlan(t, q, releaseSelection(j))
	// The existing checkpoint probe runs after plan inspection and before the
	// commit lock. Hold it to publish another owner in that exact interval.
	entered, unblock := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	q.checkpointProbe = func() bool {
		if calls.Add(1) == 1 {
			close(entered)
			<-unblock
		}
		return true
	}
	done := make(chan error, 1)
	go func() { _, err := releaseApply(q, p); done <- err }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("apply did not reach commit")
	}
	other := spec("experiment")
	other.Project = "other"
	other.Files = FileManifest{"shared": ref}
	submit(t, q, other) // Also proves control/reference publication owns no bulk lock.
	close(unblock)
	select {
	case err := <-done:
		releaseCode(t, err, "file_release_stale")
	case <-time.After(3 * time.Second):
		t.Fatal("apply stalled")
	}
	if job(t, q, j.ID).FilesExpired {
		t.Fatal("newly protected owner expired")
	}
}

func TestFileReleaseWholeGroupAndUnsuccessfulSelection(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	a, _, ref := releaseTerminal(t, q, c, "group")
	b, _, _ := releaseTerminal(t, q, c, "group")
	unselected, _, kept := releaseTerminal(t, q, c, "incomplete")
	if err := q.update(func() error {
		q.touchJob(a.ID).State = "failed"
		q.touchJob(unselected.ID).State = "failed"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(filePublicationGrace + time.Second)
	p := releasePlan(t, q, releaseSelection(a, b))
	if p.ReclaimableBytes != ref.SizeBytes {
		t.Fatal(p)
	}
	if _, err := releaseApply(q, p); err != nil {
		t.Fatal(err)
	}
	if err := q.reclaimPublishedFiles(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(q.files.blob(ref)); !os.IsNotExist(err) {
		t.Fatal("group not reclaimed", err)
	}
	if job(t, q, unselected.ID).FilesExpired {
		t.Fatal("unsuccessful unselected work expired")
	}
	if _, err := os.Stat(q.files.blob(kept)); err != nil {
		t.Fatal(err)
	}
}

func TestFileReleaseProtectsExpiredCustodyAndRecoveredResults(t *testing.T) {
	for _, kind := range []string{"expired_custody", "recovered_result", "recent_bundle", "refreshed_bundle"} {
		t.Run(kind, func(t *testing.T) {
			q, clock := fixture(t)
			c := retentionClient(t, q)
			selected, _, ref := releaseTerminal(t, q, c, kind)
			other, _, _ := releaseTerminal(t, q, c, kind)
			if err := q.update(func() error {
				other := q.touchJob(other.ID)
				// The ordinary owner is already expired, so only the protected class can
				// keep these bytes. It must still block early release by another owner.
				other.FilesExpired = true
				enrollFileReclamation(other)
				switch kind {
				case "expired_custody":
					other.Attempt = 1
				case "recovered_result":
					other.RecoveredResults = []RecoveredResult{{RecoveryRequest: RecoveryRequest{Files: FileManifest{"recovery": ref}}, ExpiresAt: clock.Now().Add(DefaultFileRetention)}}
				case "refreshed_bundle":
					q.s.BundleUploads = map[string]time.Time{ref.SHA256: clock.Now().Add(-DefaultFileRetention)}
				case "recent_bundle":
					q.s.BundleUploads = map[string]time.Time{ref.SHA256: clock.Now()}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			clock.Advance(filePublicationGrace + time.Second)
			p := releasePlan(t, q, releaseSelection(selected))
			if p.ReclaimableBytes != 0 {
				t.Fatal("protected class counted as reclaimable", p)
			}
			if _, err := releaseApply(q, p); err != nil {
				t.Fatal(err)
			}
			if err := q.reclaimPublishedFiles(); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(q.files.blob(ref)); err != nil {
				t.Fatal("protected class removed", err)
			}
		})
	}
}

func TestFileReleaseChangedCustodyInvalidatesPlan(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	j, _, _ := releaseTerminal(t, q, c, "custody-fence")
	clock.Advance(filePublicationGrace + time.Second)
	p := releasePlan(t, q, releaseSelection(j))
	if err := q.update(func() error { j := q.touchJob(j.ID); j.Attempt = 1; j.ReleaseConfirmed = true; return nil }); err != nil {
		t.Fatal(err)
	}
	_, err := releaseApply(q, p)
	releaseCode(t, err, "file_release_stale")
	if job(t, q, j.ID).FilesExpired {
		t.Fatal("changed execution expired")
	}
}

func TestFileReleaseOverlappingGroupsRetainReplay(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	a, _, _ := releaseTerminal(t, q, c, "overlap-a")
	b, _, _ := releaseTerminal(t, q, c, "overlap-b")
	clock.Advance(filePublicationGrace + time.Second)
	firstPlan := releasePlan(t, q, releaseSelection(a))
	first, err := releaseApply(q, firstPlan)
	if err != nil {
		t.Fatal(err)
	}
	secondPlan := releasePlan(t, q, releaseSelection(a, b))
	second, err := releaseApply(q, secondPlan)
	if err != nil {
		t.Fatal(err)
	}
	if err = q.reclaimPublishedFiles(); err != nil {
		t.Fatal(err)
	}
	for _, v := range []struct {
		plan   FileReleasePlan
		result FileReleaseResult
	}{{firstPlan, first}, {secondPlan, second}} {
		replay, err := releaseApply(q, v.plan)
		if err != nil || replay != v.result {
			t.Fatal("overlap lost receipt", replay, err)
		}
	}
}

func TestFileReleaseReintroducedContentAccounting(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	j, _, ref := releaseTerminal(t, q, c, "reintroduced")
	clock.Advance(filePublicationGrace + time.Second)
	first := releasePlan(t, q, releaseSelection(j))
	if first.ReclaimableBytes != ref.SizeBytes {
		t.Fatal("initial plan omitted reclaimable bytes", first)
	}
	if _, err := releaseApply(q, first); err != nil {
		t.Fatal(err)
	}
	if err := q.reclaimPublishedFiles(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(q.files.blob(ref)); !os.IsNotExist(err) {
		t.Fatal("first collection did not remove content", err)
	}
	// Importing identical bytes after completed reclamation does not enroll
	// them again under the retained job's historical manifests.
	retentionUpload(t, c, "reintroduced")
	clock.Advance(filePublicationGrace + time.Second)
	second := releasePlan(t, q, releaseSelection(j))
	if _, err := releaseApply(q, second); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := q.reclaimPublishedFiles(); err != nil {
			t.Fatal(err)
		}
	}
	var reclaimed int64
	if _, err := os.Stat(q.files.blob(ref)); os.IsNotExist(err) {
		reclaimed = ref.SizeBytes
	} else if err != nil {
		t.Fatal(err)
	}
	if second.ReclaimableBytes != reclaimed || second.Blobs[0].ReclaimableBytes != reclaimed {
		t.Fatalf("plan claimed %d reclaimable bytes; apply+collection reclaimed %d", second.ReclaimableBytes, reclaimed)
	}
	if reclaimed != 0 {
		t.Fatal("reintroduced content was re-enrolled for collection")
	}
}
