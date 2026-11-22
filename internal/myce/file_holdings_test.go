package myce

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
)

func TestProjectFileHoldingsAndCollectionAccounting(t *testing.T) {
	ctx := context.Background()
	q, clock := fixture(t)
	c := retentionClient(t, q)
	j, _, shared := releaseTerminal(t, q, c, "shared")
	spec := spec("experiment")
	spec.Project = "foreign"
	spec.Files = FileManifest{"foreign-name": shared}
	other := submit(t, q, spec)
	_, imported := retentionUpload(t, c, "untracked-import")
	pendingJob, _, pending := releaseTerminal(t, q, c, "pending-collection")
	if _, err := releaseApply(q, releasePlan(t, q, releaseSelection(pendingJob))); err != nil {
		t.Fatal(err)
	}
	_, want := policyInput(t, "declared-only")
	claim, err := q.claimFileInput(ctx, Spec{Project: "owner"}, want)
	if err != nil {
		t.Fatal(err)
	}
	var report ProjectFileHoldings
	if err := c.Call(ctx, "GET", "/api/files/holdings?project=owner", nil, &report); err != nil {
		t.Fatal(err)
	}
	if report.RetainedBytes != uint64(shared.SizeBytes+want.SizeBytes) ||
		report.RetainedBytes != projectUsage(t, q, "owner").RetainedBytes ||
		report.SharedBytes != uint64(shared.SizeBytes) ||
		report.StoredBytes != uint64(shared.SizeBytes) || len(report.Files) != 2 {
		t.Fatalf("attribution mismatch: %+v", report)
	}
	for _, kind := range []string{"input", "result", "input_claim"} {
		if report.ByKind[kind].FileCount != 1 {
			t.Fatal(kind, report.ByKind)
		}
	}
	wire, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{other.ID, "foreign-name", "foreign", imported.SHA256, pending.SHA256} {
		if strings.Contains(string(wire), value) {
			t.Fatalf("holdings disclosed an unrelated identifier: %s", value)
		}
	}
	if !strings.Contains(string(wire), j.ID) || !strings.Contains(string(wire), claim.ID) {
		t.Fatal("job or claim provenance missing", string(wire))
	}

	assertStatus := func(eligible uint64) {
		t.Helper()
		status := q.fileStoreStatus(ctx)
		if status.HeldPublishedBytes != uint64(shared.SizeBytes) ||
			status.UnreferencedPublishedBytes != uint64(imported.SizeBytes) ||
			status.PendingCollectionBytes != uint64(pending.SizeBytes) ||
			status.ReclaimableBytes != eligible ||
			status.UsedBytes != status.HeldPublishedBytes+status.PendingCollectionBytes+status.UnreferencedPublishedBytes+status.OpenUploadBytes {
			t.Fatalf("unexpected store partition: %+v", status)
		}
	}
	assertStatus(0)
	clock.Advance(filePublicationGrace + time.Second)
	assertStatus(uint64(pending.SizeBytes))
}

func TestProjectFileHoldingsPrunedReplayDoesNotKeepReleasedInputs(t *testing.T) {
	ctx := context.Background()
	q, clock := fixture(t)
	c := retentionClient(t, q)
	if err := q.ConfigureLogRetention(0); err != nil {
		t.Fatal(err)
	}
	j, s, ref := releaseTerminal(t, q, c, "released-before-history-prune")
	if _, err := releaseApply(q, releasePlan(t, q, releaseSelection(j))); err != nil {
		t.Fatal(err)
	}
	// Prune before collecting, so the job must transfer its pending hash.
	clock.Advance(jobRecordRetention + time.Second)
	if err := q.pruneJobHistory(); err != nil {
		t.Fatal(err)
	}
	r := q.s.Submissions[submissionDigest(s.Project, s.SubmissionKey)]
	if q.s.Jobs[j.ID] != nil || r.RemovedAt == nil || !q.s.RetiredJobFiles[ref.SHA256] {
		t.Fatalf("fixture did not retire the job and transfer its uncollected input: live=%t removed=%t pending=%t", q.s.Jobs[j.ID] != nil, r.RemovedAt != nil, q.s.RetiredJobFiles[ref.SHA256])
	}
	root := q.files.root
	q = reopenTierQueue(t, q, clock)
	if err := q.ConfigureFileStore(root, 1<<20, true); err != nil {
		t.Fatal(err)
	}
	if got := submit(t, q, s); got.ID != j.ID || got.State != "succeeded" {
		t.Fatal("keyed replay changed", got)
	}
	report, err := q.ProjectFileHoldings(ctx, "owner")
	if err != nil || report.RetainedBytes != 0 || len(report.Files) != 0 {
		t.Fatal("pruned replay resurrected file attribution", report, err)
	}
	status := q.fileStoreStatus(ctx)
	if len(status.Projects) != 0 || status.PendingCollectionBytes != uint64(ref.SizeBytes) || status.ReclaimableBytes != uint64(ref.SizeBytes) {
		t.Fatalf("pruned replay stranded pending collection: %+v", status)
	}
	if err := q.reclaimPublishedFiles(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(q.files.blob(ref)); !os.IsNotExist(err) {
		t.Fatal("retired hash was not collected", err)
	}
}

func TestFileReleaseLegacyCheckpointRequiresExplicitPlan(t *testing.T) {
	ctx := context.Background()
	q, clock := fixture(t)
	c := retentionClient(t, q)
	_, ref := retentionJobInput(t, c, "checkpoint-kept-after-release")
	j, _, _ := releaseTerminal(t, q, c, "job-with-checkpoint")
	if err := q.update(func() error {
		j := q.touchJob(j.ID)
		j.FileCheckpoints = map[int]FileCheckpoint{1: {Files: FileManifest{"save": ref}}}
		j.ResumeFiles = &FileCheckpoint{Files: FileManifest{"resume": ref}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(filePublicationGrace + time.Second)
	if _, err := releaseApply(q, releasePlan(t, q, releaseSelection(j))); err != nil {
		t.Fatal(err)
	}
	report, err := q.ProjectFileHoldings(ctx, "owner")
	if err != nil || report.RetainedBytes != uint64(ref.SizeBytes) ||
		report.ByKind["checkpoint"].FileCount != 1 || report.ByKind["resume_checkpoint"].FileCount != 1 {
		t.Fatal("ordinary expiry lost checkpoint or double counted it", report, err)
	}
	selection := releaseSelection(j)
	selection.IncludeCheckpoints = true
	plan := releasePlan(t, q, selection)
	if len(plan.Blobs) != 2 || plan.ReclaimableBytes < ref.SizeBytes {
		t.Fatal("explicit plan omitted checkpoint", plan)
	}
	if before := job(t, q, j.ID).FileReclamation; before == nil || before.LegacyCheckpointsExpired {
		t.Fatal("read-only planning altered expiry", before)
	}
	if _, err := releaseApply(q, plan); err != nil {
		t.Fatal(err)
	}
	report, err = q.ProjectFileHoldings(ctx, "owner")
	if err != nil || report.RetainedBytes != 0 {
		t.Fatal("accepted expiry kept checkpoint attribution", report, err)
	}
	if err := q.reclaimPublishedFiles(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(q.files.blob(ref)); !os.IsNotExist(err) {
		t.Fatal("checkpoint was not collected", err)
	}
}

func TestFileReleaseInputClaimPreservesOtherCustodyAndRollingQuota(t *testing.T) {
	ctx := context.Background()
	q, clock := fixture(t)
	c := retentionClient(t, q)
	setQuotaPolicy(t, q, "owner", ProjectPolicy{MaxAddedFileBytes24h: 64})
	path, ref := policyInput(t, "claimed")
	owner := WithFileStoreSubmission(ctx, Spec{Project: "owner"})
	if err := c.UploadJobInput(owner, path, ref); err != nil {
		t.Fatal(err)
	}
	claim, err := q.claimFileInput(owner, Spec{Project: "owner"}, ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q.claimFileInput(ctx, Spec{Project: "other"}, ref); err != nil {
		t.Fatal(err)
	}
	req := FileInputClaimRelease{Project: "other", ClaimID: claim.ID, AcceptExpiry: true}
	release := func(req FileInputClaimRelease) (out FileInputClaimReleaseResult, err error) {
		err = c.Call(ctx, "POST", "/api/files/release/claim", req, &out)
		return out, err
	}
	_, err = release(req)
	releaseCode(t, err, "file_release_invalid")
	req.Project = "owner"
	first, err := release(req)
	if err != nil || first.ClaimID != claim.ID || !first.ReleasedAt.Equal(clock.Now()) {
		t.Fatal(first, err)
	}
	if replay, err := release(req); err != nil || replay.ClaimID != first.ClaimID || !replay.ReleasedAt.Equal(first.ReleasedAt) {
		t.Fatal(replay, err)
	}
	if got := projectUsage(t, q, "owner"); got.RetainedBytes != 0 || got.AddedBytes24h != uint64(ref.SizeBytes) {
		t.Fatal("released claim still held bytes or refunded rolling charge", got)
	}
	if got := projectUsage(t, q, "other"); got.RetainedBytes != uint64(ref.SizeBytes) {
		t.Fatal("other project's claim changed", got)
	}
	_, err = q.inputClaimContext(context.WithValue(owner, fileInputClaimKey{}, claim.ID), ref, false)
	releaseCode(t, err, "file_input_claim_required")
	clock.Advance(DefaultFileRetention + time.Second)
	if err := q.reclaimPublishedFiles(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(q.files.blob(ref)); err != nil {
		t.Fatal("another project still needs the blob", err)
	}
}

func TestFileReleaseLegacyPartialClaimKeepsCollectionAuthority(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	claim, ctx := claimedCapacityPartial(t, q, c, "legacy-interrupted-input", jobInputUploadPurpose)
	imported, _ := claimedCapacityPartial(t, q, c, "legacy-interrupted-import", "")
	if err := q.update(func() error { delete(q.s.JobInputUploads, claim.File.SHA256); return nil }); err != nil {
		t.Fatal(err)
	}
	for _, release := range []FileInputClaim{claim, imported} {
		if _, err := q.ReleaseFileInputClaim(ctx, FileInputClaimRelease{Project: "owner", ClaimID: release.ID, AcceptExpiry: true}); err != nil {
			t.Fatal(err)
		}
	}
	if status := q.fileStoreStatus(ctx); status.OpenUploadBytes != 4 || status.ReclaimableBytes != 0 {
		t.Fatalf("release must leave written bytes until idle collection: %+v", status)
	}
	clock.Advance(max(testFileRetention, filePublicationGrace) + time.Second)
	if err := q.reclaimInterruptedUploads(); err != nil {
		t.Fatal(err)
	}
	if got := q.fileStoreStatus(ctx); got.OpenUploadBytes != 2 || got.ReclaimableBytes != 0 {
		t.Fatalf("collection must remove only the positively identified input: %+v", got)
	}
}
