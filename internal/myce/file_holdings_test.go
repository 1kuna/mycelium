package myce

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestProjectFileHoldingsClaimReleaseEligibility(t *testing.T) {
	ctx := context.Background()
	q, clock := fixture(t)
	c := retentionClient(t, q)
	const idleWindow, grace = time.Hour, 2 * time.Hour
	if err := q.ConfigureFileRetention(idleWindow); err != nil {
		t.Fatal(err)
	}
	if err := q.update(func() error { q.recordPublicationTiming(clock.Now(), grace); return nil }); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	_, published := retentionJobInput(t, c, "published-claim")
	keeper := spec("experiment")
	keeper.Project = "other"
	keeper.Files = FileManifest{"input": published}
	submit(t, q, keeper)
	partial, _ := claimedCapacityPartial(t, q, c, "partial-claim", jobInputUploadPurpose)
	absent := creationRef("absent-claim")
	issue := func(ref FileRef) FileInputClaim {
		t.Helper()
		claim, err := q.claimFileInput(ctx, Spec{Project: "owner", Class: "experiment"}, ref)
		if err != nil {
			t.Fatal(err)
		}
		return claim
	}
	claims := map[string]FileInputClaim{
		"published": issue(published),
		"uploading": partial,
		"absent":    issue(absent),
	}
	q.fileLifecycle.Lock()
	q.fileLifecycle.Unlock()
	read := func() ProjectFileHoldings {
		t.Helper()
		var report ProjectFileHoldings
		if err := c.Call(ctx, "GET", "/api/files/holdings?project=owner", nil, &report); err != nil {
			t.Fatal(err)
		}
		return report
	}
	release := func(claim FileInputClaim) error {
		var out FileInputClaimReleaseResult
		return c.Call(ctx, "POST", "/api/files/release/claim", FileInputClaimRelease{
			Project: "owner", ClaimID: claim.ID, AcceptExpiry: true,
		}, &out)
	}
	check := func(activity time.Time, eligibility, reason string) {
		t.Helper()
		report := read()
		if !report.CapturedAt.Equal(clock.Now()) || len(report.Files) != len(claims) {
			t.Fatalf("unexpected snapshot: %+v", report)
		}
		for _, file := range report.Files {
			claim, ok := claims[file.State]
			if !ok || file.SHA256 != claim.File.SHA256 || len(file.InputClaims) != 1 ||
				file.DurableCustody != (file.State == "published") {
				t.Fatalf("unexpected file custody: %+v", file)
			}
			got := file.InputClaims[0]
			if got.ClaimID != claim.ID || !got.LastActivity.Equal(activity) ||
				got.IdleSeconds == nil || *got.IdleSeconds != clock.Now().Sub(activity).Seconds() ||
				got.Eligibility != eligibility || got.Reason != reason || !got.EligibleAt.Equal(activity.Add(grace)) {
				t.Fatalf("%s claim: %+v", file.State, got)
			}
		}
		for i := 0; i < 2; i++ {
			if next := read(); !reflect.DeepEqual(next, report) {
				t.Fatalf("holdings read %d changed the snapshot: %+v", i, next)
			}
		}
		for _, claim := range claims {
			err := release(claim)
			if eligibility == "eligible" {
				if err != nil {
					t.Fatalf("release refused eligible claim %s: %v", claim.ID, err)
				}
			} else {
				releaseCode(t, err, "file_release_protected")
			}
		}
	}
	start := clock.Now()
	check(start, "protected", "upload_idle_window")
	clock.Advance(idleWindow)
	check(start, "protected", "publication_grace")
	if next := issue(absent); next.ID != claims["absent"].ID {
		t.Fatalf("expected to reissue absent claim, got %s", next.ID)
	}
	if err := q.files.refreshBlob(q.files.blob(published)); err != nil {
		t.Fatal(err)
	}
	if err := q.files.refreshUpload(partial.File.SHA256); err != nil {
		t.Fatal(err)
	}
	refresh := clock.Now()
	check(refresh, "protected", "upload_idle_window")
	clock.Advance(grace - time.Second)
	check(refresh, "protected", "publication_grace")
	clock.Advance(time.Second)
	check(refresh, "eligible", "idle_windows_elapsed")
	if report := read(); len(report.Files) != 0 {
		t.Fatalf("released claims still counted: %+v", report)
	}
}

func TestProjectFileHoldingsClaimActivityAndCustodyUnavailable(t *testing.T) {
	ctx := context.Background()
	q, clock := fixture(t)
	c := retentionClient(t, q)
	claim, _ := claimedCapacityPartial(t, q, c, "partial-claim", jobInputUploadPurpose)
	clock.Advance(max(testFileRetention, filePublicationGrace))
	req := FileInputClaimRelease{Project: "owner", ClaimID: claim.ID, AcceptExpiry: true}
	check := func(reason string) {
		t.Helper()
		report, err := q.ProjectFileHoldings(ctx, req.Project)
		if err != nil || len(report.Files) != 1 || len(report.Files[0].InputClaims) != 1 {
			t.Fatalf("unexpected holdings: %+v, %v", report, err)
		}
		got := report.Files[0].InputClaims[0]
		if got.Eligibility != "unknown" || got.Reason != reason {
			t.Fatalf("unexpected claim eligibility: %+v", got)
		}
		_, err = q.ReleaseFileInputClaim(ctx, req)
		releaseCode(t, err, "file_release_busy")
	}
	q.fileLifecycle.RLock()
	check("publication_busy")
	q.fileLifecycle.RUnlock()

	if err := q.update(func() error { delete(q.s.JobInputUploads, claim.File.SHA256); return nil }); err != nil {
		t.Fatal(err)
	}
	meta := filepath.Join(q.files.root, "uploads", claim.File.SHA256+".info")
	if err := os.WriteFile(meta, []byte("invalid upload metadata"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(meta, claim.At, claim.At); err != nil {
		t.Fatal(err)
	}
	check("custody_unavailable")
	if err := os.Remove(meta); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(meta, 0700); err != nil {
		t.Fatal(err)
	}
	check("activity_unavailable")
}

func TestProjectFileHoldingsAndCollectionAccounting(t *testing.T) {
	t.Run("project totals and privacy", testProjectFileHoldingTotals)
	t.Run("collector agreement", testFileOwnershipCollectorAgreement)
}

func testFileOwnershipCollectorAgreement(t *testing.T) {
	for _, tc := range []struct {
		name, keeper string
		protected    bool
		recent       bool
	}{
		{name: "expired and retained history"},
		{name: "cross-project job", keeper: "input", protected: true},
		{name: "unsettled upload claim", keeper: "input_claim", protected: true},
		{name: "legacy checkpoint", keeper: "checkpoint", protected: true},
		{name: "delayed delivery", keeper: "pending_delivery", protected: true},
		{name: "unknown worker custody", keeper: "worker_local_custody", protected: true},
		{name: "recent publication", recent: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			q, clock := fixture(t)
			c := retentionClient(t, q)
			selected, _, ref := releaseTerminal(t, q, c, "shared-payload")
			if tc.keeper == "input_claim" {
				if _, err := q.claimFileInput(ctx, Spec{Project: "foreign"}, ref); err != nil {
					t.Fatal(err)
				}
			} else if tc.protected || tc.name == "expired and retained history" {
				otherSpec := spec("experiment")
				otherSpec.Project = "foreign"
				otherSpec.Files = FileManifest{"foreign-input": ref}
				other := submit(t, q, otherSpec)
				if err := q.update(func() error {
					other := q.touchJob(other.ID)
					if tc.keeper == "input" {
						return nil
					}
					other.State = "succeeded"
					other.FilesExpired = true
					enrollFileReclamation(other)
					switch tc.keeper {
					case "checkpoint":
						other.FileCheckpoints = map[int]FileCheckpoint{1: {Files: FileManifest{"foreign-save": ref}}}
					case "pending_delivery":
						other.OutputRefs = FileManifest{"foreign-output": ref}
						other.Delivery = &OutputDelivery{State: "pending"}
					case "worker_local_custody":
						other.Attempt = 1
						other.LocalOutputs = &TaskOutputCustody{Files: FileManifest{"foreign-local": ref}}
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			if !tc.recent {
				clock.Advance(filePublicationGrace + time.Second)
			}
			holdings, err := q.ProjectFileHoldings(ctx, "owner")
			if err != nil || len(holdings.Files) != 1 {
				t.Fatal("cannot inspect holdings", holdings, err)
			}
			held := holdings.Files[0]
			if holdings.RetainedBytes != uint64(ref.SizeBytes) || holdings.StoredBytes != uint64(ref.SizeBytes) ||
				held.Shared != tc.protected || len(held.Holders) != 2 || (tc.protected && !slices.Contains(held.OtherHolderKinds, tc.keeper)) {
				t.Fatal("wrong keeper attribution", holdings)
			}
			for _, owner := range held.Holders {
				if owner.Project != "owner" || owner.JobID != selected.ID {
					t.Fatal("holdings disclosed another holder", owner)
				}
			}
			if tc.keeper == "input_claim" {
				partial, _ := claimedCapacityPartial(t, q, c, "active partial", jobInputUploadPurpose)
				report, err := q.ProjectFileHoldings(ctx, partial.Project)
				if err != nil || !slices.ContainsFunc(report.Files, func(f ProjectHeldFile) bool {
					return f.SHA256 == partial.File.SHA256 && f.State == "uploading" && len(f.InputClaims) == 1
				}) {
					t.Fatal("partial upload lost its claim", report, err)
				}
			}
			p := releasePlan(t, q, releaseSelection(selected))
			if len(p.Blobs) != 1 || p.Blobs[0].CollectionEligible == tc.protected ||
				(tc.protected && !slices.ContainsFunc(p.Blobs[0].References, func(r FileKeepingReference) bool { return r.Kind == tc.keeper })) {
				t.Fatal("wrong planned keeper or scope", p)
			}
			if tc.name == "expired and retained history" {
				refused := errors.New("reject fixture transaction")
				err := q.update(func() error {
					q.acceptFileExpiry(q.touchJob(selected.ID), p.ID, fileReleaseDigest(p.Selection))
					return refused
				})
				if !errors.Is(err, refused) || job(t, q, selected.ID).FilesExpired || releasePlan(t, q, p.Selection).ID != p.ID {
					t.Fatal("failed transaction changed release scope", err)
				}
			}
			wantBytes := int64(0)
			if !tc.protected && !tc.recent {
				wantBytes = ref.SizeBytes
			}
			if p.ReclaimableBytes != wantBytes {
				t.Fatal("wrong planned reclaimable bytes", p)
			}
			if _, err := releaseApply(q, p); err != nil {
				t.Fatal(err)
			}
			q.mu.RLock()
			expired, keep, _ := q.fileReferences(true)
			q.mu.RUnlock()
			if legacy := expired[ref.SHA256] && !keep[ref.SHA256]; legacy != p.Blobs[0].CollectionEligible {
				t.Fatalf("planned scope and current collector disagree: plan=%+v expired=%t keep=%t", p.Blobs[0], expired[ref.SHA256], keep[ref.SHA256])
			}
			if status := q.fileStoreStatus(ctx); status.Error != "" || status.ReclaimableBytes != uint64(wantBytes) {
				t.Fatal("dry-run status disagrees with plan", status)
			}
			if freed, err := q.reclaimPublishedFilesUntil(ctx, 0); err != nil || freed != uint64(wantBytes) {
				t.Fatal("collector disagrees with plan", freed, err)
			}
			_, err = os.Stat(q.files.blob(ref))
			if wantBytes > 0 && !os.IsNotExist(err) || wantBytes == 0 && err != nil {
				t.Fatal("collector changed unexpected payload", err)
			}
			if _, err := releaseApply(q, p); err != nil {
				t.Fatal("expiry receipt did not replay after collection", err)
			}
		})
	}
}

func testProjectFileHoldingTotals(t *testing.T) {
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
	if q.s.Jobs[j.ID] != nil || r.RemovedAt == nil || !r.FileCustodyPruned || !q.s.RetiredJobFiles[ref.SHA256] {
		t.Fatalf("fixture did not retire the job and transfer its uncollected input: live=%t removed=%t pruned=%t pending=%t",
			q.s.Jobs[j.ID] != nil, r.RemovedAt != nil, r.FileCustodyPruned, q.s.RetiredJobFiles[ref.SHA256])
	}
	report, err := q.ProjectFileHoldings(ctx, "owner")
	if err != nil || report.RetainedBytes != 0 || len(report.Files) != 0 {
		t.Fatal("new replay kept old file custody", report, err)
	}
	// Open a previous-release record that has the summary and replay, but no
	// explicit custody marker. The upgrade must recognize the existing state.
	if err := q.update(func() error {
		q.touchSubmission(submissionDigest(s.Project, s.SubmissionKey)).FileCustodyPruned = false
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	root := q.files.root
	q = reopenTierQueue(t, q, clock)
	if err := q.ConfigureFileStore(root, 1<<20, true); err != nil {
		t.Fatal(err)
	}
	if got := submit(t, q, s); got.ID != j.ID || got.State != "succeeded" {
		t.Fatal("keyed replay changed", got)
	}
	report, err = q.ProjectFileHoldings(ctx, "owner")
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
	if err := c.DownloadFile(ctx, ref, filepath.Join(t.TempDir(), "checkpoint")); !isResultExpired(err) {
		t.Fatal("released checkpoint lost its typed expiry", err)
	}
}

func TestFileReleaseInputClaimPreservesOtherCustodyAndRollingQuota(t *testing.T) {
	ctx := context.Background()
	q, clock := fixture(t)
	c := retentionClient(t, q)
	if err := q.ConfigureFileRetention(time.Hour); err != nil {
		t.Fatal(err)
	}
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
	clock.Advance(time.Hour + time.Second)
	_, err = release(req)
	releaseCode(t, err, "file_release_protected")
	clock.Advance(filePublicationGrace - time.Hour)
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
	if _, err := q.ReleaseFileInputClaim(ctx, FileInputClaimRelease{Project: "owner", ClaimID: claim.ID, AcceptExpiry: true}); err == nil {
		t.Fatal("recent partial transfer was released")
	}
	clock.Advance(max(testFileRetention, filePublicationGrace) + time.Second)
	for _, release := range []FileInputClaim{claim, imported} {
		if _, err := q.ReleaseFileInputClaim(ctx, FileInputClaimRelease{Project: "owner", ClaimID: release.ID, AcceptExpiry: true}); err != nil {
			t.Fatal(err)
		}
	}
	if status := q.fileStoreStatus(ctx); status.OpenUploadBytes != 4 || status.ReclaimableBytes != 2 {
		t.Fatalf("release must leave written bytes for idle collection: %+v", status)
	}
	if err := q.reclaimInterruptedUploads(); err != nil {
		t.Fatal(err)
	}
	if got := q.fileStoreStatus(ctx); got.OpenUploadBytes != 2 || got.ReclaimableBytes != 0 {
		t.Fatalf("collection must remove only the positively identified input: %+v", got)
	}
}

func TestFileReleaseInputClaimProtectsUploadAndSubmitGaps(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	path, ref := policyInput(t, strings.Repeat("a", fileUploadChunkSize+1))
	q.files.limit = ref.SizeBytes * 2
	ctx := WithFileStoreSubmission(context.Background(), Spec{Project: "owner"})
	claim, err := q.claimFileInput(ctx, Spec{Project: "owner"}, ref)
	if err != nil {
		t.Fatal(err)
	}
	// The operator saw the old claim before a second submitter took it up.
	clock.Advance(testFileRetention + time.Second)
	var checked []string
	protect := func(phase string) {
		// The response can arrive before the server releases its request fence.
		// Exercise the inter-request gap, beyond that per-request protection.
		q.fileLifecycle.Lock()
		q.fileLifecycle.Unlock()
		_, err := q.ReleaseFileInputClaim(ctx, FileInputClaimRelease{Project: "owner", ClaimID: claim.ID, AcceptExpiry: true})
		releaseCode(t, err, "file_release_protected")
		checked = append(checked, phase)
	}
	var patches int
	c.HTTP = &http.Client{Transport: clientTestTransport(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPatch && patches == 0 {
			clock.Advance(testFileRetention - time.Second)
		}
		reply, err := http.DefaultTransport.RoundTrip(r)
		if err != nil {
			return nil, err
		}
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/files/input-claims":
			protect("claim reissued before transfer")
		case r.Method == http.MethodPatch && reply.StatusCode == http.StatusNoContent:
			patches++
			if patches == 1 {
				q.fileLifecycle.Lock()
				q.fileLifecycle.Unlock()
				clock.Advance(2 * time.Second) // The issuance alone is now idle.
				protect("between PATCH requests")
			}
		case r.Method == http.MethodHead && strings.HasPrefix(r.URL.Path, "/api/files/blobs/") &&
			reply.StatusCode == http.StatusOK && patches == 2:
			protect("after verification before submission")
		}
		return reply, nil
	})}
	defer c.HTTP.CloseIdleConnections()
	if err := c.UploadJobInput(ctx, path, ref); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(checked, ", "); got != "claim reissued before transfer, between PATCH requests, after verification before submission" {
		t.Fatal("did not exercise every handoff gap", got)
	}
	var submitted Job
	s := spec("experiment")
	s.Project, s.Files = "owner", FileManifest{"input": ref}
	if err := c.Call(ctx, "POST", "/api/jobs", s, &submitted); err != nil || submitted.ID == "" {
		t.Fatal("submission lost its uploaded input", err)
	}
	if stored := q.s.FileInputClaims[claim.ID]; stored.SettledAt.IsZero() || stored.Released {
		t.Fatal("submission did not settle the same preserved claim", stored)
	}
}
