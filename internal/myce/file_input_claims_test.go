package myce

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestProjectFileStoreClaimBindsResumeAcrossRestart(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	setQuotaPolicy(t, q, "owner", ProjectPolicy{MaxRetainedFileBytes: 6})
	path, ref := policyInput(t, "data")
	owner := WithFileStoreSubmission(context.Background(), Spec{Project: "owner"})
	var claim FileInputClaim
	if err := c.Call(owner, "POST", "/api/files/input-claims", map[string]any{"project": "owner", "file": ref}, &claim); err != nil {
		t.Fatal(err)
	}
	ctx := context.WithValue(owner, fileInputClaimKey{}, claim.ID)
	headers := map[string]string{"Tus-Resumable": "1.0.0", "Upload-Length": strconv.FormatInt(ref.SizeBytes, 10), "Upload-Metadata": "sha256 " + base64.StdEncoding.EncodeToString([]byte(ref.SHA256)) + ",purpose " + base64.StdEncoding.EncodeToString([]byte(jobInputUploadPurpose))}
	res, err := c.fileRequest(ctx, "POST", "/api/files/uploads/", nil, headers, ref.SizeBytes)
	if err != nil {
		t.Fatal(err)
	}
	if err := fileResponse(res, 201); err != nil {
		t.Fatal(err)
	}
	for _, requestCtx := range []context.Context{owner, WithFileStoreSubmission(context.WithValue(context.Background(), fileInputClaimKey{}, claim.ID), Spec{Project: "other"})} {
		res, err := c.fileRequest(requestCtx, "HEAD", "/api/files/uploads/"+ref.SHA256, nil, map[string]string{"Tus-Resumable": "1.0.0"}, ref.SizeBytes)
		if err != nil {
			t.Fatal(err)
		}
		var he *HTTPError
		if err := fileResponse(res, 200); !errors.As(err, &he) || he.Status != 409 {
			t.Fatal("resume escaped claim binding", err)
		}
	}
	if u := projectUsage(t, q, "owner"); u.RetainedBytes != 4 {
		t.Fatal("pre-submit upload not attributed", u)
	}
	root := q.files.root
	q = reopenTierQueue(t, q, clock)
	if err := q.ConfigureFileStore(root, 1<<20, true); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	c = Client{URL: server.URL, Token: "test"}
	if err := c.UploadJobInput(owner, path, ref); err != nil {
		t.Fatal("restart resume failed", err)
	}
	if u := projectUsage(t, q, "owner"); u.RetainedBytes != 4 || u.AddedBytes24h != 4 {
		t.Fatal("restart lost/doubled attribution", u)
	}
	s := spec("experiment")
	s.Project = "owner"
	s.Files = FileManifest{"input": ref}
	j := submit(t, q, s)
	if err := q.update(func() error {
		j := q.touchJob(j.ID)
		j.State = "succeeded"
		now := clock.Now()
		j.FinishedAt = &now
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(filePublicationGrace + time.Second)
	if _, err := releaseApply(q, releasePlan(t, q, FileReleaseSelection{Project: "owner", Jobs: []string{j.ID}})); err != nil {
		t.Fatal(err)
	}
	if u := projectUsage(t, q, "owner"); u.RetainedBytes != 0 {
		t.Fatal("settled claim prevented owner release", u)
	}
}

func TestProjectFileStoreConcurrentClaimsAndReservedCapacity(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	q.files.limit = 12
	setQuotaPolicy(t, q, "owner", ProjectPolicy{MaxRetainedFileBytes: 6})
	setQuotaPolicy(t, q, "neighbor", ProjectPolicy{ReservedFileBytes: 8})
	owner := WithFileStoreSubmission(context.Background(), Spec{Project: "owner"})
	a, ar := policyInput(t, "aaaa")
	b, br := policyInput(t, "bbbb")
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, v := range []struct {
		path string
		ref  FileRef
	}{{a, ar}, {b, br}} {
		wg.Add(1)
		go func(path string, ref FileRef) { defer wg.Done(); errs <- c.UploadJobInput(owner, path, ref) }(v.path, v.ref)
	}
	wg.Wait()
	close(errs)
	passed := 0
	for err := range errs {
		if err == nil {
			passed++
		} else {
			assertQuotaRefusal(t, err, "retained=8")
		}
	}
	if passed != 1 {
		t.Fatal("concurrent claims overcommitted quota", passed)
	}
	path, ref := policyInput(t, "next")
	other := WithFileStoreSubmission(context.Background(), Spec{Project: "other"})
	assertFilePolicyRefusal(t, c.PreflightFileStore(other, FileManifest{"input": ref}), "reserved_file_bytes=8")
	assertFilePolicyRefusal(t, c.UploadJobInput(other, path, ref), "reserved_file_bytes=8")
	neighbor := WithFileStoreSubmission(context.Background(), Spec{Project: "neighbor"})
	if err := c.UploadJobInput(neighbor, path, ref); err != nil {
		t.Fatal("owner of reservation refused", err)
	}
	// Shared digests charge each project but consume physical bytes only once.
	if err := c.UploadJobInput(other, path, ref); err != nil {
		t.Fatal("shared input refused", err)
	}
	if status := q.fileStoreStatus(context.Background()); status.UsedBytes != 8 {
		t.Fatal("shared blob double charged", status)
	}
	production := WithFileStoreSubmission(context.Background(), Spec{Project: "service", Class: "production"})
	p, pr := policyInput(t, "prod")
	if err := c.UploadJobInput(production, p, pr); err != nil {
		t.Fatal("production refused by project reservation", err)
	}
	clock.Advance(fileStoreQuotaWindow + time.Second)
	if u := projectUsage(t, q, "owner"); u.AddedBytes24h != 0 || u.RetainedBytes != 4 {
		t.Fatal("rolling allowance lost retained input bytes", u)
	}
}

func TestProjectFileStoreDeclaredOutputKeepsPublicationExemption(t *testing.T) {
	q, _ := fixture(t)
	c := retentionClient(t, q)
	q.files.limit = 12
	setQuotaPolicy(t, q, "neighbor", ProjectPolicy{ReservedFileBytes: 12})
	setQuotaPolicy(t, q, "producer", ProjectPolicy{MaxRetainedFileBytes: 1})
	path, ref := policyInput(t, "completed")
	s := spec("experiment")
	s.Project = "producer"
	j := submit(t, q, s)
	if err := q.update(func() error {
		j := q.touchJob(j.ID)
		j.State = "running"
		j.PendingOutputRefs = FileManifest{"result": ref}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// Legacy async workers identify the already-declared result, rather than
	// presenting the newer exact-attempt header or an input claim.
	if err := c.UploadFile(context.Background(), path, ref); err != nil {
		t.Fatal("completed result newly refused", err)
	}
	if err := c.HasFile(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
}

func TestProjectFileStoreResumeUsesStoredLengthAndKeepsCustody(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	setQuotaPolicy(t, q, "small", ProjectPolicy{MaxRetainedFileBytes: 1})
	_, ref := policyInput(t, "data")
	owner, err := q.claimFileInput(context.Background(), Spec{Project: "owner"}, ref)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.WithValue(context.Background(), fileInputClaimKey{}, owner.ID)
	headers := map[string]string{"Tus-Resumable": "1.0.0", "Upload-Length": "4", "Upload-Metadata": "sha256 " + base64.StdEncoding.EncodeToString([]byte(ref.SHA256))}
	res, err := c.fileRequest(ctx, "POST", "/api/files/uploads/", nil, headers, 4)
	if err != nil {
		t.Fatal(err)
	}
	if err = fileResponse(res, 201); err != nil {
		t.Fatal(err)
	}
	smallRef := ref
	smallRef.SizeBytes = 1
	small, err := q.claimFileInput(context.Background(), Spec{Project: "small"}, smallRef)
	if err != nil {
		t.Fatal(err)
	}
	smallCtx := context.WithValue(context.Background(), fileInputClaimKey{}, small.ID)
	for _, method := range []string{"HEAD", "PATCH"} {
		res, err = c.fileRequest(smallCtx, method, "/api/files/uploads/"+ref.SHA256, nil, map[string]string{"Tus-Resumable": "1.0.0", "Content-Type": "application/offset+octet-stream", "Upload-Offset": "0"}, 1)
		if err != nil {
			t.Fatal(err)
		}
		if res.StatusCode != 409 {
			t.Fatal("short claim authorized larger upload", method, res.StatusCode)
		}
		res.Body.Close()
	}
	headers["Upload-Length"] = "1"
	res, err = c.fileRequest(smallCtx, "POST", "/api/files/uploads/", nil, headers, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err = fileResponse(res, 409); err != nil {
		t.Fatal(err)
	}
	attached, err := q.claimFileInput(context.Background(), Spec{Project: "attached"}, ref)
	if err != nil {
		t.Fatal(err)
	}
	attachedCtx := context.WithValue(context.Background(), fileInputClaimKey{}, attached.ID)
	res, err = c.fileRequest(attachedCtx, "HEAD", "/api/files/uploads/"+ref.SHA256, nil, map[string]string{"Tus-Resumable": "1.0.0"}, 4)
	if err != nil {
		t.Fatal(err)
	}
	if err = fileResponse(res, 200); err != nil {
		t.Fatal(err)
	}
	clock.Advance(filePublicationGrace + time.Second)
	if u := projectUsage(t, q, "attached"); u.RetainedBytes != 4 {
		t.Fatal("resumed allocation expired", u)
	}
}

func TestProjectFileStoreSettlementKeepsConcurrentClaimValid(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	setQuotaPolicy(t, q, "owner", ProjectPolicy{MaxRetainedFileBytes: 4})
	path, ref := policyInput(t, "data")
	owner := WithFileStoreSubmission(context.Background(), Spec{Project: "owner"})
	claim, err := q.claimFileInput(owner, Spec{Project: "owner"}, ref)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.UploadJobInput(owner, path, ref); err != nil {
		t.Fatal(err)
	}
	s := spec("experiment")
	s.Project = "owner"
	s.Files = FileManifest{"input": ref}
	submit(t, q, s)
	ctx := context.WithValue(owner, fileInputClaimKey{}, claim.ID)
	if err = c.hasFile(ctx, ref, jobInputUploadPurpose); err != nil {
		t.Fatal("sibling submission invalidated transfer", err)
	}
	if u := projectUsage(t, q, "owner"); u.RetainedBytes != 4 {
		t.Fatal("settlement double charged", u)
	}
	clock.Advance(filePublicationGrace + time.Second)
	if _, err = q.inputClaimContext(ctx, ref, false); err != nil {
		t.Fatal("elapsed time invalidated a concurrent transfer receipt", err)
	}
}

func TestProjectFileStoreOutputReservationAdmission(t *testing.T) {
	for _, productionReserve := range []int64{0, 8} {
		t.Run(strconv.FormatInt(productionReserve, 10), func(t *testing.T) {
			q, clock := fixture(t)
			q.deliveryRoot = q.root
			q.files.limit = 12 + productionReserve
			q.fileStoreProductionReserve = productionReserve
			setQuotaPolicy(t, q, "neighbor", ProjectPolicy{ReservedFileBytes: 8})
			s := asyncDeliverySpec()
			s.Project = "other"
			s.Disk.OutputBytes = 8
			if _, err := q.Submit(s); err == nil {
				t.Fatal("submission overlapped reserve pools")
			}
			s.Disk.OutputBytes = 4
			j := submit(t, q, s)
			if err := q.update(func() error {
				j := q.touchJob(j.ID)
				j.Spec.Disk.OutputBytes = 8
				j.OutputAdmissionPending = true
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			clock.Advance(time.Second)
			if err := q.admitOutputRetries(context.Background()); err != nil {
				t.Fatal(err)
			}
			if !job(t, q, j.ID).OutputAdmissionPending {
				t.Fatal("retry consumed neighbor reservation")
			}
			setQuotaPolicy(t, q, "neighbor", ProjectPolicy{})
			if err := q.admitOutputRetries(context.Background()); err != nil {
				t.Fatal(err)
			}
			if job(t, q, j.ID).OutputAdmissionPending {
				t.Fatal("retry failed to reacquire after relief")
			}
		})
	}
}

func TestProjectFileStorePendingIdentityUsesOwnerReservation(t *testing.T) {
	q, _ := fixture(t)
	c := retentionClient(t, q)
	q.deliveryRoot = q.root
	q.files.limit = 12
	setQuotaPolicy(t, q, "neighbor", ProjectPolicy{ReservedFileBytes: 8})
	s := asyncDeliverySpec()
	s.Project = "neighbor"
	s.Disk.OutputBytes = 8
	j := submit(t, q, s)
	_, ref := policyInput(t, "12345678")
	if err := q.update(func() error { q.touchJob(j.ID).PendingOutputRefs = FileManifest{"out.txt": ref}; return nil }); err != nil {
		t.Fatal(err)
	}
	path, ref := policyInput(t, "data")
	if err := c.UploadJobInput(WithFileStoreSubmission(context.Background(), Spec{Project: "other"}), path, ref); err != nil {
		t.Fatal("pending identity double charged entitlement", err)
	}
}

func TestProjectFileStoreRecoveryAndFailureDiagnosticsRemainPublishable(t *testing.T) {
	t.Run("recovery", func(t *testing.T) {
		q, _, c, p := recoveryFixture(t)
		setQuotaPolicy(t, q, job(t, q, p.ID).Spec.Project, ProjectPolicy{MaxRetainedFileBytes: 1})
		setQuotaPolicy(t, q, "neighbor", ProjectPolicy{ReservedFileBytes: 1 << 20})
		path, ref := policyInput(t, "new recovered bytes")
		plan := RecoveryPlan{Execution: p.Execution, Machine: p.Machine, Session: p.Session}
		ctx := WithFileStoreRecovery(context.Background(), plan)
		if err := c.PreflightFileStore(ctx, FileManifest{"out.txt": ref}); err != nil {
			t.Fatal(err)
		}
		if err := c.UploadJobInput(ctx, path, ref); err != nil {
			t.Fatal("recovery refused", err)
		}
		p.Files = FileManifest{"out.txt": ref}
		if _, err := c.RecoverResult(ctx, p); err != nil {
			t.Fatal(err)
		}
		plan.Attempt++
		if err := c.UploadJobInput(WithFileStoreRecovery(context.Background(), plan), path, ref); err == nil {
			t.Fatal("stale recovery upload accepted")
		}
	})
	t.Run("diagnostics", func(t *testing.T) {
		q, _ := fixture(t)
		c := retentionClient(t, q)
		m := machine("worker", "fake")
		check(t, q, m, nil)
		s := spec("experiment")
		s.Resumable = false
		_, input := retentionJobInput(t, c, "diagnostic fixture")
		s.Files = FileManifest{"input": input}
		s.Diagnostics = &DiagnosticSpec{}
		submit(t, q, s)
		j := check(t, q, m, nil).Job
		if j == nil {
			t.Fatal("missing assignment")
		}
		setQuotaPolicy(t, q, s.Project, ProjectPolicy{MaxRetainedFileBytes: 1})
		setQuotaPolicy(t, q, "neighbor", ProjectPolicy{ReservedFileBytes: 1 << 20})
		r, err := (Worker{Client: c}).finishFailureDiagnostics(*j, result(j, false), t.TempDir(), "")
		if err != nil || r.Diagnostics == nil {
			t.Fatal("failed diagnostics stranded", err)
		}
		if err = q.Complete(r); err != nil {
			t.Fatal(err)
		}
		if job(t, q, j.ID).State != "failed" {
			t.Fatal("diagnostic publication did not finish")
		}
	})
}
