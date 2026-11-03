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
		if err := fileResponse(res, 200); !errors.As(err, &he) || he.Code != "file_input_claim_required" {
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
