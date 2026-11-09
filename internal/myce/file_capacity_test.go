package myce

import (
	"context"
	"encoding/base64"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func capacityPartial(t *testing.T, c Client, content, purpose string) FileRef {
	t.Helper()
	path := filepath.Join(t.TempDir(), "input")
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	ref, err := IdentifyFile(path)
	if err != nil {
		t.Fatal(err)
	}
	metadata := "sha256 " + base64.StdEncoding.EncodeToString([]byte(ref.SHA256))
	if purpose != "" {
		metadata += ",purpose " + base64.StdEncoding.EncodeToString([]byte(purpose))
	}
	res, err := c.fileRequest(context.Background(), "POST", "/api/files/uploads/", nil, map[string]string{"Tus-Resumable": "1.0.0", "Upload-Length": strconv.FormatInt(ref.SizeBytes, 10), "Upload-Metadata": metadata}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := fileResponse(res, 201); err != nil {
		t.Fatal(err)
	}
	res, err = c.fileRequest(context.Background(), "PATCH", "/api/files/uploads/"+ref.SHA256, strings.NewReader(content[:2]), map[string]string{"Tus-Resumable": "1.0.0", "Upload-Offset": "0", "Content-Type": "application/offset+octet-stream"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := fileResponse(res, 204); err != nil {
		t.Fatal(err)
	}
	return ref
}

func TestFileCapacityUnsubmittedClaimsProtectCollection(t *testing.T) {
	for _, interrupted := range []bool{false, true} {
		t.Run(strconv.FormatBool(interrupted), func(t *testing.T) {
			q, clock := fixture(t)
			c := retentionClient(t, q)
			q.fileRetention = testFileRetention
			const content = "claimed input without any job reference"
			path, ref := policyInput(t, content)
			if interrupted {
				ref = capacityPartial(t, c, content, jobInputUploadPurpose)
			} else if err := c.UploadJobInput(context.Background(), path, ref); err != nil {
				t.Fatal(err)
			}
			owner := WithFileStoreSubmission(context.Background(), Spec{Project: "owner"})
			claim, err := q.claimFileInput(owner, Spec{Project: "owner"}, ref)
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.WithValue(owner, fileInputClaimKey{}, claim.ID)
			clock.Advance(testFileRetention + filePublicationGrace + time.Second)
			// Collect before HEAD can refresh the upload's modification time.
			if err := q.Tick(); err != nil {
				t.Fatal(err)
			}
			if status := q.fileStoreStatus(context.Background()); status.ReclaimableBytes != 0 {
				t.Fatal("claimed bytes reported reclaimable", status)
			}
			if interrupted {
				res, err := c.fileRequest(ctx, "HEAD", "/api/files/uploads/"+ref.SHA256, nil, map[string]string{"Tus-Resumable": "1.0.0"}, ref.SizeBytes)
				if err != nil {
					t.Fatal(err)
				}
				if err := fileResponse(res, 200); err != nil {
					t.Fatal("claimed resumable upload collected", err)
				}
			} else if err := c.hasFile(ctx, ref, jobInputUploadPurpose); err != nil {
				t.Fatal("claimed published input collected", err)
			}
			if err := c.UploadJobInput(owner, path, ref); err != nil {
				t.Fatal("claimed input cannot finish publication", err)
			}
			s := spec("experiment")
			s.Project = "owner"
			s.Files = FileManifest{"input": ref}
			submit(t, q, s)
		})
	}
}

func TestFileCapacityAutomaticOrphansAreNotReclaimable(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	q.fileRetention = 0
	_, complete := retentionJobInput(t, c, "unsubmitted published input")
	partial := capacityPartial(t, c, "unsubmitted interrupted input", jobInputUploadPurpose)
	clock.Advance(testFileRetention + filePublicationGrace + time.Second)
	if status := q.fileStoreStatus(context.Background()); status.ReclaimableBytes != 0 {
		t.Errorf("automatic retention overstates reclaimable bytes: %d", status.ReclaimableBytes)
	}
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{q.files.blob(complete), filepath.Join(q.files.root, "uploads", partial.SHA256)} {
		if _, err := os.Stat(path); err != nil {
			t.Fatal("automatic mode collected unique input", err)
		}
	}
	q.fileRetention = testFileRetention
	if status := q.fileStoreStatus(context.Background()); status.ReclaimableBytes != uint64(complete.SizeBytes+2) {
		t.Fatal("explicit expiry policy lost reclaimable bytes", status)
	}
}

func TestFileCapacityProtectedReferencesSurvive(t *testing.T) {
	q, clock := fixture(t)
	c := retentionClient(t, q)
	live := capacityPartial(t, c, "live recoverable input", jobInputUploadPurpose)
	checkpoint := capacityPartial(t, c, "recoverable checkpoint", jobInputUploadPurpose)
	catalog := capacityPartial(t, c, "catalog model", jobInputUploadPurpose)
	if err := q.update(func() error {
		q.s.Jobs["recovery"] = &Job{State: "lost", Spec: Spec{Files: FileManifest{"input": live}}, FileCheckpoints: map[int]FileCheckpoint{1: {Files: FileManifest{"save": checkpoint}}}}
		q.models.Models = []Model{{SHA256: catalog.SHA256}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(testFileRetention + filePublicationGrace)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	for _, ref := range []FileRef{live, checkpoint, catalog} {
		for _, suffix := range []string{"", ".info"} {
			if _, err := os.Stat(filepath.Join(q.files.root, "uploads", ref.SHA256+suffix)); err != nil {
				t.Error("protected reference lost", err)
			}
		}
	}
}
