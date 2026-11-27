package myce

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestClaimReleaseBlockedStoreDoesNotBlockQueue(t *testing.T) {
	for _, phase := range []string{"activity", "legacy custody"} {
		t.Run(phase, func(t *testing.T) {
			q, clock, c := retentionFixture(t)
			claim, ctx := claimedCapacityPartial(t, q, c, "interrupted input", jobInputUploadPurpose)
			if phase == "legacy custody" {
				if err := q.update(func() error {
					delete(q.s.JobInputUploads, claim.File.SHA256)
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			clock.Advance(max(testFileRetention, filePublicationGrace) + time.Second)
			requests := prepareFileFreeRequests(t, q)
			blocked, resume := make(chan struct{}), make(chan struct{})
			blob := q.files.blob(claim.File)
			stallAt := 1
			if phase == "legacy custody" {
				stallAt = 2
			}
			blobStats := 0
			q.files.claimLstat = func(path string) (os.FileInfo, error) {
				if path == blob {
					blobStats++
					if blobStats == stallAt {
						close(blocked)
						<-resume
					}
				}
				return os.Lstat(path)
			}
			req := FileInputClaimRelease{Project: claim.Project, ClaimID: claim.ID, AcceptExpiry: true}
			err := withStalledFileStore(t, blocked, resume, func() error {
				released, err := q.ReleaseFileInputClaim(ctx, req)
				if err == nil && released.ClaimID != claim.ID {
					return fmt.Errorf("released the wrong claim: %+v", released)
				}
				return err
			}, requests)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestClaimReleaseRejectsRefreshDuringStoreObservation(t *testing.T) {
	q, clock, c := retentionFixture(t)
	ctx := context.Background()
	_, ref := retentionJobInput(t, c, "published input")
	claim, err := q.claimFileInput(ctx, Spec{Project: "owner"}, ref)
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(max(testFileRetention, filePublicationGrace) + time.Second)
	blocked, resume := make(chan struct{}), make(chan struct{})
	q.files.claimLstat = func(path string) (os.FileInfo, error) {
		info, err := os.Lstat(path)
		if path == q.files.blob(ref) {
			// Save the old activity before letting HEAD refresh the payload.
			close(blocked)
			<-resume
		}
		return info, err
	}
	req := FileInputClaimRelease{Project: claim.Project, ClaimID: claim.ID, AcceptExpiry: true}
	err = withStalledFileStore(t, blocked, resume, func() error {
		_, err := q.ReleaseFileInputClaim(ctx, req)
		return err
	}, func() error {
		if err := c.HasFile(ctx, ref); err != nil {
			return err
		}
		// The HTTP response may arrive before the handler releases its fence.
		q.fileLifecycle.Lock()
		q.fileLifecycle.Unlock()
		return nil
	})
	releaseCode(t, err, "file_release_stale")
	q.files.claimLstat = nil
	_, err = q.ReleaseFileInputClaim(ctx, req)
	releaseCode(t, err, "file_release_protected")
}

func TestFileTrashBlockedMoveDoesNotBlockReplayOrCompletion(t *testing.T) {
	q, clock, c := retentionFixture(t)
	_, orphan := retentionJobInput(t, c, "orphan bytes")
	clock.Advance(testFileRetention + time.Nanosecond)
	requests := prepareFileFreeRequests(t, q)
	blocked, resume := make(chan struct{}), make(chan struct{})
	q.files.beforeTrash = func(string) {
		close(blocked)
		<-resume
	}
	if err := withStalledFileStore(t, blocked, resume, func() error {
		return q.reclaimPublishedFilesContext(context.Background())
	}, requests); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(q.files.blob(orphan)); !os.IsNotExist(err) {
		t.Fatal("unreferenced bytes were not purged", err)
	}
}

func prepareFileFreeRequests(t *testing.T, q *Queue) func() error {
	t.Helper()
	s := spec("experiment")
	s.Resumable = false
	s.SubmissionKey = "file-free-replay"
	j := submit(t, q, s)
	running := check(t, q, machine("worker", "fake"), nil).Job
	if running == nil || running.ID != j.ID {
		t.Fatal("worker did not receive the artifact-free job")
	}
	if err := q.Started(result(running, false)); err != nil {
		t.Fatal(err)
	}
	return func() error {
		if _, err := q.Submit(spec("experiment")); err != nil {
			return fmt.Errorf("plain submit: %w", err)
		}
		replay, err := q.Submit(s)
		if err != nil {
			return fmt.Errorf("keyed replay: %w", err)
		}
		if replay.ID != j.ID {
			return fmt.Errorf("keyed replay returned %s, want %s", replay.ID, j.ID)
		}
		if err := q.Complete(result(running, true)); err != nil {
			return fmt.Errorf("artifact-free completion: %w", err)
		}
		replay, err = q.Submit(s)
		if err != nil {
			return fmt.Errorf("completed keyed replay: %w", err)
		}
		if replay.ID != j.ID || replay.State != "succeeded" {
			return fmt.Errorf("completed keyed replay returned %s in state %s", replay.ID, replay.State)
		}
		return nil
	}
}

func withStalledFileStore(t *testing.T, blocked <-chan struct{}, resume chan<- struct{}, storeCall, requests func() error) (err error) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- storeCall() }()
	defer func() {
		close(resume)
		err = <-done
	}()

	select {
	case <-blocked:
	case early := <-done:
		done <- early
		t.Fatalf("store operation returned before reaching the stalled syscall: %v", early)
	case <-time.After(10 * time.Second):
		t.Fatal("store operation never reached the stalled syscall")
	}
	requestDone := make(chan error, 1)
	go func() { requestDone <- requests() }()
	select {
	case err := <-requestDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Cleanup(func() { <-requestDone })
		t.Fatal("unrelated requests waited for the store syscall")
	}
	return nil
}
