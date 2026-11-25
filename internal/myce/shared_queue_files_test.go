package myce

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func proofOpenReclaimQueue(t *testing.T, a *sharedQueueAuthority, dir, name string, clock Clock) (*Queue, sharedQueueTurn) {
	t.Helper()
	path := filepath.Join(dir, name, "queue.db")
	if err := PrepareStateRoots(filepath.Dir(path)); err != nil {
		t.Fatal(err)
	}
	lock, err := lockQueueDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lock.Close() })
	replica, err := openQueueSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	defer replica.Close()
	m := &sharedQueueMember{authority: a, replica: replica, timeout: 5 * time.Second}
	if err := m.bindReplica(t.Context()); err != nil {
		t.Fatal(err)
	}
	turn, err := a.acquire(t.Context(), ID(), name, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.pull(t.Context(), turn.head); err != nil {
		t.Fatal(err)
	}
	writer := &sharedQueueWriter{authority: a, turn: turn, timeout: m.timeout, sequence: turn.head}
	root := filepath.Join(dir, "store")
	q, err := openQueue(path, dir, clock, writer, lock, WithFileStore(root, 1<<30, true))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		writer.disabled.Store(true)
		_ = q.Close()
	})
	if err := q.ConfigureFileStore(root, 1<<30, true); err != nil {
		t.Fatal(err)
	}
	return q, turn
}

func TestSharedQueuePostgresStaleReclaimPreservesSuccessorReference(t *testing.T) {
	pg := startProofPostgres(t)
	a := pg.authority(t)
	dir := t.TempDir()
	clock := proofClock()
	old, first := proofOpenReclaimQueue(t, a, dir, "old", clock)
	body := []byte("shared custody fixture")
	ref := FileRef{SHA256: fmt.Sprintf("%x", sha256.Sum256(body)), SizeBytes: int64(len(body))}
	path := old.files.blob(ref)
	if err := os.WriteFile(path, body, 0600); err != nil {
		t.Fatal(err)
	}
	at := clock.Now().Add(-2 * DefaultFileRetention)
	if err := os.Chtimes(path, at, at); err != nil {
		t.Fatal(err)
	}
	if err := old.update(func() error {
		old.s.JobInputUploads = map[string]time.Time{ref.SHA256: at}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := a.renew(t.Context(), first, 0, false); err != nil {
		t.Fatal(err)
	}
	next, _ := proofOpenReclaimQueue(t, a, dir, "next", clock)
	s := proofSpec("keeper")
	s.Files = FileManifest{"input": ref}
	if _, err := next.Submit(s); err != nil {
		t.Fatal(err)
	}
	// The old local snapshot still marks this hash reclaimable. Its attempt
	// must be rejected by PostgreSQL before the blob path is unlinked.
	if err := old.reclaimPublishedFilesContext(t.Context()); !errors.Is(err, errSharedTurn) {
		t.Fatalf("stale reclaim returned %v", err)
	}
	if err := next.files.verify(t.Context(), ref); err != nil {
		t.Fatal("successor's committed input lost its bytes")
	}
}

func TestSharedQueuePostgresReclaimClaimSurvivesHandover(t *testing.T) {
	for _, sealed := range []bool{false, true} {
		name := "successor-cancels-intent"
		if sealed {
			name = "successor-waits-for-unlink"
		}
		t.Run(name, func(t *testing.T) {
			pg := startProofPostgres(t)
			a := pg.authority(t)
			dir := t.TempDir()
			clock := proofClock()
			old, turn := proofOpenReclaimQueue(t, a, dir, "old", clock)
			body := []byte("handover reclaim fixture")
			ref := FileRef{SHA256: fmt.Sprintf("%x", sha256.Sum256(body)), SizeBytes: int64(len(body))}
			path := old.files.blob(ref)
			if err := os.WriteFile(path, body, 0600); err != nil {
				t.Fatal(err)
			}
			if err := old.update(func() error {
				old.s.SharedBlobReclaims = map[string]string{ref.SHA256: ""}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			claim := ""
			if sealed {
				var err error
				claim, err = old.claimSharedBlobUnlink(t.Context(), ref.SHA256, old.fileReferenceRevision)
				if err != nil || claim == "" {
					t.Fatalf("claim not committed: %v", err)
				}
			}
			if err := a.renew(t.Context(), turn, 0, false); err != nil {
				t.Fatal(err)
			}
			next, _ := proofOpenReclaimQueue(t, a, dir, "next", clock)
			if value, exists := next.s.SharedBlobReclaims[ref.SHA256]; !exists || value != claim {
				t.Fatal("reclaim record did not survive replay")
			}
			s := proofSpec("keeper")
			s.Files = FileManifest{"input": ref}
			if sealed {
				if _, err := next.Submit(s); err == nil {
					t.Fatal("accepted a reference while an unlink actor could still remove its bytes")
				} else {
					proofUnavailable(t, err)
				}
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if err := old.finishSharedBlobUnlink(t.Context(), claim); err != nil {
					t.Fatal(err)
				}
				if err := next.clearFinishedSharedReclaims(t.Context()); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, body, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := next.Submit(s); err != nil {
				t.Fatal(err)
			}
			if _, exists := next.s.SharedBlobReclaims[ref.SHA256]; exists {
				t.Fatal("keeper retained the reclaim intent")
			}
		})
	}
}
