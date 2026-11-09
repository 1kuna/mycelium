package myce

import (
	"context"

	"errors"
	"os"
	"path/filepath"

	"syscall"
	"testing"
)

func TestEnvironmentRecipeStorePublicationFence(t *testing.T) {
	q, _ := fixture(t)
	r := binaryRecipe(t, "fake")
	catalog := EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"tools": {"fake": r}}}
	if err := q.ConfigureEnvironments(catalog); err != nil {
		t.Fatal(err)
	}
	s := spec("experiment")
	s.Environment.Name = "tools"
	entered, unblock := gateSubmitVerification(t, q, nil)
	done := startSubmitVerification(q, context.Background(), s)
	submitVerificationWait(t, "recipe publication", entered)
	// Configuration must remain responsive while blob durability is blocked,
	// and its replacement must fence the stale recipe before commit.
	submitVerificationProgress(t, "catalog replacement", func() error { return q.ConfigureEnvironments(catalog) })
	unblock()
	got := submitVerificationWait(t, "submission", done)
	if !errors.Is(got.err, ErrStale) {
		t.Fatalf("catalog replacement did not fence submission: %v", got.err)
	}
	requireNoSubmission(t, q)

	t.Run("EnvironmentRecipeStoreLockSnapshotOutsideQueue", func(t *testing.T) {

		q, _ := fixture(t)
		lock := filepath.Join(t.TempDir(), "requirements.txt")
		if err := syscall.Mkfifo(lock, 0600); err != nil {
			t.Fatal(err)
		}
		s := spec("experiment")
		s.Environment = Environment{LockFile: lock, Python: "3.12", Platforms: []string{"fake"}}
		// Opening the write end proves the snapshot reader reached the FIFO. Keep
		// its read blocked until a separate queue transaction has completed.
		writer := make(chan *os.File, 1)
		go func() {
			f, _ := os.OpenFile(lock, os.O_WRONLY, 0)
			writer <- f
		}()
		done := startSubmitVerification(q, context.Background(), s)
		f := submitVerificationWait(t, "snapshot read", writer)
		if f == nil {
			t.Fatal("could not open snapshot writer")
		}
		defer f.Close()
		submitVerificationProgress(t, "queue transaction during lock-file read", func() error {
			_, err := q.CancelSubmission("test", "unrelated")
			return err
		})
		if _, err := f.Write([]byte("example==1.0\n")); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		if got := submitVerificationWait(t, "snapshot submission", done); got.err != nil {
			t.Fatal(got.err)
		}

	})
}
