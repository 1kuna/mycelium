package myce

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestWorkerHygieneCollectionFileBudgetAndLocks(t *testing.T) {
	for _, count := range []int{2} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			clock := &fakeClock{now: time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC)}
			cache := &FileCache{Root: t.TempDir(), Limit: 16, Clock: clock}
			if err := PrepareStateRoots(cache.Root); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < count; i++ {
				path := filepath.Join(cache.Root, fmt.Sprintf("%064x", i))
				if err := os.WriteFile(path, []byte(strings.Repeat("x", 16)), 0600); err != nil {
					t.Fatal(err)
				}
				at := clock.Now().Add(time.Duration(i) * time.Second)
				if err := os.Chtimes(path, at, at); err != nil {
					t.Fatal(err)
				}
			}
			lock, err := cache.collectionLock(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			other := &FileCache{Root: cache.Root, Limit: cache.Limit, Clock: clock}
			if r := other.collect(context.Background()); !r.Busy || r.Reclaimed != 0 {
				t.Fatal("ignored active writer lock", r)
			}
			unlockEnvironment(lock)
			r := cache.collect(context.Background())
			if r.Error != "" || r.Bytes != 16 || r.Reclaimed != int64(count-1)*16 {
				t.Fatal(r)
			}
			if _, err := os.Stat(filepath.Join(cache.Root, fmt.Sprintf("%064x", count-1))); err != nil {
				t.Fatal("did not retain newest cache entry", err)
			}
			unknown := filepath.Join(cache.Root, "unknown-data")
			partial := filepath.Join(cache.Root, strings.Repeat("a", 64)+".partial")
			for _, path := range []string{unknown, partial} {
				if err := os.WriteFile(path, []byte("keep"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			r = cache.collect(context.Background())
			if !r.Protected {
				t.Fatal("unknown/partial data not reported protected")
			}
			for _, path := range []string{unknown, partial} {
				if _, err := os.Stat(path); err != nil {
					t.Fatal("removed unknown/partial data", err)
				}
			}
		})
	}
}

func TestWorkerHygieneCollectionEnvironmentBudgetReferencesAndCustody(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC)}
	cache := &EnvironmentCache{Root: t.TempDir(), Limit: 1, Clock: clock}
	if err := cache.init(); err != nil {
		t.Fatal(err)
	}
	free := pruneFixture(t, cache, 1, 48*time.Hour)
	referenced := pruneFixture(t, cache, 2, 48*time.Hour)
	active := pruneFixture(t, cache, 3, 48*time.Hour)
	lock, err := cache.lock(context.Background(), active+".use", syscall.LOCK_SH)
	if err != nil {
		t.Fatal(err)
	}
	defer unlockEnvironment(lock)
	refs := func(_ context.Context, key string) (bool, error) { return key == referenced, nil }
	r := cache.collectBudget(context.Background(), refs)
	if r.Error != "" || r.Reclaimed <= 0 || !r.Protected {
		t.Fatal(r)
	}
	if _, err := os.Stat(filepath.Join(cache.Root, free)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("unreferenced old cache entry retained", err)
	}
	for _, key := range []string{referenced, active} {
		if _, err := os.Stat(filepath.Join(cache.Root, key)); err != nil {
			t.Fatal("protected environment removed", key, err)
		}
	}
	if r := cache.collectBudget(context.Background(), func(context.Context, string) (bool, error) { return false, errors.New("reference service unavailable") }); r.Error != "environment_references_unavailable" {
		t.Fatal("did not fail closed", r)
	}
	if _, err := os.Stat(filepath.Join(cache.Root, referenced)); err != nil {
		t.Fatal("failed reference lookup removed data", err)
	}
}

func TestWorkerHygieneCollectionFailedStagingRequiresReceiptAndInactivity(t *testing.T) {
	f := newFleetInstallFixture(t, "linux")
	// The fixture's installation, manifest and receipt are all local synthetic
	// bytes; no manager, network or live database is accessed.
	path := fleetStagingPath(f.c, f.i)
	if err := os.WriteFile(path, []byte("partial download"), 0600); err != nil {
		t.Fatal(err)
	}
	unknown := filepath.Join(filepath.Dir(f.c.Binary), ".unknown.partial")
	if err := os.WriteFile(unknown, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	command := func(context.Context, string, ...string) ([]byte, error) { return []byte("not-found\n"), nil }
	if err := collectFailedFleetStaging(context.Background(), f.c, command); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal("unowned staging removed", err)
	}
	_, hash, err := fleetConfig(f.c.Config)
	if err != nil {
		t.Fatal(err)
	}
	r := fleetInstallReceipt{Instruction: f.i, ConfigSHA256: hash, BeforeSHA256: strings.Repeat("a", 64), Phase: "failed"}
	if err := writeFleetReceipt(f.c, r); err != nil {
		t.Fatal(err)
	}
	unlock, err := commandLock(f.c.State, "fleet-update.lock", false)
	if err != nil {
		t.Fatal(err)
	}
	if err := collectFailedFleetStaging(context.Background(), f.c, command); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal("active transaction staging removed", err)
	}
	unlock()
	denied := func(context.Context, string, ...string) ([]byte, error) { return nil, errors.New("inspection refused") }
	if err := collectFailedFleetStaging(context.Background(), f.c, denied); err == nil {
		t.Fatal("absence refusal ignored")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal("refused inspection removed staging", err)
	}
	if err := collectFailedFleetStaging(context.Background(), f.c, command); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("eligible exact staging retained", err)
	}
	for _, keep := range []string{f.c.Binary, f.c.Config, unknown, filepath.Join(f.c.State, fleetUpdateReceiptFile)} {
		if _, err := os.Stat(keep); err != nil {
			t.Fatal("protected installation data removed", err)
		}
	}
}
