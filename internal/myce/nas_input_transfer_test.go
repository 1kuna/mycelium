package myce

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestNASInputSequentialResumeAndChecksum(t *testing.T) {
	for _, declared := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "shared"}[declared], func(t *testing.T) {
			_, clock := fixture(t)
			nas, _ := filepath.EvalSymlinks(t.TempDir())
			body := "one sequential source read, including small inputs"
			ref := sharedFixtureFile(t, nas, "input", body)
			target := filepath.Join(t.TempDir(), "local")
			var expected *FileRef
			if declared {
				expected = &ref
			}
			var recovering atomic.Bool
			ctx := context.WithValue(context.Background(), inputTransferRecoveryKey{}, &recovering)
			reads, pauses, bytes := 0, 0, 0
			client := Client{Clock: clock, Jitter: func() float64 { return 0 }, Pause: func(_ context.Context, delay time.Duration) error {
				pauses++
				if !recovering.Load() {
					t.Error("resume not observable")
				}
				if data, err := os.ReadFile(target); err != nil || string(data) != body[:7] {
					t.Errorf("prefix not retained: %q %v", data, err)
				}
				clock.Advance(delay)
				return nil
			}}
			read := func(f *os.File, p []byte) (int, error) {
				offset, _ := f.Seek(0, io.SeekCurrent)
				if offset != int64(bytes) {
					t.Errorf("source restarted: offset=%d transferred=%d", offset, bytes)
				}
				reads++
				if reads == 1 {
					n, _ := f.Read(p[:7])
					bytes += n
					return n, syscall.ECONNRESET
				}
				n, err := f.Read(p)
				bytes += n
				return n, err
			}
			if err := copyNASInput(ctx, client, filepath.Join(nas, "input"), target, expected, read); err != nil {
				t.Fatal(err)
			}
			if bytes != len(body) || pauses != 1 || recovering.Load() {
				t.Fatalf("transfer repeated or recovery stuck: bytes=%d pauses=%d", bytes, pauses)
			}
			if err := verifyFile(ctx, target, ref); err != nil {
				t.Fatal(err)
			}
		})
	}
	// Wrong authored bytes are terminal, with no recovery/publication shortcut.
	_, clock := fixture(t)
	nas, _ := filepath.EvalSymlinks(t.TempDir())
	ref := sharedFixtureFile(t, nas, "input", "wrong digest")
	ref.SHA256 = strings.Repeat("a", 64)
	client := Client{Clock: clock, Pause: func(context.Context, time.Duration) error { t.Fatal("retried integrity refusal"); return nil }}
	if err := copyNASInput(context.Background(), client, filepath.Join(nas, "input"), filepath.Join(t.TempDir(), "local"), &ref, nil); err == nil {
		t.Fatal("bad checksum accepted")
	}
}

func TestNASInputCancelledPrefixSurvivesCacheRestart(t *testing.T) {
	_, clock := fixture(t)
	nas, root := t.TempDir(), t.TempDir()
	body := "retain cancelled staging bytes for verified local resume"
	ref := sharedFixtureFile(t, nas, "input", body)
	input := SharedInput{NAS: "input", SHA256: ref.SHA256, SizeBytes: ref.SizeBytes}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache := &SharedInputCache{cache: EnvironmentCache{Root: root, Limit: 1 << 20, Clock: clock}, Client: Client{Clock: clock}, readNAS: func(f *os.File, p []byte) (int, error) {
		n, _ := f.Read(p[:9])
		cancel()
		return n, context.Canceled
	}}
	cache.refreshCapacity()
	if _, release, err := cache.Acquire(ctx, input, nas); !errors.Is(err, context.Canceled) || release != nil {
		t.Fatalf("cancelled stage published: %v", err)
	}
	<-onlySharedStaging(t, cache).done
	stage := filepath.Join(root, identityHash("input-stage:"+input.key())+".partial")
	if data, err := os.ReadFile(filepath.Join(stage, "data")); err != nil || string(data) != body[:9] {
		t.Fatalf("cancelled prefix discarded: %q %v", data, err)
	}
	// Recreate the cache object to exercise persisted partial admission, not an
	// in-memory retry. No second source read of the prefix is allowed.
	var transferred int
	resumed := &SharedInputCache{cache: EnvironmentCache{Root: root, Limit: 1 << 20, Clock: clock}, Client: Client{Clock: clock}, readNAS: func(f *os.File, p []byte) (int, error) {
		offset, _ := f.Seek(0, io.SeekCurrent)
		if offset != int64(9+transferred) {
			t.Errorf("restart lost source offset: %d", offset)
		}
		n, err := f.Read(p)
		transferred += n
		return n, err
	}}
	resumed.refreshCapacity()
	if got := resumed.capacityReport(); got.EvictableBytes == 0 {
		t.Fatal("abandoned resumable prefix permanently withholds admission")
	}
	path, release, err := resumed.Acquire(context.Background(), input, nas)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if transferred != len(body)-9 {
		t.Fatalf("retransferred prefix: %d", transferred)
	}
	if err := verifyFile(context.Background(), path, ref); err != nil {
		t.Fatal(err)
	}
}
