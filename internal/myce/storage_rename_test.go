package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// A filesystem whose closed child handle remains busy for a logical interval.
// It delegates the final mutation to disk so publication visibility is checked.
type deferredCloseFilesystem struct {
	t              *testing.T
	now, busyUntil time.Duration
	from, to       string
	errno          syscall.Errno
	calls          int
}

func (f *deferredCloseFilesystem) rename() error {
	f.calls++
	if f.now < f.busyUntil {
		if _, err := os.Stat(f.to); !os.IsNotExist(err) {
			f.t.Fatalf("partial publication visible: %v", err)
		}
		return &os.LinkError{Op: "rename", Old: f.from, New: f.to, Err: f.errno}
	}
	return os.Rename(f.from, f.to)
}
func (f *deferredCloseFilesystem) pause(_ context.Context, d time.Duration) error {
	f.now += d
	return nil
}

func TestStorageRenameDeferredClose(t *testing.T) {
	for _, errno := range []syscall.Errno{syscall.EACCES, syscall.EBUSY} {
		t.Run(errno.Error(), func(t *testing.T) {
			root := t.TempDir()
			f := deferredCloseFilesystem{t: t, from: filepath.Join(root, "generation.tmp"), to: filepath.Join(root, "generation"), errno: errno, busyUntil: time.Second}
			if err := os.Mkdir(f.from, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(f.from, "checkpoint.json"), []byte(`{"myce_checkpoint":{"generation":7}}`), 0600); err != nil {
				t.Fatal(err)
			}
			err := renameStorageDirectoryWith(context.Background(), f.from, f.rename, func(string) time.Duration { return time.Second }, f.pause)
			if err != nil {
				t.Fatal(err)
			}
			if f.calls < 2 || f.now < time.Second || f.now > 2*time.Second {
				t.Fatalf("calls=%d waits=%s", f.calls, f.now)
			}
			if generation, err := checkpointProgress(f.to); err != nil || generation != 7 {
				t.Fatalf("generation=%d err=%v", generation, err)
			}
			if _, err := os.Stat(f.from); !os.IsNotExist(err) {
				t.Fatalf("staging remains: %v", err)
			}
		})
	}
}

func TestStorageRenamePermanentErrorsAndCancellation(t *testing.T) {
	for _, tc := range []struct {
		name      string
		errno     syscall.Errno
		directory bool
		cancel    bool
		wantWait  time.Duration
	}{
		{"permanent_permission", syscall.EACCES, true, false, 2 * time.Second},
		{"permanent_busy", syscall.EBUSY, true, false, 2 * time.Second},
		{"other_error", syscall.EIO, true, false, 0},
		{"cross_device", syscall.EXDEV, true, false, 0},
		{"file", syscall.EACCES, false, false, 0},
		{"cancel", syscall.EACCES, true, true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := filepath.Join(t.TempDir(), "source")
			var err error
			if tc.directory {
				err = os.Mkdir(source, 0700)
			} else {
				err = os.WriteFile(source, nil, 0600)
			}
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			waited := time.Duration(0)
			original := &os.LinkError{Op: "rename", Old: source, New: source + "-final", Err: tc.errno}
			err = renameStorageDirectoryWith(ctx, source, func() error { calls++; return original }, func(string) time.Duration { return time.Second }, func(_ context.Context, d time.Duration) error {
				if tc.cancel {
					cancel()
					return ctx.Err()
				}
				waited += d
				return nil
			})
			if !errors.Is(err, original) || waited != tc.wantWait {
				t.Fatalf("err=%v waits=%s", err, waited)
			}
			if tc.cancel && !errors.Is(err, context.Canceled) {
				t.Fatalf("lost cancellation: %v", err)
			}
			if tc.wantWait == 0 && calls != 1 {
				t.Fatalf("unexpected retry: %d", calls)
			}
			if _, err := os.Stat(source); err != nil {
				t.Fatalf("failed publication lost source: %v", err)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := renameStorageDirectoryWith(ctx, "unused", func() error { t.Fatal("rename after cancellation"); return nil }, nil, nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestStorageRenameMountCloseWindow(t *testing.T) {
	mounts := `1 0 0:1 / / rw - ext4 local rw
2 1 0:2 / /nas rw - cifs //server/share rw,closetimeo=4
3 2 0:3 / /nas/nested rw - cifs //server/other rw,closetimeo=0
4 1 0:4 / /escaped\040share rw - cifs //server/space rw,closetimeo=3
5 1 0:5 / /invalid rw - cifs //server/invalid rw,closetimeo=invalid
6 2 0:6 / /nas/local rw - ext4 local rw
`
	for _, tc := range []struct {
		path string
		want time.Duration
	}{
		{"/nas/job/generation", 4 * time.Second}, {"/nas/nested/job", 0},
		{"/escaped share/job", 3 * time.Second}, {"/nas/local/job", time.Second},
		{"/nas-other/job", time.Second}, {"/invalid/job", time.Second},
	} {
		if got := storageCloseWindowFromMounts(tc.path, mounts); got != tc.want {
			t.Errorf("%s: %s want %s", tc.path, got, tc.want)
		}
	}
	if got := storageCloseWindowFromMounts("/unknown", ""); got != time.Second {
		t.Fatal(got)
	}
}
