package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// renameStoragePath preserves atomic directory publication on SMB servers that
// refuse a rename while CIFS retains closed child handles. Files are not retried.
func renameStoragePath(ctx context.Context, from, to string) error {
	return renameStorageDirectory(ctx, from, func() error { return os.Rename(from, to) })
}

func renameStorageDirectory(ctx context.Context, source string, rename func() error) error {
	return renameStorageDirectoryWith(ctx, source, rename, storageCloseWindow, storageRenamePause)
}

func storageRenamePause(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func renameStorageDirectoryWith(ctx context.Context, source string, rename func() error, window func(string) time.Duration, pause func(context.Context, time.Duration) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	err := rename()
	if !errors.Is(err, syscall.EACCES) && !errors.Is(err, syscall.EBUSY) {
		return err
	}
	info, statErr := os.Lstat(source)
	if statErr != nil || !info.IsDir() {
		return err
	}
	// One deferred-close lifetime plus one lifetime of server/scheduling slack.
	// Permanent permissions/busy errors cost at most these waits; no copy, fence
	// or durability operation is replayed. Syscall time itself is not bounded.
	closeWindow := window(source)
	if closeWindow <= 0 {
		return err
	}
	remaining, delay := 2*closeWindow, closeWindow/16
	if delay <= 0 {
		delay = time.Nanosecond
	}
	for remaining > 0 {
		wait := min(delay, remaining)
		if e := pause(ctx, wait); e != nil {
			return errors.Join(err, e)
		}
		remaining -= wait
		if e := ctx.Err(); e != nil {
			return errors.Join(err, e)
		}
		err = rename()
		if !errors.Is(err, syscall.EACCES) && !errors.Is(err, syscall.EBUSY) {
			return err
		}
		delay = min(2*delay, max(closeWindow/4, time.Nanosecond))
	}
	return err
}

func storageCloseWindow(source string) time.Duration {
	// Linux mountinfo includes the effective CIFS superblock options. Resolve
	// aliases before selecting the most specific mount; never print mount data.
	source, err := filepath.EvalSymlinks(source)
	if err != nil {
		return time.Second
	}
	source, err = filepath.Abs(source)
	if err != nil {
		return time.Second
	}
	data, _ := os.ReadFile("/proc/self/mountinfo")
	return storageCloseWindowFromMounts(source, string(data))
}

func storageCloseWindowFromMounts(source, mounts string) time.Duration {
	// mount.cifs(8): SMB2/3 closetimeo defaults to one second. Use that physical
	// basis when mount inventory is unavailable (including non-Linux clients).
	window, longest := time.Second, -1
	unescape := strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`)
	for _, line := range strings.Split(mounts, "\n") {
		left, right, ok := strings.Cut(line, " - ")
		fields, fs := strings.Fields(left), strings.Fields(right)
		if !ok || len(fields) < 6 || len(fs) < 3 {
			continue
		}
		mount := unescape.Replace(fields[4])
		if !withinNAS(mount, source) || len(mount) <= longest {
			continue
		}
		longest, window = len(mount), time.Second
		if fs[0] != "cifs" && fs[0] != "smb3" {
			continue
		}
		for _, option := range strings.Split(fs[2], ",") {
			if raw, ok := strings.CutPrefix(option, "closetimeo="); ok {
				if seconds, err := strconv.ParseUint(raw, 10, 32); err == nil {
					window = time.Duration(seconds) * time.Second
				}
			}
		}
	}
	return window
}
