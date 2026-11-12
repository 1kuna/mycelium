package myce

import (
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// A worker's local extension library only grows: every prepared registration
// stays and is reported on each heartbeat. Registrations with no job within the
// retained-job window are dropped; a job that needs one again reinstalls it
// from the queue registry and re-qualifies. The job-record window (14 days),
// not the 3-day file window, so work that runs weekly never re-qualifies
// right before it is needed.
const DefaultExtensionRetention = jobRecordRetention

const extensionUseMarker = "last_used"

// Record a job's use of a registration. Failure only shortens retention.
func touchExtensionUse(root string, e Extension, now time.Time) {
	dir := filepath.Join(root, "cache", e.Name, e.Version)
	if os.MkdirAll(dir, 0700) != nil {
		return
	}
	p := filepath.Join(dir, extensionUseMarker)
	if f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY, 0600); err == nil {
		_ = f.Close()
	}
	_ = os.Chtimes(p, now, now)
}

func extensionLastUse(root string, e Extension) time.Time {
	if st, err := os.Stat(filepath.Join(root, "cache", e.Name, e.Version, extensionUseMarker)); err == nil {
		return st.ModTime()
	}
	return time.Time{}
}

// Remove registrations unused for the window. A registration whose use lock is
// held by a preparation or running attempt stays.
func pruneUnusedExtensions(root string, now time.Time, window time.Duration) (removed []string) {
	if root == "" || window <= 0 {
		return nil
	}
	extensions, err := LocalExtensions(root)
	if err != nil {
		return nil
	}
	for _, e := range extensions {
		last := extensionLastUse(root, e)
		if last.IsZero() {
			// Installed before use markers existed: its last use is unknown, so it
			// gets a full window from the first time this worker sees it.
			touchExtensionUse(root, e, now)
			continue
		}
		if now.Sub(last) < window {
			continue
		}
		cache := filepath.Join(root, "cache", e.Name, e.Version)
		release, ok := lockAllExtensionPlatforms(cache)
		if !ok {
			continue
		}
		err := os.RemoveAll(extensionFolder(root, e))
		if err == nil {
			err = os.RemoveAll(cache)
		}
		release()
		if err == nil {
			removed = append(removed, extensionKey(e))
			_ = os.Remove(filepath.Join(root, "library", e.Name))
			_ = os.Remove(filepath.Join(root, "cache", e.Name))
		}
	}
	return removed
}

func lockAllExtensionPlatforms(cache string) (func(), bool) {
	var held []*os.File
	release := func() {
		for _, f := range held {
			_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
			_ = f.Close()
		}
	}
	platforms, err := os.ReadDir(cache)
	if err != nil && !os.IsNotExist(err) {
		return nil, false
	}
	for _, p := range platforms {
		if !p.IsDir() {
			continue
		}
		f, err := os.OpenFile(filepath.Join(cache, p.Name(), "use.lock"), os.O_RDWR, 0600)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			release()
			return nil, false
		}
		if syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) != nil {
			_ = f.Close()
			release()
			return nil, false
		}
		held = append(held, f)
	}
	return release, true
}

func workerNow(c Clock) time.Time {
	if c == nil {
		return RealClock{}.Now()
	}
	return c.Now()
}
