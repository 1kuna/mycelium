package main

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"syscall"
	"time"

	"mycelium/internal/myce"
)

// Private, durable registration, outside any project checkout. Direct authority
// is stored only in this user-only directory/file, never a receipt or process
// argument; installed authority is re-resolved on each retry.
type sharedProfileRegistration struct {
	NotifyEnv []string        `json:"notify_env,omitempty"`
	Directory string          `json:"directory"`
	Observer  profileObserver `json:"observer"`
	Token     string          `json:"token,omitempty"`
	Expires   time.Time       `json:"expires"`
}

func sharedProfileRoot() (string, error) {
	root, err := os.UserCacheDir()
	if err != nil {
		return "", err
	}
	root = filepath.Join(root, "myce", "observers")
	if err = os.MkdirAll(root, 0700); err != nil {
		return "", err
	}
	return root, nil
}
func registerSharedObserver(ctx context.Context, dir string, cfg profileObserver, token string) error {
	root, err := sharedProfileRoot()
	if err != nil {
		return err
	}
	dir, err = filepath.Abs(dir)
	if err != nil {
		return err
	}
	startupCtx, cancelStartup := context.WithTimeout(ctx, 10*time.Second)
	defer cancelStartup()
	start, err := sharedProfileLock(startupCtx, filepath.Join(root, "startup.lock"))
	if err != nil {
		return err
	}
	defer start.Close()
	registration := sharedProfileRegistration{NotifyEnv: notifierEnvironment(), Directory: dir, Observer: cfg, Token: token, Expires: time.Now().Add(myce.DefaultWaitTimeout)}
	if err = saveProfileJSON(filepath.Join(root, profileHash(dir)+".json"), registration); err != nil {
		return err
	}
	lock, err := profileLock(filepath.Join(root, "observer.lock"))
	if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
		return nil
	}
	if err != nil {
		return err
	}
	defer lock.Close()
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	ready, writeReady, err := os.Pipe()
	if err != nil {
		return err
	}
	defer ready.Close()
	defer writeReady.Close()
	cmd := exec.Command(executable, "wait", "--observer-dir", root, "--detached")
	cmd.Env = append(notifierEnvironment(), "MYCE_SHARED_OBSERVER=1")
	cmd.ExtraFiles = []*os.File{lock, writeReady}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err = cmd.Start(); err != nil {
		return err
	}
	writeReady.Close()
	ack := make(chan error, 1)
	go func() { var b [1]byte; _, e := io.ReadFull(ready, b[:]); ack <- e }()
	startup, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	select {
	case err = <-ack:
	case <-startup.Done():
		err = startup.Err()
	}
	if err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return err
	}
	return cmd.Process.Release()
}

type sharedProfileWatch struct {
	registration sharedProfileRegistration
	receipt      profileReceipt
	lock         *os.File
	unknownSince time.Time
}
type sharedProfileGroup struct {
	unknownOffset int
	next          time.Time
	delay         time.Duration
	after         map[string]uint64
}

func waitSharedProfiles(ctx context.Context, root string) error {
	if os.Getenv("MYCE_SHARED_OBSERVER") != "1" {
		return waitError(2, "shared observer is started by profile submission")
	}
	syscall.CloseOnExec(3)
	lock := os.NewFile(3, "observer.lock")
	defer lock.Close()
	ready := os.NewFile(4, "ready")
	_, err := ready.Write([]byte{1})
	ready.Close()
	if err != nil {
		return err
	}
	return runSharedProfileLoop(ctx, root, lock, myce.RealClock{}, waitPause)
}

func runSharedProfileLoop(ctx context.Context, root string, lock *os.File, clock myce.Clock, pause func(context.Context, time.Duration) error) error {
	watches := map[string]*sharedProfileWatch{}
	defer func() {
		for _, w := range watches {
			w.lock.Close()
		}
	}()
	finish := func(entry string, j *myce.Job, cause error) bool {
		watch := watches[entry]
		if watch == nil {
			return true
		}
		_ = finishProfileObservation(ctx, watch.registration.Directory, watch.receipt, watch.registration.Observer, j, cause, watch.registration.NotifyEnv)
		var saved profileReceipt
		if readProfileJSON(filepath.Join(watch.registration.Directory, "receipt.json"), &saved) != nil || saved.ArmNonce != watch.receipt.ArmNonce || saved.Observation != "not_armed" {
			return false
		}
		if watch.registration.Observer.Notify != "" && saved.DeliveryStatus != "delivered" && saved.DeliveryStatus != "failed" {
			return false
		}
		// Keep ownership until both receipt persistence and registration retirement
		// succeed. A rearm cannot be deleted by a late old-owner cleanup.
		if err := os.Remove(entry); err != nil && !os.IsNotExist(err) {
			return false
		}
		delete(watches, entry)
		watch.lock.Close()
		return true
	}
	groups := map[string]*sharedProfileGroup{}
	for {
		entries, err := filepath.Glob(filepath.Join(root, "*.json"))
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if watches[entry] != nil {
				continue
			}
			var reg sharedProfileRegistration
			if readProfileJSON(entry, &reg) != nil {
				continue
			}
			receiptLock, e := profileLock(filepath.Join(reg.Directory, "watch.lock"))
			if e != nil {
				if os.IsNotExist(e) {
					reclaimSharedRegistration(root, entry)
				}
				continue
			}
			// Registration is replaceable while waiting for the receipt lock.
			// Reload both records only after ownership is established.
			directory := reg.Directory
			if readProfileJSON(entry, &reg) != nil || reg.Directory != directory {
				receiptLock.Close()
				continue
			}
			var receipt profileReceipt
			if e = readProfileJSON(filepath.Join(reg.Directory, "receipt.json"), &receipt); e != nil {
				if os.IsNotExist(e) {
					_ = os.Remove(entry)
				}
				receiptLock.Close()
				continue
			}
			if receipt.Observation != "armed" {
				_ = os.Remove(entry)
				receiptLock.Close()
				continue
			}
			watches[entry] = &sharedProfileWatch{registration: reg, receipt: receipt, lock: receiptLock}
		}
		if len(watches) == 0 {
			// Serialize idle exit with registration/startup. Release the lifetime lock
			// while holding startup.lock so a submission cannot mistake an exiting
			// process for its observer.
			start, e := profileLock(filepath.Join(root, "startup.lock"))
			if e == nil {
				entries, _ = filepath.Glob(filepath.Join(root, "*.json"))
				if len(entries) == 0 {
					lock.Close()
					start.Close()
					return nil
				}
				start.Close()
			}
		}
		byAuthority := map[string][]string{}
		for entry, w := range watches {
			key := profileHash(struct{ Queue, Config, Joining, Token string }{w.registration.Observer.Queue, w.registration.Observer.Config, w.registration.Observer.Joining, w.registration.Token})
			byAuthority[key] = append(byAuthority[key], entry)
		}
		for key, entries := range byAuthority {
			group := groups[key]
			if group == nil {
				group = &sharedProfileGroup{after: map[string]uint64{}}
				groups[key] = group
			}
			sort.Strings(entries)
			// Expiry is per receipt, including during backoff or a prolonged outage.
			active := entries[:0]
			for _, entry := range entries {
				w := watches[entry]
				if !clock.Now().Before(w.registration.Expires) || w.receipt.JobID == "" {
					cause := context.DeadlineExceeded
					if w.receipt.JobID == "" {
						cause = errors.New("receipt has no job")
					}
					finish(entry, nil, cause)
				} else {
					active = append(active, entry)
				}
			}
			entries = active
			if len(entries) == 0 || clock.Now().Before(group.next) {
				continue
			}
			sample := watches[entries[0]].registration
			c, e := runClient(sample.Observer.Config, sample.Observer.Joining, sample.Observer.Queue, sample.Token)
			if e != nil {
				group.delay = min(max(3*time.Second, group.delay*2), 30*time.Second)
				group.next = clock.Now().Add(group.delay)
				continue
			}
			for start := 0; start < len(entries); start += 256 {
				batch := entries[start:min(start+256, len(entries))]
				ids := make([]string, 0, len(batch))
				for _, entry := range batch {
					ids = append(ids, watches[entry].receipt.JobID)
				}
				readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
				reply, readErr := c.WaitBatch(readCtx, ids, group.after, 0)
				cancel()
				if readErr != nil {
					// Unknown IDs must not poison their healthy batch peers. Resolve this
					// batch individually only on a permanent unknown response.
					var api *myce.HTTPError
					if errors.As(readErr, &api) && api.Code == "job_unknown" {
						// Probe at most five rows per pass and rotate, so an
						// unknown receipt cannot consume the source allowance
						// repeatedly ahead of its healthy batch peers.
						probeCount := min(5, len(batch))
						for n := 0; n < probeCount; n++ {
							entry := batch[(group.unknownOffset+n)%len(batch)]
							w := watches[entry]
							one, cancel := context.WithTimeout(ctx, 10*time.Second)
							j, jobErr := c.GetJobForWait(one, w.receipt.JobID)
							cancel()
							if jobErr == nil {
								if j.State == "succeeded" || j.State == "failed" || j.State == "cancelled" {
									finish(entry, j, nil)
								}
								continue
							}
							if errors.As(jobErr, &api) && api.Code == "job_unknown" {
								if w.unknownSince.IsZero() {
									w.unknownSince = clock.Now()
								}
								if clock.Now().Sub(w.unknownSince) < 5*time.Second {
									continue
								}
							}
							if !myce.RetryStatusRead(jobErr) {
								finish(entry, nil, jobErr)
							}
						}
						group.unknownOffset = (group.unknownOffset + probeCount) % len(batch)
					} else if !myce.RetryStatusRead(readErr) {
						for _, entry := range batch {
							finish(entry, nil, readErr)
						}
					}
					group.delay = min(max(3*time.Second, group.delay*2), 30*time.Second)
					if errors.As(readErr, &api) && api.Code == "job_unknown" {
						group.delay = 3 * time.Second
					}
					group.next = clock.Now().Add(group.delay)
					continue
				}
				group.delay = 0
				group.next = time.Time{}
				for i, state := range reply.Jobs {
					group.after[state.ID] = state.Revision
					if state.State != "succeeded" && state.State != "failed" && state.State != "cancelled" {
						continue
					}
					entry := batch[i]
					finalCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
					j, finalErr := c.GetJobForWait(finalCtx, state.ID)
					cancel()
					if finalErr != nil {
						if !myce.RetryStatusRead(finalErr) {
							finish(entry, nil, finalErr)
						} else {
							group.delay = min(max(3*time.Second, group.delay*2), 30*time.Second)
							group.next = clock.Now().Add(group.delay)
						}
						continue
					}
					if j.State == "succeeded" || j.State == "failed" || j.State == "cancelled" {
						finish(entry, j, nil)
					}
				}
			}
			if c.HTTP != nil {
				c.HTTP.CloseIdleConnections()
			}
		}
		if err := pause(ctx, 3*time.Second); err != nil {
			return err
		}
	}
}

// Context-bounded serialization for simultaneous submissions in different
// worktrees. The lifetime lock remains nonblocking, to adopt a live observer.
func sharedProfileLock(ctx context.Context, path string) (*os.File, error) {
	for {
		lock, err := profileLock(path)
		if err == nil {
			return lock, nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
			return nil, err
		}
		if err := waitPause(ctx, 10*time.Millisecond); err != nil {
			return nil, err
		}
	}
}

// An unloaded registration cannot hold the daemon open after its receipt has
// disappeared. Registration writers serialize on startup.lock; recheck under
// that lock before retiring the stale entry, so a rearm is never removed.
func reclaimSharedRegistration(root, entry string) {
	lock, err := profileLock(filepath.Join(root, "startup.lock"))
	if err != nil {
		return
	}
	defer lock.Close()
	var latest sharedProfileRegistration
	if readProfileJSON(entry, &latest) != nil {
		return
	}
	_, statErr := os.Stat(filepath.Join(latest.Directory, "receipt.json"))
	if os.IsNotExist(statErr) {
		_ = os.Remove(entry)
	}
}
