package main

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
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
	if session := os.Getenv("MYCE_DEV_SESSION_ROOT"); session != "" {
		root := filepath.Join(session, "observers")
		return root, os.MkdirAll(root, 0700)
	}
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
	startupCtx, cancelStartup := context.WithCancel(ctx)
	defer cancelStartup()
	start, err := sharedProfileLock(startupCtx, filepath.Join(root, "startup.lock"))
	if err != nil {
		return err
	}
	defer start.Close()
	registration := sharedProfileRegistration{NotifyEnv: notifierEnvironment(), Directory: dir, Observer: cfg, Token: token, Expires: time.Time{}}
	if err = saveProfileJSON(filepath.Join(root, profileHash(dir)+".json"), registration); err != nil {
		return err
	}
	// Wake an existing observer immediately; registrations remain durable if
	// startup/recovery has not created its private socket yet.
	socketPath, socketErr := sharedProfileSocket(root)
	if wake, e := net.DialUnix("unixgram", nil, &net.UnixAddr{Name: socketPath, Net: "unixgram"}); socketErr == nil && e == nil {
		_, _ = wake.Write([]byte{1})
		wake.Close()
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
	if os.Getenv("MYCE_DEV_SESSION_ROOT") != "" {
		// The temporary queue cannot outlive its dev command. Keep this isolated
		// observer in that command's group so dev shutdown also retires it.
		cmd.SysProcAttr = nil
	}
	if err = cmd.Start(); err != nil {
		return err
	}
	writeReady.Close()
	ack := make(chan error, 1)
	go func() { var b [1]byte; _, e := io.ReadFull(ready, b[:]); ack <- e }()
	startup, cancel := context.WithCancel(ctx)
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
}
type sharedProfileGroup struct {
	unknownOffset int
	next          time.Time
	pace          myce.ClientPace
	after         map[string]uint64
}

func waitSharedProfiles(ctx context.Context, root string) error {
	if os.Getenv("MYCE_SHARED_OBSERVER") != "1" {
		return waitError(2, "shared observer is started by profile submission")
	}
	syscall.CloseOnExec(3)
	lock := os.NewFile(3, "observer.lock")
	defer lock.Close()
	// The lifetime lock proves any old socket belongs to an inactive observer.
	socketPath, socketErr := sharedProfileSocket(root)
	if socketErr != nil {
		return socketErr
	}
	if info, e := os.Lstat(socketPath); e == nil && info.Mode()&os.ModeSocket != 0 {
		_ = os.Remove(socketPath)
	}
	socket, e := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: socketPath, Net: "unixgram"})
	if e != nil {
		return e
	}
	defer os.Remove(filepath.Dir(socketPath))
	defer os.Remove(socketPath)
	defer socket.Close()
	wakes := make(chan struct{}, 1)
	done := make(chan struct{})
	defer func() { socket.Close(); <-done }()
	go func() {
		defer close(done)
		buffer := make([]byte, 1)
		for {
			if _, _, err := socket.ReadFromUnix(buffer); err != nil {
				return
			}
			select {
			case wakes <- struct{}{}:
			default:
			}
		}
	}()
	adaptivePause := func(ctx context.Context, d time.Duration) error {
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wakes:
			return nil
		case <-timer.C:
			return nil
		}
	}
	ready := os.NewFile(4, "ready")
	_, err := ready.Write([]byte{1})
	ready.Close()
	if err != nil {
		return err
	}
	return runSharedProfileLoop(ctx, root, lock, myce.RealClock{}, adaptivePause)
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
	discovery := myce.ClientPace{}
	previousCount := -1
	for {
		passStarted := clock.Now()
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
			if receipt.Spec.Deadline != nil && (reg.Expires.IsZero() || receipt.Spec.Deadline.Before(reg.Expires)) {
				reg.Expires = *receipt.Spec.Deadline
			}
			watches[entry] = &sharedProfileWatch{registration: reg, receipt: receipt, lock: receiptLock}
			for _, group := range groups {
				group.next = time.Time{}
			}
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
			started := clock.Now()
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
				if !w.registration.Expires.IsZero() && !clock.Now().Before(w.registration.Expires) || w.receipt.JobID == "" {
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
				group.pace.Observe(clock.Now().Sub(started), false)
				group.next = clock.Now().Add(group.pace.Delay(clock.Now(), time.Time{}, e, nil))
				continue
			}
			c = c.WithoutRetries()
			c.Clock = clock
			for start := 0; start < len(entries); start += 256 {
				batch := entries[start:min(start+256, len(entries))]
				ids := make([]string, 0, len(batch))
				for _, entry := range batch {
					ids = append(ids, watches[entry].receipt.JobID)
				}
				readCtx, cancel := sharedProfileReadContext(ctx, batch, watches, clock)
				seconds := group.pace.Seconds(clock.Now(), time.Time{})
				// More than one group/batch shares this coordinator. Those reads stay
				// nonblocking so one authority cannot hold another's completion.
				if len(byAuthority) > 1 || len(entries) > 256 {
					seconds = 0
				}
				reply, readErr := c.WaitBatch(readCtx, ids, group.after, seconds)
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
							one, cancel := sharedProfileReadContext(ctx, []string{entry}, watches, clock)
							j, jobErr := c.GetJobForWait(one, w.receipt.JobID)
							cancel()
							if jobErr == nil {
								if j.State == "succeeded" || j.State == "failed" || j.State == "cancelled" {
									finish(entry, j, nil)
								}
								continue
							}
							if errors.As(jobErr, &api) && api.Code == "job_unknown" {
								// This receipt records accepted intent. Visibility retry ends only
								// at its owner's deadline, not after a fixed grace period.
								continue
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
					group.pace.Observe(clock.Now().Sub(started), false)
					group.next = clock.Now().Add(group.pace.Delay(clock.Now(), time.Time{}, readErr, nil))
					continue
				}
				changed := false
				for _, state := range reply.Jobs {
					changed = changed || group.after[state.ID] != state.Revision
				}
				group.pace.Observe(clock.Now().Sub(started), changed)
				group.next = clock.Now().Add(group.pace.Delay(clock.Now(), time.Time{}, nil, nil))
				// A negotiated long-poll already paces unchanged jobs and wakes on a
				// revision. Re-enter it without an additional growing sleep.
				if seconds > 0 && len(reply.Jobs) > 0 && reply.Jobs[0].Revision > 0 {
					group.next = clock.Now()
				}
				for i, state := range reply.Jobs {
					group.after[state.ID] = state.Revision
					if state.State != "succeeded" && state.State != "failed" && state.State != "cancelled" {
						continue
					}
					entry := batch[i]
					finalCtx, cancel := sharedProfileReadContext(ctx, []string{entry}, watches, clock)
					j, finalErr := c.GetJobForWait(finalCtx, state.ID)
					cancel()
					if finalErr != nil {
						if !myce.RetryStatusRead(finalErr) {
							finish(entry, nil, finalErr)
						} else {
							group.pace.Observe(clock.Now().Sub(started), false)
							group.next = clock.Now().Add(group.pace.Delay(clock.Now(), time.Time{}, finalErr, nil))
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
		discovery.Observe(clock.Now().Sub(passStarted), previousCount != len(watches))
		previousCount = len(watches)
		delay := discovery.Delay(clock.Now(), time.Time{}, nil, nil)
		for _, group := range groups {
			if !group.next.IsZero() {
				delay = min(delay, max(0, group.next.Sub(clock.Now())))
			}
		}
		for _, watch := range watches {
			if !watch.registration.Expires.IsZero() {
				delay = min(delay, max(0, watch.registration.Expires.Sub(clock.Now())))
			}
		}
		if err := pause(ctx, delay); err != nil {
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

// Only explicit receipt/owner deadlines bound an observation, including a
// half-open read. Pending observers have no arbitrary maximum lifetime.
func sharedProfileReadContext(ctx context.Context, entries []string, watches map[string]*sharedProfileWatch, clock myce.Clock) (context.Context, context.CancelFunc) {
	var end time.Time
	for _, entry := range entries {
		if w := watches[entry]; w != nil && !w.registration.Expires.IsZero() && (end.IsZero() || w.registration.Expires.Before(end)) {
			end = w.registration.Expires
		}
	}
	if !end.IsZero() {
		return context.WithTimeout(ctx, max(0, end.Sub(clock.Now())))
	}
	return context.WithCancel(ctx)
}

// Unix-domain addresses have a physical path-length bound. Keep the private
// rendezvous short even when the installed cache/worktree path is long.
func sharedProfileSocket(root string) (string, error) {
	directory := filepath.Join("/tmp", "myce-"+strconv.Itoa(os.Getuid()))
	if err := os.MkdirAll(directory, 0700); err != nil {
		return "", err
	}
	info, err := os.Lstat(directory)
	if err != nil {
		return "", err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0700 || !ok || stat.Uid != uint32(os.Getuid()) {
		return "", errors.New("observer socket directory is not private")
	}
	return filepath.Join(directory, profileHash(root)+".sock"), nil
}
