package main

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
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
		return requestObserverUpgrade(root)
	}
	if err != nil {
		return err
	}
	defer lock.Close()
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	return startSharedObserver(ctx, executable, root, lock)
}

func startSharedObserver(ctx context.Context, executable, root string, lock *os.File) error {
	ready, writeReady, err := os.Pipe()
	if err != nil {
		return err
	}
	defer ready.Close()
	defer writeReady.Close()
	cmd := exec.Command(executable, "wait", "--observer-dir", root, "--detached")
	cmd.Env = append(notifierEnvironment(), "MYCE_SHARED_OBSERVER=1")
	output, err := os.OpenFile(filepath.Join(root, "observer.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer output.Close()
	// Real file descriptors survive submitter exit; exec copy pipes do not.
	cmd.Stdout, cmd.Stderr = output, output
	cmd.ExtraFiles = []*os.File{lock, writeReady}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if os.Getenv("MYCE_DEV_SESSION_ROOT") != "" || os.Getenv("MYCE_PROCESS_SESSION") != "" {
		// A dev command or worker attempt owns this session. Its queue/test
		// observer must remain in that session for exact-release cleanup, even
		// after the submitting command exits. Setsid would escape that custody.
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
	finishing    bool
	finalJob     *myce.Job
	finalCause   error
}
type sharedProfileGroup struct {
	unknownOffset int
	next          time.Time
	pace          myce.ClientPace
	after         map[string]uint64
	pending       bool
	cancel        context.CancelFunc
	retrying      bool
}

func waitSharedProfiles(ctx context.Context, root string) error {
	if os.Getenv("MYCE_SHARED_OBSERVER") != "1" {
		return waitError(2, "shared observer is started by profile submission")
	}
	log.SetOutput(&observerLogWriter{root: root})
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
	identity, e := os.Stat(socketPath)
	if e != nil {
		socket.Close()
		return e
	}
	defer func() { socket.Close(); _ = retireSharedProfileSocket(root, socketPath, identity) }()

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
	defer ready.Close()
	return runSharedProfileLoopStarted(ctx, root, lock, myce.RealClock{}, adaptivePause, func() error { _, err := ready.Write([]byte{1}); return err }, wakes)
}

type sharedProfileFinish struct {
	entry string
	watch *sharedProfileWatch
	job   *myce.Job
	cause error
}
type sharedProfileRead struct {
	key       string
	owner     *sharedProfileGroup
	group     sharedProfileGroup
	finished  []sharedProfileFinish
	cancelled bool
}

func sharedProfileAuthority(reg sharedProfileRegistration) string {
	return profileHash(struct{ Queue, Config, Joining, Token string }{reg.Observer.Queue, reg.Observer.Config, reg.Observer.Joining, reg.Token})
}

// The coordinator owns receipts and their fences. Each authority has one
// independent, cancellable network operation; a silent server cannot withhold
// another authority's completion or an owner's deadline.
func runSharedProfileLoop(ctx context.Context, root string, lock *os.File, clock myce.Clock, pause func(context.Context, time.Duration) error, events ...<-chan struct{}) error {
	return runSharedProfileLoopStarted(ctx, root, lock, clock, pause, nil, events...)
}

func runSharedProfileLoopStarted(ctx context.Context, root string, lock *os.File, clock myce.Clock, pause func(context.Context, time.Duration) error, startup func() error, events ...<-chan struct{}) error {
	ctx, cancelAll := context.WithCancel(ctx)
	var workers sync.WaitGroup
	readerCtx, cancelReaders := context.WithCancel(ctx)
	defer func() { cancelReaders(); cancelAll(); workers.Wait() }()
	diagnostics := newObserverDiagnostics(root, clock)
	if err := diagnostics.record("started", "", nil, true); err != nil {
		return err
	}
	defer func() {
		var owner observerDiagnostic
		if readProfileJSON(filepath.Join(root, "observer.status"), &owner) == nil && owner.PID == os.Getpid() && owner.StartedAt.Equal(diagnostics.state.StartedAt) {
			_ = diagnostics.record("stopped", "", ctx.Err(), true)
		}
	}()
	if startup != nil {
		if err := startup(); err != nil {
			return err
		}
	}
	watches := map[string]*sharedProfileWatch{}
	bindings := map[string]*observerBindingPace{}
	var pass uint64
	defer func() {
		for _, w := range watches {
			w.lock.Close()
		}
	}()
	finish := func(entry string, j *myce.Job, cause error) bool {
		// Shutdown hands durable registrations to the next owner. Cancellation
		// is not an observation failure and must never disarm a pending batch.
		if ctx.Err() != nil {
			return false
		}
		watch := watches[entry]
		if watch == nil {
			return true
		}
		if !watch.finishing {
			watch.finishing, watch.finalJob, watch.finalCause = true, j, cause
		}
		j, cause = watch.finalJob, watch.finalCause
		binding := watch.registration.Observer.Notify
		pace := bindings[binding]
		if pace == nil {
			pace = &observerBindingPace{}
			bindings[binding] = pace
		}
		stale := binding != "" && j != nil && j.FinishedAt != nil && j.FinishedAt.Before(diagnostics.state.StartedAt)
		if stale && !pace.ready(clock.Now(), pass) {
			return false
		}
		started := clock.Now()
		_ = diagnostics.record("notifier", watch.receipt.JobID, nil, false)
		// A failed registration unlink must not invoke a completed notifier again.
		var previous profileReceipt
		alreadyFinished := readProfileJSON(filepath.Join(watch.registration.Directory, "receipt.json"), &previous) == nil && previous.ArmNonce == watch.receipt.ArmNonce && previous.Observation == "not_armed" && (binding == "" || previous.DeliveryStatus == "delivered" || previous.DeliveryStatus == "failed")
		var finishErr error
		if !alreadyFinished {
			finishErr = finishProfileObservation(ctx, watch.registration.Directory, watch.receipt, watch.registration.Observer, j, cause, watch.registration.NotifyEnv)
		}
		var saved profileReceipt
		if readProfileJSON(filepath.Join(watch.registration.Directory, "receipt.json"), &saved) != nil || saved.ArmNonce != watch.receipt.ArmNonce || saved.Observation != "not_armed" {
			_ = diagnostics.record("receipt_persistence_failed", watch.receipt.JobID, finishErr, true)
			return false
		}
		if watch.registration.Observer.Notify != "" && saved.DeliveryStatus != "delivered" && saved.DeliveryStatus != "failed" {
			return false
		}
		// Keep ownership until both receipt persistence and registration retirement
		// succeed. A rearm cannot be deleted by a late old-owner cleanup.
		if err := os.Remove(entry); err != nil && !os.IsNotExist(err) {
			_ = diagnostics.record("registration_retirement_failed", watch.receipt.JobID, err, true)
			return false
		}
		delete(watches, entry)
		watch.lock.Close()
		if stale && !alreadyFinished {
			pace.pass = pass
			remaining := 0
			for _, peer := range watches {
				if peer.registration.Observer.Notify == binding {
					remaining++
				}
			}
			pace.observed(started, clock.Now(), remaining)
		}
		if saved.DeliveryStatus == "delivered" {
			diagnostics.update(func(state *observerDiagnostic) { state.Delivered++; state.LastDeliveryAt = clock.Now() })
		}
		if saved.DeliveryStatus == "failed" {
			diagnostics.update(func(state *observerDiagnostic) { state.Failed++ })
		}
		_ = diagnostics.record("receipt_finished", watch.receipt.JobID, finishErr, true)
		return true
	}
	groups := map[string]*sharedProfileGroup{}
	completed := make(chan sharedProfileRead)
	var wake <-chan struct{}
	if len(events) > 0 {
		wake = events[0]
	}
	discovery := myce.ClientPace{}
	previousCount := -1
	apply := func(result sharedProfileRead) {
		group := groups[result.key]
		if group == nil || group != result.owner {
			return
		}
		group.pending = false
		group.cancel = nil
		if !result.cancelled {
			*group = result.group
		}
		if result.cancelled {
			return
		}
		for _, action := range result.finished {
			if watches[action.entry] == action.watch {
				finish(action.entry, action.job, action.cause)
			}
		}
	}
	nextWake := func() (pending bool, due time.Time) {
		for _, watch := range watches {
			if watch.finishing {
				if pace := bindings[watch.registration.Observer.Notify]; pace != nil && (due.IsZero() || pace.next.Before(due)) {
					due = pace.next
				}
				continue
			}
			// A delivery-only authority has no outstanding read demand. Its old
			// read timer must not turn a future delivery deadline into busy polling.
			if group := groups[sharedProfileAuthority(watch.registration)]; group != nil {
				if group.pending {
					pending = true
				} else if !group.next.IsZero() && (due.IsZero() || group.next.Before(due)) {
					due = group.next
				}
			}
			if end := watch.registration.Expires; !end.IsZero() && (due.IsZero() || end.Before(due)) {
				due = end
			}
		}
		return
	}
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		pass++
		quiesce := func() {
			cancelReaders()
			workers.Wait()
			for _, group := range groups {
				group.pending = false
				group.cancel = nil
			}
			readerCtx, cancelReaders = context.WithCancel(ctx)
		}
		if handed, err := tryObserverHandoff(ctx, root, lock, watches, diagnostics, quiesce); handed {
			return nil
		} else if err != nil {
			_ = diagnostics.record("handoff_failed", "", err, true)
		}
		if err := diagnostics.record("discovery", "", nil, false); err != nil {
			return err
		}
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
			key := sharedProfileAuthority(reg)
			if group := groups[key]; group != nil {
				if group.pending {
					group.cancel()
				}
				if !group.retrying {
					group.next = time.Time{}
				}
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

		diagnostics.update(func(state *observerDiagnostic) { state.Registrations = len(watches) })
		byAuthority := map[string][]string{}
		for entry, w := range watches {
			if w.finishing {
				finish(entry, w.finalJob, w.finalCause)
				continue
			}
			key := sharedProfileAuthority(w.registration)
			byAuthority[key] = append(byAuthority[key], entry)
		}
		for key, entries := range byAuthority {
			group := groups[key]
			if group == nil {
				group = &sharedProfileGroup{after: map[string]uint64{}}
				groups[key] = group
			}
			sort.Strings(entries)
			active := entries[:0]
			for _, entry := range entries {
				w := watches[entry]
				if w.finishing {
					finish(entry, w.finalJob, w.finalCause)
					continue
				}
				if !w.registration.Expires.IsZero() && !clock.Now().Before(w.registration.Expires) || w.receipt.JobID == "" {
					cause := error(context.DeadlineExceeded)
					if w.receipt.JobID == "" {
						cause = errors.New("receipt has no job")
					}
					finish(entry, nil, cause)
				} else {
					active = append(active, entry)
				}
			}
			entries = active
			if len(entries) == 0 || group.pending || clock.Now().Before(group.next) {
				continue
			}
			snapshot := make(map[string]*sharedProfileWatch, len(entries))
			for _, entry := range entries {
				snapshot[entry] = watches[entry]
			}
			state := *group
			state.pending = false
			state.cancel = nil
			state.after = make(map[string]uint64, len(group.after))
			for id, revision := range group.after {
				state.after[id] = revision
			}
			requestCtx, stop := context.WithCancel(readerCtx)
			group.pending = true
			group.cancel = stop
			workers.Add(1)
			go func(key string, entries []string, state sharedProfileGroup, snapshot map[string]*sharedProfileWatch, owner *sharedProfileGroup, operationCtx context.Context) {
				defer workers.Done()
				defer stop()
				result := readSharedProfileAuthority(requestCtx, entries, snapshot, state, clock, diagnostics)
				result.key = key
				result.owner = owner
				select {
				case completed <- result:
				case <-operationCtx.Done():
				}
			}(key, append([]string(nil), entries...), state, snapshot, group, readerCtx)
		}
		// Retired and delivery-only groups have no network demand to schedule.
		live := map[string]bool{}
		for _, w := range watches {
			if !w.finishing {
				live[sharedProfileAuthority(w.registration)] = true
			}
		}
		for key, group := range groups {
			if !live[key] {
				if group.cancel != nil {
					group.cancel()
				}
				delete(groups, key)
			}
		}
		discovery.Observe(clock.Now().Sub(passStarted), previousCount != len(watches))
		previousCount = len(watches)
		delay := discovery.Delay(clock.Now(), time.Time{}, nil, nil)
		pending, due := nextWake()
		diagnostics.update(func(state *observerDiagnostic) {
			state.NextReadAt, state.NextDeliveryAt = time.Time{}, time.Time{}
			for _, group := range groups {
				if !group.pending && clock.Now().Before(group.next) && (state.NextReadAt.IsZero() || group.next.Before(state.NextReadAt)) {
					state.NextReadAt = group.next
				}
			}
			for _, pace := range bindings {
				if clock.Now().Before(pace.next) && (state.NextDeliveryAt.IsZero() || pace.next.Before(state.NextDeliveryAt)) {
					state.NextDeliveryAt = pace.next
				}
			}
		})
		if err := diagnostics.record("observing", "", nil, false); err != nil {
			return err
		}
		if pending {
			var timer *time.Timer
			var timerC <-chan time.Time
			if !due.IsZero() {
				timer = time.NewTimer(max(0, due.Sub(clock.Now())))
				timerC = timer.C
			}
			select {
			case result := <-completed:
				apply(result)
			case <-wake:
				for _, group := range groups {
					if group.cancel != nil {
						group.cancel()
					}
				}
			case <-timerC:
			case <-ctx.Done():
				if timer != nil {
					timer.Stop()
				}
				return ctx.Err()
			}
			if timer != nil {
				timer.Stop()
			}
			// The response can advance the next read or retire the final watch.
			// Never sleep using the discovery delay computed before that response.
			pending, due = nextWake()
			if pending || len(watches) == 0 {
				continue
			}
		}
		if !due.IsZero() {
			delay = min(delay, max(0, due.Sub(clock.Now())))
		}
		if err := pause(ctx, max(time.Nanosecond, delay)); err != nil {
			return err
		}
	}
}

func readSharedProfileAuthority(ctx context.Context, entries []string, watches map[string]*sharedProfileWatch, state sharedProfileGroup, clock myce.Clock, diagnostics ...*observerDiagnostics) (result sharedProfileRead) {
	group := &state
	var trace *observerReadTrace
	if len(diagnostics) > 0 {
		trace = diagnostics[0].beginRead(len(entries))
	}
	defer trace.done()
	defer func() { result.group = state; result.cancelled = ctx.Err() != nil }()
	finish := func(entry string, j *myce.Job, cause error) bool {
		result.finished = append(result.finished, sharedProfileFinish{entry, watches[entry], j, cause})
		return true
	}
	started := clock.Now()
	sample := watches[entries[0]].registration
	c, e := runClient(sample.Observer.Config, sample.Observer.Joining, sample.Observer.Queue, sample.Token)
	if e != nil {
		trace.phase("authority_failed", "", e)
		group.pace.Observe(clock.Now().Sub(started), false)
		group.next = clock.Now().Add(group.pace.Delay(clock.Now(), time.Time{}, e, nil))
		group.retrying = true
		return
	}
	c = c.WithoutRetries()
	c.Clock = clock
batchLoop:
	for start := 0; start < len(entries); start += 256 {
		batch := entries[start:min(start+256, len(entries))]
		ids := make([]string, 0, len(batch))
		for _, entry := range batch {
			ids = append(ids, watches[entry].receipt.JobID)
		}
		readCtx, cancel := sharedProfileReadContext(ctx, batch, watches, clock)
		seconds := group.pace.Seconds(clock.Now(), time.Time{})
		// One outstanding request per authority; other authorities run
		// independently and the coordinator keeps processing deadlines.
		if len(entries) > 256 {
			seconds = 0
		}
		trace.phase("batch_read", "", nil)
		reply, readErr := c.WaitBatch(readCtx, ids, group.after, seconds)
		cancel()
		if readErr != nil {
			trace.phase("batch_read_failed", "", readErr)
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
					} else {
						group.pace.Observe(clock.Now().Sub(started), false)
						group.next = clock.Now().Add(group.pace.Delay(clock.Now(), time.Time{}, jobErr, nil))
						group.retrying = true
						break batchLoop
					}
				}
				group.unknownOffset = (group.unknownOffset + probeCount) % len(batch)
				// An unknown accepted receipt is local to this batch. Later
				// batches must still be observed; only overload stops the authority.
				continue
			} else if !myce.RetryStatusRead(readErr) {
				for _, entry := range batch {
					finish(entry, nil, readErr)
				}
			}
			group.pace.Observe(clock.Now().Sub(started), false)
			group.next = clock.Now().Add(group.pace.Delay(clock.Now(), time.Time{}, readErr, nil))
			group.retrying = true
			break
		}
		trace.phase("batch_read_complete", "", nil)
		group.retrying = false
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
			trace.phase("terminal_read", state.ID, nil)
			j, finalErr := c.GetJobForWait(finalCtx, state.ID)
			cancel()
			if finalErr != nil {
				trace.phase("terminal_read_failed", state.ID, finalErr)
				if !myce.RetryStatusRead(finalErr) {
					finish(entry, nil, finalErr)
				} else {
					group.pace.Observe(clock.Now().Sub(started), false)
					group.next = clock.Now().Add(group.pace.Delay(clock.Now(), time.Time{}, finalErr, nil))
					group.retrying = true
					break batchLoop
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
	return
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
