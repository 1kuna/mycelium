package main

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	"mycelium/internal/myce"
)

// The batch-v1 protocol can deliberately hold a read for 20s. The notifier
// already owns a 30s delivery deadline. Add one transport recovery period
// (RFC 6298's initial RTO) before declaring either boundary silent. Completed
// reads and missed windows extend this allowance, without a deployment cap.
// A false suspicion costs only replay-safe observation and stable-ID delivery;
// it never changes compute, authority, submission keys or receipt arms.
const observerRecoveryPeriod = time.Second
const observerNotifierBudget = 30 * time.Second

type observerSupervision struct {
	PID          int               `json:"pid"`
	ChildPID     int               `json:"child_pid"`
	Restarts     int               `json:"restarts"`
	Window       time.Duration     `json:"window"`
	LastRecovery *observerRecovery `json:"last_recovery,omitempty"`
}
type observerRecovery struct {
	At         time.Time          `json:"at"`
	Reason     string             `json:"reason"`
	ExitCode   *int               `json:"exit_code,omitempty"`
	Signal     int                `json:"signal,omitempty"`
	Diagnostic observerDiagnostic `json:"diagnostic"`
}

func (s *observerSupervision) window() time.Duration {
	return max(20*time.Second+observerRecoveryPeriod, s.Window)
}
func (s *observerSupervision) stalled(now, started time.Time, d observerDiagnostic) bool {
	window := s.window()
	if d.PID != s.ChildPID {
		return !now.Before(started.Add(window))
	}
	// Inspect every outstanding read. A healthy peer or coordinator heartbeat
	// must not conceal a silent authority, even with no consumer deadline.
	for _, read := range d.Readers {
		if !now.Before(read.CapturedAt.Add(window)) {
			return true
		}
	}
	// Intentional pressure/delivery/discovery waits are not silence. Notifiers
	// run on the coordinator, so allow their existing deadline as well.
	due := d.CapturedAt
	if d.NextWakeAt.After(due) {
		due = d.NextWakeAt
	}
	return !now.Before(due.Add(max(window, observerNotifierBudget+observerRecoveryPeriod)))
}
func (s *observerSupervision) retain(root string, clock myce.Clock, reason string, exit error, d observerDiagnostic) error {
	recovery := &observerRecovery{At: clock.Now(), Reason: reason, Diagnostic: d}
	setObserverExit(recovery, exit)
	s.LastRecovery = recovery
	s.Restarts++
	if reason == "stalled" {
		s.Window = doubleObserverDuration(s.window())
	}
	// Separate from observer.status: child startup cannot erase the final
	// pre-recovery boundary. The log keeps earlier bounded recovery history.
	if err := saveProfileJSON(filepath.Join(root, "observer.supervision"), s); err != nil {
		return err
	}
	// The durable snapshot is sufficient if the bounded log is concurrently
	// owned by the child. Log contention must not stop automatic recovery.
	_ = (&observerLogWriter{root: root}).event(recovery)
	return nil
}
func setObserverExit(recovery *observerRecovery, exit error) {
	var status *exec.ExitError
	if errors.As(exit, &status) {
		code := status.ExitCode()
		recovery.ExitCode = &code
		if state, ok := status.Sys().(syscall.WaitStatus); ok && state.Signaled() {
			recovery.Signal = int(state.Signal())
		}
	}
}

func doubleObserverDuration(d time.Duration) time.Duration {
	const largest = time.Duration(1<<63 - 1)
	return d + min(d, largest-d)
}

type supervisedObserverProcess struct {
	pid    int
	ready  <-chan error
	exited <-chan error
	kill   func() error
}
type observerLauncher func() (*supervisedObserverProcess, error)

func launchSupervisedObserver(executable, root string, lock *os.File) (*supervisedObserverProcess, error) {
	ready, writeReady, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	defer writeReady.Close()
	output, writeOutput, err := os.Pipe()
	if err != nil {
		ready.Close()
		return nil, err
	}
	defer writeOutput.Close()
	cmd := exec.Command(executable, "wait", "--observer-dir", root, "--detached")
	cmd.Env = append(observerEnvironment(), "MYCE_SHARED_OBSERVER=1", "MYCE_OBSERVER_CHILD=1")
	cmd.Dir = "/"
	cmd.ExtraFiles = []*os.File{lock, writeReady}
	// A private group lets the supervisor retire a stalled owner and any
	// in-flight notifier before replacement. Dev/worker session custody remains.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	managed := os.Getenv("MYCE_DEV_SESSION_ROOT") != "" || os.Getenv("MYCE_PROCESS_SESSION") != ""
	if managed {
		// Preserve the worker/dev owner's existing group cleanup contract.
		cmd.SysProcAttr = nil
	}
	cmd.Stdout, cmd.Stderr = writeOutput, writeOutput
	if err = cmd.Start(); err != nil {
		ready.Close()
		output.Close()
		return nil, err
	}
	ack := make(chan error, 1)
	go func() { defer ready.Close(); var b [1]byte; _, err := io.ReadFull(ready, b[:]); ack <- err }()
	go func() { defer output.Close(); _, _ = io.Copy(&observerLogWriter{root: root}, output) }()
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	return &supervisedObserverProcess{pid: cmd.Process.Pid, ready: ack, exited: exited, kill: func() error {
		if managed {
			return cmd.Process.Kill()
		}
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}}, nil
}

func superviseSharedProfiles(ctx context.Context, root string, lock *os.File, ready func() error) error {
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	return runObserverSupervisor(ctx, root, lock, myce.RealClock{}, waitPause, ready, func() (*supervisedObserverProcess, error) { return launchSupervisedObserver(executable, root, lock) })
}

// Only this parent owns child process handles. No PID from a status file is
// ever signalled. The inherited lifetime descriptor remains held throughout
// recovery; startup.lock serializes replacement socket setup and idle exit.
func runObserverSupervisor(ctx context.Context, root string, lifetime *os.File, clock myce.Clock, pause func(context.Context, time.Duration) error, ready func() error, launch observerLauncher) error {
	state := observerSupervision{PID: os.Getpid()}
	var previous observerSupervision
	if readProfileJSON(filepath.Join(root, "observer.supervision"), &previous) == nil {
		state.Restarts, state.Window, state.LastRecovery = previous.Restarts, previous.Window, previous.LastRecovery
	}
	var child *supervisedObserverProcess
	var startLock *os.File
	defer func() {
		if startLock != nil {
			startLock.Close()
		}
		if child != nil {
			_ = child.kill()
			<-child.exited
		}
	}()
	acknowledged := false
	retry := observerRecoveryPeriod
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if child == nil {
			if acknowledged {
				var err error
				startLock, err = sharedProfileLock(ctx, filepath.Join(root, "startup.lock"))
				if err != nil {
					return err
				}
				entries, err := filepath.Glob(filepath.Join(root, "*.json"))
				if err != nil {
					return err
				}
				if len(entries) == 0 {
					// Release lifetime ownership before startup serialization, as
					// registration must never adopt an exiting supervisor.
					_ = lifetime.Close()
					return nil
				}
			}
			var err error
			child, err = launch()
			if err != nil {
				if !acknowledged {
					return err
				}
				if retained := state.retain(root, clock, "launch_failed", err, observerDiagnostic{}); retained != nil {
					return retained
				}
				startLock.Close()
				startLock = nil
				if err = pause(ctx, retry); err != nil {
					return err
				}
				retry = doubleObserverDuration(retry)
				continue
			}
			state.ChildPID = child.pid
		}
		started := clock.Now()
		childReady := false
		acknowledge := func(err error) error {
			if err != nil {
				return err
			}
			childReady = true
			if err = saveAtomicProfileJSON(filepath.Join(root, "observer.supervision"), state, false); err != nil {
				return err
			}
			if !acknowledged {
				if err = ready(); err != nil {
					return err
				}
				acknowledged = true
			}
			if startLock != nil {
				startLock.Close()
				startLock = nil
			}
			return nil
		}
		var diagnostic observerDiagnostic
		var exit error
		var departed *supervisedObserverProcess
		reason := "exited"
	watch:
		for {
			select {
			case exit = <-child.exited:
				// A fast terminal delivery may finish before the parent runs.
				// Consume its readiness byte before classifying the exit; process
				// exit and pipe-reader scheduling can become ready together.
				if !childReady {
					if err := <-child.ready; err == nil {
						if err = acknowledge(nil); err != nil {
							child = nil
							return err
						}
					}
				}
				departed = child
				child = nil
				break watch
			default:
			}
			if !childReady {
				select {
				case err := <-child.ready:
					if err != nil {
						_ = child.kill()
						exit = <-child.exited
						child = nil
						break watch
					}
					if err = acknowledge(nil); err != nil {
						return err
					}
				default:
				}
			}
			_ = readProfileJSON(filepath.Join(root, "observer.status"), &diagnostic)
			// A cooperative image handoff starts a successor, which writes this
			// status before the handing-off child exits. The successor's status is
			// not this child's silence. Killing the child mid-handoff broke the
			// successor's readiness pipe, so it exited and left every arm
			// unobserved until some keyed command restarted one (#919). The
			// successor's own supervisor watches it; this child only has to exit.
			if diagnostic.PID != state.ChildPID && diagnostic.StartedAt.After(started) {
				if err := pause(ctx, observerRecoveryPeriod); err != nil {
					return err
				}
				continue
			}
			// Completed operation cost includes protocol hold and transport/queue
			// service. Two measured periods admit its repeat and one missed completion.
			state.Window = max(state.Window, doubleObserverDuration(diagnostic.LastReadDuration))
			if state.stalled(clock.Now(), started, diagnostic) {
				reason = "stalled"
				// Retain the exact snapshot before terminating the child. No cooperative
				// shutdown is assumed from a silently stalled process.
				if err := state.retain(root, clock, reason, nil, diagnostic); err != nil {
					return err
				}
				_ = child.kill()
				exit = <-child.exited
				child = nil
				break watch
			}
			if err := pause(ctx, observerRecoveryPeriod); err != nil {
				return err
			}
		}
		_ = readProfileJSON(filepath.Join(root, "observer.status"), &diagnostic)
		if !acknowledged {
			return errors.New("observer exited before readiness")
		}
		// Cooperative image handoff has already started and acknowledged a new
		// supervised owner using the same inherited lifetime descriptor.
		var current observerDiagnostic
		if readProfileJSON(filepath.Join(root, "observer.status"), &current) == nil && current.PID != state.ChildPID && current.StartedAt.After(started) {
			return nil
		}
		if departed != nil {
			_ = departed.kill()
		}
		// A successful idle exit is not a recovery. Recheck under startup
		// serialization before retiring the inherited lifetime descriptor.
		if startLock == nil {
			var err error
			startLock, err = sharedProfileLock(ctx, filepath.Join(root, "startup.lock"))
			if err != nil {
				return err
			}
		}
		entries, err := filepath.Glob(filepath.Join(root, "*.json"))
		if err != nil {
			return err
		}
		if len(entries) == 0 {
			_ = lifetime.Close()
			return nil
		}
		if reason == "stalled" {
			setObserverExit(state.LastRecovery, exit)
			if err := saveProfileJSON(filepath.Join(root, "observer.supervision"), state); err != nil {
				return err
			}
		}
		if reason != "stalled" {
			if err := state.retain(root, clock, reason, exit, diagnostic); err != nil {
				return err
			}
		}
		if startLock != nil {
			startLock.Close()
			startLock = nil
		}
		// Failed births/crash loops back off without a cap, based on the transport
		// recovery seed. Actual successful reads restore the ordinary recovery pace.
		if diagnostic.LastReadAt.After(started) {
			retry = observerRecoveryPeriod
		}
		if err := pause(ctx, retry); err != nil {
			return err
		}
		retry = doubleObserverDuration(retry)
	}
}
