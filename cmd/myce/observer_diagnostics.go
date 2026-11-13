package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"mycelium/internal/myce"
)

const observerLogBytes = 1 << 20

// Two 1 MiB files bound diagnostic storage independently of observer lifetime.
// Only structured causes are retained: raw errors, authority, environment and
// notifier targets must never enter either the status record or log.
type observerLogWriter struct {
	root string
	mu   sync.Mutex
}

func (w *observerLogWriter) event(value any) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	lock, err := profileLock(filepath.Join(w.root, "observer-log.lock"))
	if err != nil {
		return err
	}
	defer lock.Close()
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if len(data) > observerLogBytes {
		return fmt.Errorf("observer diagnostic exceeds log budget")
	}
	path := filepath.Join(w.root, "observer.log")
	if stat, err := os.Stat(path); err == nil && stat.Size()+int64(len(data)) > observerLogBytes {
		if err = os.Rename(path, path+".1"); err != nil {
			return err
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(data)
	closeErr := f.Close()
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}
func (w *observerLogWriter) Write(p []byte) (int, error) {
	// Main's fallback output can include unclassified errors. Retain the event,
	// never raw strings that could contain a transport URL or authority value.
	err := w.event(struct {
		At    time.Time `json:"at"`
		Event string    `json:"event"`
		Bytes int       `json:"bytes"`
	}{time.Now(), "process_output", len(p)})
	return len(p), err
}

type observerDiagnostic struct {
	NextWakeAt       time.Time                         `json:"next_wake_at,omitempty"`
	LastReadDuration time.Duration                     `json:"last_read_duration,omitempty"`
	PID              int                               `json:"pid"`
	Version          string                            `json:"version"`
	Commit           string                            `json:"commit"`
	StartedAt        time.Time                         `json:"started_at"`
	CapturedAt       time.Time                         `json:"captured_at"`
	Phase            string                            `json:"phase"`
	JobID            string                            `json:"job_id,omitempty"`
	Registrations    int                               `json:"registrations"`
	LastReadAt       time.Time                         `json:"last_read_at,omitempty"`
	NextReadAt       time.Time                         `json:"next_read_at,omitempty"`
	NextDeliveryAt   time.Time                         `json:"next_delivery_at,omitempty"`
	LastDeliveryAt   time.Time                         `json:"last_delivery_at,omitempty"`
	Delivered        int                               `json:"delivered"`
	Failed           int                               `json:"failed"`
	LastError        string                            `json:"last_error,omitempty"`
	LastErrorAt      time.Time                         `json:"last_error_at,omitempty"`
	Readers          map[uint64]observerReadDiagnostic `json:"readers,omitempty"`
}
type observerDiagnostics struct {
	mu       sync.Mutex
	nextRead uint64
	root     string
	clock    myce.Clock
	state    observerDiagnostic
	log      observerLogWriter
}

func newObserverDiagnostics(root string, clock myce.Clock) *observerDiagnostics {
	return &observerDiagnostics{root: root, clock: clock, log: observerLogWriter{root: root}, state: observerDiagnostic{PID: os.Getpid(), Version: myce.BuildVersion, Commit: myce.BuildCommit, StartedAt: clock.Now()}}
}

// observerErrorCause refines the shared client cause for this local log only.
// A replacement observer once failed every read for eight hours as plain
// io_error, while the queue logged none of those requests (#919). Unreachable
// routes, local-network denial and TLS failures need distinct names to be
// attributed. A remaining unknown keeps the Go error type, never its text.
func observerErrorCause(err error) string {
	cause := myce.ClientErrorCause(err)
	if cause != "io_error" {
		return cause
	}
	for _, item := range []struct {
		err  error
		name string
	}{
		{myce.ErrLocalNetworkAccess, "local_network_access"},
		{syscall.EHOSTUNREACH, "EHOSTUNREACH"}, {syscall.ENETUNREACH, "ENETUNREACH"},
		{syscall.EHOSTDOWN, "EHOSTDOWN"}, {syscall.ENETDOWN, "ENETDOWN"}, {syscall.ETIMEDOUT, "ETIMEDOUT"},
	} {
		if errors.Is(err, item.err) {
			return item.name
		}
	}
	if myce.IsTLSIdentityError(err) {
		return "tls_identity"
	}
	inner := err
	for {
		next := errors.Unwrap(inner)
		if next == nil {
			break
		}
		inner = next
	}
	return fmt.Sprintf("io_error type=%T", inner)
}

func (d *observerDiagnostics) save() error {
	return saveAtomicProfileJSON(filepath.Join(d.root, "observer.status"), d.state, false)
}

func (d *observerDiagnostics) record(phase, job string, cause error, event bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.state.Phase, d.state.JobID, d.state.CapturedAt = phase, job, d.clock.Now()
	if cause != nil {
		d.state.LastError = observerErrorCause(cause)
		d.state.LastErrorAt = d.clock.Now()
	}
	if err := d.save(); err != nil {
		return err
	}
	if event {
		return d.log.event(d.state)
	}
	return nil
}

func observerStatusCommand(root string, out io.Writer) error {
	var state observerDiagnostic
	if err := readProfileJSON(filepath.Join(root, "observer.status"), &state); err != nil {
		return waitError(2, "local observer has no diagnostic record; legacy owners require operator recovery")
	}
	f, err := os.Open(filepath.Join(root, "observer.lock"))
	if err != nil {
		return err
	}
	defer f.Close()
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	locked := errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN)
	if err != nil && !locked {
		return err
	}
	if err == nil {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	}
	var supervision observerSupervision
	_ = readProfileJSON(filepath.Join(root, "observer.supervision"), &supervision)
	return json.NewEncoder(out).Encode(struct {
		observerDiagnostic
		OwnerLocked bool                `json:"owner_locked"`
		Supervision observerSupervision `json:"supervision"`
	}{state, locked, supervision})
}

// Network operations report independently of the coordinator, so a parked
// authority retains its own last boundary while other authorities make progress.
type observerReadDiagnostic struct {
	Phase      string    `json:"phase"`
	Jobs       int       `json:"jobs"`
	JobID      string    `json:"job_id,omitempty"`
	StartedAt  time.Time `json:"started_at"`
	CapturedAt time.Time `json:"captured_at"`
}
type observerReadTrace struct {
	owner *observerDiagnostics
	id    uint64
}

func (d *observerDiagnostics) update(change func(*observerDiagnostic)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	change(&d.state)
}
func (d *observerDiagnostics) beginRead(jobs int) *observerReadTrace {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.nextRead++
	if d.state.Readers == nil {
		d.state.Readers = map[uint64]observerReadDiagnostic{}
	}
	d.state.Readers[d.nextRead] = observerReadDiagnostic{Phase: "authority", Jobs: jobs, StartedAt: d.clock.Now(), CapturedAt: d.clock.Now()}
	_ = d.save()
	return &observerReadTrace{d, d.nextRead}
}
func (r *observerReadTrace) phase(phase, job string, cause error) {
	if r == nil {
		return
	}
	d := r.owner
	d.mu.Lock()
	defer d.mu.Unlock()
	entry := d.state.Readers[r.id]
	entry.Phase, entry.JobID, entry.CapturedAt = phase, job, d.clock.Now()
	d.state.Readers[r.id] = entry
	d.state.CapturedAt = d.clock.Now()
	if cause != nil && !errors.Is(cause, context.Canceled) {
		d.state.LastError = observerErrorCause(cause)
		d.state.LastErrorAt = d.clock.Now()
		_ = d.log.event(struct {
			At    time.Time `json:"at"`
			Read  uint64    `json:"read"`
			Phase string    `json:"phase"`
			Cause string    `json:"cause"`
		}{d.clock.Now(), r.id, phase, d.state.LastError})
	}
	if phase == "batch_read_complete" || phase == "terminal_read_complete" {
		d.state.LastReadAt = d.clock.Now()
		d.state.LastReadDuration = d.clock.Now().Sub(entry.StartedAt)
	}
	_ = d.save()
}
func (r *observerReadTrace) done() {
	if r == nil {
		return
	}
	d := r.owner
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.state.Readers, r.id)
	_ = d.save()
}
