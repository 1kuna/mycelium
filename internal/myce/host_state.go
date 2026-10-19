package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const hostStateFile = "host-state.json"
const hostStateVersion = 1

var ErrHostState = errors.New("host state unavailable")
var ErrHostStateUninitialized = errors.New("state_uninitialized")
var ErrHostStateCorrupt = errors.New("state_corrupt")
var ErrHostRevision = errors.New("stale host revision")

func legacyHostStateError() error {
	return fmt.Errorf("%w: legacy host control state; run myce host convert offline", ErrHostState)
}

// HostRevision binds asynchronous work to one accepted operation, including across reopen.
type HostRevision struct {
	Revision  uint64 `json:"revision"`
	Operation string `json:"operation_id"`
}

type HostObservation struct {
	HostRevision
	Boot  string    `json:"boot_id"`
	State string    `json:"state"`
	At    time.Time `json:"observed_at"`
}

// HostObligation is exact custody, never a claim of physical release. Conversion
// imports historical releases as pending; S05 owns their queue reconciliation.
type HostEngineExecution struct {
	Machine string `json:"machine"`
	Session string `json:"session"`
	Model   string `json:"model"`
	Attempt string `json:"attempt"`
}

type HostObligation struct {
	Engine      *HostEngineExecution `json:"engine,omitempty"`
	Preparation bool                 `json:"preparation,omitempty"`
	Queue       string               `json:"queue"`
	Result      Result               `json:"execution"`
}

type HostClose struct {
	HostRevision
	Settlement string     `json:"settlement"`
	SettledAt  *time.Time `json:"settled_at"`
}

// Cleanup evidence is scoped to one boot and one revision. It is not the
// privileged rest verdict (GPU/restart observation is supplied by S07).
type HostLocalExclusion struct {
	Simulated bool                 `json:"simulated,omitempty"`
	Evidence  *HostExclusionReport `json:"evidence,omitempty"`
	HostObservation
	Confirmed bool             `json:"confirmed"`
	Errors    []string         `json:"errors"`
	Released  []HostObligation `json:"released"`
	Sessions  []HostSession    `json:"sessions"`
}
type HostSession struct {
	Machine string `json:"machine"`
	Session string `json:"session"`
}
type HostIntent struct {
	MemoryAllowanceBytes int64                          `json:"memory_allowance_bytes,omitempty"`
	Errors               []string                       `json:"errors,omitempty"`
	QueueObserved        map[string]HostRevision        `json:"queue_observed,omitempty"`
	LastClose            *HostClose                     `json:"last_close"`
	LocalExclusion       *HostLocalExclusion            `json:"local_exclusion"`
	QueueAcks            map[string]HostAcknowledgement `json:"queue_acknowledgements"`

	Schema int    `json:"schema_version"`
	Host   string `json:"host_id"`
	HostRevision
	Desired       string           `json:"desired"`
	Reason        string           `json:"reason"`
	AcceptedAt    time.Time        `json:"accepted_at"`
	StopDeadline  *time.Time       `json:"stop_deadline"`
	PreviousClose uint64           `json:"previous_close_revision"`
	Pending       []HostObligation `json:"pending"`
	Holds         []string         `json:"holds"`
	Observed      *HostObservation `json:"observed"`
}

func hostRoot(dir string) (string, error) {
	if dir == "" {
		return HostStateDirectory()
	}
	if legacyUnitParent(dir) != "" {
		return "", legacyHostStateError()
	}
	return dir, nil
}

// Inspect active lookup paths only. Preserved conversion originals live under
// .host-conversion and cannot accidentally become runtime receipts again.
func legacyHostPaths(dir string) ([]string, error) {
	var paths []string
	for _, pattern := range []string{"DISABLED", "control-generation.json", "disable-receipt.json", "enable-pending.json", "forgotten-queues.json", "units/*"} {
		matches, err := filepath.Glob(filepath.Join(dir, pattern))
		if err != nil {
			return nil, err
		}
		paths = append(paths, matches...)
	}
	return paths, nil
}

func validateHostIntent(s HostIntent) error {
	if s.MemoryAllowanceBytes < 0 {
		return fmt.Errorf("%w: invalid memory allowance", ErrHostState)
	}
	if s.Schema != hostStateVersion {
		return fmt.Errorf("%w: unsupported host state schema %d; use a compatible myce version", ErrHostState, s.Schema)
	}
	if s.Host == "" || s.Revision == 0 || s.Operation == "" || s.AcceptedAt.IsZero() || (s.Desired != "open" && s.Desired != "closed") || s.PreviousClose > s.Revision {
		return fmt.Errorf("%w: invalid host intent fields", ErrHostState)
	}
	if s.Desired == "closed" && (s.StopDeadline == nil || s.PreviousClose != s.Revision) {
		return fmt.Errorf("%w: invalid closed host intent", ErrHostState)
	}
	if s.LastClose != nil {
		c := s.LastClose
		if c.Revision == 0 || c.Operation == "" || c.Revision != s.PreviousClose || c.Revision > s.Revision || (c.Settlement != "pending" && c.Settlement != "unknown" && c.Settlement != "confirmed") || (c.Settlement == "confirmed") != (c.SettledAt != nil) {
			return fmt.Errorf("%w: invalid last close", ErrHostState)
		}
	}
	for _, p := range s.Pending {
		if p.Engine != nil {
			if p.Queue == "" || p.Engine.Machine == "" || p.Engine.Session == "" || p.Engine.Model == "" || p.Engine.Attempt == "" {
				return fmt.Errorf("%w: incomplete engine custody", ErrHostState)
			}
			continue
		}
		if p.Queue == "" || p.Result.Machine == "" || p.Result.Session == "" || p.Result.ID == "" || (p.Result.Attempt <= 0 && !(p.Preparation && p.Result.ID == "preparation" && p.Result.Attempt == 0)) {
			return fmt.Errorf("%w: incomplete execution custody", ErrHostState)
		}
	}
	return nil
}

func ReadHostIntent(dir string) (HostIntent, error) {
	if os.Geteuid() == 0 {
		return HostIntent{}, ErrHostIdentity
	}
	var s HostIntent
	dir, err := hostRoot(dir)
	if err != nil {
		return s, err
	}
	if _, err = os.Stat(filepath.Join(dir, ".host-conversion", "IN_PROGRESS")); err == nil {
		return s, fmt.Errorf("%w: conversion incomplete; resume myce host convert offline", ErrHostState)
	} else if !errors.Is(err, os.ErrNotExist) {
		return s, errors.Join(ErrHostState, err)
	}
	paths, err := legacyHostPaths(dir)
	if err != nil {
		return s, errors.Join(ErrHostState, err)
	}
	if len(paths) > 0 {
		return s, legacyHostStateError()
	}
	b, err := readHostRegular(filepath.Join(dir, hostStateFile), uint32(os.Geteuid()), 16<<20)
	if errors.Is(err, os.ErrNotExist) {
		registries, e := filepath.Glob(filepath.Join(dir, "worker-*.json"))
		if e != nil {
			return s, errors.Join(ErrHostState, e)
		}
		if len(registries) > 0 {
			return s, legacyHostStateError()
		}
		return s, fmt.Errorf("%w: this host has no Mycelium host state yet; provision it with `myce host open` or `myce dev`", errors.Join(ErrHostState, ErrHostStateUninitialized))
	}
	if err != nil {
		return s, errors.Join(ErrHostState, err)
	}
	if err = json.Unmarshal(b, &s); err != nil {
		return HostIntent{}, errors.Join(ErrHostState, ErrHostStateCorrupt)
	}
	if err = validateHostIntent(s); err != nil {
		return HostIntent{}, err
	}
	return s, nil
}

// atomicHostJSON has explicit persistence boundaries for power-loss tests. The
// hook is per operation, not global, and never changes runtime persistence.
func atomicHostJSON(dir, name string, value any, boundary func(string) error) error {
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".host-stage-")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	step := func(s string) error {
		if boundary != nil {
			return boundary(s)
		}
		return nil
	}
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(append(b, '\n'))
	}
	if err == nil {
		err = step("write")
	}
	if err == nil {
		err = f.Sync()
	}
	if err == nil {
		err = step("file-sync")
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		return err
	}
	if err = os.Rename(tmp, filepath.Join(dir, name)); err != nil {
		return err
	}
	if err = step("rename"); err != nil {
		return err
	}
	if err = syncWorkerDir(dir); err != nil {
		return err
	}
	return step("directory-sync")
}

// InitializeHostIntent is explicit provisioning, never a runtime fallback.
// Fresh hosts start closed. Install/upgrade orchestration belongs to S11.
func InitializeHostIntent(dir, host string, clock Clock) (HostIntent, error) {
	var s HostIntent
	if clock == nil || host == "" {
		return s, errors.New("host identity and clock required")
	}
	dir, err := hostRoot(dir)
	if err != nil {
		return s, err
	}
	if err = os.MkdirAll(dir, 0700); err != nil {
		return s, err
	}
	unlock, err := hostOperationLock(dir, "intake.lock", true)
	if err != nil {
		return s, err
	}
	defer unlock()
	if _, err = os.Stat(filepath.Join(dir, hostStateFile)); !errors.Is(err, os.ErrNotExist) {
		return s, errors.New("host state already exists or is unreadable")
	}
	paths, err := legacyHostPaths(dir)
	if err != nil || len(paths) > 0 {
		return s, errors.Join(legacyHostStateError(), err)
	}
	registries, err := filepath.Glob(filepath.Join(dir, "worker-*.json"))
	if err != nil || len(registries) > 0 {
		return s, errors.Join(legacyHostStateError(), err)
	}
	if _, err = os.Lstat(filepath.Join(dir, ".host-conversion", "IN_PROGRESS")); !errors.Is(err, os.ErrNotExist) {
		return s, fmt.Errorf("%w: conversion incomplete; resume myce host convert offline", ErrHostState)
	}
	now := clock.Now()
	s = HostIntent{Schema: hostStateVersion, Host: host, HostRevision: HostRevision{Revision: 1, Operation: ID()}, Desired: "closed", Reason: "initial provisioning", AcceptedAt: now, StopDeadline: &now, PreviousClose: 1}
	s.LastClose = &HostClose{HostRevision: s.HostRevision, Settlement: "pending"}
	return s, atomicHostJSON(dir, hostStateFile, s, nil)
}

type hostIntentChange struct {
	Desired       string
	Expected      HostRevision
	CloseExpected *HostRevision
	Reason        string
	Grace         time.Duration
	// S05 supplies durable previous-close settlement. Called outside intake.lock;
	// the expected revision is checked again before committing open.
	FreshClose bool
	Settled    func(HostIntent) error
	boundary   func(string) error
}

func changeHostIntent(dir string, clock Clock, c hostIntentChange) (HostIntent, error) {
	return changeHostIntentContext(context.Background(), dir, clock, c)
}

func changeHostIntentContext(ctx context.Context, dir string, clock Clock, c hostIntentChange) (HostIntent, error) {
	var zero HostIntent
	if clock == nil || (c.Desired != "closed" && c.Desired != "open") || c.Grace < 0 || c.Grace > time.Minute {
		return zero, errors.New("invalid host intent")
	}
	dir, err := hostRoot(dir)
	if err != nil {
		return zero, err
	}
	if c.Desired == "open" {
		s, err := ReadHostIntent(dir)
		if err != nil {
			return zero, err
		}
		if c.Expected.Revision == 0 || s.HostRevision != c.Expected {
			return zero, ErrHostRevision
		}
		if s.PreviousClose > 0 {
			if c.Settled == nil {
				c.Settled = previousCloseSettled
			}
			if err = c.Settled(s); err != nil {
				return zero, err
			}
		}
	}
	unlock, err := hostOperationLockContext(ctx, dir, "intake.lock")
	if err != nil {
		return zero, err
	}
	defer unlock()
	s, err := ReadHostIntent(dir)
	if err != nil {
		return zero, err
	}
	if (c.Desired == "open" && s.HostRevision != c.Expected) || (c.Desired == "closed" && c.CloseExpected != nil && s.HostRevision != *c.CloseExpected) {
		return zero, ErrHostRevision
	}
	if c.Desired == "open" {
		if err = previousCloseSettled(s); err != nil {
			return zero, err
		}
	}
	highest := s.Revision
	for _, observed := range s.QueueObserved {
		highest = max(highest, observed.Revision)
	}
	if s.Desired == c.Desired && !c.FreshClose && !(c.Desired == "closed" && highest > s.Revision) {
		return s, nil
	} // Repeated close never renews stop grace.
	if highest == ^uint64(0) {
		return zero, errors.New("host revision exhausted")
	}
	s.HostRevision = HostRevision{Revision: highest + 1, Operation: ID()}
	s.Desired, s.Reason, s.AcceptedAt, s.Observed = c.Desired, c.Reason, clock.Now(), nil
	if c.Desired == "closed" {
		deadline := s.AcceptedAt.Add(c.Grace)
		// A fresh close changes identity, never extends an existing stop budget.
		if s.StopDeadline != nil && s.StopDeadline.Before(deadline) {
			deadline = *s.StopDeadline
		}
		s.StopDeadline = &deadline
		s.PreviousClose = s.Revision
		s.LastClose = &HostClose{HostRevision: s.HostRevision, Settlement: "pending"}
		s.LocalExclusion = nil
		s.QueueAcks = nil
		s.Errors = nil
	} else {
		// Previous-close history is immutable across open. Only reconciled custody
		// can be cleared; an unresolved hold cannot pass previousCloseSettled.
		s.Pending = nil
		s.Holds = nil
		s.LocalExclusion = nil
		s.QueueAcks = nil
		s.StopDeadline = nil
	}
	if err = atomicHostJSON(dir, hostStateFile, s, c.boundary); err != nil {
		return zero, err
	}
	return s, nil
}

func CheckHostRevision(dir string, expected HostRevision) error {
	s, err := ReadHostIntent(dir)
	if err != nil {
		return err
	}
	if expected.Revision != 0 && s.HostRevision != expected {
		return ErrHostRevision
	}
	if s.Desired != "open" {
		return errHostDisabled
	}
	return nil
}

func PublishHostObservation(dir string, expected HostRevision, boot, state string, clock Clock) error {
	if boot == "" || clock == nil || (state != "open" && state != "closed") {
		return errors.New("invalid host observation")
	}
	dir, err := hostRoot(dir)
	if err != nil {
		return err
	}
	unlock, err := hostOperationLock(dir, "intake.lock", true)
	if err != nil {
		return err
	}
	defer unlock()
	s, err := ReadHostIntent(dir)
	if err != nil {
		return err
	}
	if expected.Revision == 0 || s.HostRevision != expected || s.Desired != state {
		return ErrHostRevision
	}
	s.Observed = &HostObservation{HostRevision: expected, Boot: boot, State: state, At: clock.Now()}
	return atomicHostJSON(dir, hostStateFile, s, nil)
}

func (s HostIntent) Observation(boot string) *HostObservation {
	if s.Observed == nil || boot == "" || s.Observed.Boot != boot || s.Observed.HostRevision != s.HostRevision || s.Observed.State != s.Desired {
		return nil
	}
	return s.Observed
}

func HostDisabled(dir string) (string, error) {
	s, err := ReadHostIntent(dir)
	if err != nil {
		return "", err
	}
	if s.Desired == "closed" {
		if strings.TrimSpace(s.Reason) == "" {
			return "host closed", nil
		}
		return s.Reason, nil
	}
	return "", nil
}

// launch.lock serializes local dispatch, not intent intake. A close can always
// persist while a daemon call is in flight; its custody remains pending until
// the later reconciler proves the accepted operation's terminal disposition.
func hostLaunchGate(dir string, expected ...HostRevision) (func(), error) {
	dir, err := hostRoot(dir)
	if err != nil {
		return nil, err
	}
	unlock, err := hostOperationLock(dir, "launch.lock", true)
	if err != nil {
		return nil, err
	}
	var revision HostRevision
	if len(expected) > 0 {
		revision = expected[0]
	}
	if err = CheckHostRevision(dir, revision); err != nil {
		unlock()
		return nil, err
	}
	return unlock, nil
}

// A native create/start or local admission is short and serializes with intent.
// Callers must never pass network I/O to this helper.
func hostDispatch(dir string, revision HostRevision, action func() error) error {
	return hostDispatchContext(context.Background(), dir, revision, action)
}

func hostDispatchContext(ctx context.Context, dir string, revision HostRevision, action func() error) error {
	dir, err := hostRoot(dir)
	if err != nil {
		return err
	}
	unlock, err := hostOperationLockContext(ctx, dir, "intake.lock")
	if err != nil {
		return err
	}
	defer unlock()
	if err = CheckHostRevision(dir, revision); err != nil {
		return err
	}
	return action()
}
func bindHostRevision(dir string, revision *HostRevision) error {
	if revision.Revision == 0 {
		s, err := ReadHostIntent(dir)
		if err != nil {
			return err
		}
		*revision = s.HostRevision
	}
	return CheckHostRevision(dir, *revision)
}
