package myce

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

var errModelLoadStalled = errors.New("model load stalled: no advancing load signal")

// Local histories bind the immutable recipe and execution target as well as the
// catalog. Never infer cold initialization speed from transport or cached I/O.
func modelReadinessFile(dir string, model Model) string {
	identity := struct {
		Model            string
		Recipe, OS, Arch string
	}{
		modelConfigHash(ModelConfig{Models: []Model{model}}), model.readinessRecipe, runtime.GOOS, runtime.GOARCH,
	}
	return readinessFile(dir, identity)
}

func superviseModelLoad(parent context.Context, model Model, dir string, clock Clock) (context.Context, *readinessProgress, func(bool)) {
	path := modelReadinessFile(dir, model)
	history := readinessHistory{}
	if dir != "" {
		history = readReadinessHistory(path)
	}
	p := newReadinessProgress(clock, history)
	ctx, cancel := context.WithCancelCause(parent)
	ctx = context.WithValue(ctx, readinessProgressKey{}, p)
	previous, _ := parent.Value(ioProgressKey{}).(func())
	ctx = context.WithValue(ctx, ioProgressKey{}, func() {
		p.advance()
		if previous != nil {
			previous()
		}
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			budget, stalled := p.state()
			p.mu.Lock()
			observingOS := p.osObserver
			p.mu.Unlock()
			if !observingOS {
				p.observeActivity()
			}
			if stalled {
				cancel(errModelLoadStalled)
				return
			}
			// This is only an observation cadence. Unknown loads have no silence bound,
			// even after an initial progress signal. Parent/user deadlines still bind.
			if budget <= 0 {
				budget = readinessInitialRTT
			}
			if readinessPause(ctx, budget) != nil {
				return
			}
		}
	}()
	finish := func(success bool) {
		cancel(context.Canceled)
		<-done
		if success && dir != "" {
			mergeReadinessHistory(path, p.result())
		}
	}
	return ctx, p, finish
}

// One mutex serializes these tiny local state updates, outside queue/runtime
// locks. Merge the latest committed observations, never a load-start snapshot.
var readinessHistoryMu sync.Mutex

func readinessFile(dir string, identity any) string {
	b, _ := json.Marshal(identity)
	return filepath.Join(dir, fmt.Sprintf("readiness-v2-%x.json", sha256.Sum256(b)))
}
func readReadinessHistory(path string) readinessHistory {
	readinessHistoryMu.Lock()
	defer readinessHistoryMu.Unlock()
	return readReadinessHistoryLocked(path)
}
func readReadinessHistoryLocked(path string) (h readinessHistory) {
	if b, err := os.ReadFile(path); err == nil {
		_ = json.Unmarshal(b, &h)
	}
	return h
}
func mergeReadinessHistory(path string, observed readinessHistory) {
	readinessHistoryMu.Lock()
	defer readinessHistoryMu.Unlock()
	h := readReadinessHistoryLocked(path)
	h.Load.observe(observed.Load.Mean)
	h.Gap = max(h.Gap, observed.Gap)
	b, err := json.Marshal(h)
	if err == nil {
		err = publishReadinessHistory(path, b)
	}
	if err != nil {
		log.Print("readiness history unavailable; next initialization may remain unknown")
	}
}
func publishReadinessHistory(path string, b []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".readiness-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(b); err == nil {
		err = f.Sync()
	}
	err = errors.Join(err, f.Close())
	if err == nil {
		err = os.Rename(f.Name(), path)
	}
	return err
}

var modelLoadPercent = regexp.MustCompile(`([0-9]+(?:\.[0-9]+)?)%`)

// stdout/stderr share one writer, so fragmented lines cannot interleave. The
// shared diagnostic pool bounds incomplete-line and parser memory.
type modelReadinessOutput struct {
	mu       sync.Mutex
	output   io.Writer
	progress *readinessProgress
	line     diagnosticLine
}

func (w *modelReadinessOutput) Write(b []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n, err := w.output.Write(b)
	w.line.write(b[:n], w.record)
	return n, err
}

func (w *modelReadinessOutput) close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.line.clear()
}
func (w *modelReadinessOutput) record(line []byte) {
	if b, ok := bytes.CutPrefix(line, []byte("MYCE_LOAD_PROGRESS ")); ok {
		w.progress.record(b)
		return
	}
	// The llama.cpp and vLLM presets report tensor/shard loading percentages.
	// Other output, repeated percentages and process liveness are not proof.
	lower := strings.ToLower(string(line))
	if !strings.Contains(lower, "load") && !strings.Contains(lower, "tensor") {
		return
	}
	if match := modelLoadPercent.FindSubmatch(line); len(match) > 1 {
		v, err := strconv.ParseFloat(string(match[1]), 64)
		if err == nil {
			b, _ := json.Marshal(map[string]any{"stage": 1, "completed": v, "total": 100})
			w.progress.record(b)
		}
	}
}

// Host-local demand spacing and successful reload cost determine residency.
// An unseen repeat is worth one reload opportunity; after that, retain only
// while a request is expected within its measured interarrival uncertainty.
type modelDemandHistory struct {
	At      time.Time        `json:"at"`
	Spacing readinessLatency `json:"spacing"`
	Load    readinessLatency `json:"load"`
}

func (q *Queue) noteModelDemand(model, machine string) {
	if q.s.ModelUses == nil {
		q.s.ModelUses = map[string]ModelUse{}
	}
	u := q.s.ModelUses[model]
	u.Demand = maps.Clone(u.Demand)
	if u.Demand == nil {
		u.Demand = map[string]modelDemandHistory{}
	}
	d := u.Demand[machine]
	now := q.clock.Now()
	if !d.At.IsZero() && now.After(d.At) {
		d.Spacing.observe(now.Sub(d.At))
	}
	d.At = now
	u.Demand[machine] = d
	q.s.ModelUses[model] = u
}

func (q *Queue) noteModelLoaded(s *ModelSlot) {
	u := q.s.ModelUses[s.Model]
	u.Demand = maps.Clone(u.Demand)
	if u.Demand == nil {
		u.Demand = map[string]modelDemandHistory{}
	}
	d := u.Demand[s.Machine]
	if s.RunSince != nil {
		d.Load.observe(q.clock.Now().Sub(*s.RunSince))
	}
	u.Demand[s.Machine] = d
	if q.s.ModelUses == nil {
		q.s.ModelUses = map[string]ModelUse{}
	}
	q.s.ModelUses[s.Model] = u
}

func (q *Queue) modelIdleExpired(s *ModelSlot) bool {
	if q.models.Idle > 0 {
		return q.clock.Now().Sub(s.LastUsed) >= q.models.Idle
	}
	for _, c := range q.calls {
		if c.Model != s.Model || c.Error != "" {
			continue
		}
		if c.Machine != "" {
			if c.Machine == s.Machine && c.Device == s.Device && c.Session == s.Session && c.Attempt == s.Attempt {
				return false
			}
			continue
		}
		m := slotMachine(q.s.Machines[s.Machine], s)
		if m != nil && (q.models.Wait <= 0 || q.clock.Now().Sub(c.Queued) < q.models.Wait) && q.callCanUse(m, c) {
			return false
		}
	}
	d := q.s.ModelUses[s.Model].Demand[s.Machine]
	budget := time.Duration(0)
	if d.Load.Mean > 0 {
		budget = d.Load.budget()
	}
	if d.Spacing.Mean > 0 {
		budget = max(budget, d.Spacing.budget())
	}
	return q.clock.Now().Sub(s.LastUsed) >= budget
}

const coldLoadInactive = "cold load: no observed activity; custody retained"
const coldLoadContended = coldLoadInactive + "; eligible demand waits for these resources; owner attention needed"
const coldLoadActive = "cold load: activity observed; initialization duration unknown"

// Called by the shared resource observer, never by a second sampler. Nil means
// unavailable, including cached model reads the OS cannot attribute separately.
// A change on any observable CPU/read/memory counter is sufficient activity.
func (p *readinessProgress) observeActivity(values ...*float64) {
	if p == nil {
		return
	}
	p.mu.Lock()
	if len(p.activityValues) < len(values) {
		p.activityValues = make([]float64, len(values))
	}
	moving := false
	for i, v := range values {
		if v != nil && validMemoryValue(*v) {
			moving = moving || *v > p.activityValues[i]
			// Memory can be reclaimed and grow again below an earlier peak.
			// Each observed increase is activity, not just an all-time maximum.
			p.activityValues[i] = *v
		}
	}
	unchanged := p.activitySamples > 0 && p.activityRevision == p.revision && !moving
	p.activitySamples++
	p.inactive = unchanged
	p.activityRevision = p.revision
	p.mu.Unlock()
	if moving {
		p.advance()
		p.mu.Lock()
		p.activityRevision = p.revision
		p.mu.Unlock()
	}
}
func (p *readinessProgress) coldReason() string {
	if p == nil {
		return ""
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.budget > 0 || p.suspended || p.activitySamples == 0 {
		return ""
	}
	if p.inactive {
		return coldLoadInactive
	}
	return coldLoadActive
}

func modelReadinessDiagnostic(reason string) string {
	switch reason {
	case coldLoadInactive, coldLoadActive, coldLoadContended:
		return reason
	default:
		return ceilingDiagnostic(reason, "model load or unload failed")
	}
}
