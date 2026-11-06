package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

var errModelLoadStalled = errors.New("model load stalled: no advancing load signal")

// The state directory is worker-local: history never transfers between hosts.
// The catalog identity binds weights, engine, arguments and environment.
func modelReadinessFile(dir string, model Model) string {
	return filepath.Join(dir, "readiness-"+modelConfigHash(ModelConfig{Models: []Model{model}})+".json")
}

func superviseModelLoad(parent context.Context, model Model, dir string, clock Clock) (context.Context, *readinessProgress, func(bool)) {
	var history readinessHistory
	if dir != "" {
		if b, err := os.ReadFile(modelReadinessFile(dir, model)); err == nil {
			_ = json.Unmarshal(b, &history)
		}
	}
	p := newReadinessProgress(clock, history)
	// Unknown initialization is not a measured one-second load. A cold load
	// obtains its silence allowance from local weight I/O or its first actual
	// progress gap; explicit caller budgets still bound unknown custom servers.
	if history.Load.Mean <= 0 && history.Gap <= 0 {
		p.budget = 0
	}
	// An explicit caller budget is authoritative when cold calibration is
	// unavailable (for example, a custom server without local weight files).
	if until, ok := parent.Deadline(); ok {
		p.budget = max(p.budget, time.Until(until))
	}
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
			if stalled {
				cancel(errModelLoadStalled)
				return
			}
			if budget <= 0 {
				budget = readinessInitialRTT
			} // observation cadence only, never a cold load deadline
			if readinessPause(ctx, budget) != nil {
				return
			}
		}
	}()
	finish := func(success bool) {
		cancel(context.Canceled)
		<-done
		if !success || dir == "" {
			return
		}
		observed := p.result()
		history.Load.observe(observed.Load.Mean)
		history.Gap = max(history.Gap, observed.Gap)
		if err := saveWorkerJSON(dir, filepath.Base(modelReadinessFile(dir, model)), history); err != nil {
			log.Print("model readiness history unavailable; next load will recalibrate")
		}
	}
	return ctx, p, finish
}

// Cold-start calibration reads at most one OS page, then projects its measured
// latency over the declared weight bytes. This intentionally overestimates
// startup I/O (including open/stat), avoiding a universal disk/GPU speed guess.
// It runs outside queue/runtime locks. Historical load and gap evidence can
// only increase this first-load uncertainty allowance.
func (p *readinessProgress) calibrate(model Model) {
	path := model.Path
	if len(model.Files) > 0 {
		path = filepath.Join(path, filepath.FromSlash(model.Files[0].Path))
	}
	started := p.clock.Now()
	realStarted := RealClock{}.Now()
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()
	b := make([]byte, os.Getpagesize())
	n, _ := f.Read(b)
	if n == 0 {
		return
	}
	pages := max(int64(1), model.SizeBytes/int64(n))
	budget := scaleReadinessDuration(max(time.Nanosecond, p.clock.Now().Sub(started), RealClock{}.Now().Sub(realStarted)), pages)
	p.mu.Lock()
	p.budget = max(p.budget, scaleReadinessDuration(budget, 2))
	p.mu.Unlock()
	p.advance()
}

var modelLoadPercent = regexp.MustCompile(`([0-9]+(?:\.[0-9]+)?)%`)

// stdout/stderr share one writer, so fragmented lines cannot interleave. The
// existing diagnostic line budget bounds parser memory even with logs:none.
type modelReadinessOutput struct {
	mu       sync.Mutex
	output   io.Writer
	progress *readinessProgress
	line     []byte
	dropping bool
}

func (w *modelReadinessOutput) Write(b []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n, err := w.output.Write(b)
	for _, c := range b[:n] {
		if c == '\n' || c == '\r' {
			if !w.dropping {
				w.record(w.line)
			}
			w.line, w.dropping = w.line[:0], false
		} else if len(w.line) < dockerEventLineLimit && !w.dropping {
			w.line = append(w.line, c)
		} else {
			w.line, w.dropping = w.line[:0], true
		}
	}
	return n, err
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
		if c.Model == s.Model && c.Error == "" {
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
