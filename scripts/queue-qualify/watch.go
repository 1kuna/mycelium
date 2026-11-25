package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
)

// mountWatcher probes a directory on a file mount in the same process as the
// database writers. The queue process also touches the file store, so this
// tests the stronger claim: a thread stuck in a hung mount does not stall
// the process's database commits.
type mountWatcher struct {
	probe             func() error
	interval, timeout time.Duration
	probes            atomic.Int64
}

type mountWindow struct {
	Start      time.Time    `json:"start"`
	End        time.Time    `json:"end"`
	Seconds    float64      `json:"seconds"`
	Reason     string       `json:"reason"`
	OpenAtEnd  bool         `json:"open_at_end,omitempty"`
	Append     latencyStats `json:"append_response"`
	RenewMaxMs float64      `json:"renew_max_ms"`
	Errors     int          `json:"errors"`
}

type mountResults struct {
	Probes          int64         `json:"probes"`
	UnresponsiveSec float64       `json:"unresponsive_seconds"`
	Windows         []mountWindow `json:"windows"`
}

// run returns every interval during which the mount failed or stopped
// answering. While a probe is stuck it starts no other, so a hard-hung mount
// costs one blocked thread.
func (w *mountWatcher) run(ctx context.Context) []mountWindow {
	var windows []mountWindow
	var open *mountWindow
	down := func(at time.Time, reason string) {
		if open == nil {
			open = &mountWindow{Start: at.UTC(), Reason: reason}
		}
	}
	up := func(at time.Time) {
		if open != nil {
			open.End = at.UTC()
			windows = append(windows, *open)
			open = nil
		}
	}
	finish := func() []mountWindow {
		if open != nil {
			open.OpenAtEnd = true
			up(time.Now())
		}
		for i := range windows {
			windows[i].Seconds = windows[i].End.Sub(windows[i].Start).Seconds()
		}
		return windows
	}
	for {
		start := time.Now()
		result := make(chan error, 1)
		w.probes.Add(1)
		go func() { result <- w.probe() }()
		timer := time.NewTimer(w.timeout)
		select {
		case err := <-result:
			timer.Stop()
			if err != nil {
				down(start, "error: "+err.Error())
			} else {
				up(time.Now())
			}
		case <-timer.C:
			down(start, fmt.Sprintf("no answer within %s", w.timeout))
			select {
			case err := <-result:
				if err == nil {
					up(time.Now())
				}
			case <-ctx.Done():
				return finish()
			}
		case <-ctx.Done():
			timer.Stop()
			return finish()
		}
		if !sleepUntil(ctx, start.Add(w.interval)) {
			return finish()
		}
	}
}

// correlateMount attaches the commits that ran inside each window.
func correlateMount(windows []mountWindow, appends, renewals []opSample, probes int64) *mountResults {
	m := &mountResults{Probes: probes, Windows: windows}
	for i := range m.Windows {
		win := &m.Windows[i]
		m.UnresponsiveSec += win.Seconds
		var inside []opSample
		for _, s := range appends {
			if !s.At.Before(win.Start) && s.At.Before(win.End) {
				inside = append(inside, s)
			}
		}
		win.Append = statsOf(inside, true, nil)
		win.Errors = win.Append.Errors
		for _, s := range renewals {
			if !s.At.Before(win.Start) && s.At.Before(win.End) {
				win.RenewMaxMs = max(win.RenewMaxMs, ms(s.Lat))
				if s.Kind != "" {
					win.Errors++
				}
			}
		}
	}
	return m
}

// minIsolationWindow is the shortest outage that says anything: ten seconds
// spans ten renewals and over a hundred appends at the measured rate.
const minIsolationWindow = 10 * time.Second

func mountChecks(m *mountResults, maxLimitMs float64) []check {
	prevents := "an SMB stall blocking queue authority, the reason the design uses a database connection instead of a file lock"
	var long []mountWindow
	if m != nil {
		for _, w := range m.Windows {
			if w.Seconds >= minIsolationWindow.Seconds() {
				long = append(long, w)
			}
		}
	}
	if len(long) == 0 {
		return []check{{Name: "commits continue while the mount is unresponsive", Status: statusNotExercised,
			Measured: "no unresponsive window of 10 s or more", Limit: ">= 1 window", Prevents: prevents}}
	}
	ok := true
	worst, total, errs := 0.0, 0, 0
	for _, w := range long {
		worst = max(worst, w.Append.MaxMs, w.RenewMaxMs)
		total += w.Append.Count
		errs += w.Errors
		if w.Append.Count == 0 || w.Errors > 0 || w.Append.MaxMs > maxLimitMs || w.RenewMaxMs > maxLimitMs {
			ok = false
		}
	}
	st := statusPass
	if !ok {
		st = statusFail
	}
	return []check{{Name: "commits continue while the mount is unresponsive", Status: st,
		Measured: fmt.Sprintf("%d windows, %d appends, %d errors, worst %.1f ms", len(long), total, errs, worst),
		Limit:    fmt.Sprintf("appends in every window, 0 errors, max <= %.0f ms", maxLimitMs), Prevents: prevents}}
}
