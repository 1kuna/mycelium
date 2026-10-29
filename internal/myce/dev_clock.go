package myce

import (
	"context"
	"errors"
	"math"
	"net/http"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

// devClockMaxAdvance bounds each request; repeated advances also cannot overflow
// the duration offset. The offset is process-local and resets on dev restart.
const devClockMaxAdvance = 90 * 24 * time.Hour

// devClock keeps real elapsed time for ordinary dev command execution, with an
// advance-only offset for exercising queue deadlines without waiting days.
// Only StartDev installs it; ordinary Open callers never get the dev route.
type devClock struct {
	mu      sync.Mutex
	offset  time.Duration
	targets []ExtensionCheckTarget
}

func (c *devClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return RealClock{}.Now().Add(c.offset)
}

func (c *devClock) advance(seconds float64) (time.Time, error) {
	if math.IsNaN(seconds) || math.IsInf(seconds, 0) || seconds < 1e-9 || seconds > devClockMaxAdvance.Seconds() {
		return time.Time{}, errors.New("advance must be positive, at least 1ns and at most 90d")
	}
	delta := time.Duration(seconds * float64(time.Second))
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.offset > time.Duration(math.MaxInt64)-delta {
		return time.Time{}, errors.New("dev clock offset would overflow")
	}
	c.offset += delta
	return RealClock{}.Now().Add(c.offset), nil
}

func (a API) registerDevClock(mux *http.ServeMux) {
	clock, ok := a.Queue.clock.(*devClock)
	if !ok {
		return
	}
	mux.HandleFunc("GET /api/dev/extension-targets", func(w http.ResponseWriter, r *http.Request) {
		respond(w, clock.targets, nil)
	})
	mux.HandleFunc("POST /api/dev/clock/advance", func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Seconds float64 `json:"seconds"`
		}
		if !decode(w, r, &request) {
			return
		}
		var now time.Time
		// Advance inside the queue transaction: a heartbeat must not account at
		// the old time and then stamp LastSeen at the new time. That would turn
		// the skipped interval into observed usage on the next transaction.
		err := a.Queue.updateRequest(r.Context(), func() error {
			var err error
			now, err = clock.advance(request.Seconds)
			if err == nil {
				// Preserve Tick's observation window up to the last heartbeat's
				// loss deadline before applying the advanced-time decisions.
				a.Queue.accountUsage(now)
				a.Queue.maintain()
			}
			return err
		}, nil)
		if err == nil {
			err = errors.Join(a.Queue.pruneBatchLogs(r.Context()), a.Queue.reclaimJobFilesContext(r.Context()), a.Queue.pruneJobHistory(r.Context()))
		}
		// A failed transaction cannot rewind a clock other requests observed.
		respond(w, struct {
			Now time.Time `json:"now"`
		}{now}, rejectIf("dev_clock_invalid", err))
	})
}

// Only dev exposes these local process settings. Production checks require
// explicit local host/device/cache flags and never infer a remote worker.
type ExtensionCheckTarget struct {
	Host             string `json:"host"`
	Platform         string `json:"platform"`
	Device           string `json:"device"`
	EnvironmentCache string `json:"environment_cache"`
	OS               string `json:"os"`
	Arch             string `json:"arch"`
}

func ResolveExtensionCheckTarget(ctx context.Context, c Client, host, platform, device, cache string) (ExtensionCheckTarget, error) {
	target := ExtensionCheckTarget{Host: host, Platform: platform, Device: device, EnvironmentCache: cache}
	if host != "" && platform != "" && (platform == "cpu" || device != "") && cache != "" {
		return target, nil
	}
	var targets []ExtensionCheckTarget
	if err := c.Call(ctx, "GET", "/api/dev/extension-targets", nil, &targets); err != nil {
		return target, errors.New("runtime check requires --host, --gpu, --environment-cache and (for GPUs) --device; start myce dev for automatic local defaults")
	}
	if cache != "" {
		absolute, err := filepath.Abs(cache)
		if err != nil {
			return target, err
		}
		cache = absolute
	}
	matches := []ExtensionCheckTarget{}
	for _, t := range targets {
		if t.OS != runtime.GOOS || t.Arch != runtime.GOARCH || host != "" && t.Host != host || platform != "" && t.Platform != platform || device != "" && t.Device != device || cache != "" && filepath.Clean(t.EnvironmentCache) != filepath.Clean(cache) {
			continue
		}
		matches = append(matches, t)
	}
	if len(matches) != 1 {
		return target, errors.New("no unique local dev worker matches; provide --host, --gpu, --device and --environment-cache")
	}
	return matches[0], nil
}
