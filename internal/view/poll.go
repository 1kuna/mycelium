package view

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Clock is injected so pacing decisions are tested without real time.
type Clock interface {
	Now() time.Time
	After(time.Duration) <-chan time.Time
}

type realClock struct{}

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// pacer chooses the wait before the next status read.
//
//   - floor: the queue serves every unfiltered status read from one shared
//     snapshot generation for up to a second, so reading faster than that
//     returns the same picture and only adds requests.
//   - duty: wait at least nine times the observed read time, so this observer
//     occupies the status path at most a tenth of the time however slow the
//     queue is. A slow read lengthens the next wait at once; faster reads
//     bring it back down gradually.
//   - failures double the wait, up to maxBackoff, so a recovered queue shows
//     up again within a short recording; a 429's Retry-After always wins,
//     even beyond that.
type pacer struct {
	floor, maxBackoff time.Duration
	duty              float64
	smoothed          time.Duration
	failures          int
}

func newPacer() pacer {
	return pacer{floor: time.Second, maxBackoff: 30 * time.Second, duty: 9}
}

func (p *pacer) next(latency, retryAfter time.Duration, failed bool) time.Duration {
	// Latency rises at once and decays by 30% per read, so one slow answer
	// keeps the next few reads spaced out.
	if latency >= p.smoothed {
		p.smoothed = latency
	} else {
		p.smoothed = (p.smoothed*7 + latency*3) / 10
	}
	if !failed {
		p.failures = 0
		return max(p.floor, time.Duration(p.duty*float64(p.smoothed)))
	}
	p.failures++
	d := max(p.floor, time.Duration(p.duty*float64(p.smoothed)))
	for i := 0; i < p.failures && d < p.maxBackoff; i++ {
		d *= 2
	}
	return max(min(d, p.maxBackoff), retryAfter)
}

// rateLimited carries a 429's Retry-After.
type rateLimited struct{ after time.Duration }

func (e *rateLimited) Error() string { return fmt.Sprintf("queue asked to retry after %s", e.after) }

// retryAfter parses delay-seconds or an HTTP date.
func retryAfter(v string, now time.Time) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	if s, err := strconv.ParseInt(v, 10, 64); err == nil {
		if s < 0 {
			return 0
		}
		return time.Duration(s) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil && t.After(now) {
		return t.Sub(now)
	}
	return 0
}

// StatusPath is the only request the view ever makes: the default status read,
// with no query, so it shares the queue's one-second snapshot with every other
// reader and never asks for history (detail=full) or uncached filters.
const StatusPath = "/api/status"

// Upstream reads queue status. It only ever issues GET StatusPath.
type UpstreamReader struct {
	URL, Token, UserAgent string
	HTTP                  *http.Client
	Clock                 Clock
}

func (u UpstreamReader) read(ctx context.Context) (statusDoc, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(u.URL, "/")+StatusPath, nil)
	if err != nil {
		return statusDoc{}, err
	}
	req.Header.Set("Authorization", "Bearer "+u.Token)
	req.Header.Set("User-Agent", u.UserAgent)
	hc := u.HTTP
	if hc == nil {
		hc = &http.Client{Timeout: 10 * time.Second}
	}
	res, err := hc.Do(req) // the transport negotiates and decodes gzip
	if err != nil {
		return statusDoc{}, err
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusTooManyRequests {
		_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 4096))
		return statusDoc{}, &rateLimited{after: retryAfter(res.Header.Get("Retry-After"), u.Clock.Now())}
	}
	if res.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 4096))
		return statusDoc{}, fmt.Errorf("queue status: HTTP %d", res.StatusCode)
	}
	data, err := io.ReadAll(res.Body)
	if err != nil {
		return statusDoc{}, err
	}
	return decodeStatus(data)
}

// Poller makes one upstream read at a time, only while at least one page is
// watching, and fans each frame out to every watcher.
type Poller struct {
	read  func(context.Context) (statusDoc, error)
	clock Clock
	anon  *anonymizer
	log   *log.Logger

	mu     sync.Mutex
	subs   map[chan []byte]struct{}
	latest []byte
	wake   chan struct{}
}

func NewPoller(u UpstreamReader, anonymize bool, logger *log.Logger) *Poller {
	if u.Clock == nil {
		u.Clock = realClock{}
	}
	return newPoller(u.read, u.Clock, anonymize, logger)
}

func newPoller(read func(context.Context) (statusDoc, error), clock Clock, anonymize bool, logger *log.Logger) *Poller {
	p := &Poller{read: read, clock: clock, subs: map[chan []byte]struct{}{}, wake: make(chan struct{}, 1), log: logger}
	if anonymize {
		p.anon = newAnonymizer()
	}
	if p.log == nil {
		p.log = log.New(io.Discard, "", 0)
	}
	return p
}

// Subscribe returns a channel that always holds the newest frame (older
// undelivered frames are replaced) and a cancel function.
func (p *Poller) Subscribe() (<-chan []byte, func()) {
	ch := make(chan []byte, 1)
	p.mu.Lock()
	p.subs[ch] = struct{}{}
	if p.latest != nil {
		ch <- p.latest
	}
	p.mu.Unlock()
	select {
	case p.wake <- struct{}{}:
	default:
	}
	return ch, func() {
		p.mu.Lock()
		delete(p.subs, ch)
		p.mu.Unlock()
	}
}

func (p *Poller) watched() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.subs) > 0
}

func (p *Poller) publish(f Frame) {
	data, err := json.Marshal(f)
	if err != nil {
		p.log.Printf("view: encode frame: %v", err)
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.latest = data
	for ch := range p.subs {
		select {
		case <-ch:
		default:
		}
		ch <- data
	}
}

// Run polls until ctx ends. Reads never overlap, never start while nobody is
// watching, and never start before the pacer's wait has passed, even when a
// page reconnects.
func (p *Poller) Run(ctx context.Context) error {
	pace, proj := newPacer(), newProjector()
	var nextAt, lastRead time.Time
	last := Frame{Machines: []Machine{}, Jobs: []Job{}, Upstream: Upstream{State: "connecting"}}
	for {
		for !p.watched() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-p.wake:
			}
		}
		if wait := nextAt.Sub(p.clock.Now()); wait > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-p.clock.After(wait):
			}
			continue // re-check watchers after waiting
		}
		start := p.clock.Now()
		doc, err := p.read(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		end := p.clock.Now()
		latency := end.Sub(start)
		var limited *rateLimited
		var after time.Duration
		if errors.As(err, &limited) {
			after = limited.after
		}
		delay := pace.next(latency, after, err != nil)
		nextAt = end.Add(delay)
		up := Upstream{State: "ok", LatencyMS: ms(latency), NextPollMS: ms(delay), PolledAt: &end}
		switch {
		case limited != nil:
			up.State, up.RetryAfterMS = "rate_limited", ms(after)
		case err != nil:
			up.State = "unreachable"
			var decode *json.SyntaxError
			if errors.As(err, &decode) || strings.HasPrefix(err.Error(), "queue status: HTTP") {
				up.State = "error"
			}
		}
		if err != nil {
			// Anonymized runs never print the error text: it can name the queue address.
			if p.anon != nil {
				p.log.Printf("view: status read failed (%s); next read in %s", up.State, delay.Round(time.Millisecond))
			} else {
				p.log.Printf("view: status read failed: %v; next read in %s", err, delay.Round(time.Millisecond))
			}
			last.Upstream = up // keep drawing the last good picture
			p.publish(last)
			continue
		}
		gap := time.Duration(0)
		if !lastRead.IsZero() {
			gap = end.Sub(lastRead)
		}
		lastRead = end
		f := proj.project(doc, gap)
		f.Upstream = up
		if p.anon != nil {
			p.anon.frame(&f)
		}
		last = f
		p.publish(f)
	}
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
