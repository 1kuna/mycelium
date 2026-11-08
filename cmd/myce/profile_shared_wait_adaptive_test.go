package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"mycelium/internal/myce"
)

func adaptiveReceipt(t *testing.T, root, address, name string, expires time.Time) string {
	t.Helper()
	dir := filepath.Join(root, name)
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	receipt := profileReceipt{JobID: name, Status: "running", Observation: "armed", ArmNonce: name}
	if err := saveProfileJSON(filepath.Join(dir, "receipt.json"), receipt); err != nil {
		t.Fatal(err)
	}
	reg := sharedProfileRegistration{Directory: dir, Observer: profileObserver{Queue: address}, Token: "fixture", Expires: expires}
	if err := saveProfileJSON(filepath.Join(root, name+".json"), reg); err != nil {
		t.Fatal(err)
	}
	return dir
}
func adaptiveReply(w http.ResponseWriter, r *http.Request, state string) {
	if r.Method == "GET" {
		fmt.Fprintf(w, `{"id":%q,"state":%q}`, filepath.Base(r.URL.Path), state)
		return
	}
	var request myce.WaitRequest
	_ = json.NewDecoder(r.Body).Decode(&request)
	reply := myce.WaitReply{}
	for _, id := range request.IDs {
		reply.Jobs = append(reply.Jobs, myce.WaitState{ID: id, State: state, Revision: 1})
	}
	_ = json.NewEncoder(w).Encode(reply)
}
func TestSharedProfileAdaptiveSilentAuthorityIsolation(t *testing.T) {
	silent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Drain the request so net/http can observe the client's cancellation
		// while this fixture deliberately withholds its response.
		_, _ = io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
	}))
	defer silent.Close()
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { adaptiveReply(w, r, "succeeded") }))
	defer healthy.Close()
	root := t.TempDir()
	adaptiveReceipt(t, root, silent.URL, "silent", time.Time{})
	done := adaptiveReceipt(t, root, healthy.URL, "healthy", time.Time{})
	lock, err := profileLock(filepath.Join(root, "observer.lock"))
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	err = runSharedProfileLoop(ctx, root, lock, myce.RealClock{}, waitPause)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	var receipt profileReceipt
	if err := readProfileJSON(filepath.Join(done, "receipt.json"), &receipt); err != nil || receipt.Status != "succeeded" || receipt.Observation != "not_armed" {
		t.Fatal("silent authority blocked healthy completion", receipt, err)
	}
}
func TestSharedProfileAdaptiveRetryAfterAcrossBatchesAndRegistration(t *testing.T) {
	for _, status := range []int{429, 503} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			clock := &waitClock{now: time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)}
			var mu sync.Mutex
			var calls []time.Time
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				calls = append(calls, clock.Now())
				mu.Unlock()
				w.Header().Set("Retry-After", "7")
				w.WriteHeader(status)
			}))
			defer server.Close()
			root := t.TempDir()
			for i := 0; i < 257; i++ {
				adaptiveReceipt(t, root, server.URL, fmt.Sprintf("job-%03d", i), time.Time{})
			}
			lock, err := profileLock(filepath.Join(root, "observer.lock"))
			if err != nil {
				t.Fatal(err)
			}
			defer lock.Close()
			added := false
			stop := errors.New("observed second paced read")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err = runSharedProfileLoop(ctx, root, lock, clock, func(_ context.Context, d time.Duration) error {
				if !added {
					adaptiveReceipt(t, root, server.URL, "new-registration", time.Time{})
					added = true
				}
				mu.Lock()
				defer mu.Unlock()
				if len(calls) > 1 {
					return stop
				}
				clock.advance(d)
				return nil
			})
			if !errors.Is(err, stop) {
				t.Fatal(err)
			}
			mu.Lock()
			defer mu.Unlock()
			if len(calls) != 2 || calls[1].Sub(calls[0]) < 7*time.Second {
				t.Fatal("Retry-After bypassed by batch or registration", calls)
			}
		})
	}
}
func TestSharedProfileAdaptiveRetiresAuthorityPacing(t *testing.T) {
	var pendingReads atomic.Int32
	terminal := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { adaptiveReply(w, r, "succeeded") }))
	defer terminal.Close()
	pending := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { pendingReads.Add(1); adaptiveReply(w, r, "running") }))
	defer pending.Close()
	root := t.TempDir()
	done := adaptiveReceipt(t, root, terminal.URL, "done", time.Time{})
	adaptiveReceipt(t, root, pending.URL, "pending", time.Time{})
	lock, err := profileLock(filepath.Join(root, "observer.lock"))
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	clock := &waitClock{now: time.Now()}
	stop := errors.New("observed continued paced reads")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = runSharedProfileLoop(ctx, root, lock, clock, func(_ context.Context, d time.Duration) error {
		var receipt profileReceipt
		_ = readProfileJSON(filepath.Join(done, "receipt.json"), &receipt)
		if receipt.Observation == "not_armed" && d <= 0 {
			t.Fatal("retired authority forced busy polling")
		}
		clock.advance(d)
		if receipt.Observation == "not_armed" && pendingReads.Load() >= 5 {
			return stop
		}
		return nil
	})
	if !errors.Is(err, stop) {
		t.Fatal(err)
	}
}

func TestSharedProfileAdaptiveCompletionRecomputesNextRead(t *testing.T) {
	clock := &waitClock{now: time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)}
	var reads atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			adaptiveReply(w, r, "succeeded")
			return
		}
		var request myce.WaitRequest
		_ = json.NewDecoder(r.Body).Decode(&request)
		// Every response carries new progress after one observed service period.
		// Repeated coordinator scans must not add a growing delay to those events.
		clock.advance(time.Second)
		n := reads.Add(1)
		state := "running"
		if n == 50 {
			state = "succeeded"
		}
		_ = json.NewEncoder(w).Encode(myce.WaitReply{Jobs: []myce.WaitState{{ID: "job", State: state, Revision: uint64(n)}}})
	}))
	defer server.Close()
	root := t.TempDir()
	dir, _, lock := sharedWatchFixture(t, root, server.URL, clock)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := runSharedProfileLoop(ctx, root, lock, clock, func(_ context.Context, d time.Duration) error {
		if d > time.Second {
			return fmt.Errorf("progress delayed beyond observed service period: %s", d)
		}
		clock.advance(d)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var receipt profileReceipt
	if err := readProfileJSON(filepath.Join(dir, "receipt.json"), &receipt); err != nil || receipt.Observation != "not_armed" || receipt.Status != "succeeded" {
		t.Fatal("progressing observer did not retire", receipt, err)
	}
}

func TestSharedProfileAdaptiveUnknownDoesNotStarveLaterBatch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			state := "running"
			if filepath.Base(r.URL.Path) == "job-256" {
				state = "succeeded"
			}
			adaptiveReply(w, r, state)
			return
		}
		var req myce.WaitRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.IDs[0] == "job-000" {
			w.WriteHeader(404)
			_, _ = io.WriteString(w, `{"code":"job_unknown"}`)
			return
		}
		_ = json.NewEncoder(w).Encode(myce.WaitReply{Jobs: []myce.WaitState{{ID: "job-256", State: "succeeded", Revision: 1}}})
	}))
	defer server.Close()
	watches := map[string]*sharedProfileWatch{}
	entries := []string{}
	for i := 0; i < 257; i++ {
		name := fmt.Sprintf("job-%03d", i)
		entries = append(entries, name)
		watches[name] = &sharedProfileWatch{registration: sharedProfileRegistration{Observer: profileObserver{Queue: server.URL}, Token: "fixture"}, receipt: profileReceipt{JobID: name}}
	}
	result := readSharedProfileAuthority(context.Background(), entries, watches, sharedProfileGroup{after: map[string]uint64{}}, myce.RealClock{})
	if len(result.finished) != 1 || result.finished[0].entry != "job-256" {
		t.Fatalf("unknown first batch starved completion: %+v", result.finished)
	}
}

// A healthy held read paces itself, yet used to double hidden retry history.
// After enough unchanged replies, one timeout scheduled the next read centuries
// away (#919). Exercise the actual authority reader through recovery, including
// servers with more than one batch (where protocol holds are disabled).
func TestSharedProfileAdaptiveNextReadBoundedByServiceAndPressure(t *testing.T) {
	for _, count := range []int{1, 257} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			clock := &waitClock{now: time.Date(2026, 11, 8, 10, 0, 0, 0, time.UTC)}
			var mu sync.Mutex
			var failure int
			var disconnect bool
			var retryAfter time.Duration
			terminal := false
			const service = time.Second
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				clock.advance(service)
				if disconnect {
					conn, _, err := w.(http.Hijacker).Hijack()
					if err == nil {
						_ = conn.Close()
					}
					return
				}
				if failure != 0 {
					w.Header().Set("Retry-After", fmt.Sprint(int(retryAfter/time.Second)))
					w.WriteHeader(failure)
					_, _ = io.WriteString(w, `{"code":"request_failed"}`)
					return
				}
				state := "running"
				if terminal {
					state = "succeeded"
				}
				adaptiveReply(w, r, state)
			}))
			defer server.Close()
			entries := make([]string, 0, count)
			watches := map[string]*sharedProfileWatch{}
			for i := 0; i < count; i++ {
				id := fmt.Sprintf("job-%03d", i)
				entries = append(entries, id)
				watches[id] = &sharedProfileWatch{registration: sharedProfileRegistration{Observer: profileObserver{Queue: server.URL}, Token: "fixture"}, receipt: profileReceipt{JobID: id}}
			}
			group := sharedProfileGroup{after: map[string]uint64{}}
			read := func() sharedProfileRead {
				result := readSharedProfileAuthority(context.Background(), entries, watches, group, clock)
				group = result.group
				return result
			}
			// Stable revisions for more reads than it takes to saturate a
			// duration, without advancing centuries or sleeping in real time.
			for i := 0; i < 80; i++ {
				read()
				if d := group.next.Sub(clock.Now()); d < 0 || d > time.Duration((count+255)/256)*service {
					t.Fatalf("unchanged response scheduled beyond measured read cost: %s", d)
				}
				clock.advance(group.next.Sub(clock.Now()))
			}
			// A transient transport failure must not turn the previous healthy
			// read count into a backoff timer (the retained incident was timeout).
			mu.Lock()
			disconnect = true
			mu.Unlock()
			read()
			if d := group.next.Sub(clock.Now()); !group.retrying || d <= 0 || d > 2*time.Duration((count+255)/256)*service {
				t.Fatalf("failure inherited unchanged-read history: %s", d)
			}
			mu.Lock()
			disconnect = false
			mu.Unlock()
			for _, signal := range []struct {
				status int
				after  time.Duration
			}{{429, 7 * time.Second}, {503, 3 * time.Second}, {503, time.Second}} {
				clock.advance(group.next.Sub(clock.Now()))
				mu.Lock()
				failure, retryAfter = signal.status, signal.after
				mu.Unlock()
				read()
				d := group.next.Sub(clock.Now())
				// Each rejection ends the pass at its first batch. Earlier
				// two-batch service measurements may still be in the mean.
				bound := time.Duration((count+255)/256) * service
				if !group.retrying || d < retryAfter || d > max(bound, retryAfter)+bound {
					t.Fatalf("next read escaped current service/pressure bound: %s, server %s", d, retryAfter)
				}
			}
			clock.advance(group.next.Sub(clock.Now()))
			mu.Lock()
			failure, terminal = 0, true
			mu.Unlock()
			result := read()
			if group.retrying || len(result.finished) != count {
				t.Fatalf("authority failed to resume: retrying=%v completions=%d", group.retrying, len(result.finished))
			}
		})
	}
}

func TestSharedProfileAdaptiveTerminalDemandSurvivesPressure(t *testing.T) {
	clock := &waitClock{now: time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)}
	next := clock.Now()
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		now := clock.Now()
		if now.Before(next) {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(429)
			return
		}
		next = now.Add(time.Second)
		adaptiveReply(w, r, "succeeded")
	}))
	defer server.Close()
	watches := map[string]*sharedProfileWatch{}
	entries := []string{"first", "second"}
	for _, id := range entries {
		watches[id] = &sharedProfileWatch{registration: sharedProfileRegistration{Observer: profileObserver{Queue: server.URL}, Token: "fixture"}, receipt: profileReceipt{JobID: id}}
	}
	group := sharedProfileGroup{after: map[string]uint64{}}
	seen := map[string]bool{}
	// The fixture admits one request per advertised pressure period. A
	// successful batch must not keep spending that credit ahead of its pending
	// terminal GET, and retiring the first receipt must preserve the second.
	for i := 0; i < 20 && len(entries) > 0; i++ {
		result := readSharedProfileAuthority(context.Background(), entries, watches, group, clock)
		for _, action := range result.finished {
			if action.job == nil || action.job.State != "succeeded" || seen[action.job.ID] {
				t.Fatalf("invalid or duplicate completion: %+v", action)
			}
			seen[action.job.ID] = true
			delete(watches, action.entry)
		}
		active := entries[:0]
		for _, entry := range entries {
			if watches[entry] != nil {
				active = append(active, entry)
			}
		}
		entries = active
		group = result.group
		clock.advance(group.next.Sub(clock.Now()))
	}
	if len(seen) != 2 {
		t.Fatal("no terminal progress despite successful batch states and honoring each Retry-After", len(seen))
	}
}
