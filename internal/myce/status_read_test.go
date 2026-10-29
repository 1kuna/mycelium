package myce

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A diagnostic read must use the same occupied CPU ledger as admission. Four
// admitted cores with three reserved cannot place the next two-core job.
func TestStatusReadCPUReservation(t *testing.T) {
	q, _ := fixture(t)
	m := cpuMachine("cpu", "shared", "cpu", 4, 16)
	check(t, q, m, nil)
	first := submit(t, q, cpuSpec(3, 1))
	assignment := check(t, q, m, nil).Job
	if assignment == nil || assignment.ID != first.ID {
		t.Fatal("no assignment")
	}
	second := submit(t, q, cpuSpec(2, 1))
	if cpuFits(second, q.s.Machines[m.ID]) {
		t.Fatal("fixture must exhaust CPU capacity")
	}
	handler := API{Queue: q, Token: "test"}.Handler()
	for _, path := range []string{"/api/jobs/" + second.ID, "/api/status", "/api/inspection"} {
		t.Run(path, func(t *testing.T) {
			r := httptest.NewRequest("GET", path, nil)
			r.Header.Set("Authorization", "Bearer test")
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			if w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
			var got *Job
			if path == "/api/jobs/"+second.ID {
				if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
					t.Fatal(err)
				}
			} else {
				var snapshot Snapshot
				if err := json.Unmarshal(w.Body.Bytes(), &snapshot); err != nil {
					t.Fatal(err)
				}
				for _, j := range snapshot.Jobs {
					if j.ID == second.ID {
						got = j
					}
				}
			}
			if got == nil {
				t.Fatal("waiting job missing from response")
			}
			if !strings.Contains(got.Reason, "1 cores unreserved of 4 admitted") || got.QueueOrder != 0 {
				t.Fatalf("occupied CPU capacity lost: reason=%q queue_order=%d", got.Reason, got.QueueOrder)
			}
		})
	}
}

// Receipt polling must remain independent of both the usage reader and writer
// connection, and must preserve private payload redaction and exact key echo.
func TestJobReadAvoidsUsage(t *testing.T) {
	q, _ := fixture(t)
	s := spec("experiment")
	s.SubmissionKey = "poll-key"
	s.Run.Command = []string{"echo", "private-poll-payload"}
	j := submit(t, q, s)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	writer, err := q.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	reader, err := q.usageReader.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	client := Client{URL: server.URL, Token: "test"}
	var got Job
	if err := client.Call(ctx, "GET", "/api/jobs/"+j.ID, nil, &got); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(got)
	if got.ID != j.ID || got.Spec.SubmissionKey != s.SubmissionKey || strings.Contains(string(data), "private-poll-payload") {
		t.Fatal("job receipt lost identity or exposed execution payload")
	}
}

func TestJobReadCommittedSubmissionIgnoresStatusCache(t *testing.T) {
	q, _ := fixture(t)
	// Keep the injected clock fixed, so every aggregate read stays on this
	// empty generation even after the submission transaction commits.
	before, err := q.cachedStatusJSON()
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	c := Client{URL: server.URL, Token: "test"}
	j, err := c.Submit(context.Background(), spec("experiment"))
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{j.ID, j.ID[:8]} {
		got, err := c.GetJob(context.Background(), id)
		if err != nil || got.ID != j.ID || got.State != "queued" {
			t.Fatalf("committed job missing for %s: %+v %v", id, got, err)
		}
	}
	after, err := q.cachedStatusJSON()
	if err != nil || before != after {
		t.Fatal("fixture did not retain stale aggregate generation", err)
	}
}

func TestJobReadEligibilityLifecycle(t *testing.T) {
	q, _ := fixture(t)
	a, b := machine("receipt-a", "fake", "host-a"), machine("receipt-b", "fake", "host-b")
	check(t, q, a, nil)
	check(t, q, b, nil)
	j := submit(t, q, spec("production"))
	attempt := check(t, q, a, nil).Job
	if attempt == nil || attempt.ID != j.ID {
		t.Fatal("no assignment")
	}
	if err := q.Started(result(attempt, true)); err != nil {
		t.Fatal(err)
	}
	if err := q.ConfigureProjects(map[string]ProjectPolicy{"test": {Weight: 1, AllowedMachines: []string{b.ID, "unknown"}}}); err != nil {
		t.Fatal(err)
	}
	assertActive := func(state string) {
		t.Helper()
		got, err := q.jobSnapshot(j.ID[:8])
		if err != nil {
			t.Fatal(err)
		}
		if got.State != state || got.EligibleUnrestricted || got.EligibleHosts == nil || !slices.Equal(*got.EligibleHosts, []string{"host-a", "host-b"}) || !slices.Equal(got.EligibleUnknownMachines, []string{"unknown"}) {
			t.Fatalf("receipt lost current policy or execution host: %+v", got)
		}
	}
	assertActive("running")
	if err := q.Cancel(j.ID); err != nil {
		t.Fatal(err)
	}
	assertActive("stopping")
	if err := q.Complete(result(attempt, false)); err != nil {
		t.Fatal(err)
	}
	got, err := q.jobSnapshot(j.ID)
	if err != nil || got.State != "cancelled" || got.EligibleHosts != nil || got.EligibleUnrestricted || len(got.EligibleUnknownMachines) != 0 {
		t.Fatal("terminal receipt retained active eligibility", err)
	}
}

func TestStatusReadPreservesDeadlineReason(t *testing.T) {
	q, c := fixture(t)
	s := spec("production")
	j := submit(t, q, s)
	c.Advance(ProductionStartTimeout + time.Second)
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	receipt, err := q.jobSnapshot(j.ID)
	if err != nil || receipt.State != "failed" || receipt.Reason != "production start deadline exceeded" {
		t.Fatal("receipt lost deadline diagnostic", receipt, err)
	}
	status, err := q.cachedPublicSnapshot()
	if err != nil || len(status.Jobs) != 1 || status.Jobs[0].Reason != receipt.Reason {
		t.Fatal("global status lost deadline diagnostic", err)
	}
}

func TestStatusInspectionRemainsFresh(t *testing.T) {
	q, _ := fixture(t)
	s := spec("experiment")
	s.Run.Command = []string{"echo", "private-inspection-payload"}
	j := submit(t, q, s)
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	client := Client{URL: server.URL, Token: "test"}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var cached Snapshot
	if err := client.Call(ctx, "GET", "/api/status", nil, &cached); err != nil {
		t.Fatal(err)
	}
	if err := q.Cancel(j.ID); err != nil {
		t.Fatal(err)
	}
	current, err := client.inspectionSnapshot(ctx)
	if err != nil || len(current.Jobs) != 1 || current.Jobs[0].State != "cancelled" || cached.Jobs[0].State != "queued" {
		t.Fatal("inspection reused stale status", err)
	}
	data, _ := json.Marshal(current)
	if strings.Contains(string(data), "private-inspection-payload") {
		t.Fatal("inspection exposed execution payload")
	}
}

// A coalesced refresh can wait for its isolated usage snapshot while both start
// gates and cancellation continue. Its stale observation never authorizes work.
func TestStatusReadCoalescesAndDoesNotAuthorize(t *testing.T) {
	q, c := fixture(t)
	j := submit(t, q, spec("production"))
	a := check(t, q, machine("reader-test", "fake"), nil).Job
	if a == nil || a.ID != j.ID {
		t.Fatal("no assignment")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	reader, err := q.usageReader.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	captured := make(chan struct{})
	var captures atomic.Int32
	q.observeRead = func(kind string, _, _ time.Duration) {
		if kind == "status" && captures.Add(1) == 1 {
			close(captured)
		}
	}
	type answer struct {
		s   Snapshot
		err error
	}
	answers := make(chan answer, 8)
	for range 8 {
		go func() { s, err := q.cachedPublicSnapshot(); answers <- answer{s, err} }()
	}
	select {
	case <-captured:
	case <-ctx.Done():
		t.Fatal("status never captured")
	}
	launch := HostLaunch{Kind: "batch", Key: a.ID, Machine: a.Machine, Session: a.Session, Attempt: strconv.Itoa(a.Attempt)}
	controls := make(chan error, 1)
	go func() {
		err := q.Started(result(a, true))
		if err == nil {
			err = q.AuthorizeLaunch(launch)
		}
		if err == nil {
			_, err = q.CancelJob(a.ID)
		}
		controls <- err
	}()
	select {
	case err := <-controls:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("usage observation blocked a control transaction")
	}
	reader.Close()
	for range 8 {
		select {
		case got := <-answers:
			if got.err != nil || len(got.s.Jobs) != 1 || got.s.Jobs[0].CancelledAt != nil {
				t.Fatal("refresh was not a single captured generation", got.err)
			}
			got.s.Jobs[0].Spec.Needs.Platform.Any = false
			got.s.PriorityPolicy.Policy.Tiers[0].Selectors[0] = "changed"
		case <-ctx.Done():
			t.Fatal("status refresh did not finish")
		}
	}
	if captures.Load() != 1 {
		t.Fatal("concurrent status reads repeated capture", captures.Load())
	}
	if err := q.Started(result(a, true)); !errors.Is(err, ErrStale) {
		t.Fatal("stale status authorized Started", err)
	}
	if err := q.AuthorizeLaunch(launch); err == nil {
		t.Fatal("stale status authorized launch")
	}
	fresh, err := q.jobSnapshot(a.ID)
	if err != nil || fresh.CancelledAt == nil {
		t.Fatal("job polling reused stale global status", err)
	}
	// A cache hit must work even while the actual queue is exclusively locked.
	q.mu.Lock()
	hit := make(chan answer, 1)
	go func() { s, err := q.cachedPublicSnapshot(); hit <- answer{s, err} }()
	var got answer
	select {
	case got = <-hit:
	case <-ctx.Done():
		q.mu.Unlock()
		t.Fatal("cache hit acquired q.mu")
	}
	q.mu.Unlock()
	if got.err != nil || !got.s.Jobs[0].Spec.Needs.Platform.Any || got.s.PriorityPolicy.Policy.Tiers[0].Selectors[0] == "changed" {
		t.Fatal("caller mutated the cached generation")
	}
	c.Advance(time.Second)
	snap, err := q.cachedPublicSnapshot()
	if err != nil || snap.Jobs[0].CancelledAt == nil || captures.Load() != 2 {
		t.Fatal("injected-clock expiry did not refresh status", err)
	}
}

func TestStatusReadRace(t *testing.T) {
	q, c := fixture(t)
	clock := &workerFakeClock{now: c.Now()}
	q.clock = clock
	s := spec("experiment")
	s.SubmissionKey = "race-key"
	s.Run.Env = map[string]string{"PRIVATE": "private-poll-payload"}
	j := submit(t, q, s)
	m := machine("race-reader", "fake")
	check(t, q, m, nil)
	var wg sync.WaitGroup
	errs := make(chan error, 5)
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 12 {
				snapshot, err := q.cachedPublicSnapshot()
				if err != nil {
					errs <- err
					return
				}
				data, _ := json.Marshal(snapshot)
				if strings.Contains(string(data), "private-poll-payload") {
					errs <- errors.New("status exposed private payload")
					return
				}
				got, err := q.jobSnapshot(j.ID)
				if err != nil || got.Spec.SubmissionKey != s.SubmissionKey {
					errs <- errors.New("job receipt lost its key")
					return
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 12 {
			clock.Advance(time.Second)
			if _, err := q.Checkin(Checkin{Machine: m, Running: &Execution{ID: j.ID, Attempt: 1}}); err != nil {
				errs <- err
				return
			}
		}
	}()
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}
