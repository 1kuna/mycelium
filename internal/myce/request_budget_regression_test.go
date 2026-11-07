package myce

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestRequestBudgetSharedMeasurementSurvivesCallerCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered, release := make(chan struct{}), make(chan struct{})
		var calls atomic.Int32
		b := requestBudgets{memory: func(ctx context.Context) (int64, error) {
			if calls.Add(1) != 1 {
				return 0, errors.New("duplicate measurement")
			}
			close(entered)
			select {
			case <-release:
				return 64 << 20, nil
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}}
		ctx, cancel := context.WithCancel(t.Context())
		first, second := make(chan error, 1), make(chan error, 1)
		go func() { _, err := b.capacity(ctx); first <- err }()
		<-entered
		go func() { _, err := b.capacity(t.Context()); second <- err }()
		synctest.Wait()
		cancel()
		synctest.Wait()
		close(release)
		if err := <-second; err != nil {
			t.Fatal("independent cleanup inherited observer cancellation", err)
		}
		if err := <-first; !errors.Is(err, context.Canceled) {
			t.Fatal("cancelled observer did not return its own context error", err)
		}
		if calls.Load() != 1 {
			t.Fatal("overlapping requests did not share one measurement")
		}
	})
}

func TestRequestBudgetExplicitPageNeed(t *testing.T) {
	q, c := fixture(t)
	budgetHost(q, 256<<20, 1)
	// A slow prior snapshot and cheap control update used to collapse every
	// history request to one row, even when the caller requests a fitting page.
	q.requestBudgets.recordControl(time.Microsecond)
	q.requestBudgets.recordProjection(1, int(reflect.TypeFor[Job]().Size()), time.Second)
	for _, id := range []string{"first", "second", "third"} {
		q.s.Jobs[id] = &Job{ID: id, State: "succeeded", ReleaseConfirmed: true, Queued: c.Now()}
	}
	s := readStatusOptions(t, q, "?jobs=all&limit=3")
	if len(s.Jobs) != 3 || s.JobsPage.NextBefore != "" {
		t.Fatalf("fitting requested page truncated: %+v", s.JobsPage)
	}
}

func TestRequestBudgetMeasurementRetiresAfterLastWaiter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered, exited := make(chan struct{}), make(chan struct{})
		var calls atomic.Int32
		b := requestBudgets{memory: func(ctx context.Context) (int64, error) {
			if calls.Add(1) == 1 {
				close(entered)
				<-ctx.Done()
				close(exited)
				return 0, ctx.Err()
			}
			return 64 << 20, nil
		}}
		ctx, cancel := context.WithCancel(t.Context())
		result := make(chan error, 1)
		go func() { _, err := b.capacity(ctx); result <- err }()
		<-entered
		cancel()
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		<-exited
		n, err := b.capacity(t.Context())
		if err != nil || n <= 0 {
			t.Fatal("new caller inherited abandoned measurement", n, err)
		}
	})
}

func TestExtensionMeasurementFailureIsNotPayloadRefusal(t *testing.T) {
	b := requestBudgets{memory: func(context.Context) (int64, error) { return 0, errors.New("private unavailable source") }}
	n, err := extensionByteLimitFrom(&b)
	var api *HTTPError
	if n != 0 || !errors.As(err, &api) || api.Status != 503 || api.Code != "request_capacity_unavailable" {
		t.Fatalf("measurement failure became a payload size: %d %v", n, err)
	}
}

func TestRequestBudgetRowSampleExcludesFleetEnvelope(t *testing.T) {
	q, c := fixture(t)
	budgetHost(q, 256<<20, 1)
	q.s.Jobs["sample"] = &Job{ID: "sample", State: "succeeded", ReleaseConfirmed: true, Queued: c.Now()}
	snapshot := readStatusOptions(t, q, "?jobs=all&limit=1")
	data, err := json.Marshal(snapshot.Jobs)
	if err != nil {
		t.Fatal(err)
	}
	q.requestBudgets.mu.Lock()
	row := q.requestBudgets.rowBytes
	q.requestBudgets.mu.Unlock()
	if row != int64(len(data)) {
		t.Fatalf("job sample includes independent fleet/usage work: got %d, job bytes %d", row, len(data))
	}
	q.requestBudgets.mu.Lock()
	q.requestBudgets.rowBytes = int64(reflect.TypeFor[Job]().Size())
	q.requestBudgets.mu.Unlock()
	ctx := context.WithValue(t.Context(), peerResponseBudgetKey{}, int64(3*reflect.TypeFor[Job]().Size()))
	limit, err := q.requestBudgets.pageLimit(ctx, reflect.TypeFor[Job](), 100)
	if err != nil || limit != 1 {
		t.Fatal("requested page bypassed physical peer capacity", limit, err)
	}
}
