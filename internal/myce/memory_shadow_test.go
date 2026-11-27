package myce

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func shadowGB(gb float64) *float64 { return &gb }

func TestMemoryShadowProperties(t *testing.T) {
	for _, pool := range []string{"ram", "card/0"} {
		for expected := 1.0; expected <= 24; expected++ {
			for used := 0.0; used <= expected; used++ {
				for growth := 0.0; growth <= expected-used; growth++ {
					for _, free := range []float64{expected - used, expected - used + 8, expected - used + 32} {
						a := MemoryShadowAttempt{ID: "a", Attempt: 1, ExpectedGB: expected, FootprintGB: shadowGB(used), HighWaterGB: used}
						b := MemoryShadowAttempt{ID: "b", Attempt: 1, ExpectedGB: 5, FootprintGB: shadowGB(2), HighWaterGB: 5}
						p := MemoryShadowPool{Name: pool, CapacityGB: 30, FreeGB: shadowGB(free + 3), Attempts: []MemoryShadowAttempt{a, b}}
						p.sum()
						a.Growth, a.FootprintGB, a.HighWaterGB = growth, shadowGB(used+growth), used+growth
						grown := MemoryShadowPool{Name: pool, CapacityGB: p.CapacityGB, FreeGB: shadowGB(*p.FreeGB - growth), Attempts: []MemoryShadowAttempt{a, b}}
						grown.sum()
						grown.forecast(&p, 1, 11)
						remaining := MemoryShadowPool{Name: pool, CapacityGB: p.CapacityGB, FreeGB: shadowGB(*grown.FreeGB + *a.FootprintGB), Attempts: []MemoryShadowAttempt{b}}
						remaining.sum()
						if *p.SlackGB != *grown.SlackGB || *grown.ForecastFreeGB < 0 ||
							*grown.Attempts[0].HoldGB < 0 || *remaining.SlackGB-*grown.SlackGB != *grown.Attempts[0].CommitmentGB ||
							*remaining.ReachGB != *grown.ReachGB {
							t.Fatalf("pool %s E=%g used=%g growth=%g free=%g: before %+v, after %+v, released %+v", pool, expected, used, growth, free, p, grown, remaining)
						}
					}
				}
			}
		}
	}
	for _, lent := range []bool{false, true} {
		p := MemoryShadowPool{Name: "ram", CapacityGB: 100, FreeGB: shadowGB(60), Attempts: []MemoryShadowAttempt{
			{ID: "overshoot", ExpectedGB: 10, HighWaterGB: 32, FootprintGB: shadowGB(30)},
			{ID: "shrunk", ExpectedGB: 12, HighWaterGB: 7, FootprintGB: shadowGB(1), Lent: lent},
		}}
		p.sum()
		hold := 11.0
		if lent {
			hold = 6
		}
		if *p.Attempts[0].HoldGB != 2 || *p.Attempts[1].HoldGB != hold || *p.SlackGB != 60-2-hold {
			t.Fatalf("high-water and lending: %+v", p)
		}
	}
}

func TestMemoryShadowWorkerAndQueueAgree(t *testing.T) {
	ctx := context.Background()
	q, clock, m := memoryAdmissionFixture(t)
	srv := httptest.NewServer(API{Queue: q, Token: "shadow-fixture"}.Handler())
	defer srv.Close()
	wire := &atomic.Bool{}
	worker := Worker{Machine: m, Clock: clock, Interval: time.Second, Client: Client{URL: srv.URL, Token: "shadow-fixture", Clock: clock, memoryShadowWire: wire}}
	c := Checkin{Machine: m}
	if _, err := worker.checkin(ctx, c); err != nil || !wire.Load() {
		t.Fatalf("capability discovery: %v, negotiated=%v", err, wire.Load())
	}
	queued := submit(t, q, cpuSpec(1, 4))
	clock.Advance(time.Second)
	var shadow workerMemoryShadow
	c.Machine.MemoryShadow = shadow.sample(worker, &m, 21.3, nil, nil, clock.Now())
	reply, err := worker.checkin(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	got := assignedJob(reply)
	if got == nil || got.ID != queued.ID {
		t.Fatal("shadow report changed admission")
	}
	if reply.MemoryShadowSettings == nil {
		t.Fatal("missing queue shadow settings")
	}
	snap, err := q.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Machines) != 1 || snap.Machines[0].MemoryShadow == nil ||
		*snap.Machines[0].MemoryShadow.Pools[0].SlackGB != *c.Machine.MemoryShadow.Pools[0].SlackGB {
		t.Fatalf("queue did not preserve the worker's S: %+v", snap.Machines)
	}

	// A deliberate shadow shortage has no authority to issue an R4 stop.
	if err := q.Started(result(got, false)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	c.Attempts = []AttemptReport{{Execution: Execution{ID: got.ID, Attempt: got.Attempt}, Session: m.Session, Started: true}}
	p := MemoryShadowPool{Name: "ram", CapacityGB: 24, FreeGB: shadowGB(18), Attempts: []MemoryShadowAttempt{
		{ID: got.ID, Attempt: got.Attempt, ExpectedGB: 4, FootprintGB: shadowGB(5), HighWaterGB: 5},
	}}
	p.sum()
	p.ForecastFreeGB = shadowGB(-1)
	c.Machine.MemoryShadow = &MemoryShadowReport{Pools: []MemoryShadowPool{p}, ReactionSeconds: 11}
	reply, err = worker.checkin(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	for _, directive := range reply.Attempts {
		if directive.Stop || directive.Kill {
			t.Fatalf("shadow dispatched a stop: %+v", directive)
		}
	}
	if week := q.memoryShadow.week(clock.Now()); week == nil || week.OverE != 1 || week.Raises != 1 {
		t.Fatalf("expected a diagnostic R4 stop and raise: %+v", week)
	}
}

func TestMemoryShadowWireRollback(t *testing.T) {
	var mu sync.Mutex
	var sent []bool
	advertised := true
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		var raw any
		if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
			t.Error(err)
		}
		report := len(jsonFieldsPresent(raw, []string{"memory_shadow"})) != 0
		sent = append(sent, report)
		w.Header().Set("Content-Type", "application/json")
		if advertised {
			w.Header().Set(memoryShadowHeader, memoryShadowProtocol)
		}
		if !advertised && report {
			w.WriteHeader(400)
			_, _ = io.WriteString(w, `{"code":"request_invalid"}`)
			return
		}
		_, _ = io.WriteString(w, `{"attempt_protocol":"keyed-attempt-v1"}`)
	}))
	defer server.Close()
	worker := Worker{Client: Client{URL: server.URL, memoryShadowWire: &atomic.Bool{}}}
	c := Checkin{Machine: Machine{ID: "worker", Session: "session", MemoryShadow: &MemoryShadowReport{}}}
	send := func() {
		t.Helper()
		if _, err := worker.checkin(context.Background(), c); err != nil {
			t.Fatal(err)
		}
	}
	send()
	send()
	mu.Lock()
	advertised = false
	mu.Unlock()
	send()
	send()
	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(sent, []bool{false, true, true, false, false}) {
		t.Fatalf("report requests %v", sent)
	}
}
