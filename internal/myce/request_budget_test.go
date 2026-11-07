package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func budgetHost(q *Queue, memory int64, cpus int) {
	q.requestBudgets.memory = func(context.Context) (int64, error) { return memory, nil }
	q.requestBudgets.lanes = cpus + 1
}

func TestRequestBudgetInventoryGrowthAndOlderPeer(t *testing.T) {
	for _, host := range []struct {
		name        string
		ram         int64
		cpus, tools int
	}{
		{"tiny-two-host-installation", 128 << 20, 2, 100000},
		{"large-installation", 128 << 30, 32, 450000},
	} {
		t.Run(host.name, func(t *testing.T) {
			q, clock := fixture(t)
			budgetHost(q, host.ram, host.cpus)
			peers := 2
			if host.cpus > 2 {
				peers = 128
			}
			for i := range peers {
				other := machine(fmt.Sprintf("peer-%d", i), "fake")
				q.s.Machines[other.ID] = &other
			}
			names := make([]string, host.tools)
			for i := range names {
				names[i] = fmt.Sprintf("inventory-tool-%08d", i)
			}
			m := machine("budget-worker", "fake")
			m.ToolInventory = map[string][]string{"": names}
			payload := Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol}
			data, err := json.Marshal(payload)
			if err != nil {
				t.Fatal(err)
			}
			oldLimit := 1 << 20
			if host.tools > 100000 {
				oldLimit = 8 << 20
			}
			if len(data) <= oldLimit {
				t.Fatal("growth fixture does not cross old ceiling", len(data))
			}
			// The legacy worker shape has no budget/inventory-delta fields. It must join
			// the current queue without interpreting any new JSON fields or headers.
			w := httptest.NewRecorder()
			r := httptest.NewRequest("POST", "/api/workers/checkin", bytes.NewReader(data))
			r.Header.Set("Authorization", "Bearer test")
			r.Header.Set("X-Myce-Start-Gate", startGateProtocol)
			API{q, "test"}.Handler().ServeHTTP(w, r)
			if w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
			if w.Header().Get("X-Myce-Request-Budgets") != requestBudgetProtocol {
				t.Fatal("missing negotiation advertisement")
			}
			got := q.s.Machines[m.ID]
			if got == nil || got.LastSeen != clock.Now() {
				t.Fatal("worker contact lost")
			}
			if q.requestBudgets.held != 0 {
				t.Fatal("request memory charge leaked")
			}
		})
	}
}

type unreadBody struct{}

func (unreadBody) Read([]byte) (int, error) { panic("oversize advertised body was read") }
func (unreadBody) Close() error             { return nil }

func TestRequestBudgetHostileAndDecodedExpansion(t *testing.T) {
	q, _ := fixture(t)
	budgetHost(q, 128<<10, 1)
	handler := API{q, "test"}.Handler()
	r := httptest.NewRequest("POST", "/api/jobs", nil)
	r.Body = unreadBody{}
	r.ContentLength = math.MaxInt64
	r.Header.Set("Authorization", "Bearer test")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Code != 413 || !strings.Contains(w.Body.String(), "fitting wire bytes=") {
		t.Fatal(w.Code, w.Body.String())
	}
	limit, err := strconv.ParseInt(w.Header().Get("X-Myce-Max-Body-Bytes"), 10, 64)
	if err != nil || limit <= 0 {
		t.Fatal("missing fitting size")
	}
	// Chunked input cannot evade the same headroom allowance.
	r = httptest.NewRequest("POST", "/api/jobs", strings.NewReader(strings.Repeat(" ", int(limit+1))))
	r.ContentLength = -1
	r.Header.Set("Authorization", "Bearer test")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Code != 413 {
		t.Fatal(w.Code, w.Body.String())
	}
	// Wire-small arrays of sparse large structs are refused before typed decoding.
	var sparse struct{ Rows []struct{ Memory [4096]byte } }
	data := `{"Rows":[` + strings.TrimSuffix(strings.Repeat("{},", 100), ",") + `]}`
	r = httptest.NewRequest("POST", "/fixture", strings.NewReader(data))
	w = httptest.NewRecorder()
	r, finish, err := q.withRequestBudget(w, r)
	if err != nil {
		t.Fatal(err)
	}
	err = decodeBudget(w, r, &sparse)
	finish()
	var refusal *HTTPError
	if !errors.As(err, &refusal) || refusal.Status != 413 || len(sparse.Rows) != 0 {
		t.Fatal("decoded expansion was allocated", err)
	}
	if q.requestBudgets.held != 0 {
		t.Fatal("failed decode leaked capacity")
	}
}

func TestRequestBudgetSharedCapacityAndRecovery(t *testing.T) {
	var available int64 = 1200
	b := requestBudgets{memory: func(context.Context) (int64, error) { return available, nil }, lanes: 1}
	n, _ := b.capacity(t.Context())
	first := requestAllocation{budgets: &b, limit: n}
	n, _ = b.capacity(t.Context())
	second := requestAllocation{budgets: &b, limit: n}
	if !first.charge(900) || second.charge(900) {
		t.Fatal("concurrent requests overcommitted")
	}
	first.close()
	if !second.charge(900) {
		t.Fatal("completion did not release capacity")
	}
	second.close()
	available = 120
	small, _ := b.capacity(t.Context())
	available = 120000
	large, _ := b.capacity(t.Context())
	if large != 1000*small {
		t.Fatal("capacity did not follow host pressure", small, large)
	}
}

func TestRequestBudgetBatchAndNegotiatedDeadline(t *testing.T) {
	q, clock := fixture(t)
	budgetHost(q, 64<<20, 2)
	ids := make([]string, 600)
	for i := range ids {
		ids[i] = fmt.Sprintf("budget-job-%08d", i)
		q.s.Jobs[ids[i]] = &Job{ID: ids[i], State: "succeeded"}
	}
	data, _ := json.Marshal(WaitRequest{IDs: ids})
	r := httptest.NewRequest("POST", "/api/wait", bytes.NewReader(data))
	r.Header.Set("Authorization", "Bearer test")
	w := httptest.NewRecorder()
	API{q, "test"}.Handler().ServeHTTP(w, r)
	var reply WaitReply
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &reply) != nil || len(reply.Jobs) != len(ids) {
		t.Fatal(w.Code, w.Body.String())
	}
	q.requestBudgets.recordControl(100 * time.Millisecond)
	r = httptest.NewRequest("POST", "/api/wait", nil)
	r.Header.Set("X-Myce-Request-Budgets", requestBudgetProtocol)
	r.Header.Set("X-Myce-Transport-Deadline", clock.Now().Add(3*time.Second).Format(time.RFC3339Nano))
	d, err := q.requestBudgets.waitDuration(r, 100, clock.Now())
	if err != nil || d != 2900*time.Millisecond {
		t.Fatal(d, err)
	}
	clock.Advance(4 * time.Second)
	d, err = q.requestBudgets.waitDuration(r, 100, clock.Now())
	if err != nil || d != 0 {
		t.Fatal("expired transport deadline renewed", d, err)
	}
	// Unknown/older protocols keep their explicit wait intent; no new JSON fields.
	r.Header.Set("X-Myce-Request-Budgets", "unknown-protocol")
	d, err = q.requestBudgets.waitDuration(r, 100, clock.Now())
	if err != nil || d != 100*time.Second {
		t.Fatal(d, err)
	}
	_, err = q.requestBudgets.waitDuration(r, math.MaxInt, clock.Now())
	if err == nil {
		t.Fatal("duration overflow accepted")
	}
}

func TestRequestBudgetProjectionCapacityAndCost(t *testing.T) {
	b := requestBudgets{memory: func(context.Context) (int64, error) { return 300 << 20, nil }, lanes: 3}
	typ := reflect.TypeFor[Job]()
	b.recordControl(time.Second)
	b.recordProjection(1, int(typ.Size()), time.Microsecond)
	large, err := b.pageLimit(t.Context(), typ)
	if err != nil || large <= 1000 {
		t.Fatal(large, err)
	}
	b.recordControl(time.Second)
	b.recordProjection(10, 10000, 100*time.Millisecond)
	measured, err := b.pageLimit(t.Context(), typ)
	if err != nil || measured != 100 {
		t.Fatal(measured, err)
	}
	b.recordControl(10 * time.Second)
	faster, _ := b.pageLimit(t.Context(), typ)
	if faster != 1000 {
		t.Fatal("local latency did not change page", faster)
	}
	ctx := context.WithValue(t.Context(), peerResponseBudgetKey{}, int64(3*typ.Size()))
	peer, _ := b.pageLimit(ctx, typ)
	if peer != 1 {
		t.Fatal("peer response capacity ignored", peer)
	}
}

func TestRequestBudgetUploadPhysicalCommitments(t *testing.T) {
	q, _ := fixture(t)
	s := &q.uploadSpool
	// Known lengths reserve unwritten bytes. This fake disk reading is adjusted
	// after a write just as statfs would be; written bytes must not be charged twice.
	free := int64(9 << 30)
	s.free = func(string) (int64, error) { return free, nil }
	s.defaultDir = "" // no control files in this synthetic filesystem
	if err := s.reserve(9 << 30); err != nil {
		t.Fatal("old generic 8 GiB ceiling remains", err)
	}
	if err := s.reserve(1); !errors.Is(err, errUploadSpoolFull) || !strings.Contains(err.Error(), "fitting bytes=0") {
		t.Fatal(err)
	}
	s.mu.Lock()
	s.written = 4 << 30
	s.mu.Unlock()
	free -= 4 << 30
	if err := s.reserve(0); err != nil {
		t.Fatal("written bytes charged twice", err)
	}
	s.release(9 << 30)
	s.mu.Lock()
	s.written = 0
	s.mu.Unlock()
	if err := s.reserve(5 << 30); err != nil {
		t.Fatal("capacity not reusable", err)
	}
	s.release(5 << 30)
}
