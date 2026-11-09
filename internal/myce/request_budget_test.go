package myce

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func budgetHost(q *Queue, memory int64, cpus int) {
	q.requestBudgets.memory = func(context.Context) (int64, error) { return memory, nil }
	q.requestBudgets.lanes = cpus + 1
}

// Match the retained-job count in upgrade-compat. Cold status reads preflight
// each immutable record before decoding it; field metadata must not be rebuilt
// for every JSON key in every retained execution.
func BenchmarkJSONAllocationCostHistory(b *testing.B) {
	job := &Job{ID: "retained-00000", Spec: Spec{Name: "retained", Project: "upgrade-compat", Priority: "experiment"}, State: "succeeded", Attempt: 1, ReleaseConfirmed: true}
	job.Spec.Run.Command = []string{"echo", "retained"}
	data, err := json.Marshal(job)
	if err != nil {
		b.Fatal(err)
	}
	target := reflect.TypeFor[*Job]()
	b.ReportAllocs()
	b.SetBytes(6286 * int64(len(data)))
	b.ResetTimer()
	var charged int64
	for range b.N {
		charged = 0
		for range 6286 {
			cost, err := jsonAllocationCost(data, target, math.MaxInt64)
			if err != nil {
				b.Fatal(err)
			}
			charged += cost
		}
	}
	b.ReportMetric(float64(charged), "charged-B")
}

func TestJSONAllocationCostFieldMetadata(t *testing.T) {
	type Item struct{ Payload [64]byte }
	type Embedded struct {
		Items []Item `json:"items,omitempty"`
	}
	type Shape struct {
		*Embedded
		// Accounting has always used the first EqualFold match, including an
		// earlier anonymous field, rather than preferring a later exact spelling.
		Other   []byte `json:"ITEMS"`
		Ignored []Item `json:"-"`
		hidden  []Item
		Kelvin  []Item `json:"K"`
	}
	base := int64(reflect.TypeFor[Shape]().Size())
	item := int64(reflect.TypeFor[Item]().Size())
	for _, tc := range []struct {
		name, data string
		cost       int64
	}{
		{"anonymous", `{"items":[{},{}]}`, base + 4*item},
		{"case collision", `{"ITEMS":[{}]}`, base + 2*item},
		{"anonymous name", `{"Embedded":{"items":[{}]}}`, base + int64(reflect.TypeFor[Embedded]().Size()) + 2*item},
		{"ignored", `{"Ignored":[{}],"hidden":[{}],"unknown":[{}]}`, base},
		{"unicode fold", `{"K":[{}]}`, base + 2*item},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cost, err := jsonAllocationCost([]byte(tc.data), reflect.TypeFor[*Shape](), math.MaxInt64)
			if err != nil || cost != tc.cost {
				t.Fatalf("charge=%d, want %d: %v", cost, tc.cost, err)
			}
			if _, err := jsonAllocationCost([]byte(tc.data), reflect.TypeFor[*Shape](), tc.cost-1); err == nil {
				t.Fatal("underfunded allocation accepted")
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
