package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestHostMemoryBudgetDurableWhileDisabled(t *testing.T) {
	dir := t.TempDir()
	if _, err := (HostControl{Dir: dir, Service: &fakeHostService{}}).Disable(context.Background(), "legacy owner", 0); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(filepath.Join(dir, "DISABLED"))
	first, err := SetHostMemoryBudget(dir, 3)
	if err != nil {
		t.Fatal(err)
	}
	second, err := SetHostMemoryBudget(dir, 27)
	if err != nil {
		t.Fatal(err)
	}
	if first.Generation == 0 || second.Generation != first.Generation+1 {
		t.Fatal("unordered generation")
	}
	scoped, _ := WorkerUnitDirectory(dir, "myce-worker@gpu1.service")
	budget, source, err := EffectiveHostMemoryBudget(scoped, 5)
	if err != nil || budget.GiB != 27 || budget.Generation != second.Generation || !strings.HasSuffix(source, "host-memory-budget.json") {
		t.Fatal(budget, source, err)
	}
	after, _ := os.ReadFile(filepath.Join(dir, "DISABLED"))
	if string(after) != string(before) {
		t.Fatal("set cleared disable gate")
	}
	if _, err := os.Stat(filepath.Join(dir, "host-memory-budget.json.tmp")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("atomic temp remained")
	}
	st, _ := os.Stat(filepath.Join(dir, "host-memory-budget.json"))
	if st.Mode().Perm() != 0600 {
		t.Fatal("budget file not private")
	}
	if _, err := SetHostMemoryBudget(dir, 0); err == nil {
		t.Fatal("zero/unbounded override accepted")
	}
	w := Worker{HostStateDir: scoped, Machine: Machine{HostMemoryBudgetGB: 5}}
	if err := w.refreshHostMemoryBudget(); err != nil || w.Machine.HostMemoryBudgetGB != 27 {
		t.Fatal("disabled install budget not overridden", err)
	}
	if err := (HostControl{Dir: dir}).Enable(context.Background()); err != nil {
		t.Fatal("could not enable after offline budget change", err)
	}
	if err := w.refreshHostMemoryBudget(); err != nil || w.Machine.HostMemoryBudgetGB != 27 {
		t.Fatal("enable lost shared budget", err)
	}
}

func TestHostMemoryBudgetLiveRaiseLowerAndQueueStatus(t *testing.T) {
	q, c := fixture(t)
	dir := t.TempDir()
	a, b := machine("card0", "fake"), machine("card1", "fake")
	for _, m := range []*Machine{&a, &b} {
		m.HostID = "box"
		m.HostMemoryMeasured = true
		m.HostAvailableMemoryGB = 64
		m.HostMemoryBudgetGB = 3
	}
	SetHostMemoryBudget(dir, 3)
	report := func(m *Machine) {
		w := Worker{HostStateDir: dir, Machine: *m}
		if err := w.refreshHostMemoryBudget(); err != nil {
			t.Fatal(err)
		}
		*m = w.Machine
	}
	report(&a)
	report(&b)
	check(t, q, a, nil)
	check(t, q, b, nil)
	s := spec("experiment")
	s.Needs.HostMemoryGB = 10
	s.AllowedMachines = []string{a.ID}
	j := submit(t, q, s)
	if r := check(t, q, a, nil); r.Job != nil {
		t.Fatal("3 GiB admitted 10")
	}
	SetHostMemoryBudget(dir, 27)
	report(&a)
	// A sibling's older generation must not pin the host at 3 GiB after a raise.
	r := check(t, q, a, nil)
	if r.Job == nil || r.Job.ID != j.ID {
		t.Fatal("live raise not used")
	}
	running := r.Job
	c.Advance(1)
	SetHostMemoryBudget(dir, 3)
	report(&b)
	check(t, q, b, nil)
	requireState(t, q, j.ID, "running")
	s.Priority = "production"
	s.Needs.HostMemoryGB = 1
	s.AllowedMachines = []string{b.ID}
	blocked := submit(t, q, s)
	if r := check(t, q, b, nil); r.Job != nil {
		t.Fatal("over-budget host admitted more work")
	}
	requireState(t, q, j.ID, "running")
	requireState(t, q, blocked.ID, "queued")
	snap, err := q.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range snap.Machines {
		if m.EffectiveHostMemoryBudgetGB != 3 || m.HostMemoryInUseGB != 10 || m.HostMemoryBudgetStatus() != "over budget: 10 GiB in use > 3 GiB" {
			t.Fatal("missing over-budget status", m)
		}
	}
	var rendered bytes.Buffer
	if err := statusPage.Execute(&rendered, snap); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(rendered.String(), "over budget: 10 GiB in use &gt; 3 GiB") {
		t.Fatal("web status missing over-budget message")
	}
	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/status", nil)
	req.Header.Set("Authorization", "Bearer test")
	API{Queue: q, Token: "test"}.Handler().ServeHTTP(rr, req)
	var api Snapshot
	if rr.Code != 200 || json.Unmarshal(rr.Body.Bytes(), &api) != nil || len(api.Machines) != 2 {
		t.Fatal("queue API status missing host budget")
	}
	for _, m := range api.Machines {
		if m.EffectiveHostMemoryBudgetGB != 3 || m.HostMemoryInUseGB != 10 {
			t.Fatal("API lost budget usage")
		}
	}
	// A stale heartbeat cannot undo the newer shared generation.
	check(t, q, a, running)
	snap, _ = q.Snapshot()
	for _, m := range snap.Machines {
		if m.EffectiveHostMemoryBudgetGB != 3 {
			t.Fatal("stale generation replaced budget")
		}
	}
	finish(t, q, running, true)
	if r := check(t, q, b, nil); r.Job == nil || r.Job.ID != blocked.ID {
		t.Fatal("finish did not release budget")
	}
}

func TestHostMemoryBudgetWorkerReportsLiveWithoutRestart(t *testing.T) {
	dir := t.TempDir()
	h := HostControl{Dir: dir, Service: &fakeHostService{}}
	if _, err := h.Disable(context.Background(), "install disabled", 0); err != nil {
		t.Fatal(err)
	}
	SetHostMemoryBudget(dir, 3)
	if err := h.Enable(context.Background()); err != nil {
		t.Fatal(err)
	}
	reports := make(chan Machine, 100)
	server := httptest.NewServer(http.HandlerFunc(func(out http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/workers/checkin" {
			var c Checkin
			if err := json.NewDecoder(r.Body).Decode(&c); err != nil {
				t.Error(err)
			}
			select {
			case reports <- c.Machine:
			default:
			}
		}
		out.Header().Set("Content-Type", "application/json")
		out.Write([]byte(`{}`))
	}))
	defer server.Close()
	clock := &manualWorkerClock{now: time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)}
	w := recoveryWorker(t)
	w.HostStateDir = dir
	w.Clock = clock
	w.Machine.HostMemoryBudgetGB = 5
	w.Client = Client{URL: server.URL, Token: "test"}
	w.HostMemorySource = func(context.Context) (float64, error) { return 64, nil }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	defer func() {
		cancel()
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		deadline := time.After(3 * time.Second)
		for {
			select {
			case err := <-done:
				if err != nil {
					t.Error(err)
				}
				return
			case <-tick.C:
				clock.Advance(time.Second)
			case <-deadline:
				t.Error("worker did not stop")
				return
			}
		}
	}()
	wait := func(gib float64, generation uint64) {
		t.Helper()
		deadline := time.After(3 * time.Second)
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-tick.C:
				clock.Advance(time.Second)
			case m := <-reports:
				if m.HostMemoryBudgetGB == gib && m.HostMemoryBudgetGeneration == generation {
					return
				}
			case <-deadline:
				t.Fatalf("no live report for %g GiB generation %d", gib, generation)
			}
		}
	}
	wait(3, 1)
	SetHostMemoryBudget(dir, 27)
	clock.Advance(time.Second)
	wait(27, 2)
	SetHostMemoryBudget(dir, 3)
	clock.Advance(time.Second)
	wait(3, 3)
}

func TestHostMemoryBudgetSerializesGenerationsAndRefusesStaleAdmission(t *testing.T) {
	dir := t.TempDir()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := SetHostMemoryBudget(dir, 27); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	budget, _, err := EffectiveHostMemoryBudget(dir, 5)
	if err != nil || budget.Generation != 8 {
		t.Fatal("concurrent generation lost", budget, err)
	}
	w := Worker{HostStateDir: dir, Machine: Machine{HostMemoryBudgetGB: 5, HostMemoryBudgetGeneration: 7}}
	if err := w.checkHostMemoryBudget(0, 1); !errors.Is(err, errWorkerMemory) {
		t.Fatal("stale admission allowed", err)
	}
	if err := w.refreshHostMemoryBudget(); err != nil {
		t.Fatal(err)
	}
	if err := w.checkHostMemoryBudget(0, 1); err != nil {
		t.Fatal(err)
	}
	SetHostMemoryBudget(dir, 3)
	if err := w.checkHostMemoryBudget(0, 1); !errors.Is(err, errWorkerMemory) {
		t.Fatal("lower ignored during admission", err)
	}
	os.WriteFile(filepath.Join(dir, "host-memory-budget.json"), []byte(`{"gib":3,"generation":0}`), 0600)
	if _, err := SetHostMemoryBudget(dir, 27); err == nil {
		t.Fatal("corrupt generation reset")
	}
}
