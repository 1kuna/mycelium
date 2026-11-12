package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"
)

func TestToolInventoryWorkerHandshake(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)}
	path, root := filepath.Join(t.TempDir(), "queue.db"), t.TempDir()
	q, err := Open(path, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { q.Close() }()
	fakeCheckpointAvailability(t, q, true)
	var mu sync.Mutex
	var bodies [][]byte
	var reports []Checkin
	handler := API{q, "test"}.Handler()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(data))
		var c Checkin
		if err := json.Unmarshal(data, &c); err != nil {
			t.Error(err)
		}
		mu.Lock()
		bodies = append(bodies, data)
		reports = append(reports, c)
		mu.Unlock()
		handler.ServeHTTP(w, r)
	}))
	defer server.Close()
	inventory, ready := releaseToolInventory()
	m := cpuMachine("inventory-worker", "", "cpu", 8, 32)
	m.ToolInventory, m.Environments, m.EnvironmentBuilder = inventory, ready, true
	worker := Worker{Clock: clock, Client: Client{URL: server.URL, Token: "test"}, toolReport: &workerToolReport{}}
	send := func() Reply {
		t.Helper()
		clock.Advance(time.Second)
		reply, err := worker.fixtureCheckin(context.Background(), Checkin{Machine: m})
		if err != nil {
			t.Fatal(err)
		}
		return reply
	}
	send() // Initial request must use only the old schema.
	first := q.s.Machines[m.ID].toolInventory
	send()
	if q.s.Machines[m.ID].toolInventory != first {
		t.Fatal("hash-only report replaced immutable cache")
	}
	native := &Job{Spec: cpuSpec(1, 1)}
	native.Spec.Needs.Tools = []string{"native-tool-0000"}
	prepared := *native
	prepared.Spec = native.Spec
	prepared.Spec.Environment = Environment{Name: "runtime"}
	prepared.EnvironmentRecipes = map[string]EnvironmentRecipe{"cpu": {Key: ready[0]}}
	if !toolsFit(native, q.s.Machines[m.ID]) || !toolsFit(&prepared, q.s.Machines[m.ID]) {
		t.Fatal("full inventory lost placement")
	}
	inventory[""] = slices.Clone(inventory[""][1:])
	send() // Changed hash, full report.
	if toolsFit(native, q.s.Machines[m.ID]) || !toolsFit(&prepared, q.s.Machines[m.ID]) {
		t.Fatal("changed/native and prepared placement changed meaning")
	}
	send()
	// A queue cache mismatch asks for a refresh without qualifying missing tools.
	q.mu.Lock()
	q.s.Machines[m.ID].toolInventory = nil
	q.mu.Unlock()
	if reply := send(); !reply.ToolInventoryRequired {
		t.Fatal("cache miss did not request full report")
	}
	if toolsFit(&prepared, q.s.Machines[m.ID]) {
		t.Fatal("missing inventory qualified tools")
	}
	send()
	send()
	// Restart retains the small high-water fence, never the payload.
	server.Close()
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = Open(path, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	fakeCheckpointAvailability(t, q, true)
	handler = API{q, "test"}.Handler()
	// The handler variable is replaced only while the old server is closed.
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(data))
		var c Checkin
		_ = json.Unmarshal(data, &c)
		mu.Lock()
		bodies = append(bodies, data)
		reports = append(reports, c)
		mu.Unlock()
		handler.ServeHTTP(w, r)
	}))
	defer server.Close()
	worker.Client.URL = server.URL
	if reply := send(); !reply.ToolInventoryRequired {
		t.Fatal("restart did not request full report")
	}
	if toolsFit(&prepared, q.s.Machines[m.ID]) {
		t.Fatal("restart qualified unreported inventory")
	}
	send()
	send()
	if !toolsFit(&prepared, q.s.Machines[m.ID]) {
		t.Fatal("restart refresh did not recover placement")
	}
	// A worker process can restart while recovering custody in the same session.
	// Its initial legacy-shaped negotiation cannot reset the durable version.
	worker.toolReport = &workerToolReport{}
	if reply := send(); !reply.ToolInventoryRequired {
		t.Fatal("worker recovery did not negotiate above fence")
	}
	send()
	send()
	mu.Lock()
	defer mu.Unlock()
	for i, full := range []bool{true, false, true, false, false, true, false, false, true, false, true, true, false} {
		if got := reports[i].Machine.ToolInventory != nil; got != full {
			t.Fatalf("report %d full=%v want %v", i, got, full)
		}
		if full && len(bodies[i]) < 1<<20 || !full && len(bodies[i]) > 16<<10 {
			t.Fatalf("report %d unexpected bytes %d", i, len(bodies[i]))
		}
	}
	if reports[0].ToolInventoryHash != "" || reports[0].ToolInventoryVersion != 0 || reports[1].ToolInventoryVersion != 1 || reports[2].ToolInventoryVersion != 2 {
		t.Fatal("negotiation/version sequence", reports[0].ToolInventoryVersion, reports[1].ToolInventoryVersion, reports[2].ToolInventoryVersion)
	}
	t.Logf("full check-in %d bytes; unchanged %d bytes", len(bodies[0]), len(bodies[1]))
}

func TestToolInventoryFencesAndRollback(t *testing.T) {
	q, clock := fixture(t)
	m := cpuMachine("inventory-fences", "", "cpu", 8, 32)
	m.ToolInventory = map[string][]string{"": {"node", "git", "git"}}
	full := func(version uint64) Checkin {
		return Checkin{Machine: m, ToolInventoryHash: prepareToolInventory(m.ToolInventory).hash, ToolInventoryVersion: version}
	}
	c := full(2)
	if _, err := q.Checkin(c); err != nil {
		t.Fatal(err)
	}
	accepted := q.s.Machines[m.ID].toolInventory
	if !slices.Equal(accepted.tools[""], []string{"git", "node"}) {
		t.Fatal("not canonical")
	}
	// A caller cannot mutate an accepted report through its original map/slices.
	m.ToolInventory[""][0] = "python"
	if !slices.Contains(accepted.tools[""], "node") {
		t.Fatal("caller mutated accepted inventory")
	}
	clock.Advance(time.Second)
	for _, stale := range []Checkin{full(1), {Machine: m}} {
		if _, err := q.Checkin(stale); err == nil {
			t.Fatal("stale inventory accepted")
		}
		if q.s.Machines[m.ID].toolInventory != accepted {
			t.Fatal("rejected check-in lost cache during rollback")
		}
	}
	// Matching version with mismatched full contents requests the right contents.
	c.ToolInventoryVersion = 3
	c.Machine.ToolInventory = map[string][]string{"": {"wrong"}}
	reply, err := q.Checkin(c)
	if err != nil || !reply.ToolInventoryRequired || q.s.Machines[m.ID].toolInventory != nil {
		t.Fatal("hash mismatch", reply, err)
	}
	c.Machine.ToolInventory = map[string][]string{"": {"node", "git"}}
	if _, err := q.Checkin(c); err != nil {
		t.Fatal(err)
	}
	accepted = q.s.Machines[m.ID].toolInventory
	sentinel := errors.New("abort")
	if err := q.update(func() error { q.touchMachine(m.ID).toolInventory = nil; return sentinel }); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	if q.s.Machines[m.ID].toolInventory != accepted {
		t.Fatal("transaction rollback lost immutable inventory")
	}
	view, _, err := q.captureObservation("")
	if err != nil || view.s.Machines[m.ID].toolInventory != accepted {
		t.Fatal("diagnostic snapshot lost inventory", err)
	}
	// Ordinary hash-only reports survive unrelated transactions and status reads.
	c.Machine.ToolInventory = nil
	if _, err := q.Checkin(c); err != nil {
		t.Fatal(err)
	}
	if err := q.Tick(); err != nil {
		t.Fatal(err)
	}
	if q.s.Machines[m.ID].toolInventory != accepted {
		t.Fatal("transaction lost inventory")
	}
	// A fresh session cannot borrow its predecessor's cache.
	c.Machine.Session = "new-session"
	c.Fresh, c.PreviousSession, c.ToolInventoryVersion = true, m.Session, 1
	reply, err = q.Checkin(c)
	if err != nil || !reply.ToolInventoryRequired {
		t.Fatal("new session borrowed inventory", err)
	}
	if _, err := q.Checkin(Checkin{Machine: m}); err == nil {
		t.Fatal("superseded session accepted")
	}
}

func TestToolInventoryConcurrentStaleReport(t *testing.T) {
	q, _ := fixture(t)
	m := cpuMachine("concurrent-inventory", "", "cpu", 8, 32)
	m.ToolInventory = map[string][]string{"": {"git"}}
	stale := Checkin{Machine: m, ToolInventoryHash: prepareToolInventory(m.ToolInventory).hash, ToolInventoryVersion: 1}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	// Delay the older request after its bulk preparation, outside q.mu. The
	// newer version must still commit, and the delayed request must recheck it.
	q.checkpointProbe = func() bool {
		block := false
		once.Do(func() { block = true })
		if block {
			close(entered)
			<-release
		}
		return true
	}
	done := make(chan error, 1)
	go func() { _, err := q.Checkin(stale); done <- err }()
	<-entered
	newer := stale
	newer.Machine.ToolInventory = map[string][]string{"": {"node"}}
	newer.ToolInventoryHash = prepareToolInventory(newer.Machine.ToolInventory).hash
	newer.ToolInventoryVersion = 2
	_, err := q.Checkin(newer)
	close(release)
	staleErr := <-done
	if err != nil {
		t.Fatal(err)
	}
	var api *HTTPError
	if !errors.As(staleErr, &api) || api.Code != "tool_inventory_stale" {
		t.Fatal("delayed report overwrote newer version", staleErr)
	}
	got := q.s.Machines[m.ID].toolInventory
	if got.hash != newer.ToolInventoryHash || !slices.Equal(got.tools[""], []string{"node"}) {
		t.Fatal("newer inventory changed")
	}
}

// Same inventory shape as TestAttemptProtocolReleaseWorkerJoins and #790.
func releaseToolInventory() (map[string][]string, []string) {
	tools := make([]string, 2400)
	for i := range tools {
		tools[i] = fmt.Sprintf("native-tool-%04d", i)
	}
	cache := map[string][]string{"": tools}
	var ready []string
	for i := range 40 {
		key := fmt.Sprintf("environment-%02d", i)
		ready = append(ready, key)
		cache[key] = tools
	}
	return preparedToolInventory(cache, ready, ""), ready
}

// Kept source-compatible with #790 so this exact fixture can measure both
// revisions. Five workers, 6,286 settled jobs, 1,655 curves and 1,384 storage
// observations isolate inventory overhead from backlog placement. SQLite is real.
func BenchmarkToolInventoryFleet(b *testing.B) {
	for _, mode := range []string{"full", "delta"} {
		b.Run(mode, func(b *testing.B) {
			clock := &fakeClock{now: time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)}
			q, err := Open(filepath.Join(b.TempDir(), "queue.db"), b.TempDir(), clock)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { q.Close() })
			q.checkpointProbe = func() bool { return true }
			workers := make([]Machine, 5)
			inventory, ready := releaseToolInventory()
			replies := make([]Reply, len(workers))
			for i := range workers {
				id := fmt.Sprintf("inventory-worker-%d", i)
				workers[i] = cpuMachine(id, id, "cpu", 8, 32)
				workers[i].ToolInventory, workers[i].Environments = inventory, ready
				replies[i], err = q.Checkin(Checkin{Machine: workers[i]})
				if err != nil {
					b.Fatal(err)
				}
			}
			for i := range 6286 {
				id := fmt.Sprintf("retained-%05d", i)
				s := cpuSpec(1, 1)
				q.s.Jobs[id] = &Job{ID: id, Spec: s, State: "succeeded", Queued: clock.Now(), FinishedAt: &clock.now, ReleaseConfirmed: true}
			}
			q.s.WorkObservations = map[string]WorkObservation{}
			for i := range 1655 {
				key := fmt.Sprintf("curve-%05d", i)
				q.s.WorkObservations[key] = WorkObservation{LastSeconds: 10, Samples: 5, RecordedAt: clock.Now()}
			}
			q.s.StorageObservations = map[string]StorageObservation{}
			for i := range 1384 {
				key := fmt.Sprintf("storage-%05d", i)
				q.s.StorageObservations[key] = StorageObservation{RecordedAt: clock.Now()}
			}
			if err := q.update(func() error { return nil }); err != nil {
				b.Fatal(err)
			}
			bodies := make([][]byte, len(workers))
			for i, m := range workers {
				body, err := json.Marshal(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol})
				if err != nil {
					b.Fatal(err)
				}
				if mode == "delta" {
					var reply map[string]any
					data, _ := json.Marshal(replies[i])
					_ = json.Unmarshal(data, &reply)
					if reply["tool_inventory_protocol"] != "tool-inventory-v1" {
						b.Skip("baseline has no delta protocol")
					}
					var wire map[string]any
					_ = json.Unmarshal(body, &wire)
					delete(wire["machine"].(map[string]any), "tool_inventory")
					wire["tool_inventory_hash"], wire["tool_inventory_version"] = reply["tool_inventory_hash"], 1
					body, err = json.Marshal(wire)
					if err != nil {
						b.Fatal(err)
					}
				}
				bodies[i] = body
			}
			holds := make([]time.Duration, 0, b.N)
			var total time.Duration
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				if i%len(workers) == 0 {
					clock.Advance(time.Second)
				}
				var c Checkin
				if err := json.Unmarshal(bodies[i%len(workers)], &c); err != nil {
					b.Fatal(err)
				}
				span := &requestSpan{}
				ctx := context.WithValue(context.Background(), requestSpanKey{}, span)
				if _, err := q.CheckinContext(ctx, c); err != nil {
					b.Fatal(err)
				}
				holds = append(holds, span.hold)
				total += span.hold
			}
			b.StopTimer()
			slices.Sort(holds)
			b.ReportMetric(float64(len(bodies[0])), "checkin-bytes/op")
			b.ReportMetric(float64(total.Nanoseconds())/float64(b.N), "hold-ns/op")
			b.ReportMetric(float64(holds[(len(holds)-1)/2].Nanoseconds()), "hold-p50-ns")
			b.ReportMetric(float64(holds[(len(holds)-1)*99/100].Nanoseconds()), "hold-p99-ns")
		})
	}
}
