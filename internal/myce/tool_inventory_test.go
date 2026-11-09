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
	server := httptest.NewServer(API{q, "test"}.Handler())
	defer server.Close()
	ready := []string{"environment-00"}
	inventory := map[string][]string{"": {"native-tool-0000", "native-tool-0001"}, ready[0]: {"native-tool-0000"}}
	m := cpuMachine("inventory-worker", "", "cpu", 8, 32)
	m.ToolInventory, m.Environments, m.EnvironmentBuilder = inventory, ready, true
	worker := Worker{Clock: clock, Client: Client{URL: server.URL, Token: "test"}, toolReport: &workerToolReport{}}
	send := func() Reply {
		t.Helper()
		clock.Advance(time.Second)
		reply, err := worker.checkin(context.Background(), Checkin{Machine: m})
		if err != nil {
			t.Fatal(err)
		}
		return reply
	}
	send() // Initial request must use only the old schema.
	send()
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
	server = httptest.NewServer(API{q, "test"}.Handler())
	defer server.Close()
	worker.Client.URL = server.URL
	if reply := send(); !reply.ToolInventoryRequired {
		t.Fatal("restart did not request full report")
	}
	if toolsFit(&prepared, q.s.Machines[m.ID]) {
		t.Fatal("restart qualified unreported inventory")
	}
	send()
	if !toolsFit(&prepared, q.s.Machines[m.ID]) {
		t.Fatal("restart refresh did not recover placement")
	}
}

func TestToolInventoryOldStrictQueue(t *testing.T) {
	// An old queue rejects either additive request field. Its reply advertises no
	// inventory capability, so a new worker keeps reporting the old full shape.
	calls, probes := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var raw map[string]json.RawMessage
		_ = json.NewDecoder(r.Body).Decode(&raw)
		if raw["tool_inventory_hash"] != nil || raw["tool_inventory_version"] != nil {
			probes++
			w.WriteHeader(400)
			_ = json.NewEncoder(w).Encode(map[string]string{"code": "request_invalid", "error": "unknown field"})
			return
		}
		var m Machine
		_ = json.Unmarshal(raw["machine"], &m)
		if m.HygieneCollection != nil {
			t.Error("unnegotiated fields sent")
		}
		if m.ToolInventory == nil {
			t.Error("old queue did not receive full inventory")
		}
		calls++
		_ = json.NewEncoder(w).Encode(Reply{AttemptProtocol: keyedAttemptProtocol})
	}))
	defer server.Close()
	m := machine("old-queue-worker", "fake")
	m.ToolInventory = map[string][]string{"": {"git"}}
	m.HygieneCollection = &WorkerHygieneCollection{}
	worker := Worker{Client: Client{URL: server.URL}, toolReport: &workerToolReport{}, hygieneWire: &workerHygieneWire{}}
	for range 1 {
		if _, err := worker.checkin(context.Background(), Checkin{Machine: m}); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 1 || probes != 0 {
		t.Fatal(calls, probes)
	}
	// Retained capability knowledge cannot strand a worker if the queue is
	// replaced by an older strict binary. Only decode refusal permits this retry.
	worker.toolReport.supported = true
	if _, err := worker.checkin(context.Background(), Checkin{Machine: m}); err != nil {
		t.Fatal(err)
	}
	if calls != 2 || probes != 1 {
		t.Fatal("legacy full retry", calls, probes)
	}

	t.Run("AdaptiveProbeWireNegotiationAndDowngrade", func(t *testing.T) {

		q, clock := fixture(t)
		handler := API{q, "test"}.Handler()
		strict := false
		var reports []Checkin
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
			data, _ := io.ReadAll(r.Body)
			var c Checkin
			if err := json.Unmarshal(data, &c); err != nil {
				t.Error(err)
				return
			}
			reports = append(reports, c)
			if strict {
				if hasHostProbeFields(c) || c.ToolInventoryHash != "" {
					rw.WriteHeader(400)
					json.NewEncoder(rw).Encode(map[string]string{"code": "request_invalid", "error": "unknown field"})
					return
				}
				json.NewEncoder(rw).Encode(Reply{AttemptProtocol: keyedAttemptProtocol})
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(data))
			handler.ServeHTTP(rw, r)
		}))
		defer server.Close()
		m := machine("probe-wire", "fake")
		observation := &ProbeObservation{PeriodSeconds: 1, CostSeconds: .1, ValidSeconds: 2}
		m.SharedHostPressure = &SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 1, Probe: observation}
		m = profileAvailability(m, Availability{Mode: "daily", Available: true, Signals: idleSignals(), AgeSeconds: .25, Probe: observation})
		w := Worker{Clock: clock, Client: Client{URL: server.URL, Token: "test"}, toolReport: &workerToolReport{}, hygieneWire: &workerHygieneWire{}}
		send := func() {
			t.Helper()
			if _, err := w.checkin(t.Context(), Checkin{Machine: m}); err != nil {
				t.Fatal(err)
			}
		}
		send()
		send()
		if hasHostProbeFields(reports[0]) || !hasHostProbeFields(reports[1]) {
			t.Fatal("probe fields were not negotiated")
		}

		strict = true
		send()
		send()
		if len(reports) != 5 {
			t.Fatal("downgrade did not use one legacy retry", len(reports))
		}
		for _, c := range reports[3:] {
			if hasHostProbeFields(c) || c.Machine.Session != m.Session || c.AttemptProtocol != keyedAttemptProtocol {
				t.Fatal("downgrade changed protocol/custody")
			}
		}

	})
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
	gitSpec := cpuSpec(1, 1)
	gitSpec.Needs.Tools = []string{"git"}
	gitJob := submit(t, q, gitSpec)
	nodeSpec := cpuSpec(1, 1)
	nodeSpec.Needs.Tools = []string{"node"}
	nodeJob := submit(t, q, nodeSpec)
	newer.Machine.ToolInventory = nil
	reply, err := q.Checkin(newer)
	if err != nil {
		t.Fatal(err)
	}
	if reply.Job == nil || reply.Job.ID != nodeJob.ID || job(t, q, gitJob.ID).State != "queued" {
		t.Fatal("stale inventory changed public tool eligibility", reply.Job)
	}

	t.Run("ToolInventoryFencesAndRollback", func(t *testing.T) {

		q, _ := fixture(t)
		m := cpuMachine("inventory-fences", "", "cpu", 8, 32)
		m.ToolInventory = map[string][]string{"": {"node", "git", "git"}}
		full := func(version uint64) Checkin {
			return Checkin{Machine: m, ToolInventoryHash: prepareToolInventory(m.ToolInventory).hash, ToolInventoryVersion: version}
		}
		c := full(2)
		if _, err := q.Checkin(c); err != nil {
			t.Fatal(err)
		}
		c.Machine.ToolInventory = nil
		var reply Reply
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

	})
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
