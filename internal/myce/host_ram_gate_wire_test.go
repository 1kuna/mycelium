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
)

var hostRAMGateWireFields = []string{"host_ram_gate", "native_startup", "native_resident_gb", "startup_gb", "memory_pool", "device_pool"}

func jsonFieldsPresent(value any, names []string) (found []string) {
	switch v := value.(type) {
	case map[string]any:
		for key, child := range v {
			if slices.Contains(names, key) {
				found = append(found, key)
			}
			found = append(found, jsonFieldsPresent(child, names)...)
		}
	case []any:
		for _, child := range v {
			found = append(found, jsonFieldsPresent(child, names)...)
		}
	}
	return found
}

// Workers roll out first. A previous queue strictly decodes check-in, launch
// and completion, so a new worker sends the RAM gate fields only after the
// queue advertises them, and retries once without them after a rollback.
func TestHostRAMGateWireCompatibility(t *testing.T) {
	var mu sync.Mutex
	advertised, applied := false, 0
	var sent [][]string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		var body any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		fields := jsonFieldsPresent(body, hostRAMGateWireFields)
		slices.Sort(fields)
		sent = append(sent, fields)
		w.Header().Set("Content-Type", "application/json")
		if advertised {
			w.Header().Set(hostRAMGateHeader, hostRAMGateProtocol)
		} else if len(fields) > 0 {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `{"code":"request_invalid","error":"body must be one JSON object with known fields and valid field types"}`)
			return
		}
		applied++
		_, _ = io.WriteString(w, `{"attempt_protocol":"keyed-attempt-v1"}`)
	}))
	defer server.Close()
	wire := &atomic.Bool{}
	client := Client{URL: server.URL, hostRAMGateWire: wire}
	worker := Worker{Client: client}

	resident := 8.5
	checkin := Checkin{Machine: Machine{ID: "worker", Session: "s", HostRAMGate: &HostRAMGate{RawGB: 20, HeldGB: 4, PendingGB: 2}}}
	launch := workerLaunchRequest{HostLaunch: HostLaunch{Kind: "batch", NativeStartup: "continuation", NativeResidentGB: &resident}}
	refusal := Result{Interrupted: true, ExecutionCause: &RuntimeCause{Operation: "launch", Predicate: "memory_capacity",
		Admission: &AdmissionDetail{Subcheck: memoryWaitReason, Required: 36, StartupGB: 3}}}
	interrupted := Result{Interrupted: true, PriorityStop: &PriorityPreemption{Cause: memoryPressureCause, Demand: "host/h",
		MemoryPool: &MemoryPoolStop{Trigger: "below_reserve"}, DevicePool: &DevicePoolStop{Trigger: "over_budget"}}}
	send := func() {
		t.Helper()
		if _, err := worker.checkin(context.Background(), checkin); err != nil {
			t.Fatal("check-in", err)
		}
		for _, call := range []struct {
			path  string
			value any
		}{{"/api/workers/launch", launch}, {"/api/workers/complete", refusal}, {"/api/workers/complete", &interrupted}} {
			if err := client.Call(context.Background(), "POST", call.path, call.value, nil); err != nil {
				t.Fatal(call.path, err)
			}
		}
	}
	send() // Previous queue: nothing new is sent, nothing is refused.
	mu.Lock()
	advertised = true
	mu.Unlock()
	send() // The first reply advertises support.
	send() // Fields now travel.
	mu.Lock()
	advertised = false
	mu.Unlock()
	send() // Queue rollback: the first request is refused once, then all strip.

	mu.Lock()
	defer mu.Unlock()
	none, all := []string(nil), [][]string{{"host_ram_gate"}, {"native_resident_gb", "native_startup"}, {"startup_gb"}, {"device_pool", "memory_pool"}}
	want := [][]string{none, none, none, none, none, all[1], all[2], all[3], all[0], all[1], all[2], all[3], all[0], none, none, none, none}
	if len(sent) != len(want) || applied != 16 {
		t.Fatalf("sent %d requests, applied %d: %v", len(sent), applied, sent)
	}
	for i := range want {
		if !slices.Equal(sent[i], want[i]) {
			t.Fatalf("request %d sent %v, want %v (all: %v)", i, sent[i], want[i], sent)
		}
	}
	if wire.Load() || checkin.Machine.HostRAMGate == nil || launch.NativeStartup == "" || refusal.ExecutionCause.Admission.StartupGB != 3 ||
		interrupted.PriorityStop.MemoryPool == nil || interrupted.PriorityStop.DevicePool == nil {
		t.Fatal("wire stripping changed the worker's local evidence")
	}
}
