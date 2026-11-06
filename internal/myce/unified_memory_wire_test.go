package myce

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestUnifiedMemoryWireNegotiationAndDowngrade(t *testing.T) {
	strict := false
	var reports []Checkin
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		var c Checkin
		if err := json.NewDecoder(r.Body).Decode(&c); err != nil {
			t.Error(err)
			return
		}
		reports = append(reports, c)
		if strict && hasUnifiedMemoryWireFields(c) {
			rw.WriteHeader(400)
			_ = json.NewEncoder(rw).Encode(map[string]string{"code": "request_invalid", "error": "unknown field"})
			return
		}
		reply := Reply{AttemptProtocol: keyedAttemptProtocol}
		if !strict {
			reply.UnifiedMemoryProtocol = unifiedMemoryProtocol
		}
		_ = json.NewEncoder(rw).Encode(reply)
	}))
	defer server.Close()
	gb := 2.0
	sample := &CurrentHostMemory{DeviceGB: &gb, DeviceSource: deviceUnifiedResidentSource, DeviceExclusive: true, DeviceGrowthRateGB: 1}
	m := machine("memory-wire", "cuda")
	c := Checkin{Machine: m, Attempts: []AttemptReport{{Execution: Execution{ID: "owned", Attempt: 1}, Session: m.Session, Started: true, CurrentHostMemory: sample}}}
	w := Worker{Client: Client{URL: server.URL}, unifiedMemoryWire: &workerUnifiedMemoryWire{}}
	send := func() {
		t.Helper()
		if _, err := w.checkin(context.Background(), c); err != nil {
			t.Fatal(err)
		}
	}
	send()
	if !reports[0].Machine.Leaving || hasUnifiedMemoryWireFields(reports[0]) || reports[0].Attempts[0].CurrentHostMemory.DeviceGB != nil {
		t.Fatal("legacy queue received unsafe estimate", reports[0])
	}
	send()
	if reports[1].Machine.Leaving || !hasUnifiedMemoryWireFields(reports[1]) {
		t.Fatal("new queue did not receive negotiated evidence")
	}
	strict = true
	send()
	send()
	if len(reports) != 5 || !hasUnifiedMemoryWireFields(reports[2]) {
		t.Fatal("downgrade did not attempt negotiated shape once", len(reports))
	}
	for _, r := range reports[3:] {
		if !r.Machine.Leaving || hasUnifiedMemoryWireFields(r) || r.Attempts[0].ID != "owned" || r.Attempts[0].Session != m.Session || r.Attempts[0].Attempt != 1 {
			t.Fatal("legacy retry lost drain or exact custody", r)
		}
	}
	if sample.DeviceSource != deviceUnifiedResidentSource || !sample.DeviceExclusive || sample.DeviceGB == nil {
		t.Fatal("wire stripping mutated sampler evidence")
	}
}
