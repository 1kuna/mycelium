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
	if reports[0].Machine.Leaving || reports[0].Machine.MemoryError == "" || hasUnifiedMemoryWireFields(reports[0]) || reports[0].Attempts[0].CurrentHostMemory.DeviceGB != nil {
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
		if r.Machine.Leaving || r.Machine.MemoryError == "" || hasUnifiedMemoryWireFields(r) || r.Attempts[0].ID != "owned" || r.Attempts[0].Session != m.Session || r.Attempts[0].Attempt != 1 {
			t.Fatal("legacy retry lost drain or exact custody", r)
		}
	}
	if sample.DeviceSource != deviceUnifiedResidentSource || !sample.DeviceExclusive || sample.DeviceGB == nil {
		t.Fatal("wire stripping mutated sampler evidence")
	}
}

func TestProbeMemoryWireNegotiatesIndependently(t *testing.T) {
	gb := 1.0
	c := Checkin{Machine: Machine{ID: "worker", Session: "session"}, Attempts: []AttemptReport{{CurrentHostMemory: &CurrentHostMemory{FootprintGB: &gb, GrowthObserved: true, GrowthRateGB: 1, DeviceSource: "direct", DeviceGB: &gb}}}}
	w := Worker{unifiedMemoryWire: &workerUnifiedMemoryWire{machine: "worker", session: "session", supported: true}}
	legacy := w.unifiedMemoryCheckin(c).Attempts[0].CurrentHostMemory
	if legacy.FootprintGB != nil || legacy.GrowthObserved || legacy.GrowthRateGB != 0 || legacy.DeviceGB == nil || legacy.DeviceSource != "direct" {
		t.Fatal("probe negotiation changed v1 device evidence", legacy)
	}
	w.unifiedMemoryWire.probeSupported = true
	modern := w.unifiedMemoryCheckin(c).Attempts[0].CurrentHostMemory
	if modern.FootprintGB == nil || !modern.GrowthObserved || modern.GrowthRateGB != 1 {
		t.Fatal("negotiated probe evidence omitted", modern)
	}
	c.Machine.Session = "replacement"
	reset := w.unifiedMemoryCheckin(c).Attempts[0].CurrentHostMemory
	if reset.FootprintGB != nil || reset.GrowthObserved {
		t.Fatal("replacement inherited negotiation", reset)
	}
	if c.Attempts[0].CurrentHostMemory.FootprintGB == nil {
		t.Fatal("wire downgrade mutated original telemetry")
	}
}

func TestUnifiedMemoryWireLegacyWithholdsNewWork(t *testing.T) {
	q, _ := fixture(t)
	m := sharedUnifiedMachine()
	first := assignGPU(t, q, m, submit(t, q, sharedSpec("experiment", 17)))
	if err := q.Started(result(first, false)); err != nil {
		t.Fatal(err)
	}
	waiting := submit(t, q, sharedSpec("experiment", 13))
	gb := 17.0
	c := legacyUnifiedMemoryCheckin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{{Execution: Execution{ID: first.ID, Attempt: first.Attempt}, Session: first.Session, Started: true, CurrentHostMemory: &CurrentHostMemory{DeviceGB: &gb, DeviceSource: deviceUnifiedResidentSource}}}})
	r, err := q.Checkin(c)
	if err != nil || assignedJob(r) != nil {
		t.Fatal("legacy admission withholding admitted new work", r, err)
	}
	requireState(t, q, first.ID, "running")
	requireState(t, q, waiting.ID, "queued")
	// Restoring the negotiated inventory resumes packing without stopping or
	// replacing the incumbent. No leave/stop bit travels in the legacy request.
	if r := machineCheck(t, q, m, first); assignedJob(r) == nil || assignedJob(r).ID != waiting.ID {
		t.Fatal("negotiated recovery failed to resume sharing", r)
	}
}

func TestHostMemorySampleIdentityNegotiation(t *testing.T) {
	c := Checkin{Machine: Machine{ID: "worker", Session: "session"}, Attempts: []AttemptReport{{CurrentHostMemory: &CurrentHostMemory{SampleID: 123}}}}
	w := Worker{unifiedMemoryWire: &workerUnifiedMemoryWire{machine: "worker", session: "session", supported: true, probeSupported: true}}
	if got := w.unifiedMemoryCheckin(c).Attempts[0].CurrentHostMemory.SampleID; got != 0 {
		t.Fatal("unnegotiated observation identity sent")
	}
	w.unifiedMemoryWire.sampleSupported = true
	if got := w.unifiedMemoryCheckin(c).Attempts[0].CurrentHostMemory.SampleID; got != 123 {
		t.Fatal("negotiated identity omitted")
	}
	if got := legacyUnifiedMemoryCheckin(c).Attempts[0].CurrentHostMemory.SampleID; got != 0 {
		t.Fatal("strict downgrade retained identity")
	}
	if c.Attempts[0].CurrentHostMemory.SampleID != 123 {
		t.Fatal("wire stripping mutated observation")
	}
	c.Machine.Session = "replacement"
	if got := w.unifiedMemoryCheckin(c).Attempts[0].CurrentHostMemory.SampleID; got != 0 {
		t.Fatal("replacement inherited negotiation")
	}
}
