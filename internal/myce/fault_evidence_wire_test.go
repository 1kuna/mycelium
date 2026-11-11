package myce

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestFaultEvidenceLegacyWire(t *testing.T) {
	wire := &atomic.Bool{}
	c := Checkin{Machine: Machine{Devices: []MachineDevice{{FaultEvidence: []*FaultEvidence{{Event: "one"}}}}}}
	if func() bool {
		legacy, _ := faultWireInput(c, false)
		_, present := faultWireInput(legacy, true)
		return present
	}() || func() bool { _, present := faultWireInput(c, true); return !present }() {
		t.Fatal("legacy stripping mutated local evidence")
	}
	wire.Store(true)
	if func() bool { _, present := faultWireInput(c, wire.Load()); return !present }() {
		t.Fatal("negotiated evidence missing")
	}
}

func TestFaultEvidenceResultDowngrade(t *testing.T) {
	calls, applied := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		var fields map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&fields); err != nil {
			t.Error(err)
		}
		w.Header().Set("Content-Type", "application/json")
		if _, present := fields["signal_evidence"]; present {
			w.WriteHeader(400)
			_, _ = w.Write([]byte(`{"code":"request_invalid","error":"unknown field"}`))
			return
		}
		applied++
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()
	wire := &atomic.Bool{}
	wire.Store(true)
	client := Client{URL: server.URL, faultWire: wire}
	result := Result{SignalEvidence: &SignalEvidence{Signal: "SIGSEGV"}}
	if err := client.Call(context.Background(), "POST", "/api/workers/complete", &result, nil); err != nil {
		t.Fatal(err)
	}
	if calls != 2 || applied != 1 || wire.Load() || result.SignalEvidence == nil {
		t.Fatal("downgrade lost custody/evidence or applied twice", calls, applied)
	}
}
