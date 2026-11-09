package myce

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"

	"testing"
	"time"
)

func TestDeviceTelemetryWireNegotiationAndAge(t *testing.T) {
	_, clock := fixture(t)
	strict := false
	var reports []Checkin
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		var c Checkin
		if err := json.NewDecoder(r.Body).Decode(&c); err != nil {
			t.Error(err)
			return
		}
		reports = append(reports, c)
		if strict && hasDeviceTelemetry(c) {
			rw.WriteHeader(400)
			_ = json.NewEncoder(rw).Encode(map[string]string{"code": "request_invalid", "error": "unknown field"})
			return
		}
		reply := Reply{AttemptProtocol: keyedAttemptProtocol}
		if !strict {
			reply.DeviceTelemetryProtocol = deviceTelemetryProtocol
		}
		_ = json.NewEncoder(rw).Encode(reply)
	}))
	defer server.Close()
	m := sharedDiscreteMachine("xpu")
	free, active := 4.0, 0.0
	s := &DeviceTelemetry{At: clock.Now(), FreeGB: &free, ActivePercent: &active}
	m.Devices[0].Telemetry = s
	w := Worker{Clock: clock, Client: Client{URL: server.URL}, unifiedMemoryWire: &workerUnifiedMemoryWire{}}
	send := func() {
		t.Helper()
		_, err := w.checkin(context.Background(), Checkin{Machine: m})
		if err != nil {
			t.Fatal(err)
		}
	}
	send()
	clock.Advance(deviceMemorySampleFreshFor)
	send()
	if hasDeviceTelemetry(reports[0]) || !hasDeviceTelemetry(reports[1]) {
		t.Fatal("telemetry did not negotiate independently")
	}
	// A delayed inventory result carries its original sensor age. Rebase it
	// onto a queue clock with a different epoch; it must remain stale.
	c := receivedDeviceTelemetry(reports[1], clock.Now().Add(24*time.Hour))
	if deviceTelemetryReady(c.Machine.Devices[0].Telemetry, clock.Now().Add(24*time.Hour)) {
		t.Fatal("send or receipt renewed stale activity")
	}
	strict = true
	send()
	send()
	if len(reports) != 5 || !hasDeviceTelemetry(reports[2]) || hasDeviceTelemetry(reports[3]) || hasDeviceTelemetry(reports[4]) || s.AgeSeconds != 0 {
		t.Fatal("downgrade failed or mutated the original sensor evidence")
	}
}
