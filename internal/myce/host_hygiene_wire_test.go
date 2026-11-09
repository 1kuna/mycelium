package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHostHygieneWireNegotiationAndStrictDowngrade(t *testing.T) {
	q, clock := fixture(t)
	handler := API{q, "test"}.Handler()
	strict := false
	var reports []Checkin
	var statuses []int
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		data, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		var c Checkin
		if err := json.Unmarshal(data, &c); err != nil {
			t.Error(err)
			return
		}
		reports = append(reports, c)
		if strict {
			if c.Machine.HostHealth != nil || c.ToolInventoryHash != "" {
				statuses = append(statuses, 400)
				rw.WriteHeader(400)
				_ = json.NewEncoder(rw).Encode(map[string]string{"code": "request_invalid", "error": "unknown field"})
				return
			}
			statuses = append(statuses, 200)
			_ = json.NewEncoder(rw).Encode(Reply{AttemptProtocol: keyedAttemptProtocol})
			return
		}
		statuses = append(statuses, 200)
		r.Body = io.NopCloser(bytes.NewReader(data))
		handler.ServeHTTP(rw, r)
	}))
	defer server.Close()
	m := machine("health-wire", "fake")
	m.HostHealth = &HostHealth{DrainReason: "host_memory_pressure"}
	w := Worker{Clock: clock, Client: Client{URL: server.URL, Token: "test"}, toolReport: &workerToolReport{}, hygieneWire: &workerHygieneWire{}}
	send := func() {
		t.Helper()
		if _, err := w.checkin(context.Background(), Checkin{Machine: m}); err != nil {
			t.Fatal(err)
		}
	}
	send()
	if reports[0].Machine.HostHealth != nil || reports[0].Machine.Leaving || !availabilityBlocked(&reports[0].Machine) {
		t.Fatal("initial request did not preserve legacy admission drain", reports[0])
	}
	send()
	if reports[1].Machine.HostHealth == nil || reports[1].Machine.Leaving {
		t.Fatal("support was not negotiated", reports[1])
	}
	strict = true
	send()
	send()
	if len(reports) != 5 || statuses[2] != 400 || statuses[3] != 200 || statuses[4] != 200 {
		t.Fatal("downgrade did not use a single legacy fallback", statuses)
	}
	for _, c := range reports[3:] {
		if c.Machine.HostHealth != nil || c.ToolInventoryHash != "" || c.Machine.Leaving || !availabilityBlocked(&c.Machine) || c.Machine.Session != m.Session || c.AttemptProtocol != keyedAttemptProtocol {
			t.Fatal("fallback changed custody or lost drain", c)
		}
	}
	m.HostHealth = &HostHealth{}
	send()
	if reports[5].Machine.Leaving {
		t.Fatal("legacy drain did not recover")
	}
}

// The prior queue supports v1 health and collection, but strictly rejects the
// additive operation evidence. Negotiation must preserve its useful v1 reports.
