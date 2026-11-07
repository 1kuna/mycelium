package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
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
	if reports[0].Machine.HostHealth != nil || !reports[0].Machine.Leaving {
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
		if c.Machine.HostHealth != nil || c.ToolInventoryHash != "" || !c.Machine.Leaving || c.Machine.Session != m.Session || c.AttemptProtocol != keyedAttemptProtocol {
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
func TestHostHygieneMountProgressNegotiatesStrictV1Queue(t *testing.T) {
	type priorMount struct {
		Adverse        bool    `json:"adverse,omitempty"`
		Root           string  `json:"root"`
		LatencySeconds float64 `json:"latency_seconds"`
		PendingSeconds float64 `json:"pending_seconds"`
		Reason         string  `json:"reason,omitempty"`
	}
	var version atomic.Int32
	reports := make(chan Checkin, 12)
	var rejected atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		var c Checkin
		if err := json.NewDecoder(r.Body).Decode(&c); err != nil {
			t.Error(err)
			return
		}
		reports <- c
		if version.Load() == 0 && c.Machine.HostHealth != nil {
			for _, samples := range [][]HostHealthSample{c.Machine.HostHealth.History, c.Machine.HostHealth.Baseline} {
				for _, s := range samples {
					for _, m := range s.Mounts {
						data, _ := json.Marshal(m)
						decoder := json.NewDecoder(bytes.NewReader(data))
						decoder.DisallowUnknownFields()
						if err := decoder.Decode(new(priorMount)); err != nil {
							rejected.Add(1)
							rw.WriteHeader(http.StatusBadRequest)
							_ = json.NewEncoder(rw).Encode(map[string]string{"code": "request_invalid", "error": "unknown field"})
							return
						}
					}
				}
			}
		}
		reply := Reply{AttemptProtocol: keyedAttemptProtocol, HostHygieneProtocol: hostHygieneProtocol, HostCollectionProtocol: hostCollectionProtocol}
		if version.Load() > 0 {
			reply.HostMountProgressProtocol = hostMountProgressProtocol
		}
		_ = json.NewEncoder(rw).Encode(reply)
	}))
	defer server.Close()
	sample := HostHealthSample{Mounts: []HostMountHealth{{Root: "checkpoint", Reason: "mount_probe_slow", LatencySeconds: 1, PendingSeconds: 2, OperationAgeSeconds: 1, SlowThresholdSeconds: .001, StallAllowanceSeconds: checkpointAvailabilityStallBudget.Seconds()}}}
	m := machine("progress-wire", "fake")
	m.HostHealth = &HostHealth{History: []HostHealthSample{sample}, Baseline: []HostHealthSample{sample}}
	m.HygieneCollection = &WorkerHygieneCollection{}
	w := Worker{Client: Client{URL: server.URL}, hygieneWire: &workerHygieneWire{}}
	send := func() Checkin {
		t.Helper()
		if _, err := w.checkin(context.Background(), Checkin{Machine: m}); err != nil {
			t.Fatal(err)
		}
		return <-reports
	}
	send() // Initial capability discovery.
	for i := 0; i < 3; i++ {
		c := send()
		if c.Machine.HostHealth == nil || c.Machine.HygieneCollection == nil || c.Machine.HostHealth.History[0].Mounts[0].Reason != "mount_probe_slow" || c.Machine.HostHealth.History[0].Mounts[0].StallAllowanceSeconds != 0 {
			t.Fatal("strict v1 queue lost health/collection evidence", c)
		}
	}
	if rejected.Load() != 0 || m.HostHealth.History[0].Mounts[0].StallAllowanceSeconds == 0 || m.HostHealth.Baseline[0].Mounts[0].StallAllowanceSeconds == 0 {
		t.Fatal("negotiation rejected requests or mutated local evidence")
	}
	version.Store(1)
	send() // Discover support for the additive evidence.
	if c := send(); c.Machine.HostHealth.History[0].Mounts[0].StallAllowanceSeconds == 0 {
		t.Fatal("new queue lost operation evidence")
	}
	version.Store(0)
	if _, err := w.checkin(context.Background(), Checkin{Machine: m}); err != nil {
		t.Fatal(err)
	}
	<-reports // One strict rejection of the additive evidence.
	fallback := <-reports
	if fallback.Machine.HostHealth == nil || fallback.Machine.HygieneCollection == nil || hasMountProgressWireFields(fallback) {
		t.Fatal("downgrade fallback lost v1 health/collection", fallback)
	}
	for i := 0; i < 3; i++ {
		if c := send(); c.Machine.HostHealth == nil || c.Machine.HygieneCollection == nil {
			t.Fatal("downgrade repeated legacy fallback", c)
		}
	}
	if rejected.Load() != 1 {
		t.Fatal("additive fields caused repeated strict rejection", rejected.Load())
	}
}

func TestHostHygienePriorWorkerAcceptsNewQueueReply(t *testing.T) {
	q, _ := fixture(t)
	server := httptest.NewServer(API{q, "test"}.Handler())
	defer server.Close()
	// A pre-progress worker sends the original v1 mount shape and decodes its
	// original reply type, which ignores additive response capability fields.
	type priorReply struct {
		AttemptProtocol        string `json:"attempt_protocol"`
		HostHygieneProtocol    string `json:"host_hygiene_protocol"`
		HostCollectionProtocol string `json:"hygiene_collection_protocol"`
	}
	m := machine("prior-wire", "fake")
	m.HostHealth = &HostHealth{History: []HostHealthSample{{Mounts: []HostMountHealth{{Root: "checkpoint", LatencySeconds: .1, Reason: "mount_probe_slow"}}}}}
	m.HygieneCollection = &WorkerHygieneCollection{}
	var reply priorReply
	c := Client{URL: server.URL, Token: "test"}
	if err := c.Call(context.Background(), "POST", "/api/workers/checkin", Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol}, &reply); err != nil {
		t.Fatal(err)
	}
	if reply.AttemptProtocol != keyedAttemptProtocol || reply.HostHygieneProtocol != hostHygieneProtocol || reply.HostCollectionProtocol != hostCollectionProtocol {
		t.Fatal("new queue broke original worker negotiation", reply)
	}
	got := q.s.Machines[m.ID]
	if got == nil || got.HostHealth == nil || got.HygieneCollection == nil || got.HostHealth.History[0].Mounts[0].Reason != "mount_probe_slow" {
		t.Fatal("new queue lost prior worker health", got)
	}
}
