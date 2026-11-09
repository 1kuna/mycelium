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

func TestResourceProtocolCompatibility(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		for _, shape := range []string{"checkin", "saved-result", "saved-log"} {
			t.Run(shape, func(t *testing.T) {
				wire := &resourceWire{}
				old := false
				sent := []bool{}
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					b, _ := io.ReadAll(r.Body)
					has := bytes.Contains(b, []byte(`"resources"`)) || bytes.Contains(b, []byte(`"io_pressure"`))
					sent = append(sent, has)
					if old && has {
						w.WriteHeader(400)
						io.WriteString(w, `{"code":"request_invalid","error":"unknown field"}`)
						return
					}
					if !old {
						w.Header().Set(resourceProtocolHeader, resourceProtocol)
					}
					io.WriteString(w, `{"attempt_protocol":"keyed-attempt-v1"}`)
				}))
				defer server.Close()
				client := Client{URL: server.URL, HTTP: server.Client(), resourceWire: wire}
				resources := &AttemptResources{CPU: AttemptCPUTime{Source: "cgroup_v2"}}
				input := any(Checkin{Machine: Machine{IOPressure: &IOPressure{}}, Attempts: []AttemptReport{{Resources: resources}}})
				path := "/api/workers/checkin"
				if shape != "checkin" {
					r := Result{Resources: resources}
					input = r
					if shape == "saved-result" {
						input = &r
					}
					if shape == "saved-log" {
						input = &LogChunk{Result: r}
					}
					path = "/api/workers/complete"
				}
				for i := 0; i < 4; i++ {
					if i == 2 {
						old = true
					}
					var err error
					if shape == "checkin" {
						_, err = (Worker{Client: client}).checkin(context.Background(), input.(Checkin))
					} else {
						err = client.Call(context.Background(), "POST", path, input, nil)
					}
					if err != nil {
						t.Fatal(err)
					}
					if sent[len(sent)-1] != (i == 1) {
						t.Fatal("negotiated telemetry transition lost", i, sent)
					}
				}
				if !hasResourceFields(input) {
					t.Fatal("negotiation mutated durable telemetry")
				}
			})
		}
		// Older workers omit everything and still use the real current HTTP handler.
		q, _ := fixture(t)
		server := httptest.NewServer(API{q, "test"}.Handler())
		defer server.Close()
		var reply Reply
		if err := (Client{URL: server.URL, Token: "test"}).Call(context.Background(), "POST", "/api/workers/checkin", Checkin{Machine: machine("old-worker", "fake"), AttemptProtocol: keyedAttemptProtocol}, &reply); err != nil {
			t.Fatal(err)
		}

	})

	t.Run("ResourceProtocolDowngradeWithInventory", func(t *testing.T) {
		q, clock := fixture(t)
		handler := API{q, "test"}.Handler()
		old := false
		var reports []Checkin
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			data, _ := io.ReadAll(r.Body)
			var c Checkin
			if err := json.Unmarshal(data, &c); err != nil {
				t.Error(err)
				return
			}
			reports = append(reports, c)
			if old {
				if hasResourceFields(c) || c.ToolInventoryHash != "" || hasHygieneWireFields(c) {
					w.WriteHeader(400)
					io.WriteString(w, `{"code":"request_invalid","error":"unknown field"}`)
					return
				}
				io.WriteString(w, `{"attempt_protocol":"keyed-attempt-v1"}`)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(data))
			handler.ServeHTTP(w, r)
		}))
		defer server.Close()
		m := machine("resource-inventory", "fake")
		m.ToolInventory = map[string][]string{"": {"go", "python3"}}
		m.IOPressure = &IOPressure{SampledAt: clock.Now(), Unavailable: "unsupported_os"}
		w := Worker{Clock: clock, Client: Client{URL: server.URL, Token: "test", resourceWire: &resourceWire{}}, toolReport: &workerToolReport{}}
		send := func() {
			t.Helper()
			if _, err := w.checkin(context.Background(), Checkin{Machine: m}); err != nil {
				t.Fatal(err)
			}
		}
		send()
		send()
		send()
		if reports[2].ToolInventoryHash == "" || reports[2].Machine.ToolInventory != nil || reports[2].Machine.IOPressure == nil {
			t.Fatal("negotiation did not reach compact resource heartbeat")
		}
		old = true
		send()
		send()
		for _, c := range reports[4:] {
			if c.ToolInventoryHash != "" || c.ToolInventoryVersion != 0 || len(c.Machine.ToolInventory) != 1 || hasResourceFields(c) {
				t.Fatal("fallback lost full inventory or retained additive telemetry")
			}
		}

	})
}

// The real worker combines all negotiated check-in downgrades in one fallback.
// A transport-level resource-only retry would re-send the rejected compact
// inventory and add a second 400 before the full-inventory retry (#790 pairing).
