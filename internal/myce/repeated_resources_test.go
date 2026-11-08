package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestRepeatedResourcesSchedulingPressure(t *testing.T) {
	_, clock := fixture(t)
	a := resourceCgroupFixture(t)
	path := filepath.Join(a.path, "cpu.pressure")
	for _, value := range []string{"some avg10=0 avg60=0 avg300=0 total=0\n", "some avg10=0 avg60=0 avg300=0 total=456\n"} {
		if err := os.WriteFile(path, []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
		r := cgroupResources(a, clock.Now())
		if r.CPU.Pressure == nil || r.CPU.Pressure.Some == nil || r.CPU.Pressure.Unavailable != "" {
			t.Fatal("scheduler contention coverage missing", r.CPU.Pressure)
		}
		if bytes.Contains([]byte(value), []byte("456")) && r.CPU.Pressure.Some.TotalUS != 456 {
			t.Fatal("scheduler delay lost")
		}
		copy := safeAttemptResources(r)
		r.CPU.Pressure.Some.TotalUS++
		if copy.CPU.Pressure.Some.TotalUS == r.CPU.Pressure.Some.TotalUS {
			t.Fatal("ingress retained mutable CPU pressure")
		}
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if p := cgroupResources(a, clock.Now()).CPU.Pressure; p == nil || p.Some != nil || p.Unavailable == "" {
		t.Fatal("missing CPU sensor became zero contention", p)
	}
}

// Keep the prior CPU/I/O wire contract intact with a queue that supports v1 but
// has a strict decoder predating CPU pressure. Every worker delivery shape and
// a live downgrade preserve durable local evidence while stripping only fields
// unsupported by that queue.
func TestRepeatedResourcesWireCompatibility(t *testing.T) {
	for _, shape := range []string{"checkin", "result", "saved-result", "saved-log"} {
		t.Run(shape, func(t *testing.T) {
			version, refusals := 2, 0
			var sent []int
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				data, _ := io.ReadAll(r.Body)
				kind := 0
				if bytes.Contains(data, []byte(`"resources"`)) {
					kind = 1
				}
				var decoded struct {
					Resources *AttemptResources `json:"resources"`
					Attempts  []AttemptReport   `json:"attempts"`
				}
				if err := json.Unmarshal(data, &decoded); err != nil {
					t.Error(err)
				}
				resources := decoded.Resources
				if len(decoded.Attempts) > 0 {
					resources = decoded.Attempts[0].Resources
				}
				if resources != nil && resources.CPU.Pressure != nil {
					kind = 2
				}
				sent = append(sent, kind)
				w.Header().Set(resourceProtocolHeader, resourceProtocol)
				if version == 2 {
					w.Header().Set(cpuPressureProtocolHeader, cpuPressureProtocol)
				}
				if version < kind {
					refusals++
					w.WriteHeader(400)
					io.WriteString(w, `{"code":"request_invalid","error":"unknown field"}`)
					return
				}
				io.WriteString(w, `{"attempt_protocol":"keyed-attempt-v1"}`)
			}))
			defer server.Close()
			wire := &resourceWire{}
			c := Client{URL: server.URL, HTTP: server.Client(), resourceWire: wire}
			r := cleanRepeatedResult(&Job{})
			var input any = r
			if shape == "saved-result" {
				input = &r
			} else if shape == "saved-log" {
				input = &LogChunk{Result: r}
			}
			for i := 0; i < 4; i++ {
				if i == 2 {
					version = 1
				}
				var err error
				if shape == "checkin" {
					_, err = (Worker{Client: c}).checkin(context.Background(), Checkin{Attempts: []AttemptReport{{Resources: r.Resources}}})
				} else {
					err = c.Call(context.Background(), "POST", "/api/workers/complete", input, nil)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			if refusals != 1 || !reflect.DeepEqual(sent, []int{0, 2, 2, 0, 1}) {
				t.Fatal("CPU-pressure downgrade lost legacy telemetry", sent, refusals)
			}
			if r.Resources.CPU.Pressure == nil {
				t.Fatal("negotiation mutated retained local evidence")
			}
		})
	}
}
