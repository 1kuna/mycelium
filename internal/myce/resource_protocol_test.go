package myce

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestResourceProtocolCompatibility(t *testing.T) {
	for _, result := range []bool{false, true} {
		t.Run(fmt.Sprint(result), func(t *testing.T) {
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
				io.WriteString(w, `{}`)
			}))
			defer server.Close()
			client := Client{URL: server.URL, HTTP: server.Client(), resourceWire: wire}
			resources := &AttemptResources{CPU: AttemptCPUTime{Source: "cgroup_v2"}}
			input := any(Checkin{Machine: Machine{IOPressure: &IOPressure{}}, Attempts: []AttemptReport{{Resources: resources}}})
			path := "/api/workers/checkin"
			if result {
				input = Result{Resources: resources}
				path = "/api/workers/complete"
			}
			for i := 0; i < 4; i++ {
				if i == 2 {
					old = true
				}
				if err := client.Call(context.Background(), "POST", path, input, nil); err != nil {
					t.Fatal(err)
				}
			}
			if !reflect.DeepEqual(sent, []bool{false, true, true, false, false}) {
				t.Fatal(sent)
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
}
