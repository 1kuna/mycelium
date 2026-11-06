package myce

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestProcessTelemetryNegotiationAndDowngrade(t *testing.T) {
	for _, shape := range []string{"checkin", "saved-result", "saved-log"} {
		t.Run(shape, func(t *testing.T) {
			wire := &resourceWire{}
			version := 1
			var sent []bool
			var retained []bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				data, _ := io.ReadAll(r.Body)
				has := bytes.Contains(data, []byte("process_peak"))
				retained = append(retained, bytes.Contains(data, []byte("usage_seconds")))
				w.Header().Set(resourceProtocolHeader, resourceProtocol)
				sent = append(sent, has)
				if version == 1 && has {
					w.WriteHeader(400)
					io.WriteString(w, `{"code":"request_invalid"}`)
					return
				}
				w.Header().Set(resourceProtocolHeader, resourceProtocol)
				if version == 2 {
					w.Header().Set(processTelemetryHeader, processTelemetryProtocol)
				}
				io.WriteString(w, `{"attempt_protocol":"keyed-attempt-v1"}`)
			}))
			defer server.Close()
			client := Client{URL: server.URL, HTTP: server.Client(), resourceWire: wire}
			count := uint64(2000)
			resources := &AttemptResources{ProcessPeak: &count, ProcessInitial: &count, CPU: AttemptCPUTime{UsageSeconds: findingValue(1)}}
			input := any(Checkin{Attempts: []AttemptReport{{Resources: resources}}})
			path := "/api/workers/checkin"
			if shape != "checkin" {
				r := Result{Resources: resources}
				input = &r
				path = "/api/workers/complete"
				if shape == "saved-log" {
					input = &LogChunk{Result: r}
				}
			}
			for _, v := range []int{1, 2, 2, 1, 1} {
				version = v
				var err error
				if shape == "checkin" {
					_, err = (Worker{Client: client}).checkin(context.Background(), input.(Checkin))
				} else {
					err = client.Call(context.Background(), "POST", path, input, nil)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			if !reflect.DeepEqual(sent, []bool{false, false, true, true, false, false}) {
				t.Fatal(sent)
			}
			if !reflect.DeepEqual(retained, []bool{false, true, true, true, true, true}) {
				t.Fatalf("CPU/I/O lost during process downgrade: %v", retained)
			}
			if resources.ProcessPeak == nil || *resources.ProcessPeak != 2000 {
				t.Fatal("durable telemetry mutated")
			}
		})
	}
}
