package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"mycelium/internal/myce"
)

func TestJobResizeCLIUsesOneBatchRequest(t *testing.T) {
	want := myce.ResizeJobsRequest{Jobs: []string{"first", "second"}, Project: "example", HostMemoryGB: 11, RequestedBy: "owner", Reason: "measured peak"}
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != http.MethodPost || r.URL.Path != "/api/jobs/resize" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		var got myce.ResizeJobsRequest
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil || !reflect.DeepEqual(got, want) {
			t.Errorf("request = %+v, %v", got, err)
		}
		_ = json.NewEncoder(w).Encode(myce.ResizeJobsReceipt{Jobs: want.Jobs, HostMemoryGB: want.HostMemoryGB})
	}))
	defer server.Close()
	var out bytes.Buffer
	err := jobCommand(t.Context(), []string{"resize", "--queue", server.URL, "--token", "fixture",
		"--project", "example", "--host-memory-gb", "11", "--by", "owner", "--reason", "measured peak", "first", "second"}, &out)
	var receipt myce.ResizeJobsReceipt
	if decodeErr := json.Unmarshal(out.Bytes(), &receipt); err != nil || decodeErr != nil || calls != 1 ||
		!reflect.DeepEqual(receipt.Jobs, want.Jobs) || receipt.HostMemoryGB != want.HostMemoryGB {
		t.Fatalf("resize CLI returned %s, err=%v, decode=%v, calls=%d", out.String(), err, decodeErr, calls)
	}
}
