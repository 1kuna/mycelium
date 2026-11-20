package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"mycelium/internal/myce"
)

func TestPauseCLIYieldAttemptFence(t *testing.T) {
	var request myce.PauseJobRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "GET" && r.URL.Path == "/api/jobs/work":
			json.NewEncoder(w).Encode(myce.Job{ID: "work", Attempt: 4})
		case r.Method == "POST" && r.URL.Path == "/api/jobs/work/pause":
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
			}
			json.NewEncoder(w).Encode(myce.Job{ID: "work", Attempt: 4, State: "stopping"})
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	var out bytes.Buffer
	if err := pauseCommand(context.Background(), "pause", []string{"work", "--queue", server.URL, "--token", "fixture", "--yield", "--project", "example", "--reason", "free capacity", "--confirm-restart"}, &out); err != nil {
		t.Fatal(err)
	}
	attempt := 4
	want := myce.PauseJobRequest{Project: "example", Reason: "free capacity", Yield: true, ConfirmRestart: true, IfAttempt: &attempt}
	if !reflect.DeepEqual(request, want) {
		t.Fatalf("yield request: %+v", request)
	}
	var reply myce.Job
	if err := json.Unmarshal(out.Bytes(), &reply); err != nil || reply.State != "stopping" {
		t.Fatal("CLI claimed pause before release", out.String(), err)
	}
	for _, flags := range [][]string{{"--yield"}, {"--confirm-restart"}} {
		args := append([]string{"work", "--reason", "capacity"}, flags...)
		if err := pauseCommand(context.Background(), "pause", args, &out); err == nil {
			t.Fatal("unsafe flags accepted", flags)
		}
	}
}
