package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"mycelium/internal/myce"
)

func TestFleetUpgradeCLICriticalIntent(t *testing.T) {
	var request myce.FleetUpgradeRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/api/fleet/upgrade" {
			t.Error("wrong fleet control route")
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		_ = json.NewEncoder(w).Encode(myce.FleetUpgradeStatus{Urgency: request.Urgency, Operation: "fixture-operation"})
	}))
	defer server.Close()
	for _, name := range []string{"MYCE_TOKEN", "MYCE_QUEUE", "MYCE_JOINING_FILE"} {
		t.Setenv(name, "")
	}
	var out bytes.Buffer
	if err := fleetCommand(context.Background(), []string{"upgrade", "--urgency", "critical", "--queue", server.URL, "--token", "test"}, &out); err != nil {
		t.Fatal(err)
	}
	if request.Urgency != "critical" {
		t.Fatal("CLI lost explicit critical urgency", request)
	}
	var response myce.FleetUpgradeStatus
	if err := json.Unmarshal(out.Bytes(), &response); err != nil || response.Urgency != "critical" || response.Operation != "fixture-operation" {
		t.Fatal("CLI lost returned durable intent", err)
	}
}
