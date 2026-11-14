package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"mycelium/internal/myce"
)

func TestFleetUpgradeCLICriticalIntent(t *testing.T) {
	var request myce.FleetUpgradeRequest
	deadline := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	plan := myce.FleetPlannedStop{Obligation: myce.HostObligation{Result: myce.Result{Execution: myce.Execution{ID: "resumable-job", Attempt: 2}}}, StopAfter: deadline, Reason: "measured planned stop cost"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/api/fleet/upgrade" {
			t.Error("wrong fleet control route")
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		status := myce.FleetUpgradeStatus{Urgency: request.Urgency, Operation: "fixture-operation"}
		if request.Urgency == "normal" {
			status.Hosts = []myce.FleetHostStatus{{Host: "worker", Phase: "draining", PlannedStops: []myce.FleetPlannedStop{plan}}}
		}
		_ = json.NewEncoder(w).Encode(status)
	}))
	defer server.Close()
	for _, name := range []string{"MYCE_TOKEN", "MYCE_QUEUE", "MYCE_JOINING_FILE"} {
		t.Setenv(name, "")
	}
	for _, urgency := range []string{"normal", "critical"} {
		t.Run(urgency, func(t *testing.T) {
			var out bytes.Buffer
			args := []string{"upgrade", "--queue", server.URL, "--token", "test"}
			if urgency == "critical" {
				args = append(args, "--urgency", "critical")
			}
			if err := fleetCommand(context.Background(), args, &out); err != nil {
				t.Fatal(err)
			}
			if request.Urgency != urgency {
				t.Fatal("CLI lost urgency", request)
			}
			var response myce.FleetUpgradeStatus
			if err := json.Unmarshal(out.Bytes(), &response); err != nil || response.Urgency != urgency || response.Operation != "fixture-operation" {
				t.Fatal("CLI lost returned durable intent", err)
			}
			if urgency == "normal" {
				if len(response.Hosts) != 1 || len(response.Hosts[0].PlannedStops) != 1 {
					t.Fatal("CLI omitted planned stop", out.String())
				}
				got := response.Hosts[0].PlannedStops[0]
				if got.Obligation.Result.ID != plan.Obligation.Result.ID || got.Obligation.Result.Attempt != 2 || got.Reason != plan.Reason || !got.StopAfter.Equal(deadline) {
					t.Fatal("CLI lost planned job, reason or deadline", got)
				}
			}
		})
	}
}
