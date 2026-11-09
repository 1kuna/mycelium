package myce

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestPlacementStatusAuthoredAndScheduler(t *testing.T) {
	for _, pinned := range []bool{false, true} {
		t.Run(map[bool]string{false: "free", true: "pinned"}[pinned], func(t *testing.T) {
			q, clock := fixture(t)
			m := cpuMachine("worker", "host", "cpu", 8, 32)
			check(t, q, m, nil)
			s := cpuSpec(1, 1)
			s.Run.Command = []string{"echo", "private-command"}
			s.Run.Env = map[string]string{"PRIVATE": "private-secret"}
			s.Run.Workdir = "/private/source"
			if pinned {
				s.AllowedMachines = []string{m.ID}
			}
			submitted := submit(t, q, s)
			assigned := check(t, q, m, nil).Job
			if assigned == nil || assigned.ID != submitted.ID {
				t.Fatal("no assignment")
			}
			frozen := cloneJob(q.s.Jobs[submitted.ID])
			if err := q.Started(result(assigned, true)); err != nil {
				t.Fatal(err)
			}
			clock.Advance(time.Second)
			finish(t, q, assigned, true)
			server := httptest.NewServer(API{Queue: q, Token: "fixture"}.Handler())
			defer server.Close()
			client := Client{URL: server.URL, Token: "fixture"}
			for _, path := range []string{"/api/jobs/" + submitted.ID, "/api/inspection", "/api/status", "/api/status?detail=full"} {
				var got Job
				if strings.HasPrefix(path, "/api/jobs/") {
					if err := client.Call(context.Background(), "GET", path, nil, &got); err != nil {
						t.Fatal(err)
					}
				} else {
					var snapshot Snapshot
					if err := client.Call(context.Background(), "GET", path, nil, &snapshot); err != nil {
						t.Fatal(err)
					}
					for _, j := range snapshot.Jobs {
						if j.ID == submitted.ID {
							got = *j
						}
					}
				}
				intent, decision := got.PlacementIntent, got.Placement
				if intent == nil || intent.Origin != "authored" || decision == nil || decision.Origin != "scheduler" || decision.Attempt != assigned.Attempt || decision.Host != "host" || decision.Reason == "" || decision.AssignedAt.IsZero() {
					t.Fatal(got)
				}
				if pinned {
					if intent.Scope != "host_device_restriction" || decision.HostChoice != "authored_pin" || !slices.Contains(decision.LimitedBy, "authored_host") {
						t.Fatal(intent, decision)
					}
				} else {
					if intent.Scope != "work_resources" || decision.HostChoice != "scheduler" {
						t.Fatal(intent, decision)
					}
				}
				if len(got.AttemptHistory) != 1 || !reflect.DeepEqual(got.AttemptHistory[0].Placement, decision) {
					t.Fatal("attempt lost assignment", got.AttemptHistory)
				}
				validateWireValue(t, compileWireSchema(t, "job-v1.schema.json"), got)
				if !reflect.DeepEqual(frozen.Placement, decision) {
					t.Fatal("historical decision changed")
				}
				data, _ := json.Marshal(got)
				for _, private := range []string{"private-command", "private-secret", "/private/source", "private authored reason"} {
					if strings.Contains(string(data), private) {
						t.Fatal("private payload leaked", string(data))
					}
				}
			}
			page, err := q.JobLedger(submitted.ID, 0, -1, 256)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, entry := range page.Entries {
				if entry.Kind == "assigned" {
					found = true
					if !reflect.DeepEqual(entry.Placement, frozen.Placement) {
						t.Fatal(entry)
					}
				}
			}
			if !found {
				t.Fatal("assignment ledger missing")
			}
			// The durable record retains intent and historical decision after completion.
			var data []byte
			data = durableQueueRecord(t, q.db, "jobs/"+submitted.ID)
			var stored Job
			if err = json.Unmarshal(data, &stored); err != nil || !reflect.DeepEqual(stored.Placement, frozen.Placement) || !reflect.DeepEqual(stored.PlacementIntent, frozen.PlacementIntent) {
				t.Fatal("placement not persisted", err)
			}
		})
	}
}
