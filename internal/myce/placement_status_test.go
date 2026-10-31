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
			if err = q.db.QueryRow("SELECT data FROM queue_state WHERE key=?", "jobs/"+submitted.ID).Scan(&data); err != nil {
				t.Fatal(err)
			}
			var stored Job
			if err = json.Unmarshal(data, &stored); err != nil || !reflect.DeepEqual(stored.Placement, frozen.Placement) || !reflect.DeepEqual(stored.PlacementIntent, frozen.PlacementIntent) {
				t.Fatal("placement not persisted", err)
			}
		})
	}
}

func TestPlacementStatusRetainsEachAttempt(t *testing.T) {
	q, clock := fixture(t)
	submit(t, q, spec("experiment"))
	first := check(t, q, machine("one", "fake"), nil).Job
	if first == nil || first.Placement == nil {
		t.Fatal("missing first placement")
	}
	if err := q.update(func() error {
		q.requeue(q.touchJob(first.ID), "planned", false)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	second := check(t, q, machine("two", "fake"), nil).Job
	if second == nil || second.Placement == nil || second.Attempt <= first.Attempt {
		t.Fatal("missing replacement placement")
	}
	got, err := q.jobSnapshot(first.ID)
	if err != nil || !reflect.DeepEqual(got.Placement, second.Placement) || len(got.AttemptHistory) != 1 || !reflect.DeepEqual(got.AttemptHistory[0].Placement, first.Placement) {
		t.Fatal("replacement changed historical placement", got, err)
	}
	page, err := q.JobLedger(first.ID, 0, -1, 256)
	if err != nil {
		t.Fatal(err)
	}
	assigned := 0
	for _, entry := range page.Entries {
		if entry.Kind != "assigned" {
			continue
		}
		assigned++
		want := first.Placement
		if entry.Attempt == second.Attempt {
			want = second.Placement
		}
		if !reflect.DeepEqual(entry.Placement, want) {
			t.Fatal("ledger placement belongs to another attempt", entry)
		}
	}
	if assigned != 2 {
		t.Fatal("missing assigned ledger entries", assigned)
	}
}

func TestPlacementStatusProductionBindingIsScheduler(t *testing.T) {
	q, _, a, _, _ := holdFleet(t, true)
	s := spec("production")
	s.Needs.MemoryGB = 1
	submitted := submit(t, q, s)
	got := job(t, q, submitted.ID)
	if got.ProductionHost == "" {
		t.Fatal("fixture did not acquire production binding")
	}
	if got.PlacementIntent == nil || got.PlacementIntent.HostConstraint != "none" {
		t.Fatal("binding counted as authored", got)
	}
	public, err := q.jobSnapshot(submitted.ID)
	if err != nil || public.ProductionBinding == nil || public.ProductionBinding.Origin != "scheduler" || public.ProductionBinding.Host != got.ProductionHost {
		t.Fatal(public, err)
	}
	assignment := check(t, q, a, nil).Job
	if assignment == nil {
		t.Fatal("bound production job not assigned")
	}
	public, err = q.jobSnapshot(submitted.ID)
	if err != nil || public.Placement.HostChoice != "scheduler" || !slices.Contains(public.Placement.LimitedBy, "scheduler_production_binding") {
		t.Fatal(public, err)
	}
}

func TestPlacementStatusSafeConstraintsAndLegacyUnknown(t *testing.T) {
	s := Spec{Targets: []ExecutionTarget{{OS: "linux", Arch: "arm64", Platform: "cuda"}}, Needs: ResourceNeeds{Platform: Platform{Required: []string{"cuda"}, Preferred: []string{"metal"}}, GPU: &GPUSelection{Devices: []string{"device-secret-selector"}}}, Environment: Environment{Name: "private-environment"}, Reason: "/private/reason"}
	j := &Job{Spec: s, EnvironmentRecipes: map[string]EnvironmentRecipe{"cuda": {Backend: "uv", LockFile: "/private/lock", Files: map[string][]byte{"private-input": []byte("secret")}}}}
	p := submissionPlacementIntent(s, j)
	if p.Scope != "host_device_restriction" || p.DeviceConstraint != "selector" || !reflect.DeepEqual(p.Targets, s.Targets) || !slices.Equal(p.RequiredBackends, []string{"cuda"}) || !slices.Equal(p.Environment.Platforms, []string{"cuda"}) || p.Parity != "unknown" {
		t.Fatal(p)
	}
	data, _ := json.Marshal(p)
	for _, private := range []string{"/private", "secret", "private-environment", "private-input"} {
		if strings.Contains(string(data), private) {
			t.Fatal("unsafe summary", string(data))
		}
	}
	inline := s
	inline.Environment = Environment{LockFile: "/private/requirements.txt", Python: "3.12"}
	inlineIntent := submissionPlacementIntent(inline, j)
	if inlineIntent.Environment.Origin != "submission_snapshot" {
		t.Fatal("inline inputs attributed to catalog", inlineIntent)
	}
	extension := Spec{Extension: "sample@1"}
	p = submissionPlacementIntent(extension, &Job{Extensions: []Extension{{Variants: []ExtensionVariant{{Target: s.Targets[0]}}}}})
	if p.Environment.State != "targets" || !reflect.DeepEqual(p.Environment.Targets, s.Targets) || p.Qualification != "required" {
		t.Fatal(p)
	}
	p = submissionPlacementIntent(s, &Job{})
	if p.Environment.State != "unknown" {
		t.Fatal("missing coverage claimed", p)
	}
	legacy := statusJobCopy(&Job{ProductionHost: "bound-host", Spec: s})
	if legacy.PlacementIntent.Origin != "unknown" || legacy.Placement != nil || legacy.ProductionBinding.Origin != "scheduler" {
		t.Fatal("legacy provenance inferred", legacy)
	}
}

func TestPlacementStatusDeviceConstraint(t *testing.T) {
	for _, device := range []string{"", "1"} {
		t.Run("selector-"+device, func(t *testing.T) {
			q, _ := fixture(t)
			m := m3Machine()
			machineCheck(t, q, m)
			s := m3Spec("experiment", device, 1, 1)
			submitted := submit(t, q, s)
			assigned := assignedJob(machineCheck(t, q, m))
			if assigned == nil || assigned.ID != submitted.ID {
				t.Fatal("no device assignment")
			}
			got, err := q.jobSnapshot(submitted.ID)
			if err != nil || got.Placement == nil || len(got.Placement.Devices) != 1 {
				t.Fatal(got, err)
			}
			want := "scheduler"
			if device != "" {
				want = "authored_selector"
			}
			if got.Placement.DeviceChoice != want {
				t.Fatal(got.Placement)
			}
		})
	}
}

func TestPlacementStatusSelectedRecipeConstraint(t *testing.T) {
	q, _ := fixture(t)
	m := m3Machine()
	target := m.Devices[0].machine(m)
	selectedTarget := machineTarget(&target)
	plain := Extension{Digest: "plain", Variants: []ExtensionVariant{{Target: selectedTarget}}}
	pinned := Extension{Digest: "pinned", Variants: []ExtensionVariant{{Target: selectedTarget, Needs: ResourceNeeds{GPU: &GPUSelection{Devices: []string{"0"}}}}}}
	j := &Job{ID: "job", Attempt: 1, Extensions: []Extension{plain, pinned}, Extension: &ExtensionSelection{Digest: plain.Digest}}
	j.PlacementIntent = submissionPlacementIntent(Spec{Extension: "sample@1"}, j)
	q.recordPlacementDecision(j, &target)
	if j.Placement.DeviceChoice != "scheduler" {
		t.Fatal("unselected recipe counted as an authored restriction", j.Placement)
	}
	j.Extension = &ExtensionSelection{Digest: pinned.Digest}
	q.recordPlacementDecision(j, &target)
	if j.Placement.DeviceChoice != "authored_recipe_selector" || !slices.Contains(j.Placement.LimitedBy, "authored_recipe_device") {
		t.Fatal(j.Placement)
	}
}
