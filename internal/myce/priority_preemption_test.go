package myce

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPriorityPreemptionEveryBatchKind(t *testing.T) {
	for _, kind := range []string{"resumable", "restart", "native-group"} {
		for _, higher := range []bool{false, true} {
			t.Run(kind+"/higher="+map[bool]string{false: "no", true: "yes"}[higher], func(t *testing.T) {
				q, _ := fixture(t)
				victim := spec("experiment")
				victim.Resumable = kind != "restart"
				if kind == "native-group" {
					victim.Run.Sidecars = []NativeSidecar{{Name: "helper", Command: []string{"helper"}, MemoryGB: 1}}
					victim.Needs.HostMemoryGB = 2
				}
				submit(t, q, victim)
				m := machine("one", "fake")
				m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 32
				batch := check(t, q, m, nil).Job
				if batch == nil {
					t.Fatal("no victim")
				}
				tier := "experiment"
				if higher {
					tier = "deadline"
				}
				submit(t, q, spec(tier))
				got := job(t, q, batch.ID)
				if (got.State == "stopping" || got.State == "queued") != higher {
					t.Fatalf("victim state %s", got.State)
				}
				if higher && got.State == "stopping" {
					want := 10 * time.Second
					if !victim.Resumable {
						want = 0
					}
					if got.StopAt == nil || got.StopAt.Sub(q.clock.Now()) != want {
						t.Fatal("tier grace", got.StopAt)
					}
				}
			})
		}
	}
}

func TestPriorityPreemptionProductionBudgetAndCompletion(t *testing.T) {
	q, clock := fixture(t)
	policy := PriorityPolicy{Version: 1, Tiers: []PriorityTier{{Name: "urgent", Selectors: []string{"urgent/*"}}, {Name: "other", Selectors: []string{"*/*"}}}}
	if _, err := q.SetPriorityPolicy(0, policy); err != nil {
		t.Fatal(err)
	}
	if err := q.SetLossTimeouts(time.Hour, time.Hour, time.Hour); err != nil {
		t.Fatal(err)
	}
	victim := submit(t, q, spec("production"))
	m := machine("one", "fake")
	for i := 0; i < 4; i++ {
		batch := check(t, q, m, nil).Job
		if batch == nil || batch.ID != victim.ID {
			t.Fatal("victim lost", batch)
		}
		if err := q.Started(result(batch, false)); err != nil {
			t.Fatal(err)
		}
		clock.Advance(10 * time.Second)
		s := spec("experiment")
		s.Project = "urgent"
		demand := submit(t, q, s)
		if job(t, q, batch.ID).State != "stopping" {
			t.Fatalf("not preempted at %d: victim=%+v demand=%+v", i, job(t, q, batch.ID), demand)
		}
		r := result(batch, false)
		r.Interrupted = true
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		got := job(t, q, batch.ID)
		if got.State != "queued" || got.InfrastructureRetries != 0 || got.Losses != 0 {
			t.Fatal("planned stop charged retry", got)
		}
		clock.Advance(10 * time.Second)
		next := check(t, q, m, nil).Job
		if next == nil || next.ID != demand.ID {
			t.Fatal("no replacement", next)
		}
		finish(t, q, next, true)
	}
	batch := check(t, q, m, nil).Job
	if batch == nil {
		t.Fatal("victim did not survive")
	}
	s := spec("experiment")
	s.Project = "urgent"
	submit(t, q, s)
	finish(t, q, batch, true)
	if got := job(t, q, batch.ID); got.State != "succeeded" {
		t.Fatal("completion lost to preemption", got)
	}
}

func preemptionPolicy(t *testing.T, q *Queue) {
	t.Helper()
	if _, err := q.SetPriorityPolicy(0, PriorityPolicy{Version: 1, Tiers: []PriorityTier{{Name: "urgent", Selectors: []string{"urgent/*"}}, {Name: "other", Selectors: []string{"*/*"}}}}); err != nil {
		t.Fatal(err)
	}
	if err := q.SetLossTimeouts(time.Hour, time.Hour, time.Hour); err != nil {
		t.Fatal(err)
	}
}
func TestPriorityPreemptionModelsAndPins(t *testing.T) {
	for _, kind := range []string{"call", "load", "idle", "pin"} {
		for _, higher := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/higher=%v", kind, higher), func(t *testing.T) {
				q, _, m := modelFixture(t)
				preemptionPolicy(t, q)
				var c *ModelCall
				if kind == "pin" {
					if err := q.Pin(ModelPin{Project: "test", Model: "test", Machine: m.ID}); err != nil {
						t.Fatal(err)
					}
				} else {
					c = addModelCall(t, q, "experiment")
				}
				if kind != "load" {
					reportModel(t, q, m, "ready")
				}
				if kind == "idle" {
					if err := q.finishCall(c.ID); err != nil {
						t.Fatal(err)
					}
				}
				s := q.s.ModelSlots[slotKey(m.ID, "test")]
				demand := spec("experiment")
				if higher {
					demand.Project = "urgent"
				}
				high := submit(t, q, demand)
				if higher {
					if s.PriorityStop == nil || s.PriorityStop.Demand != high.ID || s.PriorityStop.Policy.Revision != 1 {
						t.Fatal("missing typed engine stop", s)
					}
					if (s.State != "unloading" && s.State != "draining") || s.StopAt == nil {
						t.Fatal("engine not interrupted", s)
					}
					if kind == "call" && (c.Error != priorityPreemptionCause || c.PriorityStop == nil || q.callAttemptLive(c)) {
						t.Fatal("active call not interrupted", c)
					}
					if check(t, q, m, nil).Job != nil {
						t.Fatal("replacement before engine cleanup")
					}
					if kind == "call" {
						if err := q.finishCall(c.ID); err != nil {
							t.Fatal(err)
						}
					}
					if kind == "pin" && len(q.s.Pins) != 1 {
						t.Fatal("pin intent lost")
					}
					if kind == "load" && c.Error != "" {
						t.Fatal("queued load caller lost", c)
					}
					r := recoveryReport(t, q, m, s, "unloaded")
					if r.Job == nil || r.Job.ID != high.ID {
						t.Fatal("no replacement after exact cleanup", r)
					}
					finish(t, q, r.Job, true)
					check(t, q, m, nil)
					if kind == "pin" && q.s.ModelSlots[slotKey(m.ID, "test")] == nil {
						t.Fatal("pin did not return")
					}
				} else {
					if s.PriorityStop != nil || kind == "call" && c.Error != "" {
						t.Fatal("same tier preempted")
					}
					if kind != "idle" && s.State != "ready" && s.State != "loading" {
						t.Fatal("same tier interrupted", s)
					}
				}
			})
		}
	}
}

func TestPriorityPreemptionEnvironmentBuild(t *testing.T) {
	for _, higher := range []bool{false, true} {
		t.Run(fmt.Sprint(higher), func(t *testing.T) {
			q, _, m := modelFixture(t)
			preemptionPolicy(t, q)
			m.EnvironmentBuilder = true
			if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"build": {"fake": binaryRecipe(t, "fake")}}}); err != nil {
				t.Fatal(err)
			}
			s := spec("experiment")
			s.Environment.Name = "build"
			victim := submit(t, q, s)
			reply := check(t, q, m, nil)
			if len(reply.EnvironmentPlan) != 1 {
				t.Fatal("no build")
			}
			launch := *reply.EnvironmentPlan[0].Launch
			if err := q.AuthorizeLaunch(launch); err != nil {
				t.Fatal(err)
			}
			m.EnvironmentReports = []EnvironmentReport{{Key: launch.Key, Attempt: launch.Attempt, State: "building"}}
			check(t, q, m, nil)
			d := spec("experiment")
			if higher {
				d.Project = "urgent"
			}
			demand := submit(t, q, d)
			reply = check(t, q, m, nil)
			saved := q.s.PreparationPlans[launch.Attempt]
			if (saved.PriorityStop != nil) != higher {
				t.Fatal("build priority stop", saved)
			}
			if !higher {
				return
			}
			if reply.PreparationStopAt == nil || len(reply.EnvironmentPlan) != 0 || reply.Job != nil {
				t.Fatal("missing bounded preparation stop", reply)
			}
			if err := q.AuthorizeLaunch(launch); err != ErrStale {
				t.Fatal("stale build authorized", err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			rt := &environmentRuntime{key: launch.Key, cancel: cancel}
			rt.sync(reply.EnvironmentPlan, false, false)
			if ctx.Err() == nil {
				t.Fatal("build not cancelled")
			}
			m.EnvironmentReports[0].Attempt = "wrong"
			m.EnvironmentReports[0].State = "paused"
			if check(t, q, m, nil).Job != nil {
				t.Fatal("wrong cleanup released build")
			}
			m.EnvironmentReports[0].Attempt = launch.Attempt
			reply = check(t, q, m, nil)
			if reply.Job == nil || reply.Job.ID != demand.ID {
				t.Fatal("exact cleanup did not admit demand", reply)
			}
			if got := job(t, q, victim.ID); got.State != "queued" || got.Losses != 0 {
				t.Fatal("build victim lost", got)
			}
		})
	}
}

func TestPriorityPreemptionOutcomePrecedence(t *testing.T) {
	for _, priority := range []string{"production", "experiment"} {
		for _, outcome := range []string{"complete", "interrupted", "independent-exit", "oom", "cancel"} {
			t.Run(priority+"/"+outcome, func(t *testing.T) {
				q, clock := fixture(t)
				preemptionPolicy(t, q)
				victim := submit(t, q, spec(priority))
				m := machine("one", "fake")
				batch := check(t, q, m, nil).Job
				d := spec("experiment")
				d.Project = "urgent"
				submit(t, q, d)
				stop := priorityStop(q.s.Jobs[victim.ID])
				if stop == nil {
					t.Fatal("no planned stop")
				}
				facts := attemptCause{PriorityStop: stop, memberOutcome: memberOutcome{Exit: &memberExit{Status: 143, ObservedAt: clock.Now().Add(time.Second)}}}
				if outcome == "interrupted" || outcome == "cancel" {
					facts.Signals = []deliveredSignal{{Signal: "TERM", At: clock.Now()}}
				}
				if outcome == "oom" {
					facts.OutOfMemory = true
				}
				r := result(batch, outcome == "complete")
				if r.Success {
					facts.Exit.Status = 0
				}
				if outcome == "cancel" {
					if err := q.Cancel(victim.ID); err != nil {
						t.Fatal(err)
					}
					facts.Cancelled = true
				}
				r = classifyAttemptOutcome(r, facts)
				if (r.PriorityStop != nil) != (outcome == "interrupted") {
					t.Fatal("intent manufactured interruption or lost provenance", r)
				}
				// The deadline authorizes kill, never retries ahead of the member's result.
				clock.Advance(10 * time.Second)
				check(t, q, m, batch)
				if err := q.Complete(r); err != nil {
					t.Fatal(err)
				}
				want := map[string]string{"complete": "succeeded", "interrupted": "queued", "independent-exit": "failed", "oom": "failed", "cancel": "cancelled"}[outcome]
				got := job(t, q, victim.ID)
				if got.State != want || got.InfrastructureRetries != 0 || got.Losses != 0 {
					t.Fatal("precedence", got)
				}
			})
		}
	}
}

func TestPriorityPreemptionHysteresisAlternatingDemand(t *testing.T) {
	q, clock := fixture(t)
	preemptionPolicy(t, q)
	victim := submit(t, q, spec("experiment"))
	m := machine("one", "fake")
	for i := 0; i < 5; i++ {
		batch := check(t, q, m, nil).Job
		if batch == nil || batch.ID != victim.ID {
			t.Fatal("no progress", batch)
		}
		if err := q.Started(result(batch, false)); err != nil {
			t.Fatal(err)
		}
		d := spec("experiment")
		d.Project = "urgent"
		demand := submit(t, q, d)
		if i >= 2 {
			if job(t, q, batch.ID).State != "running" {
				t.Fatal("resumed work never got runtime")
			}
			clock.Advance(10*time.Second - time.Nanosecond)
			check(t, q, m, batch)
			if job(t, q, batch.ID).State != "running" {
				t.Fatal("minimum runtime shortened")
			}
			clock.Advance(time.Nanosecond)
		}
		check(t, q, m, batch)
		if job(t, q, batch.ID).State != "stopping" {
			t.Fatal("hysteresis became permanent reservation")
		}
		r := result(batch, false)
		r.Interrupted = true
		if err := q.Complete(r); err != nil {
			t.Fatal(err)
		}
		clock.Advance(10 * time.Second)
		high := check(t, q, m, nil).Job
		if high == nil || high.ID != demand.ID {
			t.Fatal("higher tier failed to progress", high)
		}
		finish(t, q, high, true)
	}
}

func TestPriorityPreemptionModelClassDemand(t *testing.T) {
	q, _, m := modelFixture(t)
	if _, err := q.SetPriorityPolicy(0, PriorityPolicy{Version: 1, Tiers: []PriorityTier{{Name: "urgent", Selectors: []string{"*/interactive"}}, {Name: "other", Selectors: []string{"*/*"}}}}); err != nil {
		t.Fatal(err)
	}
	victim := submit(t, q, spec("production"))
	batch := check(t, q, m, nil).Job
	c := &ModelCall{Project: "test", Class: "interactive", Model: "test"}
	if err := q.callDefaults(c, q.models.Models[0], http.Header{"X-Myce-Class": []string{"interactive"}}, true); err != nil {
		t.Fatal(err)
	}
	if err := q.addCall(c); err != nil {
		t.Fatal(err)
	}
	if got := job(t, q, victim.ID); got.State != "stopping" || priorityStop(got) == nil || priorityStop(got).Demand != c.ID {
		t.Fatal("model tier did not reclaim batch", got)
	}
	r := result(batch, false)
	r.Interrupted = true
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	check(t, q, m, nil)
	if s := q.s.ModelSlots[slotKey(m.ID, "test")]; s == nil || s.Class != "interactive" {
		t.Fatal("load lost tier identity", s)
	}
}

func TestPriorityPreemptionStreamingTypedError(t *testing.T) {
	for _, generic := range []bool{false, true} {
		t.Run(fmt.Sprint(generic), func(t *testing.T) {
			q, _, m := modelFixture(t)
			preemptionPolicy(t, q)
			worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				fmt.Fprint(w, "data: first\n\n")
				w.(http.Flusher).Flush()
				<-r.Context().Done()
			}))
			defer worker.Close()
			m.ModelURL = worker.URL
			check(t, q, m, nil)
			c := addModelCall(t, q, "experiment")
			reportModel(t, q, m, "ready")
			if err := q.finishCall(c.ID); err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
			defer server.Close()
			endpoint := "/v1/chat/completions"
			if generic {
				endpoint = "/models/test/v1/chat/completions"
			}
			req, _ := http.NewRequest("POST", server.URL+endpoint, strings.NewReader(`{"model":"test","stream":true,"messages":[]}`))
			req.Header.Set("Authorization", "Bearer test")
			req.Header.Set("X-Myce-Project", "test")
			req.Header.Set("X-Myce-Class", "experiment")
			ctx, cancel := context.WithTimeout(context.Background(), testWaitTimeout)
			defer cancel()
			req = req.WithContext(ctx)
			res, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer res.Body.Close()
			reader := bufio.NewReader(res.Body)
			first, err := reader.ReadString('\n')
			if err != nil || first != "data: first\n" {
				t.Fatal(first, err)
			}
			d := spec("experiment")
			d.Project = "urgent"
			submit(t, q, d)
			data, err := io.ReadAll(reader)
			if err != nil {
				t.Fatal(err)
			}
			if generic {
				if res.Trailer.Get("X-Myce-Error-Code") != priorityPreemptionCause {
					t.Fatal("missing typed stream trailer", res.Trailer)
				}
			} else {
				if !strings.Contains(string(data), `"code":"priority_preemption"`) || !strings.Contains(string(data), `"type":"interruption"`) {
					t.Fatal("missing typed stream error", string(data))
				}
			}
			if strings.Contains(string(data), "[DONE]") {
				t.Fatal("interrupted stream advertised completion")
			}
		})
	}
}

func TestPriorityPreemptionCheckpointQueueRestart(t *testing.T) {
	q, clock := fixture(t)
	preemptionPolicy(t, q)
	s := spec("production")
	s.SubmissionKey = "priority-resume"
	victim := submit(t, q, s)
	m := machine("one", "fake")
	batch := check(t, q, m, nil).Job
	if err := q.Started(result(batch, false)); err != nil {
		t.Fatal(err)
	}
	batch.Checkpoint = filepath.Join(q.root, batch.Checkpoint)
	checkpoint := attemptCheckpoint(*batch)
	if err := os.MkdirAll(checkpoint, 0700); err != nil {
		t.Fatal(err)
	}
	durable := []byte(`{"myce_checkpoint":{"generation":7},"value":42}`)
	if err := os.WriteFile(filepath.Join(checkpoint, "checkpoint.json"), durable, 0600); err != nil {
		t.Fatal(err)
	}
	d := spec("experiment")
	d.Project = "urgent"
	demand := submit(t, q, d)
	deadline := *job(t, q, victim.ID).StopAt
	var path string
	if err := q.db.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&path); err != nil {
		t.Fatal(err)
	}
	root := q.root
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	var err error
	q, err = Open(path, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if err = q.SetLossTimeouts(time.Hour, time.Hour, time.Hour); err != nil {
		t.Fatal(err)
	}
	q.checkpointProbe = func() bool { return true }
	q.nasAvailable = true
	got := job(t, q, victim.ID)
	if got.StopAt == nil || !got.StopAt.Equal(deadline) || priorityStop(got) == nil || priorityStop(got).Demand != demand.ID {
		t.Fatal("restart lost stop provenance", got)
	}
	check(t, q, m, batch)
	r := result(batch, false)
	r.Interrupted = true
	if err = q.Complete(r); err != nil {
		t.Fatal(err)
	}
	clock.Advance(10 * time.Second)
	high := check(t, q, m, nil).Job
	if high == nil || high.ID != demand.ID {
		t.Fatal("no high assignment", high)
	}
	finish(t, q, high, true)
	resumed := check(t, q, m, nil).Job
	if resumed == nil || resumed.ID != victim.ID || resumed.Attempt != 2 || resumed.Spec.SubmissionKey != s.SubmissionKey || resumed.InfrastructureRetries != 0 {
		t.Fatal("logical identity lost", resumed)
	}
	resumed.Checkpoint = filepath.Join(q.root, resumed.Checkpoint)
	dest, err := prepareCheckpoint(*resumed)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dest, "checkpoint.json"))
	if err != nil || string(data) != string(durable) {
		t.Fatal("checkpoint not resumed", string(data), err)
	}
	if priorityStop(resumed) != nil {
		t.Fatal("stop cause transferred to new attempt")
	}
	if err = q.Complete(result(batch, true)); err != ErrStale {
		t.Fatal("stale preempted completion accepted", err)
	}
}

func TestPriorityPreemptionPreparationHysteresis(t *testing.T) {
	q, clock, m := modelFixture(t)
	preemptionPolicy(t, q)
	m.EnvironmentBuilder = true
	if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"build": {"fake": binaryRecipe(t, "fake")}}}); err != nil {
		t.Fatal(err)
	}
	s := spec("experiment")
	s.Environment.Name = "build"
	victim := submit(t, q, s)
	for i := 0; i < 4; i++ {
		reply := check(t, q, m, nil)
		if len(reply.EnvironmentPlan) != 1 {
			t.Fatal("build never resumed", reply)
		}
		launch := *reply.EnvironmentPlan[0].Launch
		if err := q.AuthorizeLaunch(launch); err != nil {
			t.Fatal(err)
		}
		m.EnvironmentReports = []EnvironmentReport{{Key: launch.Key, Attempt: launch.Attempt, State: "building"}}
		check(t, q, m, nil)
		d := spec("experiment")
		d.Project = "urgent"
		demand := submit(t, q, d)
		reply = check(t, q, m, nil)
		if i >= 2 {
			if reply.PreparationStopAt != nil || reply.Job != nil || len(reply.EnvironmentPlan) != 1 || reply.EnvironmentPlan[0].Launch.Attempt != launch.Attempt {
				t.Fatal("resumed build got no runtime", reply)
			}
			clock.Advance(10 * time.Second)
			reply = check(t, q, m, nil)
		}
		if reply.PreparationStopAt == nil || len(reply.EnvironmentPlan) != 0 {
			t.Fatal("build floor became a reservation", reply)
		}
		m.EnvironmentReports[0].State = "paused"
		reply = check(t, q, m, nil)
		if reply.Job == nil || reply.Job.ID != demand.ID {
			t.Fatal("no higher tier progress", reply)
		}
		finish(t, q, reply.Job, true)
		if got := job(t, q, victim.ID); got.State != "queued" || got.InfrastructureRetries != 0 || got.Losses != 0 {
			t.Fatal("build lost", got)
		}
	}
}

func TestPriorityPreemptionEngineHysteresis(t *testing.T) {
	for _, kind := range []string{"load", "pin", "pin-host-hold"} {
		t.Run(kind, func(t *testing.T) {
			pin := kind != "load"
			q, clock, m := modelFixture(t)
			preemptionPolicy(t, q)
			if kind == "pin-host-hold" {
				if err := q.ConfigureHosts(map[string]HostPolicy{hostID(&m): {ProductionHold: true}}); err != nil {
					t.Fatal(err)
				}
			}
			if pin {
				if err := q.Pin(ModelPin{Project: "test", Model: "test", Machine: m.ID}); err != nil {
					t.Fatal(err)
				}
			} else {
				addModelCall(t, q, "experiment")
			}
			for i := 0; i < 4; i++ {
				check(t, q, m, nil)
				slot := q.s.ModelSlots[slotKey(m.ID, "test")]
				if slot == nil || slot.State != "loading" {
					t.Fatal("engine never resumed", slot)
				}
				d := spec("experiment")
				d.Project = "urgent"
				if kind == "pin-host-hold" {
					d.Priority = "production"
				}
				demand := submit(t, q, d)
				if i >= 2 {
					if slot.PriorityStop != nil {
						t.Fatal("resumed engine got no runtime", slot)
					}
					clock.Advance(10 * time.Second)
					reportModel(t, q, m, "loading")
				}
				if slot.PriorityStop == nil {
					t.Fatal("engine floor became a reservation", slot)
				}
				reply := recoveryReport(t, q, m, slot, "unloaded")
				if reply.Job == nil || reply.Job.ID != demand.ID {
					t.Fatal("no higher tier progress", reply)
				}
				finish(t, q, reply.Job, true)
			}
		})
	}
}

func TestPriorityPreemptionInheritsSharedBuild(t *testing.T) {
	q, _, m := modelFixture(t)
	preemptionPolicy(t, q)
	m.EnvironmentBuilder = true
	if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"build": {"fake": binaryRecipe(t, "fake")}}}); err != nil {
		t.Fatal(err)
	}
	s := spec("experiment")
	s.Environment.Name = "build"
	submit(t, q, s)
	reply := check(t, q, m, nil)
	if len(reply.EnvironmentPlan) != 1 {
		t.Fatal("no build")
	}
	launch := *reply.EnvironmentPlan[0].Launch
	if err := q.AuthorizeLaunch(launch); err != nil {
		t.Fatal(err)
	}
	m.EnvironmentReports = []EnvironmentReport{{Key: launch.Key, Attempt: launch.Attempt, State: "building"}}
	check(t, q, m, nil)
	s.Project = "urgent"
	demand := submit(t, q, s)
	reply = check(t, q, m, nil)
	l := q.s.PreparationPlans[launch.Attempt]
	if l.PriorityStop != nil || l.Spec == nil || l.Spec.Project != "urgent" || l.Demand != demand.ID || reply.Job != nil || len(reply.EnvironmentPlan) != 1 || reply.EnvironmentPlan[0].Launch.Attempt != launch.Attempt {
		t.Fatal("shared useful build was interrupted", reply, l)
	}
}

func TestPriorityPreemptionRetainsEngineCleanup(t *testing.T) {
	q, _, m := modelFixture(t)
	preemptionPolicy(t, q)
	addModelCall(t, q, "experiment")
	slot := q.s.ModelSlots[slotKey(m.ID, "test")]
	d := spec("experiment")
	d.Project = "urgent"
	demand := submit(t, q, d)
	if slot.State != "unloading" {
		t.Fatal("load stop not planned", slot)
	}
	for i := 0; i < 3; i++ {
		if err := q.Tick(); err != nil {
			t.Fatal(err)
		}
		if slot.State != "unloading" {
			t.Fatal("cleanup rewritten", slot)
		}
	}
	r := recoveryReport(t, q, m, slot, "unloaded")
	if r.Job == nil || r.Job.ID != demand.ID {
		t.Fatal("retained cleanup did not release", r)
	}
}
