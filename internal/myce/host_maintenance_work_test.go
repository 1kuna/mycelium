package myce

import (
	"context"
	"testing"
	"time"
)

// Every host work kind crosses the same maintenance and wait boundary. Sidecars
// share their parent execution's exact release, including pending Docker creates.
func TestMaintenanceEveryWorkKind(t *testing.T) {
	for _, kind := range []string{"batch-production", "batch-experiment", "model-production", "model-experiment", "engine", "engine-load", "engine-failed-pinned", "environment-build", "environment-plan-pending", "preparation", "benchmark", "sidecar", "native-sidecar"} {
		for _, action := range []struct {
			name, action string
			stop         bool
		}{{"drain", "drain", false}, {"disable", "disable", false}, {"disable-stop", "disable", true}} {
			t.Run(kind+"/"+action.name, func(t *testing.T) {
				q, clock, m := modelFixture(t, "host")
				q.models.Wait = time.Hour // isolate maintenance from the ordinary load timeout
				m.ModelConfigHash = modelConfigHash(q.models)
				m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 32
				m.EnvironmentBuilder = true
				if err := q.ConfigureEnvironments(EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"pending": {"fake": binaryRecipe(t, "fake")}}}); err != nil {
					t.Fatal(err)
				}
				check(t, q, m, nil)
				if err := q.SetLossTimeouts(time.Hour, time.Hour, time.Hour); err != nil {
					t.Fatal(err)
				}
				var batch *Job
				var call *ModelCall
				var slot *ModelSlot
				var benchmark *HostLaunch
				switch kind {
				case "benchmark":
					m.BenchmarkPending = true
					benchmark = check(t, q, m, nil).BenchmarkPlan
					if benchmark == nil {
						t.Fatal("no benchmark plan")
					}
					if err := q.AuthorizeLaunch(*benchmark); err != nil {
						t.Fatal(err)
					}
				case "batch-production", "batch-experiment", "sidecar", "native-sidecar":
					priority := "experiment"
					if kind == "batch-production" {
						priority = "production"
					}
					s := spec(priority)
					s.Needs.HostMemoryGB = 2
					if kind == "sidecar" {
						m.Capabilities = []string{"docker"}
						m.CapabilityProfile.OS = "linux"
						s = dockerGroupJob(t).Spec
						s.Priority = priority
					}
					if kind == "native-sidecar" {
						s.Run.Sidecars = []NativeSidecar{{Name: "helper", Command: []string{"helper"}, MemoryGB: 1}}
					}
					submit(t, q, s)
					batch = check(t, q, m, nil).Job
					if batch == nil {
						t.Fatal("no batch assignment")
					}
					if err := q.Started(result(batch, false)); err != nil {
						t.Fatal(err)
					}
				case "model-production", "model-experiment", "engine", "engine-load":
					priority := "experiment"
					if kind == "model-production" {
						priority = "production"
					}
					call = addModelCall(t, q, priority)
					slot = q.s.ModelSlots[slotKey(m.ID, "test")]
					if kind != "engine-load" {
						reportModel(t, q, m, "ready")
					}
					if kind == "engine" || kind == "engine-load" {
						if err := q.finishCall(call.ID); err != nil {
							t.Fatal(err)
						}
						call = nil
					}
				case "engine-failed-pinned":
					if err := q.Pin(ModelPin{Model: "test", Project: "test", Machine: m.ID}); err != nil {
						t.Fatal(err)
					}
					slot = q.s.ModelSlots[slotKey(m.ID, "test")]
					recoveryReport(t, q, m, slot, "ready")
					recoveryReport(t, q, m, slot, "failed")
				case "environment-plan-pending":
					env := spec("experiment")
					env.Environment.Name = "pending"
					submit(t, q, env)
					if len(check(t, q, m, nil).EnvironmentPlan) != 1 {
						t.Fatal("no pending plan")
					}
					if len(q.hostObligations("host")) == 0 {
						t.Fatal("pending plan not enumerated")
					}
				case "environment-build":
					m.EnvironmentReports = []EnvironmentReport{{Key: "build", State: "building"}}
					check(t, q, m, nil)
				case "preparation":
					m.CopyReports = []CopyReport{{Model: "test", State: "copying"}}
					check(t, q, m, nil)
				}
				pulse := func() Reply {
					t.Helper()
					request := Checkin{Machine: m}
					if batch != nil {
						e := result(batch, false).Execution
						request.Running = &e
					}
					if slot != nil {
						state := "ready"
						if kind == "engine-load" {
							state = "loading"
						}
						if kind == "engine-failed-pinned" {
							state = "failed"
						}
						request.ModelReports = []ModelReport{{Name: slot.Model, Attempt: slot.Attempt, State: state, RuntimeFailed: state == "failed", CallsMeasured: state == "failed"}}
					}
					r, err := q.Checkin(request)
					if err != nil {
						t.Fatal(err)
					}
					return r
				}
				status := maintenanceChange(t, q, action.action, action.stop)
				if kind == "environment-plan-pending" {
					if !status.Quiescent || status.Preparations != 0 {
						t.Fatal("unstarted plan not cancelled", status)
					}
					if reply := pulse(); !reply.AdmissionBlocked || len(reply.EnvironmentPlan) != 0 {
						t.Fatal("maintenance restarted pending plan", reply)
					}
					return
				}
				if kind == "engine-failed-pinned" {
					attempt := slot.Attempt
					if !status.Quiescent || status.Sidecars != 0 || len(q.hostObligations("host")) != 0 {
						t.Fatal("cleaned pin counted as work", status)
					}
					clock.Advance(2 * StopGrace)
					reply := pulse()
					if !reply.AdmissionBlocked || slot.State != "failed" || slot.Attempt != attempt {
						t.Fatal("pin restarted during maintenance", reply)
					}
					if len(q.s.Pins) != 1 || q.s.Pins[0].Assigned != m.ID {
						t.Fatal("maintenance removed pin intent", q.s.Pins)
					}
					maintenanceChange(t, q, "resume", false)
					if slot.State != "loading" || slot.Attempt == attempt {
						t.Fatal("resume did not restart pin", slot)
					}
					return
				}
				if status.Quiescent || len(q.hostObligations("host")) == 0 {
					t.Fatal("wait ignored active work", status)
				}
				deadline := clock.Now().Add(StopGrace)
				if action.stop && (status.Policy.StopAt == nil || !status.Policy.StopAt.Equal(deadline)) {
					t.Fatal("missing shared deadline", status.Policy)
				}
				// Offer real backlog for every admission channel. Queued demand is
				// deliberately still present when the final wait becomes quiescent.
				submit(t, q, spec("experiment"))
				envJob := spec("experiment")
				envJob.Environment.Name = "pending"
				submit(t, q, envJob)
				if err := q.SyncModel(SyncRequest{Model: "test", To: m.ID}); err != nil {
					t.Fatal(err)
				}
				reply := pulse()
				if !reply.AdmissionBlocked || reply.Job != nil || len(reply.CopyPlan) != 0 || len(reply.EnvironmentPlan) != 0 {
					t.Fatal("new work admitted", reply)
				}
				waiting := addModelCall(t, q, "experiment")
				if waiting.Machine != "" {
					t.Fatal("new call admitted")
				}
				if err := q.finishCall(waiting.ID); err != nil {
					t.Fatal(err)
				}
				// Worker channels preserve running preparations on drain/disable, and
				// cancel them on --stop. No missing plan may silently restart one.
				if kind == "environment-build" || kind == "preparation" {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					keep := (reply.HostMaintenance == "drained" || reply.HostMaintenance == "disabled") && reply.PreparationStopAt == nil
					if kind == "environment-build" {
						rt := &environmentRuntime{key: "build", cancel: cancel}
						rt.sync(reply.EnvironmentPlan, false, keep)
					} else {
						rt := &modelRuntime{copies: map[string]*workerCopy{"test": {cancel: cancel}}}
						rt.syncCopies(reply.CopyPlan, reply.CacheUsage, reply.CachePins, false, keep)
					}
					if (ctx.Err() != nil) != action.stop {
						t.Fatal("incorrect preparation stop channel", ctx.Err())
					}
					if action.stop && (reply.PreparationStopAt == nil || reply.PreparationStopAt.After(deadline)) {
						t.Fatal("unbounded preparation stop", reply)
					}
				}
				clock.Advance(StopGrace - time.Nanosecond)
				reply = pulse()
				if call != nil && call.Error != "" {
					t.Fatal("call ended before grace", call.Error)
				}
				if batch != nil && !action.stop && reply.Stop {
					t.Fatal("drain/disable interrupted authorized batch")
				}
				if slot != nil && kind == "engine-load" && !action.stop && slot.State != "loading" {
					t.Fatal("drain interrupted load", slot)
				}
				if action.stop {
					// Repeated requests cannot extend the shared deadline.
					repeated := maintenanceChange(t, q, "disable", true)
					if repeated.Policy.StopAt == nil || !repeated.Policy.StopAt.Equal(deadline) {
						t.Fatal("deadline extended")
					}
				}
				clock.Advance(time.Nanosecond)
				reply = pulse()
				if action.stop {
					if batch != nil && (!reply.Stop || !reply.Kill) {
						t.Fatal("batch/sidecar stop not bounded", reply)
					}
					if call != nil && (call.StopAt == nil || call.StopAt.After(deadline) || call.Error == "" || q.callAttemptLive(call)) {
						t.Fatal("call stop not bounded", call)
					}
					if slot != nil && (slot.StopAt == nil || slot.StopAt.After(deadline) || slot.State != "unloading") {
						t.Fatal("engine stop not bounded", slot)
					}
				} else {
					if batch != nil && reply.Stop {
						t.Fatal("active batch interrupted")
					}
					if call != nil && (call.StopAt != nil || call.Error != "" || !q.callAttemptLive(call)) {
						t.Fatal("active call interrupted", call)
					}
				}
				status, err := q.HostMaintenanceStatus("host")
				if err != nil || status.Quiescent {
					t.Fatal("deadline claimed physical release", status, err)
				}
				// Wrong execution evidence and sibling checkins cannot settle custody.
				if slot != nil {
					if _, err := q.Checkin(Checkin{Machine: m, ModelReports: []ModelReport{{Name: slot.Model, Attempt: "wrong-attempt", State: "unloaded", CallsMeasured: true}}}); err != nil {
						t.Fatal(err)
					}
				}
				if batch != nil {
					wrong := result(batch, false)
					wrong.ReleaseOnly = true
					wrong.Attempt++
					if err := q.Complete(wrong); err != nil {
						t.Fatal(err)
					}
				}
				if check(t, q, machine("sibling", "fake", "host"), nil).Job != nil {
					t.Fatal("sibling admitted")
				}
				status, err = q.HostMaintenanceStatus("host")
				if err != nil || status.Quiescent {
					t.Fatal("inexact evidence discharged work", status, err)
				}
				if call != nil {
					if err := q.finishCall(call.ID); err != nil {
						t.Fatal(err)
					}
				}
				if slot != nil {
					if kind == "engine-load" && !action.stop {
						reportModel(t, q, m, "ready")
					}
					r, err := q.Checkin(Checkin{Machine: m, ModelReports: []ModelReport{{Name: slot.Model, Attempt: slot.Attempt, State: "unloaded", CallsMeasured: true}}})
					if err != nil || len(r.ModelPlan) != 0 {
						t.Fatal("exact engine cleanup did not settle", r, err)
					}
				}
				if batch != nil {
					r := result(batch, !action.stop)
					r.Interrupted = action.stop
					r.ReleaseOnly = job(t, q, batch.ID).Machine != batch.Machine
					r.ReleasePending = kind == "sidecar"
					if err := q.Complete(r); err != nil {
						t.Fatal(err)
					}
					if kind == "sidecar" {
						status, err = q.HostMaintenanceStatus("host")
						if err != nil || status.Quiescent {
							t.Fatal("pending sidecar cleanup settled", status, err)
						}
						r.ReleasePending, r.ReleaseOnly = false, true
						if err := q.Complete(r); err != nil {
							t.Fatal(err)
						}
					}
				}
				m.EnvironmentReports, m.CopyReports = nil, nil
				if benchmark != nil {
					m.BenchmarkPending = false
					if _, err := q.Checkin(Checkin{Machine: m, BenchmarkDone: benchmark.Attempt}); err != nil {
						t.Fatal(err)
					}
				}
				check(t, q, m, nil)
				status, err = q.HostMaintenanceStatus("host")
				if err != nil || !status.Quiescent || len(q.hostObligations("host")) != 0 {
					t.Fatal("exact completion did not settle wait", status, err)
				}
			})
		}
	}
}

func TestMaintenanceEngineForcesUnloadAtDeadline(t *testing.T) {
	for _, inflight := range []int{0, 1} {
		engine := &lifecycleEngine{}
		clock := &fakeClock{now: time.Unix(1000, 0)}
		deadline := clock.Now().Add(StopGrace)
		model := &workerModel{model: Model{Name: "test"}, engine: engine, attempt: "exact", state: "ready", inflight: inflight}
		rt := &modelRuntime{clock: clock, models: map[string]*workerModel{"test": model}, session: "session"}
		plan := []ModelSlot{{Model: "test", Attempt: "exact", Session: "session", State: "draining", InFlight: 1, StopAt: &deadline}}
		rt.sync(plan)
		if engine.unloads.Load() != 0 {
			t.Fatal("unloaded before grace")
		}
		clock.Advance(StopGrace)
		plan[0].State = "unloading"
		rt.sync(plan)
		rt.wg.Wait()
		if engine.unloads.Load() != 1 || model.state != "unloaded" {
			t.Fatal("in-flight engine escaped bounded stop", inflight, model.state)
		}
		rt.sync(plan)
		rt.wg.Wait()
		if engine.unloads.Load() != 1 {
			t.Fatal("duplicate unload")
		}
	}
}

func TestMaintenanceStopDeadlineSurvivesRestart(t *testing.T) {
	q, clock, m := modelFixture(t, "host")
	if err := q.SetLossTimeouts(time.Hour, time.Hour, time.Hour); err != nil {
		t.Fatal(err)
	}
	call := addModelCall(t, q, "production")
	reportModel(t, q, m, "ready")
	original := maintenanceChange(t, q, "disable", true)
	slot := *q.s.ModelSlots[slotKey(m.ID, "test")]
	if call.StopAt == nil || !call.StopAt.Equal(*original.Policy.StopAt) {
		t.Fatal("call missed shared deadline")
	}
	path, root := "", q.root
	if err := q.db.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&path); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	var err error
	q, err = Open(path, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if err = q.ConfigureModels(ModelConfig{Idle: time.Hour, Wait: time.Hour, Models: []Model{{Name: "test", Engine: "fake", Path: "fake", MemoryGB: 8, Platforms: []string{"fake"}}}}); err != nil {
		t.Fatal(err)
	}
	m.ModelConfigHash = modelConfigHash(q.models)
	clock.Advance(StopGrace)
	reply := reportModel(t, q, m, "ready")
	if len(reply.ModelPlan) != 1 || reply.ModelPlan[0].Attempt != slot.Attempt || reply.ModelPlan[0].State != "unloading" || !reply.ModelPlan[0].StopAt.Equal(*original.Policy.StopAt) {
		t.Fatal("restart extended stop or lost exact custody", reply)
	}
	status, err := q.HostMaintenanceStatus("host")
	if err != nil || status.Quiescent {
		t.Fatal("restart certified unreleased engine", status, err)
	}
	if _, err = q.Checkin(Checkin{Machine: m, ModelReports: []ModelReport{{Name: slot.Model, Attempt: slot.Attempt, State: "unload_failed", CallsMeasured: true}}}); err != nil {
		t.Fatal(err)
	}
	status, err = q.HostMaintenanceStatus("host")
	if err != nil || status.Quiescent {
		t.Fatal("failed cleanup discharged engine", status, err)
	}
}
