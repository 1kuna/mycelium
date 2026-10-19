package myce

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// A partition does not release a known engine. A same-session worker can
// resume its exact live engine without inventing a replacement attempt.
func TestEngineRecoveryRejoinSameSessionLiveEngine(t *testing.T) {
	for _, pinned := range []bool{false, true} {
		t.Run(fmt.Sprintf("pinned=%v", pinned), func(t *testing.T) {
			q, clock := fixture(t)
			model := serverTestModel(t, "ready")
			weights := []byte("synthetic immutable weights")
			model.Path = filepath.Join(t.TempDir(), "weights")
			if err := os.WriteFile(model.Path, weights, 0600); err != nil {
				t.Fatal(err)
			}
			model.SizeBytes = int64(len(weights))
			model.SHA256 = fmt.Sprintf("%x", sha256.Sum256(weights))
			model.Device = ""
			model.DeviceEnv = nil
			config := ModelConfig{Models: []Model{model}, Idle: time.Hour, Wait: time.Hour}
			if err := q.ConfigureModels(config); err != nil {
				t.Fatal(err)
			}
			m := machine("engine-worker", "fake")
			m.ModelConfigHash = modelConfigHash(config)
			rt, err := startModels(Worker{Machine: m, ModelConfig: config, Clock: RealClock{}, Client: Client{Token: "test"}, MemorySource: func(context.Context) (float64, error) { return 32, nil }}, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := rt.close(); err != nil {
					t.Error(err)
				}
			}()
			m.ModelURL = rt.url
			other := machine("other", "fake")
			other.ModelURL = ""
			other.ModelConfigHash = m.ModelConfigHash
			heartbeat := func() Reply {
				t.Helper()
				reports, _ := rt.report()
				reply, err := q.Checkin(Checkin{Machine: m, ModelReports: reports})
				if err != nil {
					t.Fatal(err)
				}
				rt.sync(reply.ModelPlan)
				return reply
			}
			heartbeat()
			check(t, q, other, nil)
			if pinned {
				if err := q.Pin(ModelPin{Model: model.Name, Project: "test", Machine: m.ID}); err != nil {
					t.Fatal(err)
				}
			}
			call := &ModelCall{Model: model.Name, Project: "test", Priority: "experiment"}
			if err := q.addCall(call); err != nil {
				t.Fatal(err)
			}
			heartbeat()
			await(t, func() bool { r, _ := rt.report(); return len(r) == 1 && r[0].State == "ready" })
			heartbeat()
			if call.Machine != m.ID {
				t.Fatal("not dispatched")
			}
			if err := q.finishCall(call.ID); err != nil {
				t.Fatal(err)
			}
			oldAttempt := q.s.ModelSlots[slotKey(m.ID, model.Name)].Attempt
			// Partition: only "other" checks in past the loss timeout.
			if err := q.SetLossTimeouts(5*time.Second, 5*time.Second, 5*time.Second); err != nil {
				t.Fatal(err)
			}
			clock.Advance(5 * time.Second)
			check(t, q, other, nil)
			if !q.s.Machines[m.ID].Gone {
				t.Fatal("not gone")
			}
			if q.s.ModelSlots[slotKey(m.ID, model.Name)] == nil {
				t.Fatal("loss discarded exact engine custody")
			}
			second := &ModelCall{Model: model.Name, Project: "test", Priority: "experiment"}
			if err := q.addCall(second); err != nil {
				t.Fatal(err)
			}
			// Rejoin with same session; engine still alive; call already queued.
			heartbeat()
			await(t, func() bool { heartbeat(); return second.Machine != "" })
			s := q.s.ModelSlots[slotKey(m.ID, model.Name)]
			reports, _ := rt.report()
			if s.Attempt != oldAttempt || reports[0].Attempt != s.Attempt {
				t.Fatal("same-session rejoin replaced a retained engine identity")
			}
			if second.Machine == "" {
				t.Fatalf("call never dispatched; slot state %s attempt %s, worker reports %+v", s.State, s.Attempt, reports)
			}
			if err := q.finishCall(second.ID); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Each failed warm-up must fail its own queued calls, regardless of history.
func TestEngineRecoveryRepeatedInitialLoadFailure(t *testing.T) {
	q, clock, m := modelFixture(t)
	q.models.Wait = time.Hour
	if err := q.Pin(ModelPin{Model: "test", Project: "test", Machine: m.ID}); err != nil {
		t.Fatal(err)
	}
	s := q.s.ModelSlots[slotKey(m.ID, "test")]
	first := addModelCall(t, q, "experiment")
	if _, err := q.Checkin(Checkin{Machine: m, ModelReports: []ModelReport{{Name: "test", Attempt: s.Attempt, State: "failed", Error: "verification failed"}}}); err != nil {
		t.Fatal(err)
	}
	if first.Error == "" {
		t.Fatal("first call not failed")
	}
	clock.Advance(time.Second)
	check(t, q, m, nil)
	if s.State != "loading" {
		t.Fatal("pin did not retry warm-up")
	}
	second := addModelCall(t, q, "experiment")
	if _, err := q.Checkin(Checkin{Machine: m, ModelReports: []ModelReport{{Name: "test", Attempt: s.Attempt, State: "failed", Error: "verification failed"}}}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		check(t, q, m, nil)
	}
	if second.Error == "" {
		t.Fatalf("second call to persistently-failing pinned model not failed promptly; slot state=%s crash=%d", s.State, s.CrashCount)
	}
}
