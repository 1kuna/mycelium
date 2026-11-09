package myce

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestSharedHostDevPressureCheckpointRequeueAndCancel(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		oldPrepare := workerCheckpointPrepare
		workerCheckpointPrepare = prepareCheckpoint
		t.Cleanup(func() { workerCheckpointPrepare = oldPrepare })
		oldRSS := workerMemoryRSS
		workerMemoryRSS = func(context.Context) (float64, error) { return 0, nil }
		t.Cleanup(func() { workerMemoryRSS = oldRSS })
		var pressure atomic.Pointer[SharedHostPressure]
		pressure.Store(&SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 1})
		root := t.TempDir()
		d, err := StartDev(DevConfig{EnvironmentCacheLimit: 1 << 30, Address: "127.0.0.1:0", Database: filepath.Join(root, "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"), NASCheckpoints: true, Token: "dev-fixture", GPU: "cpu", Workers: 1, Interval: 20 * time.Millisecond, SharedPressureSource: func(context.Context) (SharedHostPressure, error) { return *pressure.Load(), nil }})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		})
		c := d.Client()
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		path := "/api/hosts/dev-host/maintenance"
		var status HostMaintenanceStatus
		readStatus := func() {
			t.Helper()
			if err := c.Call(ctx, "GET", path, nil, &status); err != nil {
				t.Fatal(err)
			}
		}
		await(t, func() bool {
			readStatus()
			return len(status.Workers) == 1 && status.Workers[0].Fresh && status.ExternalMemory != nil
		})
		change := func(r HostMaintenanceRequest) {
			t.Helper()
			if err := c.Call(ctx, "POST", path, r, &status); err != nil {
				t.Fatal(err)
			}
		}
		change(HostMaintenanceRequest{Action: "drain"})
		await(t, func() bool { readStatus(); return status.Quiescent })
		cores, ram := 2, 2.0
		change(HostMaintenanceRequest{Action: "budget", CPUBudgetCores: &cores, MemoryBudgetGB: &ram})
		change(HostMaintenanceRequest{Action: "profile", SharedHost: &SharedHostProfile{InteractiveReserveGB: .125}})
		change(HostMaintenanceRequest{Action: "resume"})
		s := cpuSpec(1, .01)
		s.Resumable = true
		s.SubmissionKey = "shared-pressure-resume"
		s.Run.Command = []string{"sh", "-c", `test "$GOMAXPROCS" = 1 || exit 40
if test "$MYCE_ATTEMPT" -gt 1; then
  grep -q '"value":42' "$MYCE_CHECKPOINT_DIR/checkpoint.json" || exit 41
  echo checkpoint-resumed
fi
trap 'printf '\''{"myce_checkpoint":{"generation":1},"value":42}'\'' > "$MYCE_CHECKPOINT_DIR/checkpoint.json"; exit 0' TERM
echo payload-ready
while :; do sleep .02; done`}
		var j Job
		if err := c.Call(ctx, "POST", "/api/jobs", s, &j); err != nil {
			t.Fatal(err)
		}
		id := j.ID
		readJob := func() {
			t.Helper()
			if err := c.Call(ctx, "GET", "/api/jobs/"+id, nil, &j); err != nil {
				t.Fatal(err)
			}
			if j.State == "failed" {
				t.Fatalf("dev payload failed: %+v", j)
			}
		}
		hasLog := func(want string) bool {
			var logs []Log
			if err := c.Call(ctx, "GET", "/api/jobs/"+id+"/logs", nil, &logs); err != nil {
				t.Fatal(err)
			}
			for _, l := range logs {
				if strings.Contains(l.Text, want) {
					return true
				}
			}
			return false
		}
		await(t, func() bool { readJob(); return j.StartedAt != nil && hasLog("payload-ready") })
		// Advance only the dev clock to refresh the injected sensor immediately.
		setPressure := func(p SharedHostPressure) {
			t.Helper()
			pressure.Store(&p)
			if err := c.Call(ctx, "POST", "/api/dev/clock/advance", map[string]float64{"seconds": signalRefresh.Seconds() + .01}, nil); err != nil {
				t.Fatal(err)
			}
		}
		setPressure(SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 2})
		await(t, func() bool { readJob(); return j.State == "queued" })
		readStatus()
		if status.Workers[0].Machine.SharedHostYieldReason != "shared host memory pressure" || j.InfrastructureRetries != 0 || j.Losses != 0 || j.Spec.SubmissionKey != s.SubmissionKey {
			t.Fatal("pressure stop lost provenance or logical work", status, j)
		}
		setPressure(SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 1})
		await(t, func() bool { readJob(); return j.Attempt == 2 && hasLog("checkpoint-resumed") })
		if err := c.Call(ctx, "POST", "/api/jobs/"+id+"/cancel", nil, nil); err != nil {
			t.Fatal(err)
		}
		await(t, func() bool { readJob(); return j.State == "cancelled" && j.ReleaseConfirmed })
		if j.InfrastructureRetries != 0 || j.Losses != 0 {
			t.Fatal("planned yield/cancel charged infrastructure", j)
		}

	})

	t.Run("SharedHostWorkerAdmitsAdvertisedEnvironment", func(t *testing.T) {
		h := &machineHarness{controls: map[string]AttemptDirective{}}
		w := harnessWorker(t, h)
		w.Machine.SharedHost = &SharedHostProfile{InteractiveReserveGB: .125}
		w.sharedCPUDev = true
		w.SharedPressureSource = func(context.Context) (SharedHostPressure, error) {
			return SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 1}, nil
		}
		w.EnvironmentRoot, w.EnvironmentLimit = t.TempDir(), 1<<20
		r := binaryRecipe(t, "cpu")
		cache := EnvironmentCache{Root: w.EnvironmentRoot, Limit: w.EnvironmentLimit, Clock: RealClock{}}
		_, release, err := cache.Acquire(context.Background(), r)
		if err != nil {
			t.Fatal(err)
		}
		release()
		j := harnessJob(t, "prepared-cpu", "", "exit 0")
		j.Spec.Environment.Name = "tools"
		j.EnvironmentRecipes = map[string]EnvironmentRecipe{"cpu": r}
		j.Spec.Run.Command = []string{"gpu-task"}
		h.jobs = []Job{j}
		startHarness(t, w, h)
		await(t, func() bool { return h.completed(j.ID) })
		h.mu.Lock()
		advertised := slices.ContainsFunc(h.reports, func(c Checkin) bool {
			return slices.Contains(c.Machine.Environments, r.Key)
		})
		h.mu.Unlock()
		if !advertised {
			t.Fatal("prepared cache entry was not advertised")
		}
		if result := h.result(j.ID); !result.Success {
			t.Fatalf("advertised prepared environment refused locally: %+v", result)
		}

	})

	t.Run("SharedHostPressureYieldHysteresisAndExactRelease", func(t *testing.T) {
		for _, sensor := range []string{"darwin memory", "linux PSI", "owner latency"} {
			t.Run(sensor, func(t *testing.T) {
				q, clock, m, run := sharedRunning(t, true)
				p := SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 1}
				switch sensor {
				case "darwin memory":
					p.MemoryLevel = 2
				case "linux PSI":
					p.MemoryPSI = sharedPSIHigh
				default:
					p.WakeDelayMS = sharedWakeHighMS
				}
				m.SharedHostPressure = &p
				clock.Advance(time.Second)
				r := check(t, q, m, run)
				j := job(t, q, run.ID)
				if j.State != "stopping" || priorityStop(j) == nil || priorityStop(j).Cause != sharedHostPressureCause || !r.Stop || r.Kill || r.GraceSeconds != q.preemptionGrace().Seconds() {
					t.Fatalf("missing planned grace: %+v %+v", j, r)
				}
				stopAt := *j.StopAt
				// A middling sample does not clear the latch or renew stop grace.
				p.MemoryLevel = 1
				p.MemoryPSI = (sharedPSIHigh + sharedPSILow) / 2
				p.WakeDelayMS = (sharedWakeHighMS + sharedWakeLowMS) / 2
				clock.Advance(time.Second)
				check(t, q, m, run)
				if j = job(t, q, run.ID); j.StopAt == nil || !j.StopAt.Equal(stopAt) || q.s.Machines[m.ID].SharedHostYieldReason == "" {
					t.Fatal("flapped or renewed grace")
				}
				clock.Advance(q.preemptionGrace())
				r = check(t, q, m, run)
				if !r.Kill || job(t, q, run.ID).State != "stopping" || q.s.Machines[m.ID].CPUInUseCores != 1 {
					t.Fatal("elapsed grace released custody", r)
				}
				finishInterrupted(t, q, run)
				j = job(t, q, run.ID)
				if j.State != "queued" || j.InfrastructureRetries != 0 || j.Losses != 0 || j.Spec.SubmissionKey != "owner-pressure-logical-job" {
					t.Fatal("yield changed logical work/accounting", j)
				}
				if next := check(t, q, m, nil).Job; next != nil {
					t.Fatal("readmitted before recovery")
				}
				p.MemoryPSI = sharedPSILow
				p.WakeDelayMS = sharedWakeLowMS
				r = check(t, q, m, nil)
				if sensor == "owner latency" {
					// The first latency interruption now requires affirmative quiet
					// observations covering this attempt's measured interference span.
					if r.Job != nil {
						t.Fatal("latency cooldown skipped")
					}
					for i := 0; i < 16; i++ {
						clock.Advance(time.Second)
						r = check(t, q, m, nil)
						if r.Job != nil {
							break
						}
					}
				}
				if r.Job == nil || r.Job.ID != run.ID || r.Job.Attempt <= run.Attempt {
					t.Fatal("fresh recovery did not requeue same job", r)
				}
				if err := q.Complete(result(run, true)); !errors.Is(err, ErrStale) {
					t.Fatal("stale attempt accepted", err)
				}
			})
		}

	})

	t.Run("SharedHostPressureUnknownAndStale", func(t *testing.T) {
		q, clock, m, run := sharedRunning(t, false)
		for _, p := range []*SharedHostPressure{nil, {}, {MemoryKnown: true, WakeKnown: true, MemoryLevel: 1, AgeSeconds: signalMaxAge.Seconds()}, {MemoryKnown: true, WakeKnown: true, MemoryLevel: 1, MemoryPSI: -1}} {
			m.SharedHostPressure = p
			check(t, q, m, run)
			if job(t, q, run.ID).State != "running" || q.s.Machines[m.ID].SharedHostReason == "" {
				t.Fatal("unknown became pressure or admitted work")
			}
		}
		m.SharedHostPressure = &SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 1}
		check(t, q, m, run)
		clock.Advance(signalMaxAge)
		q.mu.Lock()
		q.refreshHostMemory()
		reason := q.s.Machines[m.ID].SharedHostReason
		q.mu.Unlock()
		if reason == "" {
			t.Fatal("cached sample did not expire with injected clock")
		}
		// Queue-owned times and latches cannot be supplied by a worker.
		m.SharedHostPressure = &SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 1, ReceivedAt: clock.Now().Add(time.Hour)}
		m.SharedHostYieldReason = "forged pressure"
		check(t, q, m, run)
		if q.s.Machines[m.ID].SharedHostYieldReason != "" || !q.s.Machines[m.ID].SharedHostPressure.ReceivedAt.Equal(clock.Now()) {
			t.Fatal("trusted worker timestamp/latch")
		}
		// Stale evidence must not clear a real pressure latch.
		m.SharedHostPressure = &SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 4}
		check(t, q, m, run)
		m.SharedHostPressure = &SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 1, AgeSeconds: signalMaxAge.Seconds()}
		check(t, q, m, run)
		if q.s.Machines[m.ID].SharedHostYieldReason == "" {
			t.Fatal("stale recovery cleared latch")
		}

	})

	t.Run("SharedHostPressureFailureAndCancellationPrecedence", func(t *testing.T) {
		for _, ending := range []string{"independent failure", "cancellation", "completed before signal"} {
			t.Run(ending, func(t *testing.T) {
				q, _, m, run := sharedRunning(t, false)
				m.SharedHostPressure = &SharedHostPressure{MemoryKnown: true, WakeKnown: true, MemoryLevel: 2}
				r := check(t, q, m, run)
				if !r.Kill || r.GraceSeconds != 0 {
					t.Fatal("nonresumable work got checkpoint delay")
				}
				out := result(run, false)
				want := "failed"
				if ending == "cancellation" {
					if err := q.Cancel(run.ID); err != nil {
						t.Fatal(err)
					}
					out.Interrupted = true
					want = "cancelled"
				} else if ending == "completed before signal" {
					out.Success = true
					want = "succeeded"
				}
				if err := q.Complete(out); err != nil {
					t.Fatal(err)
				}
				requireState(t, q, run.ID, want)
			})
		}

	})

	t.Run("SharedHostPressureRetiredWorkerDoesNotStopReplacement", func(t *testing.T) {
		q, _, m, run := sharedRunning(t, true)
		q.mu.Lock()
		retired := *q.s.Machines[m.ID]
		retired.ID = "retired-worker"
		retired.Gone = true
		retired.SharedHostYieldReason = "shared host memory pressure"
		q.s.Machines[retired.ID] = &retired
		q.mu.Unlock()
		r := check(t, q, m, run)
		if r.Stop || r.Kill || job(t, q, run.ID).State != "running" {
			t.Fatal("retired pressure stopped healthy replacement", r)
		}

	})

	t.Run("SharedHostRequiresPreparedSidecarEnvironment", func(t *testing.T) {
		m := cpuMachine("worker", "physical", "cpu", 4, 16)
		m.SharedHost = &SharedHostProfile{InteractiveReserveGB: 4}
		side := NativeSidecar{Name: "helper", Environment: "tools", Command: []string{"sh", "-c", "exit 0"}}
		r := binaryRecipe(t, "cpu")
		j := Job{Spec: cpuSpec(1, 1), SidecarEnvironmentRecipes: map[string]map[string]EnvironmentRecipe{"helper": {"cpu": r}}}
		j.Spec.Run.Sidecars = []NativeSidecar{side}
		if err := normalizeResources(&j.Spec); err != nil {
			t.Fatal(err)
		}
		if sharedHostJobFits(&j, &m) {
			t.Error("cold sidecar environment admitted on shared host")
		}
		m.Environments = []string{r.Key}
		if !sharedHostJobFits(&j, &m) {
			t.Fatal("prepared sidecar environment refused")
		}
		// The advertised ready entry can disappear before launch. Do not rebuild
		// with a batch authorization: preparations are outside phase one.
		builds := 0
		w := Worker{Machine: m, environmentCache: &EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20, Run: func(context.Context, []string, string, []string, io.Writer, io.Writer) error {
			builds++
			return errors.New("unexpected environment build")
		}}}
		g := nativeGroup{worker: w, ctx: t.Context()}
		_, release, err := g.sidecarEnvironment(j, side, "")
		if release != nil {
			release()
		}
		if err == nil || !strings.Contains(err.Error(), "environment preparation required") || builds != 0 {
			t.Fatalf("missing prepared sidecar rebuilt: builds=%d, err=%v", builds, err)
		}

	})

	t.Run("SharedHostDoesNotRebuildMainEnvironment", func(t *testing.T) {
		q, clock := fixture(t)
		m := sharedHostFixture(t, q)
		m.SharedHost = &SharedHostProfile{InteractiveReserveGB: 4}
		r := binaryRecipe(t, "cpu")
		m.Environments = []string{r.Key} // Readiness reported before the cache disappeared.
		w := Worker{Machine: m, Clock: clock, StateDir: t.TempDir(), HostMemorySource: func(context.Context) (float64, error) { return 12, nil }, environmentCache: &EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20, Clock: clock}}
		store, err := w.openStore()
		if err != nil {
			t.Fatal(err)
		}
		defer store.close()
		j := Job{ID: "cold-main", Attempt: 1, Spec: cpuSpec(1, 1), EnvironmentRecipes: map[string]EnvironmentRecipe{"cpu": r}}
		if err := normalizeResources(&j.Spec); err != nil {
			t.Fatal(err)
		}
		p, err := startAfterFixtureInputs(t, w, store, j)
		if p != nil {
			p.stop(clock.Now(), 0, true)
			<-p.done
		}
		if err == nil || !strings.Contains(err.Error(), "environment preparation required") || w.environmentCache.ready(r.Key) {
			t.Fatalf("missing prepared main environment rebuilt: %v", err)
		}

	})
}
