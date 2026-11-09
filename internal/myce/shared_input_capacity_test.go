package myce

import (
	"context"
	"errors"
	"fmt"

	"strings"
	"sync"
	"testing"
	"time"
)

func cacheCapacityMachine(id string, capacity uint64) Machine {
	m := machine(id, "fake")
	m.Capabilities = append(m.Capabilities, "shared_inputs_v1", "extension_shared_inputs_v1")
	m.SharedInputCache = &SharedInputCapacity{CapacityBytes: capacity, FreeBytes: capacity}
	return m
}
func sharedCapacitySpec() Spec {
	s := spec("experiment")
	s.SharedInputs = map[string]SharedInput{
		"large": {NAS: "large", SHA256: strings.Repeat("a", 64), SizeBytes: 33 << 29},
		"small": {NAS: "small", SHA256: strings.Repeat("b", 64), SizeBytes: 34 * (1 << 30) / 10},
	}
	return s
}

func TestSharedInputCapacityPressure(t *testing.T) {
	q, clock := fixture(t)
	big := cacheCapacityMachine("big", 80<<30)
	big.SharedInputCache.FreeBytes = 1 << 30
	big.SharedInputCache.EvictableBytes = 2 << 30
	check(t, q, big, nil)
	j := submit(t, q, sharedCapacitySpec())
	if r := check(t, q, big, nil); r.Job != nil {
		t.Fatal("busy cache accepted job")
	}
	held := job(t, q, j.ID)
	if held.DiagnosticCode != "shared_input_cache" || !strings.Contains(held.Reason, "evictable=2147483648") {
		t.Fatalf("untyped hold: %+v", held)
	}
	clock.Advance(time.Second)
	big.SharedInputCache.EvictableBytes = 40 << 30
	if r := check(t, q, big, nil); r.Job == nil || r.Job.ID != j.ID {
		t.Fatal("evictable capacity did not recover")
	}

	t.Run("pending aliases", func(t *testing.T) {
		q, _ := fixture(t)
		m := cacheCapacityMachine("worker", 10<<30)
		check(t, q, m, nil)
		input := SharedInput{NAS: "first", SHA256: strings.Repeat("a", 64), SizeBytes: 6 << 30}
		alias := input
		alias.NAS = "other/path"
		s := sharedCapacitySpec()
		s.SharedInputs = map[string]SharedInput{"first": input, "alias": alias}
		first := submit(t, q, s)
		assigned := check(t, q, m, nil).Job
		if assigned == nil || assigned.ID != first.ID {
			t.Fatal("aliases were not placed")
		}
		s.SharedInputs = map[string]SharedInput{"second": {NAS: "second", SHA256: strings.Repeat("b", 64), SizeBytes: 6 << 30}}
		second := submit(t, q, s)
		if reply := check(t, q, m, assigned); reply.Job != nil || job(t, q, second.ID).State != "queued" || job(t, q, second.ID).DiagnosticCode != "shared_input_cache" {
			t.Fatal("pending assignment left its cache bytes available")
		}
	})
}

func TestSharedInputCapacityLegacyWorkerLargeInputs(t *testing.T) {
	for _, peers := range []int{0} {
		t.Run(fmt.Sprintf("hosts-%d", peers+1), func(t *testing.T) {
			q, clock := fixture(t)
			s := sharedCapacitySpec()
			// No report at startup still cannot prove permanent impossibility.
			if _, err := q.Resolve(ResolveRequest{Spec: s}); err != nil {
				t.Fatal("startup resolve refused", err)
			}
			j := submit(t, q, s)
			if got := job(t, q, j.ID); got.DiagnosticCode != "shared_input_cache_unreported" {
				t.Fatal("startup wait was untyped", got.Reason)
			}
			for i := range peers {
				small := cacheCapacityMachine(fmt.Sprintf("reporting-%03d", i), 10<<30)
				if reply := check(t, q, small, nil); reply.Job != nil {
					t.Fatal("undersized reporting worker accepted shared inputs")
				}
			}
			old := cacheCapacityMachine("legacy", 40<<30)
			old.SharedInputCache = nil // shared_inputs_v1 predates capacity reports.
			clock.Advance(time.Second)
			reply := check(t, q, old, nil)
			if reply.Job == nil || reply.Job.ID != j.ID {
				t.Fatal("large-input job stranded on legacy worker", job(t, q, j.ID).Reason)
			}
			if err := q.Started(result(reply.Job, true)); err != nil {
				t.Fatal("legacy worker could not start assigned job", err)
			}
			// Unknown cache capacity retains every other qualification check.
			s.RequireTarget = true
			if _, err := q.Resolve(ResolveRequest{Spec: s}); err != nil {
				t.Fatal("mixed rollout resolve refused", err)
			}
			second := submit(t, q, s)
			if got := job(t, q, second.ID); got.DiagnosticCode == "shared_input_cache_unreported" {
				t.Fatal("legacy capacity report still required", got.Reason)
			}
			s.Needs.MemoryGB = 1e6
			if _, err := q.Submit(s); err == nil {
				t.Fatal("legacy cache bypassed hardware qualification")
			}
		})
	}
}

func TestSharedInputCapacityRefusalSizesThroughClient(t *testing.T) {
	q, clock := fixture(t)
	m := cacheCapacityMachine("small", 10<<30)
	check(t, q, m, nil)
	client := Client{URL: testServerForEnvironments(t, q), Token: "test"}
	s := sharedCapacitySpec()
	s.SharedInputs = map[string]SharedInput{"archive": {NAS: "archive", SHA256: strings.Repeat("a", 64), SizeBytes: 3 << 30, UnpackedBytes: 8 << 30, Archive: "tar.gz"}}
	big := cacheCapacityMachine("big", 40<<30)
	check(t, q, big, nil)
	if _, err := q.ChangeHostMaintenance(hostID(&big), HostMaintenanceRequest{Action: "disable"}); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/api/jobs"} {
		var body any = s
		if path == "/api/resolve" {
			body = ResolveRequest{Spec: s}
		}
		err := client.Call(context.Background(), "POST", path, body, nil)
		var refusal *HTTPError
		if !errors.As(err, &refusal) || refusal.Status != 409 || refusal.Code != "shared_input_cache_capacity" || !strings.Contains(refusal.Message, fmt.Sprint(sharedInputRequired(s, nil))) || !strings.Contains(refusal.Message, "10737418240") {
			t.Fatalf("%s sizes lost: %v", path, err)
		}
	}

	if _, err := q.ChangeHostMaintenance(hostID(&big), HostMaintenanceRequest{Action: "resume"}); err != nil {
		t.Fatal(err)
	}
	var j Job
	if err := client.Call(context.Background(), "POST", "/api/jobs", s, &j); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	if r := check(t, q, m, nil); r.Job != nil {
		t.Fatal("placed on small cache", r.Job.ID)
	}
	if r := check(t, q, big, nil); r.Job == nil || r.Job.ID != j.ID {
		t.Fatal("did not route to fitting cache")
	}
}

func TestSharedInputCapacityDetachedCancellationRecovers(t *testing.T) {
	q, clock := fixture(t)
	cache := &SharedInputCache{cache: EnvironmentCache{Root: t.TempDir(), Limit: 10 << 20, Clock: clock}}
	cache.refreshCapacity()
	entered, unblock := make(chan struct{}), make(chan struct{})
	unblockWriter := sync.OnceFunc(func() { close(unblock) })
	t.Cleanup(unblockWriter)
	cache.beforeStage = func(context.Context) { close(entered); <-unblock }
	input := SharedInput{NAS: "input", SHA256: strings.Repeat("a", 64), SizeBytes: 6 << 20}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	nas := t.TempDir()
	result := make(chan error, 1)
	go func() {
		_, release, err := cache.Acquire(ctx, input, nas)
		if release != nil {
			release()
		}
		result <- err
	}()
	<-entered
	writer := onlySharedStaging(t, cache)
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	m := cacheCapacityMachine("worker", 10<<20)
	m.SharedInputCache = cache.capacityReport()
	check(t, q, m, nil)
	s := spec("experiment")
	s.SharedInputs = map[string]SharedInput{"data": input}
	j := submit(t, q, s)
	if reply := check(t, q, m, nil); reply.Job != nil {
		t.Fatal("outstanding writer reservation was ignored")
	}
	unblockWriter()
	<-writer.done
	// No new Acquire/restart: the detached writer's cleanup must publish space.
	m.SharedInputCache = cache.capacityReport()
	clock.Advance(time.Second)
	if reply := check(t, q, m, nil); reply.Job == nil || reply.Job.ID != j.ID {
		t.Fatalf("cleanup left queue blocked: %+v", m.SharedInputCache)
	}
}

func TestSharedInputCapacityUsesCPUExtensionVariant(t *testing.T) {
	q, _ := fixture(t)
	e := compositionExtension(t, false)
	cpu := e.Variants[0]
	cpu.Target = ExecutionTarget{OS: "linux", Arch: "amd64", Platform: "cpu"}
	cpu.Needs = ResourceNeeds{CPUCores: 1, MemoryGB: 1}
	gpu := cpu
	gpu.Target.Platform = "cuda"
	e.Variants = []ExtensionVariant{cpu, gpu}
	e = freezeComposition(t, e)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	big, small := cacheCapacityMachine("cpu", 40<<30), cacheCapacityMachine("gpu", 10<<30)
	big.GPU = "cpu"
	small.GPU = "cuda"
	big.HostMemoryMeasured, big.FreeHostMemoryGB, big.HostAvailableMemoryGB = true, 16, 16
	big.ExtensionFingerprint = "fixture"
	report := newExtensionReport(e, &big, "kind:sample")
	report.Setup, report.Qualified, report.CheckID = true, true, "fixture-check"
	big.ExtensionReports = []ExtensionReport{report}
	check(t, q, big, nil)
	check(t, q, small, nil)
	s := sharedCapacitySpec()
	s.Kind = "sample"
	s.Run = RunSpec{}
	s.Needs = ResourceNeeds{}
	resolved, err := q.Resolve(ResolveRequest{Spec: s})
	if err != nil {
		t.Fatal("fitting CPU variant refused at resolve", err)
	}
	var cpuEligible bool
	for _, target := range resolved.Targets {
		cpuEligible = cpuEligible || target.Machine == big.ID && target.Eligible
	}
	if !cpuEligible {
		t.Fatalf("CPU variant was not eligible: %+v", resolved.Targets)
	}
	j := submit(t, q, s)
	if reply := check(t, q, small, nil); reply.Job != nil {
		t.Fatal("placed extension on undersized GPU cache")
	}
	if reply := check(t, q, big, nil); reply.Job == nil || reply.Job.ID != j.ID {
		t.Fatal("did not place CPU variant", job(t, q, j.ID).Reason)
	}
}
