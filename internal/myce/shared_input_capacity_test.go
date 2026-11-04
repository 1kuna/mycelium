package myce

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
func TestSharedInputCapacityRefusalAndRouting(t *testing.T) {
	q, clock := fixture(t)
	small, big := cacheCapacityMachine("small", 10<<30), cacheCapacityMachine("big", 40<<30)
	check(t, q, small, nil)
	s := sharedCapacitySpec()
	required := sharedInputRequired(s, nil)
	for _, resolve := range []bool{false, true} {
		var err error
		if resolve {
			_, err = q.Resolve(ResolveRequest{Spec: s})
		} else {
			_, err = q.Submit(s)
		}
		var httpErr *HTTPError
		if !errors.As(err, &httpErr) || httpErr.Code != "shared_input_cache_capacity" || !strings.Contains(httpErr.Message, fmt.Sprint(required)) || !strings.Contains(httpErr.Message, "10737418240") {
			t.Fatalf("resolve=%v refusal=%v", resolve, err)
		}
	}
	check(t, q, big, nil)
	j := submit(t, q, s)
	clock.Advance(time.Second)
	if r := check(t, q, small, nil); r.Job != nil {
		t.Fatal("placed on small cache", r.Job.ID)
	}
	if r := check(t, q, big, nil); r.Job == nil || r.Job.ID != j.ID {
		t.Fatal("did not route to fitting cache")
	}
}
func TestSharedInputCapacityUnknownAndPressure(t *testing.T) {
	q, clock := fixture(t)
	old := cacheCapacityMachine("legacy", 40<<30)
	old.SharedInputCache = nil
	check(t, q, old, nil)
	s := sharedCapacitySpec()
	_, err := q.Submit(s)
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.Code != "shared_input_cache_unreported" {
		t.Fatal(err)
	}
	big := cacheCapacityMachine("big", 80<<30)
	big.SharedInputCache.FreeBytes = 1 << 30
	big.SharedInputCache.EvictableBytes = 2 << 30
	check(t, q, big, nil)
	j := submit(t, q, s)
	if r := check(t, q, old, nil); r.Job != nil {
		t.Fatal("legacy worker accepted shared inputs")
	}
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
}
func TestSharedInputCapacityAccounting(t *testing.T) {
	q, clock := fixture(t)
	_ = q
	nas := t.TempDir()
	ref := sharedFixtureFile(t, nas, "input", strings.Repeat("x", 64<<10))
	input := SharedInput{NAS: "input", SHA256: ref.SHA256, SizeBytes: ref.SizeBytes}
	cache := &SharedInputCache{cache: EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20, Clock: clock}}
	cache.refreshCapacity()
	before := cache.capacityReport()
	path, release, err := cache.Acquire(context.Background(), input, nas)
	if err != nil {
		t.Fatal(err)
	}
	during := cache.capacityReport()
	if during.FreeBytes >= before.FreeBytes || during.EvictableBytes != 0 || during.Protected[input.key()] == 0 {
		t.Fatalf("active reader not protected: %+v", during)
	}
	release()
	after := cache.capacityReport()
	if after.EvictableBytes == 0 || len(after.Protected) != 0 {
		t.Fatalf("release did not make entry evictable: %+v", after)
	}
	// Heartbeat readback is a detached in-memory copy, even if storage vanishes.
	if err := os.Rename(cache.cache.Root, filepath.Join(t.TempDir(), "retained")); err != nil {
		t.Fatal(err)
	}
	if got := cache.capacityReport(); got.FreeBytes != after.FreeBytes {
		t.Fatal("report touched disk")
	}
	_ = path
}
func TestSharedInputCacheAutomaticAndExplicitSizing(t *testing.T) {
	q, clock := fixture(t)
	_ = q
	root := t.TempDir()
	for _, explicit := range []int64{0, 32 << 30} {
		w := Worker{Clock: clock, FileCacheRoot: root, FileCacheLimit: explicit}
		if err := w.initFileTransfers(context.Background(), &workerStore{dir: t.TempDir()}); err != nil {
			t.Fatal(err)
		}
		if explicit > 0 && w.sharedInputs.cache.Limit != explicit {
			t.Fatal("explicit cache size changed")
		}
		if explicit == 0 && (w.sharedInputs.cache.Limit < 4<<30 || w.sharedInputs.cache.Limit > 80<<30) {
			t.Fatal("automatic cache size outside policy")
		}
	}
}

func TestSharedInputCapacityIncludesPendingAssignments(t *testing.T) {
	q, _ := fixture(t)
	m := cacheCapacityMachine("worker", 10<<30)
	check(t, q, m, nil)
	s := sharedCapacitySpec()
	s.SharedInputs = map[string]SharedInput{"first": {NAS: "first", SHA256: strings.Repeat("a", 64), SizeBytes: 6 << 30}}
	first := submit(t, q, s)
	assigned := check(t, q, m, nil).Job
	if assigned == nil || assigned.ID != first.ID {
		t.Fatal("first assignment missing")
	}
	s.SharedInputs = map[string]SharedInput{"second": {NAS: "second", SHA256: strings.Repeat("b", 64), SizeBytes: 6 << 30}}
	// The worker still reports its pre-assignment free space. The queue's view
	// must cover the assigned first input before it appears in a cache receipt.
	target := q.placementTargets(q.s.Machines[m.ID])[0]
	if reason := sharedInputCacheReason(s, target, false); reason == "" {
		t.Fatal("pending assignment left its cache bytes available")
	}
}

func TestSharedInputCapacityDisabledWorkerAndArchive(t *testing.T) {
	q, _ := fixture(t)
	small, big := cacheCapacityMachine("small", 10<<30), cacheCapacityMachine("big", 80<<30)
	check(t, q, small, nil)
	check(t, q, big, nil)
	q.s.HostMaintenance[hostID(&big)] = HostMaintenance{Mode: "disabled"}
	if _, err := q.Submit(sharedCapacitySpec()); err == nil {
		t.Fatal("disabled worker made impossible input appear viable")
	}
	archive := Spec{SharedInputs: map[string]SharedInput{"archive": {SizeBytes: 3 << 30, UnpackedBytes: 8 << 30, Archive: "tar.gz"}}}
	if reason := sharedInputCacheReason(archive, &small, true); !strings.Contains(reason, "11811160064") {
		t.Fatal("archive staging bytes omitted", reason)
	}
}

func TestSharedInputCapacityRefusalSizesThroughClient(t *testing.T) {
	q, _ := fixture(t)
	m := cacheCapacityMachine("small", 10<<30)
	check(t, q, m, nil)
	client := Client{URL: testServerForEnvironments(t, q), Token: "test"}
	s := sharedCapacitySpec()
	for _, path := range []string{"/api/jobs", "/api/resolve"} {
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
}
func TestSharedInputCapacityReclaimsAbandonedReservation(t *testing.T) {
	_, clock := fixture(t)
	cache := &SharedInputCache{cache: EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20, Clock: clock}}
	if err := cache.cache.init(); err != nil {
		t.Fatal(err)
	}
	reservation := filepath.Join(cache.cache.Root, strings.Repeat("a", 64)+".reservation")
	if err := os.WriteFile(reservation, []byte("1048576"), 0600); err != nil {
		t.Fatal(err)
	}
	cache.refreshCapacity()
	report := cache.capacityReport()
	if report.FreeBytes != report.CapacityBytes {
		t.Fatalf("abandoned reservation wedged admission: %+v", report)
	}
}
