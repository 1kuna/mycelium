package myce

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestExtensionSharedInputReuse(t *testing.T) {
	root, nas := t.TempDir(), t.TempDir()
	e := fakeExtension(t)
	e.Files["run.sh"] = []byte("test -z \"$MYCE_TOKEN\" || exit 99\ncat \"$MYCE_INPUT_DATA\" > output\n")
	var err error
	e, err = extensionFromFiles(e.Files)
	if err != nil {
		t.Fatal(err)
	}
	installQualified(t, root, e, "0")
	ref := sharedFixtureFile(t, nas, "data", "cached input")
	dir := t.TempDir()
	d, err := StartDev(DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(dir, "queue.db"), Checkpoints: nas, NASCheckpoints: true, FileStore: filepath.Join(dir, "files"), Token: "throwaway", Workers: 1, MemoryGB: 8, ExtensionRoot: root, JobFileRoot: nas, Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	})
	c := Client{URL: d.URL, Token: "throwaway"}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := c.Call(ctx, "POST", "/api/extensions", e, nil); err != nil {
		t.Fatal(err)
	}
	s := Spec{Name: "extension-cache", Project: "test", Priority: "experiment", Kind: "sample", SharedInputs: map[string]SharedInput{"data": {NAS: "data", SHA256: ref.SHA256, SizeBytes: ref.SizeBytes}}, Outputs: map[string]string{"output": "results"}}
	for i := 0; i < 2; i++ {
		var j Job
		if err := c.Call(ctx, "POST", "/api/jobs", s, &j); err != nil {
			t.Fatal(err)
		}
		id := j.ID
		await(t, func() bool {
			var snap Snapshot
			if err := c.Call(ctx, "GET", "/api/status", nil, &snap); err != nil {
				t.Fatal(err)
			}
			for _, job := range snap.Jobs {
				if job.ID == id && terminal(job.State) {
					if job.State != "succeeded" {
						var logs []Log
						_ = c.Call(ctx, "GET", "/api/jobs/"+id+"/logs", nil, &logs)
						t.Fatalf("extension failed: %s %s cause=%+v history=%+v logs=%+v", job.State, job.Reason, job.ExecutionCause, job.AttemptHistory, logs)
					}
					j = *job
					return true
				}
			}
			return false
		})
		b, err := os.ReadFile(filepath.Join(nas, j.OutputFiles["output"]))
		if err != nil || string(b) != "cached input" {
			t.Fatal(string(b), err)
		}
		if i == 0 {
			if err := os.Remove(filepath.Join(nas, "data")); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestExtensionSharedInputConcurrentAndChangedIdentity(t *testing.T) {
	nas := t.TempDir()
	ref := sharedFixtureFile(t, nas, "data", "first")
	input := SharedInput{NAS: "data", SHA256: ref.SHA256, SizeBytes: ref.SizeBytes}
	cache := &SharedInputCache{cache: EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20}}
	t.Cleanup(func() { _ = removeSharedEntry(cache.cache.Root) })
	type result struct {
		path string
		done func()
		err  error
	}
	results := make(chan result, 2)
	start := make(chan struct{})
	for i := 0; i < 2; i++ {
		go func() {
			<-start
			path, done, err := cache.Acquire(context.Background(), input, nas)
			results <- result{path, done, err}
		}()
	}
	close(start)
	a, b := <-results, <-results
	if a.err != nil || b.err != nil {
		t.Fatal(a.err, b.err)
	}
	defer a.done()
	defer b.done()
	if a.path != b.path {
		t.Fatal("concurrent users did not share a copy")
	}
	ref = sharedFixtureFile(t, nas, "data", "second")
	changed := SharedInput{NAS: "data", SHA256: ref.SHA256, SizeBytes: ref.SizeBytes}
	path, done, err := cache.Acquire(context.Background(), changed, nas)
	if err != nil {
		t.Fatal(err)
	}
	defer done()
	if path == a.path {
		t.Fatal("changed identity reused old copy")
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "second" {
		t.Fatal(string(data), err)
	}
	if err := os.Remove(filepath.Join(nas, "data")); err != nil {
		t.Fatal(err)
	}
	restarted := &SharedInputCache{cache: EnvironmentCache{Root: cache.cache.Root, Limit: 1 << 20}}
	path2, release, err := restarted.Acquire(context.Background(), changed, nas)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if path2 != path {
		t.Fatal("restart did not reuse locally verified input")
	}
}

func TestSharedInputBlockedColdMissDoesNotBlockOtherInputs(t *testing.T) {
	nas := t.TempDir()
	first := sharedFixtureFile(t, nas, "blocked", "blocked content")
	second := sharedFixtureFile(t, nas, "independent", "independent content")
	entered, unblock := make(chan struct{}), make(chan struct{})
	cache := &SharedInputCache{cache: EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20}}
	cache.resolveNAS = func(root, name string) (string, error) {
		if name == "blocked" {
			close(entered)
			<-unblock
		}
		return confinedPath(root, name)
	}
	t.Cleanup(func() { _ = removeSharedEntry(cache.cache.Root) })
	blocked := SharedInput{NAS: "blocked", SHA256: first.SHA256, SizeBytes: first.SizeBytes}
	independent := SharedInput{NAS: "independent", SHA256: second.SHA256, SizeBytes: second.SizeBytes}
	type result struct {
		done func()
		err  error
	}
	pending := make(chan result, 1)
	go func() {
		_, done, err := cache.Acquire(context.Background(), blocked, nas)
		pending <- result{done, err}
	}()
	<-entered
	finished := make(chan result, 1)
	go func() {
		_, done, err := cache.Acquire(context.Background(), independent, nas)
		finished <- result{done, err}
	}()
	select {
	case r := <-finished:
		if r.err != nil {
			close(unblock)
			t.Fatal(r.err)
		}
		r.done()
	case <-time.After(2 * time.Second):
		close(unblock)
		<-pending
		r := <-finished
		if r.done != nil {
			r.done()
		}
		t.Fatal("independent input waited for blocked NAS read")
	}
	close(unblock)
	r := <-pending
	if r.err != nil {
		t.Fatal(r.err)
	}
	r.done()
}

func TestSharedInputReadinessAffinityAndBudget(t *testing.T) {
	input := SharedInput{NAS: "input", SHA256: digestBytes([]byte("x")), SizeBytes: 1}
	job := &Job{Spec: Spec{SharedInputs: map[string]SharedInput{"data": input}}}
	cold := &Machine{}
	warm := &Machine{CachedSharedInputs: []string{input.key()}}
	if affinity(job, warm) <= affinity(job, cold) {
		t.Fatal("ready cache has no placement affinity")
	}
	volume := DiskVolume{Roots: []string{"file-cache"}}
	coldNeed, _ := diskVolumeNeed(job, volume, cold)
	warmNeed, _ := diskVolumeNeed(job, volume, warm)
	if coldNeed != 1 || warmNeed != 0 {
		t.Fatalf("cold=%d warm=%d", coldNeed, warmNeed)
	}
}

func TestExtensionSharedInputsRequireNewWorkerCapability(t *testing.T) {
	input := SharedInput{NAS: "input", SHA256: digestBytes([]byte("a")), SizeBytes: 1}
	s := Spec{Kind: "sample", SharedInputs: map[string]SharedInput{"data": input}}
	old := &Machine{Capabilities: []string{"shared_inputs_v1"}}
	if sharedInputsFit(s, old) {
		t.Fatal("old worker could accept a NAS-output extension job")
	}
	upgraded := &Machine{Capabilities: []string{"shared_inputs_v1", "extension_shared_inputs_v1"}}
	if !sharedInputsFit(s, upgraded) {
		t.Fatal("upgraded worker refused extension shared input")
	}
	s.Kind = ""
	if !sharedInputsFit(s, old) {
		t.Fatal("old worker lost plain shared input compatibility")
	}
	q, _ := fixture(t)
	e := fakeExtension(t)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	s.Kind, s.Name, s.Project, s.Priority = "sample", "extension", "test", "experiment"
	s.Outputs = map[string]string{"output": "results"}
	j := submit(t, q, s)
	m := machine("worker", "fake")
	m.HostMemoryMeasured, m.FreeHostMemoryGB, m.HostAvailableMemoryGB = true, 8, 8
	m.ExtensionReports = installQualified(t, t.TempDir(), e, "")
	m.Capabilities = append(m.Capabilities, "shared_inputs_v1")
	if reply := check(t, q, m, nil); reply.Job != nil {
		t.Fatal("old worker assigned extension input")
	}
	m.Capabilities = append(m.Capabilities, "extension_shared_inputs_v1")
	reply := check(t, q, m, nil)
	if reply.Job == nil || reply.Job.ID != j.ID {
		t.Fatal("new worker not assigned", reply, job(t, q, j.ID).Reason)
	}
	m.Capabilities = []string{"native_isolation_landlock", "shared_inputs_v1"}
	check(t, q, m, nil)
	current := job(t, q, j.ID)
	if err := q.Started(Result{Machine: m.ID, Session: m.Session, Execution: Execution{ID: j.ID, Attempt: current.Attempt}}); err == nil {
		t.Fatal("old worker authorized to start extension input")
	}
}

func TestConcurrentSharedBuildsReserveRemainingBytes(t *testing.T) {
	nas := t.TempDir()
	const size = 128 << 10
	a := sharedFixtureFile(t, nas, "a", strings.Repeat("a", size))
	b := sharedFixtureFile(t, nas, "b", strings.Repeat("b", size))
	c := sharedFixtureFile(t, nas, "c", strings.Repeat("c", size))
	cache := &SharedInputCache{cache: EnvironmentCache{Root: t.TempDir(), Limit: 2*size + 64<<10, Clock: &workerFakeClock{now: time.Date(2026, 10, 31, 0, 0, 0, 0, time.UTC)}}}
	t.Cleanup(func() { _ = removeSharedEntry(cache.cache.Root) })
	ready, publish := make(chan struct{}, 2), make(chan struct{})
	var arrivals atomic.Int32
	cache.beforePublish = func() {
		if arrivals.Add(1) <= 2 {
			ready <- struct{}{}
			<-publish
		}
	}
	type result struct {
		done func()
		err  error
	}
	results := make(chan result, 2)
	pending := 0
	t.Cleanup(func() {
		close(publish)
		for ; pending > 0; pending-- {
			r := <-results
			if r.done != nil {
				r.done()
			}
			if r.err != nil {
				t.Error(r.err)
			}
		}
	})
	for _, pair := range []struct {
		name string
		ref  FileRef
	}{{"a", a}, {"b", b}} {
		pending++
		go func() {
			_, done, err := cache.Acquire(context.Background(), SharedInput{NAS: pair.name, SHA256: pair.ref.SHA256, SizeBytes: pair.ref.SizeBytes}, nas)
			results <- result{done, err}
		}()
		// Admission measures remaining reservations before allocated bytes. Let
		// this copy finish before starting the next: growth between those scans
		// can conservatively refuse a build that would fit after copying stops.
		// Both writers still hold their reservations and remain unpublished.
		select {
		case <-ready:
		case r := <-results:
			pending--
			if r.done != nil {
				r.done()
			}
			t.Fatalf("build %s returned before publication barrier: %v", pair.name, r.err)
		}
	}
	// Both in-progress files already occupy their reservation. A third does
	// not fit and cannot evict either active build.
	_, done, err := cache.Acquire(context.Background(), SharedInput{NAS: "c", SHA256: c.SHA256, SizeBytes: c.SizeBytes}, nas)
	if err == nil {
		done()
		t.Fatal("admitted third build beyond cache capacity")
	}
}
