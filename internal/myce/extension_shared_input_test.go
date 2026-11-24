package myce

import (
	"context"
	"os"
	"path/filepath"
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
	d, err := startTestDev(t, DevConfig{EnvironmentCacheLimit: 1 << 30, Address: "127.0.0.1:0", Database: filepath.Join(dir, "queue.db"), Checkpoints: nas, NASCheckpoints: true, FileStore: filepath.Join(dir, "files"), Token: "throwaway", Workers: 1, MemoryGB: 8, ExtensionRoot: root, JobFileRoot: nas, Interval: 10 * time.Millisecond})
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
	await(t, func() bool {
		snap, err := d.Queue.Snapshot()
		return err == nil && len(snap.Machines) > 0 && snap.Machines[0].SharedInputCache != nil
	})
	s := Spec{Name: "extension-cache", Project: "test", Priority: "experiment", Kind: "sample", SharedInputs: map[string]SharedInput{"data": {NAS: "data", SHA256: ref.SHA256, SizeBytes: ref.SizeBytes}}, Outputs: map[string]string{"output": "results"}}
	for i := 0; i < 2; i++ {
		var j Job
		if err := c.Call(ctx, "POST", "/api/jobs", s, &j); err != nil {
			t.Fatal(err)
		}
		id := j.ID
		await(t, func() bool {
			var snap Snapshot
			if err := c.Call(ctx, "GET", "/api/status?detail=full", nil, &snap); err != nil {
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
	m := cacheCapacityMachine("worker", 1<<20)
	check(t, q, m, nil)
	j := submit(t, q, s)
	m.Capabilities = []string{"native_isolation_landlock", "shared_inputs_v1"}
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
