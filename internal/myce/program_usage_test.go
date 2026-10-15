package myce

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestProgramUsageRejectsInvalidOrUnownedReports(t *testing.T) {
	for _, data := range []string{`{"memory_gb":-1}`, `{"memory_gb":1e999}`, `{"memory_gb":4,"unknown":true}`, `{"memory_gb":4} {}`, string(make([]byte, 4097))} {
		p := filepath.Join(t.TempDir(), "report")
		if err := os.WriteFile(p, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		r := Result{}
		collectProgramUsage(&r, nil, p)
		if r.Usage != nil || r.OutOfMemory {
			t.Fatal("accepted invalid observation")
		}
	}
	root := t.TempDir()
	target, link := filepath.Join(root, "target"), filepath.Join(root, "link")
	if err := os.WriteFile(target, []byte(`{"memory_gb":40,"out_of_memory":true}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	r := Result{}
	collectProgramUsage(&r, nil, link)
	if r.Usage != nil || r.OutOfMemory {
		t.Fatal("accepted symlink observation")
	}
}

func TestProgramUsageRealRunnerLearnsWithoutHardware(t *testing.T) {
	q, _ := fixture(t)
	server := httptest.NewServer(API{Queue: q, Token: "throwaway"}.Handler())
	t.Cleanup(server.Close)
	e := fakeExtension(t)
	root, shared := t.TempDir(), t.TempDir()
	installQualified(t, root, e, "")
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	script := `test -z "$MYCE_TOKEN" || exit 99
printf '{"memory_gb":2,"host_memory_gb":0.5}' > "$MYCE_USAGE_FILE"
printf useful-result > output
`
	if err := os.WriteFile(filepath.Join(shared, "script"), []byte(script), 0600); err != nil {
		t.Fatal(err)
	}
	s := placementSpec(true)
	s.Inputs["script"] = "script"
	s.Outputs = map[string]string{"output": "results"}
	s.Run.Env = map[string]string{"MYCE_USAGE_FILE": "payload-must-not-choose-report"}
	j := submit(t, q, s)
	ctx, cancel := context.WithCancel(context.Background())
	w := Worker{Client: Client{URL: server.URL, Token: "throwaway"}, Machine: machine("fake-hypha", "fake"), ExtensionRoot: root, JobFileRoot: shared, StateDir: t.TempDir(), HostStateDir: t.TempDir(), Interval: 5 * time.Millisecond}
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(testWaitTimeout):
			t.Error("worker did not exit")
		}
	})
	await(t, func() bool { return terminal(job(t, q, j.ID).State) })
	completed := job(t, q, j.ID)
	if completed.State != "succeeded" || completed.MeasuredNeeds == nil || completed.MeasuredNeeds.MemoryGB != 2 || completed.MeasuredNeeds.HostMemoryGB < .5 {
		t.Fatal("real runner did not measure", completed)
	}
	learned := submit(t, q, s)
	if learned.Spec.Needs.MemoryGB != 2.5 || learned.Spec.Needs.HostMemoryGB < .625 {
		t.Fatal("next submission did not reuse measurements", learned.Spec.Needs)
	}
	data, err := os.ReadFile(filepath.Join(shared, completed.OutputFiles["output"]))
	if err != nil || string(data) != "useful-result" {
		t.Fatal("output mismatch", err)
	}
	// Persisted attempt result carries its observation through reconnect/replay.
	entries, err := filepath.Glob(filepath.Join(w.StateDir, "*.result.json"))
	if err != nil || len(entries) != 1 {
		t.Fatal("result spool missing", err)
	}
	data, err = os.ReadFile(entries[0])
	var r Result
	if err != nil || json.Unmarshal(data, &r) != nil || r.Usage == nil || r.Usage.MemoryGB != 2 {
		t.Fatal("observation not attempt-persistent", err)
	}
}

func TestProgramUsageRelativeStateNoNAS(t *testing.T) {
	q, _ := fixture(t)
	if err := q.ConfigureFileStore(t.TempDir(), 1<<20, true); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(API{Queue: q, Token: "throwaway"}.Handler())
	t.Cleanup(server.Close)
	c := Client{URL: server.URL, Token: "throwaway"}
	e := fakeExtension(t)
	root := t.TempDir()
	installQualified(t, root, e, "")
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(t.TempDir(), "script")
	script := `test -z "$MYCE_TOKEN" || exit 99
case "$MYCE_USAGE_FILE" in /*) ;; *) exit 97;; esac
case "$MYCE_CHECKPOINT_DIR" in /*) ;; *) exit 96;; esac
printf '{"memory_gb":2,"host_memory_gb":0.5}' > "$MYCE_USAGE_FILE"
printf useful-result > output
`
	if err := os.WriteFile(source, []byte(script), 0600); err != nil {
		t.Fatal(err)
	}
	ref, err := IdentifyFile(source)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.UploadFile(context.Background(), source, ref); err != nil {
		t.Fatal(err)
	}
	spec := placementSpec(true)
	spec.Inputs = nil
	spec.Files = FileManifest{"script": ref}
	spec.Outputs = map[string]string{"output": "results"}
	j := submit(t, q, spec)
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	state, err := filepath.Rel(cwd, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	w := Worker{Client: c, Machine: machine("relative-state", "fake"), ExtensionRoot: root, StateDir: state, HostStateDir: t.TempDir(), Interval: 5 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(testWaitTimeout):
			t.Error("worker did not exit")
		}
	})
	await(t, func() bool { return terminal(job(t, q, j.ID).State) })
	completed := job(t, q, j.ID)
	if completed.State != "succeeded" || completed.MeasuredNeeds == nil || completed.MeasuredNeeds.MemoryGB != 2 {
		t.Fatalf("relative spool: state=%s reason=%s usage=%v", completed.State, completed.Reason, completed.MeasuredNeeds)
	}
	dest := filepath.Join(t.TempDir(), "output")
	if err = c.DownloadFile(context.Background(), completed.OutputRefs["output"], dest); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(dest)
	if err != nil || string(data) != "useful-result" {
		t.Fatal("output mismatch", err)
	}
}
