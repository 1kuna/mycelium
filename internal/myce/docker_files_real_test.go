package myce

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// No image pulls or fleet access. A local busybox:1.37.0 makes this automatic;
// MYCE_REAL_DOCKER_IMAGE can select another local digest-pinned POSIX image.
func TestDockerFilesRealHashRoundTrip(t *testing.T) {
	t.Setenv("PATH", dockerTestOriginalPath)
	binary, err := exec.LookPath("docker")
	if err != nil {
		t.Skip("Docker CLI unavailable")
	}
	endpoint := defaultDockerEndpoint(os.Getenv("DOCKER_HOST"))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	info := exec.CommandContext(ctx, binary, "--host", endpoint, "info", "--format", "{{.OSType}}")
	info.Env = dockerClientEnvironment()
	if out, err := info.Output(); err != nil || strings.TrimSpace(string(out)) != "linux" {
		t.Skip("local Linux Docker daemon unavailable")
	}
	image := os.Getenv("MYCE_REAL_DOCKER_IMAGE")
	if image == "" {
		inspect := exec.CommandContext(ctx, binary, "--host", endpoint, "image", "inspect", "busybox:1.37.0", "--format", "{{json .RepoDigests}}")
		inspect.Env = dockerClientEnvironment()
		out, err := inspect.Output()
		var digests []string
		if err != nil || json.Unmarshal(out, &digests) != nil || len(digests) == 0 {
			t.Skip("local digest-pinned busybox:1.37.0 unavailable; pull it separately")
		}
		image = digests[0]
	}
	t.Setenv("DOCKER_CONTEXT", "")
	t.Setenv("MYCE_TOKEN", "must-not-reach-container")
	t.Setenv("HOST_ONLY_SECRET", "must-not-reach-container")
	for _, sidecar := range []bool{false, true} {
		name := "main"
		if sidecar {
			name = "managed-sidecar"
		}
		t.Run(name, func(t *testing.T) { dockerFilesRealRoundTrip(t, binary, endpoint, image, sidecar) })
	}
}

func dockerFilesRealRoundTrip(t *testing.T, binary, endpoint, image string, sidecar bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	q, clock := fixture(t)
	if err := q.ConfigureFileStore(t.TempDir(), 1<<20, true); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	client := Client{URL: server.URL, Token: "test"}
	files := FileManifest{}
	for name, data := range map[string]string{
		"data/input.txt": "hash-in hash-out\n",
		"scripts/run.sh": `set -eu
test "$PWD" = /myce/work
test -z "${MYCE_TOKEN:-}"
test -z "${HOST_ONLY_SECRET:-}"
test "$MYCE_INPUT_DIR" = /myce/inputs
test "$MYCE_OUTPUT_DIR" = /myce/work
test ! -d /myce/progress/inputs
test ! -d /myce/progress/work
if (printf changed > /myce/progress/inputs/data/input.txt) 2>/dev/null; then exit 90; fi
if (printf changed > /myce/inputs/data/input.txt) 2>/dev/null; then exit 91; fi
if (chmod u+w /myce/inputs/data/input.txt) 2>/dev/null; then exit 92; fi
if (touch /myce/inputs/new) 2>/dev/null; then exit 93; fi
mkdir -p nested
cat /myce/inputs/data/input.txt > nested/result.txt
printf '%s' '{"completed":1,"total":1,"unit":"item"}' > "$MYCE_PROGRESS_FILE"
printf checkpoint > "$MYCE_CHECKPOINT_DIR/proof"
`,
	} {
		path := filepath.Join(t.TempDir(), "source")
		if err := os.WriteFile(path, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		ref, err := IdentifyFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err = client.UploadFile(ctx, path, ref); err != nil {
			t.Fatal(err)
		}
		files[name] = ref
	}
	spec := Spec{Name: "docker-files", Project: "test", Priority: "experiment", SubmissionKey: "real-docker-files", Files: files, Outputs: map[string]string{"nested/result.txt": "results"}, Run: RunSpec{Docker: true, Command: []string{"docker", "run", image, "sh", "/myce/inputs/scripts/run.sh"}}, Needs: ResourceNeeds{CPUCores: 1, MemoryGB: .1, Platform: Platform{Required: []string{"cpu"}}}}
	if sidecar {
		spec.Needs.HostMemoryGB = .5
		spec.Run.DockerNetwork = "private"
		spec.Run.DockerMemoryGB = .125
		spec.Run.Command = []string{"docker", "run", image, "sh", "-ec", "while [ ! -f helper-ready ]; do sleep .05; done; exec sh /myce/inputs/scripts/run.sh"}
		spec.Run.DockerSidecars = []DockerSidecar{{Name: "helper", MemoryGB: .125, Command: []string{"docker", "run", image, "sh", "-ec", `test -z "${MYCE_TOKEN:-}"; test -z "${HOST_ONLY_SECRET:-}"; test "$(cat /myce/inputs/data/input.txt)" = 'hash-in hash-out'; if (touch /myce/inputs/new) 2>/dev/null; then exit 94; fi; test ! -d /myce/progress/inputs; touch /myce/work/helper-ready; exec sleep 300`}}}
	}
	// Use the real queue and start authorization, with synthetic CPU inventory.
	j := submit(t, q, spec)
	m := machine("real-docker-files", "cpu")
	m.HostMemoryMeasured = true
	m.FreeHostMemoryGB, m.HostAvailableMemoryGB = 32, 32
	m.Capabilities = append(m.Capabilities, "docker", dockerFilesCapability)
	assigned := check(t, q, m, nil).Job
	if assigned == nil {
		t.Fatalf("not assigned: %+v", job(t, q, j.ID))
	}
	w := Worker{EnvironmentLimit: 1 << 20, DockerEndpoint: endpoint, Client: client, Machine: m, StateDir: newTestStateRoot(t), Clock: clock, HostMemorySource: func(context.Context) (float64, error) { return 32, nil }}
	w.dockerCommand = func(ctx context.Context, args ...string) ([]byte, error) {
		cmd := exec.CommandContext(ctx, binary, append([]string{"--host", endpoint}, args...)...)
		cmd.Env = dockerClientEnvironment()
		out, err := cmd.Output()
		if failure, ok := err.(*exec.ExitError); ok {
			t.Logf("Docker %s refused: %s", args[0], failure.Stderr)
		}
		return out, err
	}
	w.fileCache = &FileCache{Root: newTestStateRoot(t), Limit: 1 << 20, Clock: clock, Client: client}
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	store.state.Session = assigned.Session
	assigned.Checkpoint = filepath.Join(store.dir, "file-checkpoints", assigned.ID)
	p, err := startDockerFixture(w, store, *assigned)
	if err != nil {
		t.Fatal(err)
	}
	defer p.docker.close()
	defer func() {
		p.docker.stop(clock.Now(), 0, true)
		// Exact owned resources only; keep the host's other containers intact.
		await(t, func() bool { done, err := p.docker.reconcile(); return err == nil && done })
		containers, err := p.docker.containers()
		if err != nil {
			t.Error(err)
			return
		}
		for _, c := range containers {
			if dockerLive(c) {
				t.Error("test container still running")
				continue
			}
			if _, err := p.docker.call("rm", c.ID); err != nil {
				t.Error(err)
			}
		}
	}()
	select {
	case finished := <-p.done:
		if finished.err != nil || !finished.result.Success || finished.result.ReleasePending {
			t.Fatalf("Docker file job failed: %+v", finished)
		}
		if progress := readApplicationProgress(store.dir, *store.state.Job, clock.Now()); progress == nil || *progress.Completed != 1 {
			t.Fatal("Docker progress mount is not readable by the worker", progress)
		}
		published, err := w.finishFileResult(*store.state.Job, finished.result, store.dir)
		if err != nil || !published.Success {
			t.Fatal(published, err)
		}
		if err := q.Complete(published); err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	accepted := job(t, q, j.ID)
	if accepted.State != "succeeded" || accepted.OutputRefs["nested/result.txt"] != files["data/input.txt"] {
		t.Fatal("hash round trip was not accepted", accepted.State, accepted.OutputRefs)
	}
	download := filepath.Join(t.TempDir(), "result")
	if err := client.DownloadResult(ctx, j.ID, "nested/result.txt", download); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(download); err != nil || string(data) != "hash-in hash-out\n" {
		t.Fatal(string(data), err)
	}
	if replay := submit(t, q, spec); replay.ID != j.ID {
		t.Fatal("submission key replay created another job")
	}
	t.Log("real Linux container: read-only hash input, private output/checkpoint/progress mounts, filtered environment and accepted downloaded output verified")
}
