package myce

import (
	"context"
	"errors"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestDockerFilesAdmission(t *testing.T) {
	base := dockerJob().Spec
	base.Files = FileManifest{"scripts/run.sh": {SHA256: strings.Repeat("a", 64), SizeBytes: 1}}
	base.Outputs = map[string]string{"result.txt": "results"}
	if err := base.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*Spec){
		"NAS":                  func(s *Spec) { s.Inputs = map[string]string{"data": "data"} },
		"workdir":              func(s *Spec) { s.Run.Workdir = "/tmp" },
		"environment":          func(s *Spec) { s.Run.Env = map[string]string{"SECRET": "private"} },
		"shared inputs":        func(s *Spec) { s.SharedInputs = map[string]SharedInput{"data": {SHA256: strings.Repeat("a", 64)}} },
		"prepared environment": func(s *Spec) { s.Environment.Name = "tools" },
	} {
		t.Run(name, func(t *testing.T) {
			s := cloneJob(&Job{Spec: base}).Spec
			change(&s)
			if err := s.Validate(); err == nil {
				t.Fatal("unsafe Docker/files combination accepted")
			}
		})
	}
	for _, flag := range []string{"--volume=/tmp:/myce/inputs", "-v=/tmp:/other", "--mount=type=bind,src=/tmp,dst=/other", "--env=SECRET=value", "--env=MYCE_TOKEN=value", "--workdir=/myce/work/subdir", "--user=0", "--group-add=0", "--privileged"} {
		for _, sidecar := range []bool{false, true} {
			s := cloneJob(&Job{Spec: base}).Spec
			command := slices.Insert(s.Run.Command, 2, flag)
			if sidecar {
				s.Run.DockerNetwork = "private"
				s.Needs.HostMemoryGB = 1
				s.Run.DockerSidecars = []DockerSidecar{{Name: "helper", MemoryGB: .1, Command: command}}
			} else {
				s.Run.Command = command
			}
			if err := s.Validate(); err == nil {
				t.Errorf("accepted %s (sidecar=%v)", flag, sidecar)
			}
		}
	}
	// Payload arguments that resemble flags remain untouched.
	base.Run.Command = append(base.Run.Command, "--env=payload-argument")
	if err := base.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestDockerFilesPlacement(t *testing.T) {
	j := dockerJob()
	j.Spec.Outputs = map[string]string{"result": "results"}
	m := machine("docker", "fake")
	m.Capabilities = append(m.Capabilities, "docker")
	if dockerRuntimeFits(&j, &m) || !strings.Contains(dockerRuntimeWaitingReason(&j, map[string]*Machine{m.ID: &m}), dockerFilesCapability) {
		t.Fatal("file job accepted by an older Docker worker")
	}
	m.Capabilities = append(m.Capabilities, dockerFilesCapability)
	if !dockerRuntimeFits(&j, &m) {
		t.Fatal("new Docker worker rejected")
	}
}

func TestDockerFilesImageVolumes(t *testing.T) {
	for _, sidecar := range []bool{false, true} {
		j := dockerJob()
		j.Spec.Outputs = map[string]string{"result": "results"}
		if sidecar {
			j.Spec.Run.DockerSidecars = []DockerSidecar{{Name: "helper", Command: j.Spec.Run.Command}}
		}
		calls := 0
		d := dockerAttempt{timeout: time.Second, command: func(_ context.Context, args ...string) ([]byte, error) {
			calls++
			if args[0] != "image" || args[1] != "inspect" {
				t.Fatalf("dispatched before checking image volumes: %v", args)
			}
			if !sidecar || calls == 2 {
				return []byte(`{"/data": {}}`), nil
			}
			return []byte("null"), nil
		}}
		if err := d.checkFileImages(j); !errors.Is(err, errDockerFileImageVolumes) {
			t.Fatal("image-created host volume accepted", err)
		}
	}
	// Daemon inspection failure retains the existing infrastructure retry policy;
	// it is distinct from an observed, unsupported image VOLUME declaration.
	d := dockerAttempt{clock: RealClock{}, timeout: time.Second, command: func(context.Context, ...string) ([]byte, error) {
		return nil, context.DeadlineExceeded
	}}
	err := d.checkFileImages(dockerJob())
	if err == nil || !dockerOutcome(d.state, Result{}, err).Interrupted {
		t.Fatal("daemon inspection outage became a payload failure", err)
	}
}

func TestDockerFilesMountEnforcement(t *testing.T) {
	w := recoveryWorker(t)
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	j := dockerJob()
	j.Spec.Outputs = map[string]string{"result": "results"}
	if err := w.prepareExtensionFiles(&j, store.dir); err != nil {
		t.Fatal(err)
	}
	checkpoint := filepath.Join(store.dir, "checkpoints", j.ID, "attempt-1")
	for _, path := range []string{checkpoint, filepath.Dir(applicationProgressFile(store.dir, j))} {
		if err := os.MkdirAll(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	mounts, err := dockerFileMounts(j, store.dir, checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	spool := filepath.Join(filepath.Dir(mounts[1].Source), ".myce-result-spool")
	for _, mount := range mounts {
		within := func(path string) bool {
			return path == mount.Source || strings.HasPrefix(path, mount.Source+string(filepath.Separator))
		}
		if mount.RW && within(mounts[0].Source) || within(spool) {
			t.Fatalf("mount aliases read-only inputs or exposes result spool: %+v", mount)
		}
	}
	d := dockerAttempt{state: dockerExecution{FileMounts: mounts}}
	if why := d.fileMountViolation(dockerInspection{Mounts: slices.Clone(mounts)}); why != "" {
		t.Fatal(why)
	}
	for _, change := range []func([]dockerFileMount) []dockerFileMount{
		func(m []dockerFileMount) []dockerFileMount { m[0].RW = true; return m },
		func(m []dockerFileMount) []dockerFileMount { m[0].Source = "/host/cache"; return m },
		func(m []dockerFileMount) []dockerFileMount { m[0].Destination = "/myce/work"; return m },
		func(m []dockerFileMount) []dockerFileMount {
			return append(m, dockerFileMount{Type: "volume", Source: "/daemon/volume", Destination: "/data", RW: true})
		},
	} {
		if d.fileMountViolation(dockerInspection{Mounts: change(slices.Clone(mounts))}) == "" {
			t.Error("unsafe daemon mount accepted")
		}
	}
}

func dockerFilesPublicationFixture(t *testing.T) (*Queue, Worker, *workerStore, Job) {
	t.Helper()
	q, clock := fixture(t)
	if err := q.ConfigureFileStore(t.TempDir(), 1<<20, false); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	t.Cleanup(server.Close)
	m := machine("docker-files", "fake")
	m.Capabilities = append(m.Capabilities, "docker", dockerFilesCapability)
	s := dockerJob().Spec
	s.SubmissionKey = "docker-files-key"
	s.Outputs = map[string]string{"result.txt": "results"}
	original := submit(t, q, s)
	if replay := submit(t, q, s); replay.ID != original.ID {
		t.Fatal("submission key did not deduplicate")
	}
	j := check(t, q, m, nil).Job
	if j == nil {
		t.Fatal("Docker file job not assigned")
	}
	w := recoveryWorker(t)
	w.Client, w.Clock, w.Machine = Client{URL: server.URL, Token: "test"}, clock, m
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.close() })
	if err = w.prepareExtensionFiles(j, store.dir); err != nil {
		t.Fatal(err)
	}
	store.state.Session, store.state.Job = j.Session, j
	return q, w, store, *j
}

func TestDockerFilesPublicationReplay(t *testing.T) {
	q, w, store, j := dockerFilesPublicationFixture(t)
	path := filepath.Join(j.Spec.Run.Workdir, "result.txt")
	if err := os.WriteFile(path, []byte("accepted bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	r := result(&j, true)
	r.ReleasePending = true
	if pending, err := w.finishFileResult(j, r, store.dir); err != nil || len(pending.OutputRefs) != 0 {
		t.Fatal("published before Docker writers settled", err)
	}
	if _, err := os.Stat(deliverySpoolPath(j, store.dir)); !os.IsNotExist(err) {
		t.Fatal("captured files while writers still had custody", err)
	}
	r.ReleasePending = false
	finished, err := w.finishFileResult(j, r, store.dir)
	if err != nil || !finished.Success || len(finished.OutputRefs) != 1 {
		t.Fatal("publish", finished, err)
	}
	// The server commits, then the response is lost. The worker retains its
	// job/result and immutable spool through restart until exact acceptance.
	w.Client.HTTP = &http.Client{Transport: fetchTransport(func(req *http.Request) (*http.Response, error) {
		response, err := http.DefaultTransport.RoundTrip(req)
		if err == nil && req.URL.Path == "/api/workers/complete" {
			response.Body.Close()
			return nil, errors.New("lost completion acknowledgement")
		}
		return response, err
	})}
	if err := w.Client.Call(context.Background(), "POST", "/api/workers/complete", finished, nil); err == nil || job(t, q, j.ID).State != "succeeded" {
		t.Fatal("lost acknowledgement fixture did not commit", err)
	}
	store.state.Result = &r
	if err := store.save(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("changed after exit"), 0600); err != nil {
		t.Fatal(err)
	}
	store.close()
	restored, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer restored.close()
	w.Client.HTTP = nil
	replayed, err := w.finishFileResult(*restored.state.Job, *restored.state.Result, restored.dir)
	if err != nil || !maps.Equal(replayed.OutputRefs, finished.OutputRefs) {
		t.Fatal("restart did not use immutable spool", replayed, err)
	}
	if err := w.Client.Call(context.Background(), "POST", "/api/workers/complete", replayed, nil); err != nil {
		t.Fatal("accepted completion replay", err)
	}
	replayed.Attempt++
	if err := q.Complete(replayed); !errors.Is(err, ErrStale) {
		t.Fatal("wrong attempt published", err)
	}
}

func TestDockerFilesInvalidOutput(t *testing.T) {
	for _, kind := range []string{"missing", "directory", "escape", "unreadable"} {
		t.Run(kind, func(t *testing.T) {
			_, w, store, j := dockerFilesPublicationFixture(t)
			path := filepath.Join(j.Spec.Run.Workdir, "result.txt")
			switch kind {
			case "directory":
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
			case "escape":
				if err := os.Symlink(filepath.Join(t.TempDir(), "secret"), path); err != nil {
					t.Fatal(err)
				}
			case "unreadable":
				if os.Geteuid() == 0 {
					t.Skip("root bypasses mode bits")
				}
				if err := os.WriteFile(path, []byte("private"), 0000); err != nil {
					t.Fatal(err)
				}
			}
			r, err := w.finishFileResult(j, result(&j, true), store.dir)
			if err != nil || r.Success || len(r.OutputRefs) != 0 {
				t.Fatal("invalid output did not visibly fail", r, err)
			}
		})
	}
}
