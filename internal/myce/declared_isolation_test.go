//go:build linux || darwin

package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"maps"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

const isolationHostProof = `test -z "${MYCE_TOKEN-}" && test -z "${SSH_AUTH_SOCK-}"
test "$MYCE_DEVICE" = cpu
test "$TMPDIR" = "$PWD/.tmp" && test "$TMP" = "$TMPDIR" && test "$TEMP" = "$TMPDIR"
test "$MYCE_SCRATCH_DIR" = "$PWD/.scratch"
test "$(cat "$ISOLATION_HOST")" = synthetic-boundary
`

func isolationClient(t *testing.T, q *Queue) Client {
	t.Helper()
	server := httptest.NewServer((API{Queue: q, Token: "isolation-test-token"}).Handler())
	t.Cleanup(server.Close)
	return Client{URL: server.URL, Token: "isolation-test-token"}
}

func isolationObserve(t *testing.T, q *Queue, client Client, id, state, reason string) {
	t.Helper()
	// Expire the normal public status cache through the injected queue clock.
	q.mu.Lock()
	q.clock.(*fakeClock).Advance(time.Second)
	q.mu.Unlock()
	var j Job
	if err := client.Call(context.Background(), "GET", "/api/jobs/"+id, nil, &j); err != nil {
		t.Fatal(err)
	}
	want := "unsandboxed: " + reason
	if j.State != state || j.Isolation != want || !strings.Contains(JobReason(&j), want) {
		t.Fatalf("GET state=%s isolation=%q reason=%q", j.State, j.Isolation, JobReason(&j))
	}
	if len(j.Extensions) != 0 || j.Spec.Reason != "" || len(j.Spec.Run.Env) != 0 || len(j.Spec.Run.Command) != 0 {
		t.Fatal("isolation observation exposed execution payload")
	}
	var status Snapshot
	if err := client.Call(context.Background(), "GET", "/api/status", nil, &status); err != nil {
		t.Fatal(err)
	}
	for _, entry := range status.Jobs {
		if entry.ID == id {
			if entry.Isolation != want || !strings.Contains(JobReason(entry), want) {
				t.Fatalf("status lost isolation: %+v", entry)
			}
			return
		}
	}
	t.Fatal("status omitted job")
}

func isolationWorker(t *testing.T, client Client, e *Extension) Worker {
	t.Helper()
	w := recoveryWorker(t)
	w.Client = client
	w.HostMemorySource = func(context.Context) (float64, error) { return 64, nil }
	w.MemorySource = func(context.Context) (float64, error) { return 64, nil }
	w.Machine.Capabilities = nil // Explicitly no kernel or native-network capability.
	w.Machine.CapabilityProfile.OS, w.Machine.CapabilityProfile.Arch = runtime.GOOS, runtime.GOARCH
	w.Machine.HostMemoryMeasured, w.Machine.FreeHostMemoryGB, w.Machine.HostAvailableMemoryGB = true, 64, 64
	if e != nil {
		host, err := os.Hostname()
		if err != nil {
			t.Fatal(err)
		}
		w.Machine.HostID = host // PrepareExtension qualifies this actual local host.
		w.ExtensionRoot = t.TempDir()
		w.Machine.ExtensionReports = installQualified(t, w.ExtensionRoot, *e, "")
		fp, err := ExtensionFingerprint(context.Background(), "fake", "")
		if err != nil {
			t.Fatal(err)
		}
		w.extensionFingerprint, w.Machine.ExtensionFingerprint = fp, fp
	}
	return w
}

func isolationRun(t *testing.T, q *Queue, client Client, w Worker, j *Job, group bool, reason string) {
	t.Helper()
	// A sibling/control file is outside every sandbox's payload grants. Reading
	// it proves actual unconfined execution rather than a renamed policy field.
	host := filepath.Join(w.StateDir, "control", "authority")
	if err := os.MkdirAll(filepath.Dir(host), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(host, []byte("synthetic-boundary"), 0600); err != nil {
		t.Fatal(err)
	}
	reply := check(t, q, w.Machine, nil)
	if reply.Job == nil || reply.Job.ID != j.ID {
		t.Fatal("exception was blocked by kernel admission", reply.Job)
	}
	root, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer root.close()
	root.state.Session = w.Machine.Session
	var logs bytes.Buffer
	previousLog := log.Writer()
	log.SetOutput(&logs)
	defer log.SetOutput(previousLog)
	var p *process
	await(t, func() bool {
		p, err = w.start(root, *reply.Job)
		return !errors.Is(err, errWorkerCheckpoint) || !strings.Contains(err.Error(), "input copy pending")
	})
	if err != nil {
		t.Fatal(err)
	}
	finished := false
	defer func() {
		if !finished {
			p.stopDirective(w.Clock.Now(), 0, true, true)
			select {
			case <-p.done:
			case <-time.After(10 * time.Second):
				t.Error("exception fixture cleanup stalled")
			}
		}
	}()
	work := p.job.Spec.Run.Workdir
	ready, finish := filepath.Join(work, ".tmp", "passed"), filepath.Join(work, ".tmp", "finish")
	if group {
		ready, finish = filepath.Join(work, "main-ready"), filepath.Join(work, "finish")
	}
	await(t, func() bool { _, err := os.Stat(ready); return err == nil })
	isolationObserve(t, q, client, j.ID, "running", reason)
	if err := os.WriteFile(finish, nil, 0600); err != nil {
		t.Fatal(err)
	}
	select {
	case f := <-p.done:
		finished = true
		wantSuccess := !group
		if f.err != nil || f.result.Success != wantSuccess || f.result.ReleasePending {
			t.Fatalf("unconfined execution/cleanup failed: %+v", f)
		}
		if err := q.Complete(f.result); err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("tiny exception fixture stalled")
	}
	// Reload the persisted warning receipt, as on recovery, and attempt it again.
	data, err := os.ReadFile(filepath.Join(root.dir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	reloaded := &workerStore{dir: root.dir}
	if err := json.Unmarshal(data, &reloaded.state); err != nil {
		t.Fatal(err)
	}
	if err := reloaded.warnNativeIsolation(reply.Job); err != nil {
		t.Fatal(err)
	}
	if strings.Count(logs.String(), "WARNING job "+j.ID+" attempt ") != 1 {
		t.Fatalf("expected one warning across members/recovery: %s", logs.String())
	}
	nativeNoSurvivors(t, &nativeTestRun{p: p, dir: work})
	state := "succeeded"
	if group {
		state = "failed" // The group main deliberately exits 7 after its proof.
	}
	isolationObserve(t, q, client, j.ID, state, reason)
}

func TestDeclaredNativeIsolation(t *testing.T) {
	t.Setenv("MYCE_TOKEN", "ambient-secret")
	t.Setenv("SSH_AUTH_SOCK", "/synthetic-agent")
	const reason = "native helper needs ordinary host IPC"
	t.Run("author-validation", func(t *testing.T) {
		q, _ := fixture(t)
		client := isolationClient(t, q)
		for _, mode := range []string{"unknown", "unsandboxed"} {
			s := spec("experiment")
			s.Isolation = mode
			if err := client.Call(context.Background(), "POST", "/api/jobs", s, nil); err == nil {
				t.Errorf("accepted %s without a reason", mode)
			}
			e := fakeExtension(t)
			// Author the invalid declaration in bundle bytes, so server-side
			// revalidation cannot accidentally trust transport-only fields.
			e.Files["extension.yaml"] = append(e.Files["extension.yaml"], []byte("isolation: "+mode+"\nreason: '  '\n")...)
			if err := client.Call(context.Background(), "POST", "/api/extensions", e, nil); err == nil {
				t.Errorf("accepted extension %s without a nonblank reason", mode)
			}
		}
	})
	t.Run("single-command", func(t *testing.T) {
		q, _ := fixture(t)
		client := isolationClient(t, q)
		w := isolationWorker(t, client, nil)
		s := spec("experiment")
		s.Resumable = false
		s.Isolation, s.Reason = "unsandboxed", reason
		s.Needs.Network = true // No native_network report, and Linux stays usable.
		s.Run.Command = []string{"sh", "-ec", isolationHostProof + `echo passed > "$TMPDIR/passed"; while ! test -f "$TMPDIR/finish"; do sleep .01; done`}
		s.Run.Env = map[string]string{"ISOLATION_HOST": filepath.Join(w.StateDir, "control", "authority"), "MYCE_TOKEN": "authored-secret", "TMPDIR": "/ignored"}
		check(t, q, w.Machine, nil)
		var j Job
		if err := client.Call(context.Background(), "POST", "/api/jobs", s, &j); err != nil {
			t.Fatal(err)
		}
		isolationObserve(t, q, client, j.ID, "queued", reason)
		isolationRun(t, q, client, w, &j, false, reason)
	})
	t.Run("plain-native-default", func(t *testing.T) {
		q, _ := fixture(t)
		client := isolationClient(t, q)
		w := isolationWorker(t, client, nil)
		s := spec("experiment")
		s.Resumable = false
		s.Run.Command = []string{"sh", "-ec", isolationHostProof + `echo passed > "$TMPDIR/passed"; while ! test -f "$TMPDIR/finish"; do sleep .01; done`}
		s.Run.Env = map[string]string{"ISOLATION_HOST": filepath.Join(w.StateDir, "control", "authority"), "MYCE_TOKEN": "authored-secret", "TMPDIR": "/ignored"}
		check(t, q, w.Machine, nil)
		var j Job
		if err := client.Call(context.Background(), "POST", "/api/jobs", s, &j); err != nil {
			t.Fatal(err)
		}
		isolationObserve(t, q, client, j.ID, "queued", plainNativeIsolationReason)
		isolationRun(t, q, client, w, &j, false, plainNativeIsolationReason)
	})

	t.Run("extension-main-and-sidecar", func(t *testing.T) {
		q, _ := fixture(t)
		client := isolationClient(t, q)
		w := isolationWorker(t, client, nil)
		s := nativeSpec(t)
		s.Run.Env["ISOLATION_HOST"] = filepath.Join(w.StateDir, "control", "authority")
		s.Run.Sidecars[0].Env["ISOLATION_HOST"] = s.Run.Env["ISOLATION_HOST"]
		wrap := func(argv []string) []string {
			return append([]string{"sh", "-ec", isolationHostProof + `exec "$@"`, "isolation-member"}, argv...)
		}
		s.Run.Command = wrap(s.Run.Command)
		s.Run.Command[2] = isolationHostProof + `"$@"; exit 7`
		s.Run.Sidecars[0].Command = wrap(s.Run.Sidecars[0].Command)
		e := compositionExtension(t, false)
		e.Resumable, e.Isolation, e.Reason = false, "unsandboxed", reason
		e.Variants[0].Needs.HostMemoryGB = 4
		e.Variants[0].Execution = ExecutionSpec{Main: ExecutionMember{Command: s.Run.Command, Env: s.Run.Env}, Sidecars: s.Run.Sidecars}
		e = freezeComposition(t, e)
		// Reuse this worker's state root, while adding actual local qualification.
		qualified := isolationWorker(t, client, &e)
		qualified.StateDir = w.StateDir
		w = qualified
		if err := client.Call(context.Background(), "POST", "/api/extensions", e, nil); err != nil {
			t.Fatal(err)
		}
		var j Job
		if err := client.Call(context.Background(), "POST", "/api/jobs", Spec{Name: "declared", Project: "test", Extension: extensionKey(e)}, &j); err != nil {
			t.Fatal(err)
		}
		isolationObserve(t, q, client, j.ID, "queued", reason)
		// Check payloads lower through the same stage policy and retain sidecars.
		checkSpec := Spec{Extension: extensionKey(e), ExtensionCheck: &ExtensionCheckRequest{Work: "kind:sample", Directory: t.TempDir()}}
		// Dispatch selects the variant's whole-job reservation before lowering.
		candidates := extensionJobs(&Job{Spec: checkSpec, Extensions: j.Extensions, State: "queued"}, &w.Machine, true)
		if len(candidates) != 1 {
			t.Fatal("qualification check did not select one worker variant", candidates)
		}
		checkSpec = candidates[0].Spec
		if err := j.Extensions[0].lower(&checkSpec, &w.Machine); err != nil || checkSpec.Isolation != "unsandboxed" || checkSpec.Reason != reason || len(checkSpec.Run.Sidecars) != 1 {
			t.Fatal("qualification lowering lost stage isolation", checkSpec, err)
		}
		isolationRun(t, q, client, w, &j, true, reason)
		path := strings.TrimSuffix(q.lock.Name(), ".lock")
		if err := q.Close(); err != nil {
			t.Fatal(err)
		}
		restarted, err := Open(path, q.root, q.clock, WithCheckpointAvailability(func() bool { return true }))
		if err != nil {
			t.Fatal(err)
		}
		defer restarted.Close()
		isolationObserve(t, restarted, isolationClient(t, restarted), j.ID, "failed", reason)
	})
}

func TestLegacyExtensionIsolationPersistence(t *testing.T) {
	dir := t.TempDir()
	clock := &fakeClock{now: time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC)}
	open := func() *Queue {
		q, err := Open(filepath.Join(dir, "queue.db"), filepath.Join(dir, "checkpoints"), clock, WithCheckpointAvailability(func() bool { return true }))
		if err != nil {
			t.Fatal(err)
		}
		return q
	}
	q := open()
	defer func() { q.Close() }()
	e := fakeExtension(t)
	e.Resumable = false
	e.Files["run.sh"] = []byte(isolationHostProof + `echo passed > "$TMPDIR/passed"; while ! test -f "$TMPDIR/finish"; do sleep .01; done`)
	e = freezeComposition(t, e)
	// Seed the actual pre-policy persisted shape, with no registration marker.
	if err := q.update(func() error { q.s.Extensions = map[string]Extension{extensionKey(e): e}; return nil }); err != nil {
		t.Fatal(err)
	}
	client := isolationClient(t, q)
	w := isolationWorker(t, client, &e)
	s := Spec{Name: "legacy", Project: "test", Kind: "sample", Run: RunSpec{Env: map[string]string{"ISOLATION_HOST": filepath.Join(w.StateDir, "control", "authority")}}}
	var frozen Job
	if err := client.Call(context.Background(), "POST", "/api/jobs", s, &frozen); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q = open()
	client = isolationClient(t, q)
	w.Client = client
	// An incoming transport marker cannot change an already registered digest.
	e.SandboxDefault = true
	if err := client.Call(context.Background(), "POST", "/api/extensions", e, nil); err != nil {
		t.Fatal(err)
	}
	var registered []Extension
	if err := client.Call(context.Background(), "GET", "/api/extensions", nil, &registered); err != nil || len(registered) != 1 || registered[0].SandboxDefault || registered[0].Digest != e.Digest {
		t.Fatal("identical legacy registration changed policy/identity", registered, err)
	}
	// Finish the frozen job before submitting its peer: equal queue timestamps
	// are ordered by ID, so check-in need not pick the first submitted job.
	isolationObserve(t, q, client, frozen.ID, "queued", legacyIsolationReason)
	isolationRun(t, q, client, w, &frozen, false, legacyIsolationReason)
	var later Job
	if err := client.Call(context.Background(), "POST", "/api/jobs", s, &later); err != nil {
		t.Fatal(err)
	}
	isolationObserve(t, q, client, later.ID, "queued", legacyIsolationReason)
	isolationRun(t, q, client, w, &later, false, legacyIsolationReason)
	// New registrations default sandboxed; an explicit legacy opt-in changes
	// authored bytes/version, never the old digest or its frozen jobs.
	optIn := e
	optIn.Files = maps.Clone(e.Files)
	optIn.Version, optIn.Isolation = "2", "sandboxed"
	optIn = freezeComposition(t, optIn)
	if err := client.Call(context.Background(), "POST", "/api/extensions", optIn, nil); err != nil {
		t.Fatal(err)
	}
	var fresh Job
	if err := client.Call(context.Background(), "POST", "/api/jobs", Spec{Name: "opt-in", Project: "test", Extension: extensionKey(optIn)}, &fresh); err != nil || len(fresh.Extensions) != 1 || fresh.Extensions[0].unsandboxedReason() != "" || !fresh.Extensions[0].SandboxDefault || optIn.Digest == e.Digest {
		t.Fatal("new registration did not retain explicit sandbox opt-in", fresh, err)
	}
	newDefault := optIn
	newDefault.Files = maps.Clone(optIn.Files)
	newDefault.Version, newDefault.Isolation = "3", ""
	newDefault = freezeComposition(t, newDefault)
	if err := client.Call(context.Background(), "POST", "/api/extensions", newDefault, nil); err != nil {
		t.Fatal(err)
	}
	if err := client.Call(context.Background(), "POST", "/api/jobs", Spec{Name: "default", Project: "test", Extension: extensionKey(newDefault)}, &fresh); err != nil || len(fresh.Extensions) != 1 || !fresh.Extensions[0].SandboxDefault || fresh.Extensions[0].unsandboxedReason() != "" {
		t.Fatal("omitted isolation on a new registration did not default sandboxed", fresh, err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q = open()
	client = isolationClient(t, q)
	isolationObserve(t, q, client, frozen.ID, "succeeded", legacyIsolationReason)
	isolationObserve(t, q, client, later.ID, "succeeded", legacyIsolationReason)
}
