package myce

import (
	"context"
	"errors"
	"maps"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func recoveryFixture(t *testing.T) (*Queue, *fakeClock, Client, RecoveryRequest) {
	t.Helper()
	q, clock := fixture(t)
	if err := q.ConfigureFileStore(t.TempDir(), 1<<20, false); err != nil {
		t.Fatal(err)
	}
	api := httptest.NewServer(API{Queue: q, Token: "recovery-fixture-authority"}.Handler())
	t.Cleanup(api.Close)
	c := Client{URL: api.URL, Token: "recovery-fixture-authority"}
	_, ref := retentionJobInput(t, c, "retained payload output")
	m := machine("worker", "fake")
	check(t, q, m, nil)
	s := spec("experiment")
	s.Resumable = false
	s.Files = FileManifest{"input": ref}
	s.Outputs = map[string]string{"out.txt": "results"}
	submit(t, q, s)
	j := check(t, q, m, nil).Job
	if j == nil {
		t.Fatal("missing assignment")
	}
	// Historical receipt: payload exit zero, but old cleanup inspection made
	// Success false and skipped publication. Release later proves clean custody.
	r := result(j, false)
	r.Error = "inspect process groups: exec: WaitDelay expired before I/O complete"
	r.CleanupCause = &RuntimeCause{Operation: "process_group", Class: "io_error"}
	r.ReleasePending = true
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	r.ReleasePending, r.ReleaseOnly = false, true
	if err := q.Complete(r); err != nil {
		t.Fatal(err)
	}
	failed, err := c.GetJob(context.Background(), j.ID)
	if err != nil || failed.State != "failed" || !failed.ReleaseConfirmed {
		t.Fatalf("failed receipt: %+v %v", failed, err)
	}
	p := RecoveryRequest{Execution: r.Execution, Machine: r.Machine, Session: r.Session, Key: "cleanup-recovery", RequestedBy: "project-owner", Reason: "payload exited zero; cleanup inspection failed", Source: RecoverySource{Kind: "nas", Location: "results/retained-attempt"}, Files: FileManifest{"out.txt": ref}}
	return q, clock, c, p
}

func TestRecoveredResultFailedAttempt(t *testing.T) {
	q, clock, c, p := recoveryFixture(t)
	before := cloneJob(q.s.Jobs[p.ID])
	r, err := c.RecoverResult(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	if r.Kind != "recovered" || r.RecoveredBy != "operator" || r.RequestedBy != p.RequestedBy || r.Source != p.Source || r.Reason != p.Reason || r.RecoveredAt != clock.Now() || r.ExpiresAt != clock.Now().Add(DefaultFileRetention) || len(r.ResultID) != 64 || len(r.ManifestSHA256) != 64 || !maps.Equal(r.Files, p.Files) {
		t.Fatalf("recovery provenance: %+v", r)
	}
	after := cloneJob(q.s.Jobs[p.ID])
	if len(after.RecoveredResults) != 1 {
		t.Fatal("recovery was not linked")
	}
	after.RecoveredResults = nil
	if !reflect.DeepEqual(before, after) {
		t.Fatal("recovery changed original status/outcome/provenance")
	}
	got, err := c.GetJob(context.Background(), p.ID)
	if err != nil || got.State != "failed" || len(got.OutputRefs) != 0 || len(got.RecoveredResults) != 1 {
		t.Fatalf("GET: %+v %v", got, err)
	}
	var status Snapshot
	if err = c.Call(context.Background(), "GET", "/api/status", nil, &status); err != nil {
		t.Fatal(err)
	}
	visible, err := FindJob(status.Jobs, p.ID)
	if err != nil || len(visible.RecoveredResults) != 1 || visible.State != "failed" {
		t.Fatalf("status: %+v %v", visible, err)
	}
	if err = c.DownloadResult(context.Background(), p.ID, "out.txt", filepath.Join(t.TempDir(), "normal")); err == nil {
		t.Fatal("recovery promoted to ordinary success")
	}
	dest := filepath.Join(t.TempDir(), "out.txt")
	if err = c.DownloadRecoveredResult(context.Background(), p.ID, r.ResultID, "out.txt", dest); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(dest); err != nil || string(data) != "retained payload output" {
		t.Fatalf("recovered download: %q %v", data, err)
	}
	clock.Advance(time.Hour)
	again, err := c.RecoverResult(context.Background(), p)
	if err != nil || !reflect.DeepEqual(r, again) || len(q.s.Jobs[p.ID].RecoveredResults) != 1 {
		t.Fatalf("idempotency: %+v %v", again, err)
	}
	p.Reason = "additional owner verification"
	if _, err = c.RecoverResult(context.Background(), p); recoveryCode(err) != "recovery_conflict" {
		t.Fatalf("conflicting replay: %v", err)
	}
	p.Key = "second-observation"
	if _, err = c.RecoverResult(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	if len(q.s.Jobs[p.ID].RecoveredResults) != 2 || !reflect.DeepEqual(q.s.Jobs[p.ID].RecoveredResults[0], *r) {
		t.Fatal("append changed earlier evidence")
	}
}

func recoveryCode(err error) string {
	var api *HTTPError
	if errors.As(err, &api) {
		return api.Code
	}
	return ""
}

func TestRecoveredResultRejections(t *testing.T) {
	q, _, c, original := recoveryFixture(t)
	for _, variant := range []string{"unauthorized", "hash", "size", "attempt", "session", "machine", "undeclared"} {
		t.Run(variant, func(t *testing.T) {
			p, caller := original, c
			p.Files = maps.Clone(p.Files)
			want := "recovery_stale"
			switch variant {
			case "unauthorized":
				caller.Token = "not-authorized"
				want = "authentication_required"
			case "hash":
				ref := p.Files["out.txt"]
				if err := os.WriteFile(q.files.blob(ref), []byte(strings.Repeat("x", int(ref.SizeBytes))), 0600); err != nil {
					t.Fatal(err)
				}
				defer os.WriteFile(q.files.blob(ref), []byte("retained payload output"), 0600)
				want = "recovery_verification_failed"
			case "size":
				ref := p.Files["out.txt"]
				ref.SizeBytes++
				p.Files["out.txt"] = ref
				want = "recovery_verification_failed"
			case "attempt":
				p.Attempt++
			case "session":
				p.Session += "-stale"
			case "machine":
				p.Machine += "-stale"
			case "undeclared":
				p.Files = FileManifest{"other": p.Files["out.txt"]}
				want = "recovery_invalid"
			}
			if _, err := caller.RecoverResult(context.Background(), p); recoveryCode(err) != want {
				t.Fatalf("want %s: %v", want, err)
			}
			if len(q.s.Jobs[p.ID].RecoveredResults) != 0 {
				t.Fatal("rejected recovery changed history")
			}
		})
	}
}

func TestRecoveredResultVerificationFence(t *testing.T) {
	q, _, c, p := recoveryFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	q.files.syncDir = func(path string) error { close(entered); <-release; return syncWorkerDir(path) }
	done := make(chan error, 1)
	go func() { _, err := c.RecoverResult(context.Background(), p); done <- err }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("verification did not start")
	}
	progress := make(chan error, 1)
	go func() {
		// Model operator requeue/new execution while the byte verification waits.
		progress <- q.update(func() error { j := q.touchJob(p.ID); j.Attempt++; return nil })
	}()
	select {
	case err := <-progress:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("verification held q.mu")
	}
	unblock()
	select {
	case err := <-done:
		if recoveryCode(err) != "recovery_stale" {
			t.Fatalf("stale acceptance: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("recovery did not finish")
	}
	if len(q.s.Jobs[p.ID].RecoveredResults) != 0 {
		t.Fatal("stale execution acquired evidence")
	}
}

func TestRecoveredResultRestartAndRetention(t *testing.T) {
	q, clock, c, p := recoveryFixture(t)
	// Ordinary result expiry must not prevent importing retained source bytes.
	clock.Advance(DefaultFileRetention + time.Hour)
	if err := q.update(func() error { q.maintainFileExpiry(); return nil }); err != nil {
		t.Fatal(err)
	}
	r, err := c.RecoverResult(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "restored.db")
	if err = PrepareStateRoots(filepath.Dir(path)); err != nil {
		t.Fatal(err)
	}
	// SQLite backup is the supported consistent snapshot for a live queue.
	if _, err = q.db.Exec("VACUUM INTO ?", path); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, q.root, clock, WithCheckpointAvailability(func() bool { return true }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.Close() })
	if err = reopened.ConfigureFileStore(q.files.root, 1<<20, false); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(reopened.s.Jobs[p.ID].RecoveredResults[0], *r) {
		t.Fatal("restart changed evidence")
	}
	if err = reopened.reclaimPublishedFiles(); err != nil {
		t.Fatal(err)
	}
	if err = reopened.files.verify(context.Background(), p.Files["out.txt"]); err != nil {
		t.Fatal("recovery bytes were reclaimed early", err)
	}
	clock.Advance(DefaultFileRetention)
	if err = reopened.reclaimPublishedFiles(); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(reopened.files.blob(p.Files["out.txt"])); !os.IsNotExist(err) {
		t.Fatal("expired recovery bytes were retained", err)
	}
	if err = reopened.pruneBatchLogs(); err != nil {
		t.Fatal(err)
	}
	if err = reopened.pruneJobHistory(); err != nil {
		t.Fatal(err)
	}
	if j := reopened.s.Jobs[p.ID]; j == nil || len(j.RecoveredResults) != 1 || j.State != "failed" {
		t.Fatal("append-only history was deleted")
	}
	replay, err := reopened.recoverResult(context.Background(), p)
	if err != nil || !reflect.DeepEqual(replay, r) {
		t.Fatalf("expired replay changed evidence: %+v %v", replay, err)
	}
}
