package myce

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Runs a single queue in its own OS process with the fixture's shared file
// store and checkpoint root, as the original queue host would.
func serveProofSingle(t *testing.T) {
	dir := os.Getenv("MYCE_SHARED_PROOF_FILES")
	q, err := Open(os.Getenv("MYCE_SHARED_PROOF_DATABASE"), filepath.Join(dir, "checkpoints"), proofClock(),
		WithFileStore(filepath.Join(dir, "files"), 100<<30, true))
	if err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", os.Getenv("MYCE_SHARED_PROOF_LISTEN"))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(l.Addr().String())
	ctx, cancel := signal.NotifyContext(t.Context(), syscall.SIGTERM)
	defer cancel()
	server := &http.Server{Handler: API{Queue: q, Token: proofToken}.Handler(), ConnContext: QueueConnectionContext, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = server.Serve(l) }()
	runErr := q.Run(ctx)
	q.BeginShutdown()
	if err := errors.Join(runErr, server.Close(), q.Close()); err != nil {
		t.Fatal(err)
	}
}

func startProofSingle(t *testing.T, dir, name, addr string) *proofProcess {
	t.Helper()
	p := startProofProcess(t, "TestSharedQueueProofProcess", dir, name, addr, "MYCE_SHARED_PROOF_SINGLE=1")
	p.client.SharedQueueProof = false
	return p
}

// A consumer client that retries transient refusals, like the CLI.
func proofConsumer(p *proofProcess) Client {
	return Client{URL: p.client.URL, Token: proofToken, HTTP: &http.Client{Timeout: 20 * time.Second}}
}

func proofMigration(pg *proofPostgres, dir, db string) SharedQueueMigration {
	return SharedQueueMigration{Postgres: pg.dsn, Database: db, Checkpoints: filepath.Join(dir, "checkpoints"),
		FileStore: filepath.Join(dir, "files"), Clock: proofClock()}
}

// The laptop rehearsal of #1176 stage 3: cut a running single queue over to
// shared members, kill the original, prove the six consumer operations, then
// roll back and prove them again. One CPU attempt runs across both switches.
func TestSharedQueueCutoverAndRollbackRehearsal(t *testing.T) {
	pg := startProofPostgres(t)
	a := pg.authority(t)
	dir := t.TempDir()
	old := startProofSingle(t, dir, "old", "127.0.0.1:0")
	// The worker keeps one endpoint throughout, the way it keeps its local
	// member's address. It is not told about either switch.
	startProofWorker(t, Client{URL: old.client.URL, Token: proofToken, HTTP: &http.Client{Timeout: 5 * time.Second}}, dir)
	consumer := proofConsumer(old)
	before, unblock, finished := proofBlockingJob(t, consumer, dir)

	migration := proofMigration(pg, dir, old.db)
	if _, err := SharedQueueCutover(t.Context(), migration); err == nil || !strings.Contains(err.Error(), "stop the single queue") {
		t.Fatalf("cutover with the old writer running = %v", err)
	}
	// Hold old writes the hard way: the original process dies mid-service.
	old.stop(t, syscall.SIGKILL)
	cut, err := SharedQueueCutover(t.Context(), migration)
	if err != nil || cut.Sequence == 0 || len(cut.Files.Problems) != 0 || cut.Files.Checked == 0 {
		t.Fatalf("cutover = %+v, %v", cut, err)
	}
	if _, err := Open(old.db, migration.Checkpoints, proofClock()); !errors.Is(err, errSharedCopy) {
		t.Fatalf("the fenced original opened as a single queue: %v", err)
	}
	again, err := SharedQueueCutover(t.Context(), migration)
	if err != nil || !again.Already || again.Sequence != cut.Sequence {
		t.Fatalf("repeated cutover = %+v, %v", again, err)
	}

	local := startProofMember(t, dir, "local", strings.TrimPrefix(old.client.URL, "http://"), pg.dsn)
	other := startProofMember(t, dir, "other", "127.0.0.1:0", pg.dsn)
	turn := proofLeader(t, a, local, 0)
	if turn.head < cut.Sequence {
		t.Fatalf("shared head %d is behind the import %d", turn.head, cut.Sequence)
	}
	shared := proofConsumer(other)
	assertProofAttempt(t, before, proofJobState(t, shared, before.ID, func(j *Job) bool { return j.State == "running" }))
	proveQueueOperations(t, shared, dir, "shared")
	assertProofAttempt(t, before, proofJobState(t, shared, before.ID, func(j *Job) bool { return j.State == "running" }))

	restored := filepath.Join(dir, "restored", "queue.db")
	if _, err := SharedQueueRollback(t.Context(), proofMigration(pg, dir, old.db)); err == nil || !strings.Contains(err.Error(), "older pre-cutover database") {
		t.Fatalf("rollback over the original database = %v", err)
	}
	back, err := SharedQueueRollback(t.Context(), proofMigration(pg, dir, restored))
	if err != nil || back.Sequence < cut.Sequence || back.Files.Checked == 0 || len(back.Files.Problems) != 0 {
		t.Fatalf("rollback = %+v, %v", back, err)
	}
	// Fenced members stay up but can no longer decide anything.
	err = Client{URL: other.client.URL, Token: proofToken, HTTP: &http.Client{Timeout: 5 * time.Second}, singleAttempt: true}.
		Call(t.Context(), http.MethodPost, "/api/jobs", proofSpec("after-fence"), nil)
	proofUnavailable(t, err)
	if current, err := a.current(t.Context()); err != nil || current.head != back.Sequence || current.ready {
		t.Fatalf("shared writers kept writing after the fence: %+v, %v", current, err)
	}
	local.stop(t, syscall.SIGKILL)
	other.stop(t, syscall.SIGKILL)

	single := startProofSingle(t, dir, "restored", strings.TrimPrefix(old.client.URL, "http://"))
	consumer = proofConsumer(single)
	assertProofAttempt(t, before, proofJobState(t, consumer, before.ID, func(j *Job) bool { return j.State == "running" }))
	proveQueueOperations(t, consumer, dir, "restored")
	unblock()
	finished()
	after := proofJobState(t, consumer, before.ID, proofSucceeded)
	assertProofAttempt(t, before, after)
	dest := filepath.Join(dir, "running-download")
	if err := consumer.DownloadResult(t.Context(), after.ID, "result.txt", dest); err != nil {
		t.Fatal(err)
	}
	if b, err := os.ReadFile(dest); err != nil || string(b) != "proof" {
		t.Fatalf("verified result = %q, %v", b, err)
	}
}

// proveQueueOperations covers submit, assignment, wait, cancel, checkpoint
// resume and verified result fetch through one endpoint with real commands.
func proveQueueOperations(t *testing.T, c Client, dir, phase string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	source := filepath.Join(dir, phase+"-input")
	if err := os.WriteFile(source, []byte(phase+" input"), 0600); err != nil {
		t.Fatal(err)
	}
	ref, err := IdentifyFile(source)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.UploadFile(ctx, source, ref); err != nil {
		t.Fatal(err)
	}
	s := proofSpec(phase + "-plain")
	s.Files = FileManifest{"input": ref}
	s.Outputs = map[string]string{"result.txt": "results"}
	s.Run.Command = []string{"/bin/sh", "-c", "cat input > result.txt"}
	job := proofSubmit(t, c, s)
	done, err := c.Wait(ctx, job.ID)
	if err != nil || done.State != "succeeded" || done.Machine == "" || done.Attempt != 1 {
		t.Fatalf("%s: submitted job = %+v, %v", phase, done, err)
	}
	dest := filepath.Join(dir, phase+"-download")
	if err := c.DownloadResult(ctx, job.ID, "result.txt", dest); err != nil {
		t.Fatal(err)
	}
	if b, err := os.ReadFile(dest); err != nil || string(b) != phase+" input" {
		t.Fatalf("%s: verified result = %q, %v", phase, b, err)
	}

	started := filepath.Join(dir, phase+"-cancel-started")
	s = proofSpec(phase + "-cancel")
	s.Run.Command = []string{"/bin/sh", "-c", `: > "$1"; while :; do sleep 0.05; done`, "cancel", started}
	job = proofSubmit(t, c, s)
	proofEventually(t, phase+" cancel target starts", func() bool { _, err := os.Stat(started); return err == nil })
	if _, err := c.Cancel(ctx, job.ID); err != nil {
		t.Fatal(err)
	}
	if j := proofJobState(t, c, job.ID, func(j *Job) bool { return terminal(j.State) }); j.State != "cancelled" {
		t.Fatalf("%s: cancelled job = %s", phase, j.State)
	}

	s = proofSpec(phase + "-resume")
	s.Resumable = true
	s.Files = FileManifest{"input": ref} // Declared outputs need stored inputs.
	s.Outputs = map[string]string{"resumed.json": "results"}
	s.Run.Command = []string{"/bin/sh", "-c", `
if [ -f "$MYCE_CHECKPOINT_DIR/save.json" ]; then cp "$MYCE_CHECKPOINT_DIR/save.json" resumed.json; exit 0; fi
printf '{"myce_checkpoint":{"generation":1},"phase":"%s"}' "$1" > "$MYCE_CHECKPOINT_DIR/save.json"
trap 'exit 143' TERM
while :; do sleep 0.05; done`, "resume", phase}
	job = proofSubmit(t, c, s)
	// Resume uses the latest accepted generation. A local save alone can lose
	// the race with release and leave the restarted fixture waiting for a pause.
	running := proofJobState(t, c, job.ID, func(j *Job) bool {
		return j.State == "running" && j.FileCheckpoints[j.Attempt].Generation == 1
	})
	attempt := running.Attempt
	var paused Job
	if err := c.Call(ctx, http.MethodPost, "/api/jobs/"+job.ID+"/pause", PauseJobRequest{
		Reason: "rehearse checkpoint resume", Project: s.Project, Yield: true, IfAttempt: &attempt}, &paused); err != nil {
		t.Fatal(err)
	}
	held := proofJobState(t, c, job.ID, func(j *Job) bool { return j.State == "paused" })
	if err := c.Call(ctx, http.MethodPost, "/api/jobs/"+job.ID+"/resume", ResumeJobRequest{IfRevision: held.PauseRevision}, nil); err != nil {
		t.Fatal(err)
	}
	done, err = c.Wait(ctx, job.ID)
	if err != nil || done.State != "succeeded" || done.Attempt != attempt+1 {
		t.Fatalf("%s: resumed job = %+v, %v", phase, done, err)
	}
	dest = filepath.Join(dir, phase+"-resumed")
	if err := c.DownloadResult(ctx, job.ID, "resumed.json", dest); err != nil {
		t.Fatal(err)
	}
	if b, err := os.ReadFile(dest); err != nil || !bytes.Contains(b, []byte(`"phase":"`+phase+`"`)) {
		t.Fatalf("%s: resumed from checkpoint = %q, %v", phase, b, err)
	}
}

// Cutover verifies every retained reference against the store's bytes,
// reports problems without deleting anything, and changes nothing until that
// exact report is acknowledged.
func TestSharedQueueCutoverVerifiesFileBytes(t *testing.T) {
	pg := startProofPostgres(t)
	a := pg.authority(t)
	dir := t.TempDir()
	migration := proofMigration(pg, dir, filepath.Join(dir, "old", "queue.db"))
	q, err := Open(migration.Database, migration.Checkpoints, proofClock(), WithFileStore(migration.FileStore, 100<<30, true))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(API{Queue: q, Token: proofToken}.Handler())
	c := Client{URL: server.URL, Token: proofToken}
	refs := map[string]FileRef{}
	for _, name := range []string{"intact", "corrupt", "missing"} {
		source := filepath.Join(dir, name)
		if err := os.WriteFile(source, []byte(name+" bytes"), 0600); err != nil {
			t.Fatal(err)
		}
		ref, err := IdentifyFile(source)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.UploadFile(t.Context(), source, ref); err != nil {
			t.Fatal(err)
		}
		s := proofSpec("verify-" + name)
		s.Files = FileManifest{"input": ref}
		if _, err := c.Submit(t.Context(), s); err != nil {
			t.Fatal(err)
		}
		refs[name] = ref
	}
	server.Close()
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	blob := func(name string) string { return filepath.Join(migration.FileStore, "blobs", refs[name].SHA256) }
	if err := os.WriteFile(blob("corrupt"), []byte("CORRUPT bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(blob("missing")); err != nil {
		t.Fatal(err)
	}

	report := func(r SharedQueueMigrationResult) string {
		var out []string
		for _, p := range r.Files.Problems {
			out = append(out, p.Problem+":"+p.SHA256+":"+p.Holder.Kind)
		}
		return strings.Join(out, ",")
	}
	want := "missing:" + refs["missing"].SHA256 + ":input"
	wantCorrupt := "mismatch:" + refs["corrupt"].SHA256 + ":input"
	refused, err := SharedQueueCutover(t.Context(), migration)
	if !errors.Is(err, ErrSharedFilesUnacknowledged) || refused.Files.Checked != 3 || len(refused.Files.Problems) != 2 ||
		!strings.Contains(report(refused), want) || !strings.Contains(report(refused), wantCorrupt) || refused.Files.Digest == "" {
		t.Fatalf("unacknowledged problems = %s %+v, %v", report(refused), refused.Files, err)
	}
	migration.Acknowledge = "0123456789abcdef"
	if _, err := SharedQueueCutover(t.Context(), migration); !errors.Is(err, ErrSharedFilesUnacknowledged) {
		t.Fatalf("a different acknowledgement = %v", err)
	}
	migration.Acknowledge, migration.VerifyOnly = "", true
	if checked, err := SharedQueueCutover(t.Context(), migration); err != nil || checked.Files.Digest != refused.Files.Digest {
		t.Fatalf("verify-only = %+v, %v", checked.Files, err)
	}
	if turn, err := a.current(t.Context()); err != nil || turn.head != 0 {
		t.Fatalf("refused cutover changed the authority: %+v, %v", turn, err)
	}
	reopened, err := Open(migration.Database, migration.Checkpoints, proofClock(), WithFileStore(migration.FileStore, 100<<30, true))
	if err != nil {
		t.Fatalf("refused cutover fenced the single queue: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}

	migration.Acknowledge, migration.VerifyOnly = refused.Files.Digest, false
	cut, err := SharedQueueCutover(t.Context(), migration)
	if err != nil || cut.Sequence == 0 {
		t.Fatalf("acknowledged cutover = %+v, %v", cut, err)
	}
	if b, err := os.ReadFile(blob("corrupt")); err != nil || string(b) != "CORRUPT bytes" {
		t.Fatalf("verification changed a mismatched file: %q, %v", b, err)
	}
	if b, err := os.ReadFile(blob("intact")); err != nil || string(b) != "intact bytes" {
		t.Fatalf("verification changed an intact file: %q, %v", b, err)
	}
	if turn, err := a.current(t.Context()); err != nil || turn.head != cut.Sequence {
		t.Fatalf("authority after cutover: %+v, %v", turn, err)
	}
}
