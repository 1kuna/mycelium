package myce

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Every member of a shared queue mounts the share at its own local path. A
// member records uploads, a checkpointed attempt, a pending NAS delivery and
// jobs with model and environment inputs, then hands over to a member whose
// host mounts the share elsewhere (renamed, not symlinked: a symlink resolves
// to the same real path). The successor serves all of them, and nothing it
// holds or offers names the predecessor's mount (#1176).
func TestSharedQueuePostgresRootsMountedElsewhere(t *testing.T) {
	pg := startProofPostgres(t)
	a := pg.authority(t)
	dir := t.TempDir()
	ctx := t.Context()
	clock := proofClock()
	serve := func(q *Queue) Client {
		api := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
		t.Cleanup(api.Close)
		return Client{URL: api.URL, Token: "test"}
	}
	// Environment catalogs are member-local configuration, as installed.
	environments := func(name string) {
		t.Helper()
		local := filepath.Join(dir, name+"-environments")
		if err := os.MkdirAll(local, 0700); err != nil {
			t.Fatal(err)
		}
		for file, contents := range map[string]string{"catalog.yaml": "environments:\n  tools:\n    fake: {python: '3.12', lock_file: requirements.txt}\n", "requirements.txt": "# locked fixture\n"} {
			if err := os.WriteFile(filepath.Join(local, file), []byte(contents), 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	configure := func(q *Queue, name string) {
		t.Helper()
		catalog, err := ReadEnvironments(filepath.Join(dir, name+"-environments", "catalog.yaml"))
		if err != nil {
			t.Fatal(err)
		}
		if err := q.ConfigureEnvironments(catalog); err != nil {
			t.Fatal(err)
		}
	}
	open := func(name, mount string) (*Queue, sharedQueueTurn) {
		t.Helper()
		q, turn := proofOpenSharedQueueRoots(t, a, dir, name, filepath.Join(mount, "checkpoints"), filepath.Join(mount, "store"), true, clock, WithOutputDeliveryRoot(filepath.Join(mount, "outputs")))
		if err := a.renew(ctx, turn, time.Minute, true); err != nil {
			t.Fatal(err)
		}
		fakeCheckpointAvailability(t, q, true)
		configure(q, name)
		// As a member does when it takes the turn.
		if err := q.registerDeliveryDestinations(); err != nil {
			t.Fatal(err)
		}
		return q, turn
	}
	firstMount := filepath.Join(dir, "first-mount")
	for _, sub := range []string{"checkpoints", "outputs", "checkpoints/models"} {
		if err := os.MkdirAll(filepath.Join(firstMount, sub), 0700); err != nil {
			t.Fatal(err)
		}
	}
	const weights = "model weights on the share"
	if err := os.WriteFile(filepath.Join(firstMount, "checkpoints", "models", "weights.bin"), []byte(weights), 0600); err != nil {
		t.Fatal(err)
	}
	environments("old")
	environments("next")
	old, firstTurn := open("old", firstMount)
	c := serve(old)

	// Uploads: one left partial, one published.
	const content = "interrupted on the first member"
	partial := capacityPartial(t, c, content, jobInputUploadPurpose)
	_, published := retentionJobInput(t, c, "published on the first member")

	// A resumable job with a model input checkpoints generation 1 under the
	// first member's mount, then is interrupted.
	resume := spec("experiment")
	resume.AllowedMachines = []string{"first-host", "recovery-worker"}
	resume.SharedInputs = map[string]SharedInput{"weights": {NAS: "models/weights.bin", SHA256: digestBytes([]byte(weights)), SizeBytes: int64(len(weights))}}
	resume.Run.Command = []string{"sh", "-ec", `grep -q '"generation":1' "$MYCE_CHECKPOINT_DIR/state.json"; test "$(cat "$MYCE_INPUT_WEIGHTS")" = "` + weights + `"`}
	resumed := submit(t, old, resume)
	first := machine("first-host", "fake")
	first.Capabilities = append(first.Capabilities, "shared_inputs_v1")
	attempt := check(t, old, first, nil).Job
	if attempt == nil || attempt.ID != resumed.ID {
		t.Fatalf("first attempt: %+v", attempt)
	}
	writeSave(t, filepath.Join(attempt.Checkpoint, "attempt-1"), "1")
	finishInterrupted(t, old, attempt)

	// A result accepted on the first member waits for NAS delivery.
	publisher := machine("publisher", "fake")
	check(t, old, publisher, nil)
	deliver := asyncDeliverySpec()
	deliver.AllowedMachines = []string{publisher.ID}
	delivered := submit(t, old, deliver)
	assigned := check(t, old, publisher, nil).Job
	if assigned == nil || assigned.ID != delivered.ID {
		t.Fatalf("delivery assignment: %+v", assigned)
	}
	output := deliveryResult(t, old, assigned, "delivered after a handover")
	if err := old.Complete(output); err != nil {
		t.Fatal(err)
	}
	if d := job(t, old, delivered.ID).Delivery; d == nil || d.State != "pending" {
		t.Fatalf("delivery before the handover: %+v", d)
	}

	// A job with an environment input, for a worker that has not checked in.
	environment := spec("experiment")
	environment.AllowedMachines = []string{"environment-host"}
	environment.Environment = Environment{Name: "tools"}
	environed := submit(t, old, environment)

	if err := a.renew(ctx, firstTurn, 0, false); err != nil {
		t.Fatal(err)
	}
	// The successor's host mounts the same share somewhere else.
	secondMount := filepath.Join(dir, "second-mount")
	if err := os.Rename(firstMount, secondMount); err != nil {
		t.Fatal(err)
	}
	checkpoints := filepath.Join(secondMount, "checkpoints")
	next, _ := open("next", secondMount)
	c = serve(next)

	noFirstMount := func(what string, data []byte) {
		t.Helper()
		if i := strings.Index(string(data), firstMount); i >= 0 {
			t.Errorf("%s names the predecessor's mount: …%s…", what, string(data[max(0, i-80):min(len(data), i+len(firstMount)+80)]))
		}
	}
	state, err := marshalQueueState(t, next)
	if err != nil {
		t.Fatal(err)
	}
	noFirstMount("successor state", state)

	// Uploads resume, publish and fetch on the successor.
	if offset := uploadHeadOffset(t, ctx, c, partial.SHA256); offset != "2" {
		t.Fatalf("successor resume offset = %q", offset)
	}
	if err := c.UploadJobInput(ctx, writeInput(t, content), partial); err != nil {
		t.Fatal(err)
	}
	for _, ref := range []FileRef{published, output.OutputRefs["out.txt"]} {
		if err := c.DownloadFile(ctx, ref, filepath.Join(t.TempDir(), "fetched")); err != nil {
			t.Fatalf("fetch %s on the successor: %v", ref.SHA256, err)
		}
	}
	s := proofSpec("mounted-elsewhere")
	s.Files = FileManifest{"resumed": partial, "published": published}
	s.AllowedMachines = []string{publisher.ID}
	if _, err := next.Submit(s); err != nil {
		t.Fatal(err)
	}

	// The pending delivery publishes from the successor (#2148).
	var verified *Job
	await(t, func() bool {
		advanceDelivery(t, next)
		verified = job(t, next, delivered.ID)
		if verified.Delivery.State == "failed" {
			t.Fatalf("delivery failed on the successor: %s", verified.Delivery.Cause)
		}
		return verified.Delivery.State == "verified"
	})
	dest := filepath.Join(secondMount, "outputs", outputRelative(*verified, "out.txt", "results"))
	if data, err := os.ReadFile(dest); err != nil || string(data) != "delivered after a handover" {
		t.Fatalf("delivered bytes: %q %v", data, err)
	}

	// The environment job's preparation plan names nothing under the
	// predecessor's mount.
	builder := machine("environment-host", "fake")
	builder.EnvironmentBuilder = true
	var offer Reply
	for range 3 {
		if offer, err = next.Checkin(Checkin{Machine: builder, period: time.Second}); err != nil {
			t.Fatal(err)
		}
		if len(offer.EnvironmentPlan) > 0 {
			break
		}
		_, _ = clock.advance(1)
	}
	if len(offer.EnvironmentPlan) == 0 {
		t.Fatalf("no environment plan for %s: %s", environed.ID, job(t, next, environed.ID).Reason)
	}
	wire, err := json.Marshal(offer)
	if err != nil {
		t.Fatal(err)
	}
	noFirstMount("successor environment plan", wire)

	// A worker on the successor's host resumes from generation 1 and reads
	// its model input through its own mount.
	w := recoveryWorker(t)
	w.CheckpointRoot, w.Client = checkpoints, c
	w.checkpointPrepare = prepareCheckpoint // The test binary stubs the copy.
	w.sharedInputs = &SharedInputCache{cache: EnvironmentCache{Root: filepath.Join(t.TempDir(), "shared-inputs"), Limit: 1 << 20, Clock: w.Clock}, Client: w.Client}
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(runCtx) }()
	var runErr error
	exited := false
	await(t, func() bool {
		select {
		case runErr = <-done:
			exited = true
			return true
		default:
			j := job(t, next, resumed.ID)
			return terminal(j.State)
		}
	})
	cancel()
	if exited {
		t.Fatalf("worker exited: %v", runErr)
	}
	if runErr = <-done; runErr != nil && !errors.Is(runErr, context.Canceled) {
		t.Fatal(runErr)
	}
	if j := job(t, next, resumed.ID); j.State != "succeeded" || j.Attempt != 2 {
		logs, _ := next.Logs(resumed.ID, 0)
		t.Fatalf("resumed job: state=%s attempt=%d reason=%q logs=%+v", j.State, j.Attempt, j.Reason, logs)
	}
}

// A delivery recorded under another path to the same destination publishes;
// a destination that no host recorded it under is refused (#2148).
func TestOutputDeliveryDestinationAcrossPaths(t *testing.T) {
	q, _ := fixture(t)
	share := t.TempDir()
	q.deliveryRoot = filepath.Join(share, "first")
	if err := os.Mkdir(q.deliveryRoot, 0700); err != nil {
		t.Fatal(err)
	}
	if err := q.registerDeliveryDestinations(); err != nil {
		t.Fatal(err)
	}
	m := machine("publisher", "fake")
	check(t, q, m, nil)
	submit(t, q, asyncDeliverySpec())
	j := check(t, q, m, nil).Job
	if err := q.Complete(deliveryResult(t, q, j, "moved")); err != nil {
		t.Fatal(err)
	}
	accepted := job(t, q, j.ID)
	moved := filepath.Join(share, "moved")
	if err := os.Rename(q.deliveryRoot, moved); err != nil {
		t.Fatal(err)
	}
	if err := publishOutputDelivery(*accepted, q.files, moved); err != nil {
		t.Fatalf("the same destination at another path: %v", err)
	}
	other := t.TempDir()
	if err := publishOutputDelivery(*accepted, q.files, other); !errors.Is(err, errDeliveryDestination) {
		t.Fatalf("another destination: %v", err)
	}
	// Through the queue: a changed destination fails with its cause and retries.
	q.deliveryRoot = other
	failed := awaitDeliveryState(t, q, j.ID, "failed")
	if !strings.Contains(failed.Delivery.Cause, "missing or changed") || failed.Delivery.NextAttemptAt.IsZero() {
		t.Fatalf("changed destination: %+v", failed.Delivery)
	}
}
