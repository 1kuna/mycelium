package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"mycelium/internal/myce"
)

// Use the real queue API and notifier executable: local receipts cannot be the
// authority for cancellation, foreground collection or cross-receipt delivery.
func TestCompletionObserverUsesQueueDecision(t *testing.T) {
	for _, mode := range []string{"cancel", "supersede", "wait", "client_wait", "replay_wait", "unobserved", "in_flight"} {
		t.Run(mode, func(t *testing.T) {
			q, clock, _, _ := waitQueue(t)
			j := waitSubmit(t, q)
			waitFinish(t, q, clock, true)
			server := httptest.NewServer(myce.API{Queue: q, Token: "fixture"}.Handler())
			defer server.Close()
			c := myce.Client{URL: server.URL, Token: "fixture", Clock: clock}
			ctx := context.Background()
			root := t.TempDir()
			payloads := filepath.Join(root, "payloads")
			hook := filepath.Join(root, "notify")
			if err := os.WriteFile(hook, []byte("#!/bin/sh\ncat >> "+shellArg(payloads)+"\nprintf '\\n' >> "+shellArg(payloads)+"\n"), 0700); err != nil {
				t.Fatal(err)
			}
			r := profileReceipt{JobID: j.ID, Project: j.Spec.Project, Key: "fixture", Nonce: "nonce", ArmNonce: "arm", Status: "queued", Observation: "armed"}
			cfg := profileObserver{Queue: server.URL, Notify: hook}
			var pending *profileNotification
			switch mode {
			case "cancel":
				if _, err := c.Cancel(ctx, j.ID); err != nil {
					t.Fatal(err)
				}
			case "supersede":
				if err := runCommand(ctx, []string{"--queue", server.URL, "--token", "fixture", "--project", j.Spec.Project, "--supersedes", j.ID, "--detach", "--", "true"}, io.Discard, io.Discard); err != nil {
					t.Fatal(err)
				}
			case "wait":
				if err := waitJobs(ctx, c, []string{j.ID}, 0, io.Discard, io.Discard, clock, waitPause); err != nil {
					t.Fatal(err)
				}
			case "client_wait":
				if _, err := c.Wait(ctx, j.ID); err != nil {
					t.Fatal(err)
				}
			case "replay_wait":
				submit := func(w io.Writer) error { return json.NewEncoder(w).Encode(r) }
				t.Setenv("MYCE_TOKEN", "fixture")
				if err := runReceiptWait(ctx, submit, os.Getppid(), "", "", "", server.URL, "fixture", io.Discard, io.Discard); err != nil {
					t.Fatal(err)
				}
			case "in_flight":
				n := profileNotification{JobID: j.ID, Key: r.Key, Status: "succeeded", DeliveryID: "original-in-flight"}
				if _, err := c.ChangeCompletion(ctx, j.ID, myce.CompletionRequest{Operation: "claim", DeliveryID: n.DeliveryID}); err != nil {
					t.Fatal(err)
				}
				pending = &n
			}
			observed, err := c.GetJobForWait(ctx, j.ID)
			if err != nil {
				t.Fatal(err)
			}
			r.PendingNotification = pending
			if err := finishProfileObservationUsing(ctx, &c, root, r, cfg, observed, nil); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(payloads)
			if mode != "unobserved" && mode != "in_flight" {
				if !os.IsNotExist(err) {
					t.Fatalf("suppressed completion invoked receiver: %s %v", data, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			var notification profileNotification
			if err := json.Unmarshal(data, &notification); err != nil {
				t.Fatal(err)
			}
			if mode == "in_flight" && notification.DeliveryID != pending.DeliveryID {
				t.Fatal("recovery changed receiver deduplication identity")
			}
			// Simulate a second checkout with a different nonce and an obsolete frozen
			// pending payload. Queue acknowledgement must prevent invoking the receiver.
			r.Nonce = "different-checkout"
			r.PendingNotification = &notification
			if err := finishProfileObservationUsing(ctx, &c, root, r, cfg, nil, nil); err != nil {
				t.Fatal(err)
			}
			data, err = os.ReadFile(payloads)
			if err != nil || strings.Count(string(data), "delivery_id") != 1 {
				t.Fatal("completion invoked receiver twice", string(data), err)
			}
			current, err := c.GetJob(ctx, j.ID)
			if err != nil || current.Completion.DeliveredAt == nil {
				t.Fatal("delivery acknowledgement was not durable", err)
			}
		})
	}
}

// A cancelled wait isn't collection: detached owners retain the automatic wake.
func TestCompletionTimedOutWaitKeepsWake(t *testing.T) {
	q, clock, _, _ := waitQueue(t)
	j := waitSubmit(t, q)
	server := httptest.NewServer(myce.API{Queue: q, Token: "fixture"}.Handler())
	defer server.Close()
	c := myce.Client{URL: server.URL, Token: "fixture", Clock: clock}
	err := waitJobs(context.Background(), c, []string{j.ID}, time.Second, io.Discard, io.Discard, clock, func(ctx context.Context, d time.Duration) error { clock.advance(time.Second); return nil })
	if exitCode(err) != 3 {
		t.Fatal(err)
	}
	waitFinish(t, q, clock, true)
	claim, err := c.ChangeCompletion(context.Background(), j.ID, myce.CompletionRequest{Operation: "claim", DeliveryID: "unobserved"})
	if err != nil || !claim.Allowed {
		t.Fatal("timed-out waiter suppressed wake", claim, err)
	}
}

func TestCompletionResultCollectionAfterVerifiedDownload(t *testing.T) {
	root := t.TempDir()
	d, err := myce.StartDev(myce.DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(root, "queue.db"), FileStore: filepath.Join(root, "files"), Checkpoints: filepath.Join(root, "checkpoints"), Token: "fixture", GPU: "cpu", Workers: 1, Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	ctx, cancel := context.WithTimeout(context.Background(), devTestTimeout)
	defer cancel()
	c := d.Client()
	s := myce.Spec{Name: "result", Project: "fixture", SubmissionKey: "result", Run: myce.RunSpec{Command: []string{"sh", "-c", "printf accepted > answer"}}, Needs: myce.ResourceNeeds{CPUCores: 1, MemoryGB: 1, Platform: myce.Platform{Required: []string{"cpu"}}}}
	manifest, cleanup, err := attachRunOutputs(&s, []string{"answer"})
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if err := c.UploadJobInput(myce.WithFileStoreSubmission(ctx, s), manifest, s.Files[runOutputManifest]); err != nil {
		t.Fatal(err)
	}
	j, err := c.Submit(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	j, err = c.ObserveSubmitted(ctx, j)
	if err != nil {
		t.Fatal(err)
	}
	if j.Completion == nil || j.Completion.CollectedAt != nil {
		t.Fatal("read-only observer collected result")
	}
	// A failed download must not suppress an otherwise unobserved completion.
	blocked := filepath.Join(root, "blocked")
	if err := os.WriteFile(blocked, []byte("occupied"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := fetchResult(ctx, c, j.ID, blocked, 0, "", io.Discard); err == nil {
		t.Fatal("expected failed download")
	}
	j, err = c.GetJob(ctx, j.ID)
	if err != nil || j.Completion.CollectedAt != nil {
		t.Fatal("failed download suppressed wake", err)
	}
	out := filepath.Join(root, "accepted")
	if err := fetchResult(ctx, c, j.ID, out, 0, "", io.Discard); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(out, "answer"))
	if err != nil || string(data) != "accepted" {
		t.Fatal("result was not received", err)
	}
	claim, err := c.ChangeCompletion(ctx, j.ID, myce.CompletionRequest{Operation: "claim", DeliveryID: "after-result"})
	if err != nil || claim.Allowed {
		t.Fatal("verified result did not suppress later wake", claim, err)
	}
}

func TestCompletionSameKeyProfileWaitDoesNotRearmSavedNotifier(t *testing.T) {
	root := t.TempDir()
	d, err := myce.StartDev(myce.DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(root, "queue.db"), FileStore: filepath.Join(root, "files"), Checkpoints: filepath.Join(root, "checkpoints"), Token: "fixture", GPU: "cpu", Workers: 1, Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	ctx, cancel := context.WithTimeout(context.Background(), devTestTimeout)
	defer cancel()
	c := d.Client()
	profiles := filepath.Join(root, ".mycelium", "profiles")
	if err := os.MkdirAll(profiles, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "source"), 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(profiles, "fixture.yaml")
	if err := os.WriteFile(path, []byte("name: fixture\nproject: fixture\nsource: {directory: source}\nrun: {command: [true]}\nneeds: {cpu_cores: 1, memory_gb: 1, platform: cpu}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	var receipt strings.Builder
	// Prepare the accepted receipt without starting a detached process. It is
	// unobserved in the queue until the same-key foreground replay below.
	if err := runProfile(ctx, path, []string{}, nil, "same-key", "", "", "", d.URL, "fixture", &receipt, io.Discard, profileOverrides{Foreground: true}); err != nil {
		t.Fatal(err)
	}
	var r profileReceipt
	if err := json.Unmarshal([]byte(receipt.String()), &r); err != nil {
		t.Fatal(err)
	}
	j, err := c.ObserveSubmitted(ctx, &myce.Job{ID: r.JobID, Spec: r.Spec})
	if err != nil || j.State != "succeeded" {
		t.Fatal(j, err)
	}
	dir := filepath.Join(root, ".mycelium", "receipts", profileHash([]string{r.Project, r.Key}))
	// A saved automatic binding must not be checked, armed or inherited by --wait.
	// The obsolete executable makes an accidental arm fail visibly.
	if err := saveProfileJSON(filepath.Join(dir, "observer.json"), profileObserver{Queue: d.URL, Notify: filepath.Join(root, "obsolete-notifier")}); err != nil {
		t.Fatal(err)
	}
	if err := runCommand(ctx, []string{"--profile", path, "--key", "same-key", "--wait", "--queue", d.URL, "--token", "fixture"}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	j, err = c.GetJob(ctx, r.JobID)
	if err != nil || j.Completion == nil || j.Completion.CollectedAt == nil {
		t.Fatal("same-key replay did not durably collect original job", err)
	}
}
