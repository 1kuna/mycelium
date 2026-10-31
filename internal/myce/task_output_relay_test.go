package myce

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTaskOutputsSubmitterHTTPStreamingAndPartialPull(t *testing.T) {
	f := newTaskOutputFixture(t, "submitter", "")
	f.pending(t)
	j := job(t, f.q, f.j.ID)
	receipt := taskOutputReceipt(*j)
	// A cancelled/partial pull cannot produce a delivery receipt or a disk relay.
	ctx, cancel := context.WithCancel(context.Background())
	res, err := f.w.Client.fileRequest(ctx, "GET", "/api/task-outputs/"+j.ID+"/files/out.txt?manifest="+j.Delivery.ManifestSHA256, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	res.Body.Close()
	await(t, func() bool {
		f.q.taskOutputMu.Lock()
		defer f.q.taskOutputMu.Unlock()
		return len(f.q.taskOutputRelays) == 0
	})
	if job(t, f.q, j.ID).Delivery.State != "pending" {
		t.Fatal("partial pull verified delivery")
	}
	dest := t.TempDir()
	done := make(chan error, 1)
	go func() { done <- f.w.Client.DeliverTaskOutputs(context.Background(), j.ID, dest) }()
	for range 2 {
		await(t, func() bool {
			f.q.taskOutputMu.Lock()
			defer f.q.taskOutputMu.Unlock()
			for _, r := range f.q.taskOutputRelays {
				if !r.claimed {
					return true
				}
			}
			return false
		})
		f.pending(t)
	}
	if err = <-done; err != nil {
		t.Fatal("submitter pull failed", err)
	}
	j = job(t, f.q, j.ID)
	if j.Delivery.State != "verified" {
		t.Fatal("synced submitter receipt not accepted", j.Delivery)
	}
	for name, ref := range receipt.Files {
		if err = verifyFile(context.Background(), filepath.Join(dest, j.ID, fmt.Sprint(j.Attempt), name), ref); err != nil {
			t.Fatal(err)
		}
	}
	if blobs, err := f.q.files.inventory(context.Background()); err != nil || len(blobs) != 0 {
		t.Fatal("relay allocated shared payloads", blobs, err)
	}
	// Lost ACK/replay verifies the existing local tree without another stream.
	if err = f.w.Client.DeliverTaskOutputs(context.Background(), j.ID, dest); err != nil {
		t.Fatal("submitter replay failed", err)
	}
	if err = os.WriteFile(filepath.Join(dest, j.ID, fmt.Sprint(j.Attempt), "out.txt"), []byte("bad"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = f.w.Client.DeliverTaskOutputs(context.Background(), j.ID, dest); err == nil {
		t.Fatal("corrupt submitter receipt replay accepted")
	}
	// A historical submitter receipt cannot authorize collection while that
	// submitter is absent. Reconnecting streams its current destination bytes
	// for a fresh worker-side hash immediately before collection.
	data, err := os.ReadFile(filepath.Join(deliverySpoolPath(f.j, f.store.dir), "data", "out.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dest, j.ID, fmt.Sprint(j.Attempt), "out.txt"), data, 0600); err != nil {
		t.Fatal(err)
	}
	f.c.Advance(DefaultFileRetention)
	if err = f.q.Tick(); err != nil {
		t.Fatal(err)
	}
	f.pending(t)
	taskOutputAssertSpool(t, f, true)
	if job(t, f.q, j.ID).Delivery.State != "failed" {
		t.Fatal("absent submitter retained stale verification")
	}
	receipt = taskOutputReceipt(*job(t, f.q, j.ID))
	if err = f.q.ConfirmTaskOutputs(context.Background(), receipt); err != nil {
		t.Fatal(err)
	}
	go func() {
		res, err := f.w.Client.fileRequest(context.Background(), "PUT", "/api/task-outputs/"+j.ID+"/preservation", strings.NewReader("partial"), map[string]string{"X-Myce-Delivery": receipt.ManifestSHA256, "X-Myce-Session": receipt.Session, "X-Myce-Machine": receipt.Machine})
		if err == nil {
			err = fileTransferError(res, "partial destination proof")
			res.Body.Close()
		}
		done <- err
	}()
	await(t, func() bool {
		f.q.taskOutputMu.Lock()
		defer f.q.taskOutputMu.Unlock()
		return f.q.taskOutputPreservation != nil
	})
	f.pending(t)
	if err = <-done; err == nil {
		t.Fatal("partial destination proof accepted")
	}
	taskOutputAssertSpool(t, f, true)
	if job(t, f.q, j.ID).LocalOutputs.CollectedAt != nil {
		t.Fatal("partial proof released custody")
	}
	go func() { done <- f.w.Client.DeliverTaskOutputs(context.Background(), j.ID, dest) }()
	await(t, func() bool {
		select {
		case err := <-done:
			done <- err
			t.Fatalf("submitter exited before preservation: %v", err)
		default:
		}
		f.q.taskOutputMu.Lock()
		defer f.q.taskOutputMu.Unlock()
		return f.q.taskOutputPreservation != nil
	})
	if err = f.step(t); err != nil {
		t.Fatal("fresh submitter preservation failed", err)
	}
	if err = <-done; err != nil {
		t.Fatal("submitter collection acknowledgement", err)
	}
	taskOutputAssertSpool(t, f, false)
	if job(t, f.q, j.ID).LocalOutputs.CollectedAt == nil {
		t.Fatal("submitter backing not collected")
	}
}
