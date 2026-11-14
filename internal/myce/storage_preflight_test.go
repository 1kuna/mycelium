package myce

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestFleetStorageFailureReceiptAndQueue(t *testing.T) {
	for _, fault := range []string{"open", "state", "inventory"} {
		t.Run(fault, func(t *testing.T) {
			f := newFleetInstallFixture(t, "darwin")
			f.i.StorageDiagnostics = true
			want := StoragePreflightFailure{Root: "checkpoint", Operation: "open", Errno: int(syscall.EISDIR)}
			if fault == "inventory" {
				f.c.RequireMountedNAS = true
				data, _ := json.Marshal(f.c)
				if err := os.WriteFile(f.c.Config, data, 0600); err != nil {
					t.Fatal(err)
				}
				command := f.deps.command
				f.deps.command = func(ctx context.Context, name string, args ...string) ([]byte, error) {
					if name == cachedMountInventoryCommand {
						return nil, &os.PathError{Op: "open", Path: "/private/fixture-secret", Err: syscall.EACCES}
					}
					return command(ctx, name, args...)
				}
				want.Root, want.Errno = "nas", int(syscall.EACCES)
			}
			if err := f.launch(); err != nil {
				t.Fatal(err)
			}
			switch fault {
			case "open":
				dir := filepath.Join(f.c.NAS, "mycelium", "checkpoints")
				if err := os.MkdirAll(storageProbePath(dir, ".myce-install-probe-", "install"), 0700); err != nil {
					t.Fatal(err)
				}
			case "state":
				if err := os.Remove(filepath.Join(f.c.ModelCache, FormatMarker)); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(f.c.ModelCache, "legacy"), []byte("owned data"), 0600); err != nil {
					t.Fatal(err)
				}
				want.Root, want.Operation, want.Errno = "model_cache", "format", 0
			}
			if err := f.apply(); err == nil || err.Error() != "fleet_storage_preflight_failed" {
				t.Fatal(err)
			}
			f.assertBinary(f.old)
			if f.restarts != 0 || f.fetches != 0 {
				t.Fatal("preflight activated release")
			}
			r, err := readFleetReceipt(filepath.Join(f.c.State, fleetUpdateReceiptFile))
			if err != nil {
				t.Fatal(err)
			}
			got := r.StorageFailure
			if got == nil || got.Root != want.Root || got.Operation != want.Operation || got.Errno != want.Errno || got.Elapsed <= 0 || got.TimedOut {
				t.Fatalf("receipt lost failure: %+v, want %+v", got, want)
			}
			replay, err := ReadFleetUpdateFailure(f.c)
			if err != nil || replay == nil || !reflect.DeepEqual(got, replay.StorageFailure) {
				t.Fatalf("replay lost failure: %+v %v", replay, err)
			}
			// Both direct acknowledgement and outage replay use the same sanitized payload.
			var live FleetUpgradeAck
			client := Client{URL: "http://queue.invalid", HTTP: fleetCaptureAckClient(t, &live)}
			if err := fleetAck(context.Background(), client, r); err != nil || !reflect.DeepEqual(got, live.StorageFailure) {
				t.Fatalf("live ack lost detail: %+v %v", live, err)
			}
			q, _, _ := fleetFixture(t, "queue", "worker")
			if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
				t.Fatal(err)
			}
			fleetTick(t, q)
			i := fleetInstructionFor(t, q)
			if !i.StorageDiagnostics {
				t.Fatal("queue omitted diagnostic capability")
			}
			replay.Operation, replay.Revision, replay.Host, replay.Machine, replay.Session = i.Operation, i.Revision, i.Host, i.Machine, i.Session
			if err := q.AckFleetUpgrade(*replay); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, fleetHostStatus(t, q, i.Host).StorageFailure) {
				t.Fatal("status lost storage detail")
			}
		})
	}
}

func TestOnboardingStorageFailureEvidence(t *testing.T) {
	for _, root := range []string{"checkpoint", "model", "environment"} {
		for _, operation := range []string{"open", "fsync", "read", "rename"} {
			t.Run(root+"/"+operation, func(t *testing.T) {
				c, command := onboardingNAS(t, "darwin")
				paths := map[string]string{"checkpoint": c.Checkpoints, "model": c.ModelRoot, "environment": c.EnvironmentRoot}
				fs := onboardingStorageFS{lstat: os.Lstat, mkdir: os.Mkdir, probeContext: func(ctx context.Context, abort context.CancelCauseFunc, dir string) error {
					if dir != paths[root] {
						return nil
					}
					return onboardingFSError(ctx, abort, func() error { return &os.PathError{Op: operation, Path: "/private/fixture-secret", Err: syscall.EIO} }, operation)
				}}
				_, err := prepareOnboardingStorageFS(context.Background(), c, false, command, fs)
				requirePreflightCode(t, err, "nas_unreachable")
				got := storageFailureDetail(err)
				if got == nil || got.Root != root || got.Operation != operation || got.Errno != int(syscall.EIO) || got.Elapsed <= 0 {
					t.Fatalf("probe evidence discarded: %+v", got)
				}
				data, _ := json.Marshal(got)
				if strings.Contains(string(data), "private") || strings.Contains(err.Error(), "fixture-secret") {
					t.Fatal("private error exposed")
				}
			})
		}
	}
}

func TestFleetStorageFailureSanitizesReceiptAndStatus(t *testing.T) {
	f := newFleetInstallFixture(t, "darwin")
	f.i.StorageDiagnostics = true
	if err := f.launch(); err != nil {
		t.Fatal(err)
	}
	r, _ := readFleetReceipt(filepath.Join(f.c.State, fleetUpdateReceiptFile))
	r.Phase, r.Failure = "failed", "fleet_storage_preflight_failed"
	r.StorageFailure = &StoragePreflightFailure{Root: "/private/secret", Operation: "secret text", Errno: -1, Elapsed: -time.Second}
	if err := writeFleetReceipt(f.c, r); err != nil {
		t.Fatal(err)
	}
	replay, err := ReadFleetUpdateFailure(f.c)
	if err != nil {
		t.Fatal(err)
	}
	want := &StoragePreflightFailure{Root: "unknown", Operation: "unknown"}
	if !reflect.DeepEqual(replay.StorageFailure, want) {
		t.Fatalf("unsafe replay: %+v", replay)
	}
	q, _, _ := fleetFixture(t, "queue", "worker")
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	i := fleetInstructionFor(t, q)
	if err := q.AckFleetUpgrade(FleetUpgradeAck{Operation: i.Operation, Revision: i.Revision, Host: i.Host, Machine: i.Machine, Session: i.Session, Phase: "failed", Failure: r.Failure, StorageFailure: r.StorageFailure}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(fleetHostStatus(t, q, i.Host).StorageFailure, want) {
		t.Fatal("unsafe status")
	}
	// Errors remain inspectable without leaking paths into structured diagnostics.
	err = storageCheckError("checkpoint", "write", time.Second, &os.PathError{Op: "write", Path: "/private/secret", Err: syscall.ENOSPC})
	if !errors.Is(err, syscall.ENOSPC) {
		t.Fatal("lost error identity")
	}
}

func fleetCaptureAckClient(t *testing.T, ack *FleetUpgradeAck) *http.Client {
	t.Helper()
	return &http.Client{Transport: clientTestTransport(func(r *http.Request) (*http.Response, error) {
		if err := json.NewDecoder(r.Body).Decode(ack); err != nil {
			t.Error(err)
		}
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("{}"))}, nil
	})}
}

func TestFleetStorageFailureWithPreviousQueue(t *testing.T) {
	f := newFleetInstallFixture(t, "darwin") // Previous queues omit storage_diagnostics.
	if err := f.launch(); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(f.c.NAS, "mycelium", "checkpoints")
	if err := os.MkdirAll(storageProbePath(dir, ".myce-install-probe-", "install"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := f.apply(); err == nil {
		t.Fatal("storage refusal missing")
	}
	r, err := readFleetReceipt(filepath.Join(f.c.State, fleetUpdateReceiptFile))
	if err != nil {
		t.Fatal(err)
	}
	if r.StorageFailure == nil {
		t.Fatal("previous queue erased local diagnostics")
	}
	var body strings.Builder
	client := Client{URL: "http://queue.invalid", HTTP: &http.Client{Transport: clientTestTransport(func(req *http.Request) (*http.Response, error) {
		data, err := io.ReadAll(req.Body)
		if err != nil {
			t.Fatal(err)
		}
		body.Write(data)
		// The previous queue has a strict decoder with only these fields.
		var old struct {
			Operation, Host, Machine, Session, Phase, Failure string
			Revision                                          int
		}
		decoder := json.NewDecoder(strings.NewReader(string(data)))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&old); err != nil {
			t.Errorf("broke previous queue: %v", err)
		}
		if old.Failure != "fleet_storage_preflight_failed" {
			t.Error("lost failure code")
		}
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("{}"))}, nil
	})}}
	if err := fleetAck(context.Background(), client, r); err != nil {
		t.Fatal(err)
	}
	replay, err := ReadFleetUpdateFailure(f.c)
	if err != nil || replay.StorageFailure != nil {
		t.Fatal("unsafe legacy replay", err)
	}
	if strings.Contains(body.String(), "storage_failure") {
		t.Fatal("unsupported field sent to previous queue")
	}
}
