package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
	"testing/synctest"
	"time"
)

func TestInstallStorageClassifiesMisconfigurationAndReachability(t *testing.T) {
	for _, fault := range []struct {
		name, code string
		errno      syscall.Errno
		transient  bool
	}{
		{"missing", "nas_unreachable", syscall.ENOENT, false},
		{"permission", "nas_unreachable", syscall.EACCES, false},
		{"wrong-filesystem", "nas_not_mounted", 0, false},
		{"io", "nas_unreachable", syscall.EIO, true},
		{"unreachable", "nas_unreachable", syscall.EHOSTUNREACH, true},
	} {
		t.Run(fault.name, func(t *testing.T) {
			c, command := onboardingNAS(t, "linux")
			fs := onboardingStorageFS{lstat: os.Lstat, mkdir: os.Mkdir, probeContext: func(ctx context.Context, _ context.CancelCauseFunc, _ string) error {
				return storageRootRefusal(ctx, "rename", preflightError(fault.code, "storage", "root probe failed"), storageCheckError("checkpoint", "rename", time.Millisecond, fault.errno))
			}}
			if fault.name == "missing" {
				fs.lstat = func(path string) (os.FileInfo, error) {
					if path == c.NAS {
						return nil, syscall.ENOENT
					}
					return os.Lstat(path)
				}
			}
			if fault.name == "wrong-filesystem" {
				command = func(context.Context, string, ...string) ([]byte, error) {
					return []byte("source " + c.NAS + " ext4 rw 0 0\n"), nil
				}
			}
			for _, preflight := range []bool{false, true} {
				var warning *StoragePreflightFailure
				var err error
				if preflight {
					warning, err = preflightFleetStorageResult(context.Background(), c, command, &fs)
				} else {
					warning, err = prepareInstallStorage(context.Background(), c, command, installStorageWrite, &fs)
				}
				if fault.transient {
					if err != nil || warning == nil || warning.Errno != int(fault.errno) {
						t.Fatalf("lost transient warning: %+v %v", warning, err)
					}
				} else {
					requirePreflightCode(t, err, fault.code)
					if warning != nil {
						t.Fatal("misconfiguration downgraded")
					}
				}
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := installStorageOutcome(ctx, storageCheckError("nas", "inventory", time.Second, context.DeadlineExceeded)); !errors.Is(err, context.Canceled) {
		t.Fatal("caller cancellation swallowed", err)
	}
}

func TestFleetInstallStorageWarningReceiptAndStatus(t *testing.T) {
	f := newFleetInstallFixture(t, "linux")
	f.i.StorageWarnings = true
	if err := f.launch(); err != nil {
		t.Fatal(err)
	}
	// A fake stalled syscall preserves the same innermost evidence as the
	// channel-blocked rename above, without involving the fixture HTTP timers.
	f.deps.storageFS = &onboardingStorageFS{lstat: os.Lstat, mkdir: os.Mkdir, probeContext: func(ctx context.Context, _ context.CancelCauseFunc, _ string) error {
		return storageCheckError(storageContextRoot(ctx), "rename", onboardingFilesystemStallBudget, errCheckpointIOTimeout)
	}}
	mounts := f.deps.command
	f.deps.command = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		if name == "cat" {
			return []byte("source " + f.c.NAS + " nfs4 rw 0 0\n"), nil
		}
		return mounts(ctx, name, args...)
	}
	if err := f.apply(); err != nil {
		t.Fatal(err)
	}
	f.assertBinary(f.next)
	if f.restarts != 1 {
		t.Fatal("warning blocked restart")
	}
	r, err := readFleetReceipt(filepath.Join(f.c.State, fleetUpdateReceiptFile))
	if err != nil {
		t.Fatal(err)
	}
	if r.Phase != "restarting" || r.StorageWarning == nil || r.StorageFailure != nil || r.StorageWarning.Operation != "rename" || !r.StorageWarning.TimedOut {
		t.Fatalf("lost advisory receipt: %+v", r)
	}
	var ack FleetUpgradeAck
	if err := fleetAck(context.Background(), Client{URL: "http://queue.invalid", HTTP: fleetCaptureAckClient(t, &ack)}, r); err != nil {
		t.Fatal(err)
	}
	q, _, _ := fleetFixture(t, "queue", "worker")
	if _, err := q.StartFleetUpgrade(FleetUpgradeRequest{}); err != nil {
		t.Fatal(err)
	}
	fleetTick(t, q)
	i := fleetInstructionFor(t, q)
	ack.Operation, ack.Revision, ack.Host, ack.Machine, ack.Session = i.Operation, i.Revision, i.Host, i.Machine, i.Session
	if err := q.AckFleetUpgrade(ack); err != nil {
		t.Fatal(err)
	}
	got := fleetHostStatus(t, q, i.Host)
	if got.Phase != "worker_health" || got.Failure != "" || !reflect.DeepEqual(got.StorageWarning, r.StorageWarning) {
		t.Fatalf("warning blocked rollout or lost status: %+v", got)
	}
	r.Instruction.StorageWarnings = false
	if fleetReceiptStorageWarning(r) != nil {
		t.Fatal("unsupported field sent to previous queue")
	}
}

func TestFleetStorageStalledRenameIsAdvisory(t *testing.T) {
	c, command := onboardingNAS(t, "darwin")
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		defer joinOnboardingNASSequence()
		defer close(release)
		fs := onboardingStorageFS{lstat: os.Lstat, mkdir: os.Mkdir, probeContext: func(ctx context.Context, abort context.CancelCauseFunc, _ string) error {
			return onboardingFSError(ctx, abort, func() error { <-release; return nil }, "rename")
		}}
		warning, err := preflightFleetStorageResult(context.Background(), c, command, &fs)
		if err != nil || warning == nil || warning.Root != "checkpoint" || warning.Operation != "rename" || !warning.TimedOut || warning.Elapsed != onboardingFilesystemStallBudget {
			t.Fatalf("fleet preflight blocked or lost warning: %+v %v", warning, err)
		}
	})
}
