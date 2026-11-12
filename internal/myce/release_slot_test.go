package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The helper installs over the running bytes, or restarts onto the release
// already in place; any other installed bytes are refused untouched.
func TestReleaseSlotFleetReconciliation(t *testing.T) {
	for _, state := range []string{"approved", "unknown", "match"} {
		t.Run(state, func(t *testing.T) {
			f := newFleetInstallFixture(t, "linux")
			slot := f.old
			if state == "approved" {
				slot = f.next
			}
			if state == "unknown" {
				slot = []byte("unknown unapproved executable")
			}
			if err := os.WriteFile(f.c.Binary, slot, 0700); err != nil {
				t.Fatal(err)
			}
			err := f.launch()
			if state == "unknown" {
				if err == nil || err.Error() != "fleet_install_changed" || f.launches != 0 || f.restarts != 0 || f.fetches != 0 {
					t.Fatalf("unknown slot not refused untouched: %v", err)
				}
				if _, err := os.Stat(filepath.Join(f.c.State, fleetUpdateReceiptFile)); !os.IsNotExist(err) {
					t.Fatal("unknown slot wrote a receipt", err)
				}
				f.assertBinary(slot)
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := f.apply(); err != nil {
				t.Fatal(err)
			}
			f.assertBinary(f.next)
			if f.restarts != 1 || (state == "approved") != (f.fetches == 0) {
				t.Fatal("did not activate the installed release", f.restarts, f.fetches)
			}
		})
	}
}

func TestReleaseSlotInstallerReconciliation(t *testing.T) {
	for _, state := range []string{"approved", "unknown", "match"} {
		t.Run(state, func(t *testing.T) {
			f := newFleetInstallFixture(t, "linux")
			slot := f.old
			if state == "approved" {
				slot = f.next
			}
			if state == "unknown" {
				slot = []byte("unapproved saved bytes")
			}
			if err := os.WriteFile(f.c.Binary, slot, 0700); err != nil {
				t.Fatal(err)
			}
			candidate := filepath.Join(t.TempDir(), "candidate")
			if err := os.WriteFile(candidate, []byte("next installer candidate"), 0700); err != nil {
				t.Fatal(err)
			}
			injected := errors.New("synthetic activation failure")
			restarts := 0
			command := func(_ context.Context, name string, args ...string) ([]byte, error) {
				if name != "systemctl" {
					t.Fatalf("unexpected command: %s", name)
				}
				text := strings.Join(args, " ")
				if strings.Contains(text, "MainPID") {
					return []byte(fmt.Sprintf("ActiveState=active\nSubState=running\nMainPID=%d\nNRestarts=0\nResult=success\n", f.managerPID)), nil
				}
				if strings.Contains(text, "UnitFileState") {
					return []byte("enabled\n"), nil
				}
				if len(args) > 1 && args[1] == "restart" {
					restarts++
					if restarts == 1 {
						return nil, injected
					}
				}
				return nil, nil
			}
			err := installWithFiles(context.Background(), f.c, candidate, "", command, installFiles{})
			if state == "unknown" {
				var refusal *HTTPError
				if !errors.As(err, &refusal) || refusal.Code != "fleet_slot_unapproved" || !strings.Contains(err.Error(), fleetBytesHash(f.old)) || !strings.Contains(err.Error(), fleetBytesHash(slot)) || restarts != 0 {
					t.Fatalf("unknown slot not safely refused: %v restarts=%d", err, restarts)
				}
			} else if !errors.Is(err, injected) || strings.Contains(err.Error(), "rollback incomplete") {
				t.Fatalf("activation did not restore snapshot: %v", err)
			}
			f.assertBinary(slot)
			// A failed install must roll back its reconciliation receipt too.
			if _, err := os.Stat(filepath.Join(f.c.State, "slot-reconciliation.json")); !os.IsNotExist(err) {
				t.Fatal("rollback retained new reconciliation", err)
			}
			if state == "approved" {
				// A successful install retains the explicit rollback choice and identities.
				restarts = 1
				if err := installWithFiles(context.Background(), f.c, candidate, "", command, installFiles{}); err != nil {
					t.Fatal(err)
				}
				data, err := os.ReadFile(filepath.Join(f.c.State, "slot-reconciliation.json"))
				var receipt slotReconciliation
				if err != nil || json.Unmarshal(data, &receipt) != nil || receipt.Slot.SHA256 != fleetBytesHash(slot) || receipt.Running.SHA256 != fleetBytesHash(f.old) || receipt.RollbackTarget != "installed_slot" {
					t.Fatalf("missing reconciliation receipt: %s %v", data, err)
				}
			}
		})
	}
}

func TestReleaseSlotObservationDetectsPublicationBeforeInstall(t *testing.T) {
	path := filepath.Join(t.TempDir(), "installed")
	if err := os.WriteFile(path, []byte("running image"), 0700); err != nil {
		t.Fatal(err)
	}
	before, err := observeReleaseSlot(path)
	if err != nil {
		t.Fatal(err)
	}
	replacement := path + ".next"
	if err := os.WriteFile(replacement, []byte("approved slot ahead"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, path); err != nil {
		t.Fatal(err)
	}
	after, err := observeReleaseSlot(path)
	if err != nil || before.SHA256 == after.SHA256 || after.SHA256 != fleetBytesHash([]byte("approved slot ahead")) {
		t.Fatalf("publication concealed: %+v %v", after, err)
	}
	// The normal runtime observer reports the mismatch without any install.
	c := fleetRuntimeTestConfig(t)
	c.Binary = path
	command := func(context.Context, string, ...string) ([]byte, error) {
		return []byte(fmt.Sprintf("ActiveState=active\nSubState=running\nMainPID=%d\nNRestarts=0\nResult=success\n", os.Getpid())), nil
	}
	p, err := collectFleetRuntime(context.Background(), c, "running-session", command)
	if err != nil || p == nil || !p.SlotMismatch || p.InstalledSlot == nil || p.InstalledSlot.SHA256 != after.SHA256 || p.SHA256 == after.SHA256 {
		t.Fatalf("runtime failed to observe mismatch: %+v %v", p, err)
	}
	encoded, err := json.Marshal(p)
	if err != nil || !bytes.Contains(encoded, []byte(`"slot_mismatch":true`)) || !bytes.Contains(encoded, []byte(p.SHA256)) || !bytes.Contains(encoded, []byte(after.SHA256)) {
		t.Fatalf("status lost slot mismatch: %s %v", encoded, err)
	}
}

func TestReleaseSlotAfterFailedFleetActivation(t *testing.T) {
	f := newFleetInstallFixture(t, "linux")
	if err := f.launch(); err != nil {
		t.Fatal(err)
	}
	f.restartError = true
	if err := f.apply(); err == nil || err.Error() != "fleet_restart_failed" {
		t.Fatalf("expected failed activation: %v", err)
	}
	f.assertBinary(f.next)
	// A later explicit operation must retain the published slot independently
	// of the unchanged running process, rather than silently conflating them.
	f.restartError = false
	f.i.Operation = "explicit-recovery-operation"
	if err := f.launch(); err != nil {
		t.Fatal(err)
	}
	r, err := readFleetReceipt(filepath.Join(f.c.State, fleetUpdateReceiptFile))
	if err != nil || r.BeforeRuntime.SHA256 != fleetBytesHash(f.old) || r.BeforeSHA256 != fleetBytesHash(f.next) {
		t.Fatalf("failed activation lost identities: %+v %v", r, err)
	}
	if err := f.apply(); err != nil || f.restarts != 2 || f.fetches != 1 {
		t.Fatal("rerun did not restart onto the published release", err, f.restarts, f.fetches)
	}
}

func TestReleaseSlotRepairVerifiedAddress(t *testing.T) {
	f := newFleetInstallFixture(t, "linux")
	server, authority := joiningTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/maintenance") {
			_ = json.NewEncoder(w).Encode(HostMaintenanceStatus{Workers: []MaintenanceWorker{{Fresh: true, Machine: Machine{ID: f.c.Name, Session: f.i.Session, Fleet: &FleetRuntimeProof{Version: f.runtimeVersion, SHA256: fleetBytesHash(f.old), Session: f.i.Session, PID: f.runtimePID, ServiceHealthy: true, RunnerSmoke: true, StartGate: StartGateProtocol, AttemptProtocol: AttemptProtocol}}}}})
		}
	}))
	stale := httptest.NewTLSServer(nil)
	oldAddress := stale.URL
	stale.Close()
	authority.Address = oldAddress
	f.c.Queue = oldAddress
	f.c.JoiningFile = filepath.Join(f.c.State, "joining.json")
	if err := WriteInvitation(f.c.JoiningFile, authority); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(f.c)
	if err := os.WriteFile(f.c.Config, data, 0600); err != nil {
		t.Fatal(err)
	}
	next := f.c
	next.Queue = server.URL
	source := filepath.Join(t.TempDir(), "candidate")
	if err := os.WriteFile(source, f.old, 0700); err != nil {
		t.Fatal(err)
	}
	command := func(_ context.Context, name string, args ...string) ([]byte, error) {
		if name != "systemctl" {
			t.Fatalf("unexpected manager: %s", name)
		}
		text := strings.Join(args, " ")
		if strings.Contains(text, "MainPID") {
			return []byte(fmt.Sprintf("ActiveState=active\nSubState=running\nMainPID=%d\nNRestarts=0\nResult=success\n", f.managerPID)), nil
		}
		if strings.Contains(text, "UnitFileState") {
			return []byte("enabled\n"), nil
		}
		return nil, nil
	}
	err := installWithFiles(context.Background(), next, source, "", command, installFiles{})
	if err != nil {
		t.Fatalf("verified queue address repair failed: %v", err)
	}
}

func TestReleaseSlotMatchThenChangesBeforeSnapshot(t *testing.T) {
	f := newFleetInstallFixture(t, "linux")
	candidate := filepath.Join(t.TempDir(), "candidate")
	if err := os.WriteFile(candidate, f.next, 0700); err != nil {
		t.Fatal(err)
	}
	unitReads, restarts := 0, 0
	unapproved := []byte("unknown bytes published after preflight")
	failure := errors.New("synthetic activation failure")
	command := func(_ context.Context, name string, args ...string) ([]byte, error) {
		if name != "systemctl" {
			t.Fatalf("unexpected command %s", name)
		}
		text := strings.Join(args, " ")
		if strings.Contains(text, "MainPID") {
			return []byte(fmt.Sprintf("ActiveState=active\nSubState=running\nMainPID=%d\nNRestarts=0\nResult=success\n", f.managerPID)), nil
		}
		if strings.Contains(text, "UnitFileState") {
			unitReads++
			if unitReads == 2 {
				if err := os.WriteFile(f.c.Binary, unapproved, 0700); err != nil {
					t.Fatal(err)
				}
			}
			return []byte("enabled\n"), nil
		}
		if len(args) > 1 && args[1] == "restart" {
			restarts++
			if restarts == 1 {
				return nil, failure
			}
		}
		return nil, nil
	}
	err := installWithFiles(context.Background(), f.c, candidate, "", command, installFiles{})
	got, readErr := os.ReadFile(f.c.Binary)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if errors.Is(err, failure) && string(got) == string(unapproved) {
		t.Fatalf("installer published and then restored unapproved rollback bytes after matching preflight; restarts=%d", restarts)
	}
	if err == nil || err.Error() != "fleet_install_changed" {
		t.Fatalf("wanted slot-change refusal, got %v", err)
	}
}
