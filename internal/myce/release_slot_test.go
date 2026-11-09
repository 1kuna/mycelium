package myce

import (
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
				var refusal *HTTPError
				if !errors.As(err, &refusal) || refusal.Code != "fleet_slot_unapproved" || !strings.Contains(err.Error(), fleetBytesHash(f.old)) || !strings.Contains(err.Error(), fleetBytesHash(slot)) {
					t.Fatalf("missing identities/refusal: %v", err)
				}
				if f.launches != 0 || f.restarts != 0 || f.fetches != 0 {
					t.Fatal("unknown slot mutated installation")
				}
				if _, err := os.Stat(filepath.Join(f.c.State, fleetUpdateReceiptFile)); !os.IsNotExist(err) {
					t.Fatal("unknown slot wrote intent", err)
				}
				f.assertBinary(slot)
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			receipt, err := readFleetReceipt(filepath.Join(f.c.State, fleetUpdateReceiptFile))
			if err != nil {
				t.Fatal(err)
			}
			if state == "approved" {
				r := receipt.SlotReconciliation
				if r == nil || r.Running.SHA256 != fleetBytesHash(f.old) || r.Slot.SHA256 != fleetBytesHash(f.next) || r.Slot.Version != f.i.Release.Manifest.Version || r.RollbackTarget != "installed_slot" || receipt.BeforeSHA256 != r.Slot.SHA256 {
					t.Fatalf("ambiguous rollback: %+v", receipt)
				}
			} else if receipt.SlotReconciliation != nil {
				t.Fatal("matching images reported mismatch")
			}
			if err := f.apply(); err != nil {
				t.Fatal(err)
			}
			f.assertBinary(f.next)
			if f.restarts != 1 {
				t.Fatal("did not activate installed release")
			}
		})
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
