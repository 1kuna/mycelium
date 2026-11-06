package myce

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
)

// ReleaseSlotIdentity names saved bytes, independently of a running process.
// Version is present only after matching a verified approved artifact.
type ReleaseSlotIdentity struct {
	SHA256  string `json:"sha256"`
	Size    int64  `json:"size"`
	Version string `json:"version,omitempty"`
}

type slotReconciliation struct {
	Running        FleetRuntimeProof   `json:"running"`
	Slot           ReleaseSlotIdentity `json:"slot"`
	RollbackTarget string              `json:"rollback_target"`
}

func slotRefusal(running FleetRuntimeProof, slot ReleaseSlotIdentity) error {
	return &HTTPError{Status: 409, Code: "fleet_slot_unapproved", Message: fmt.Sprintf("fleet_slot_unapproved: running=%s/sha256:%s slot=sha256:%s; verify and approve the installed release before retrying", running.Version, running.SHA256, slot.SHA256)}
}

// The caller obtains approval over the existing authenticated queue connection;
// a signature alone is not approval. Never execute unknown slot bytes.
func qualifyReleaseSlot(c ServiceConfig, running FleetRuntimeProof, slot ReleaseSlotIdentity, approved EnrollmentRelease, key ed25519.PublicKey) (ReleaseSlotIdentity, error) {
	if slot.SHA256 == running.SHA256 {
		slot.Version = running.Version
		return slot, nil
	}
	m, err := VerifyRelease(approved.SignedRelease, key)
	if err != nil || !reflect.DeepEqual(m, approved.Manifest) {
		return slot, slotRefusal(running, slot)
	}
	a, err := m.Select(c.Detection.OS, c.Detection.Arch)
	if err != nil || a.SHA256 != slot.SHA256 || a.Size != slot.Size {
		return slot, slotRefusal(running, slot)
	}
	slot.Version = m.Version
	return slot, nil
}

func readReleaseSlot(path string) (ReleaseSlotIdentity, error) {
	st, err := os.Lstat(path)
	if err != nil || !st.Mode().IsRegular() {
		return ReleaseSlotIdentity{}, fmt.Errorf("fleet_binary_unavailable")
	}
	hash, err := FileSHA256(path)
	if err != nil {
		return ReleaseSlotIdentity{}, err
	}
	after, err := os.Lstat(path)
	if err != nil || !os.SameFile(st, after) || st.Size() != after.Size() || !st.ModTime().Equal(after.ModTime()) {
		return ReleaseSlotIdentity{}, fmt.Errorf("fleet_install_changed")
	}
	return ReleaseSlotIdentity{SHA256: hash, Size: st.Size()}, nil
}

// Existing fleet-capable workers can reconcile via the ordinary installer.
// Legacy/non-fleet installs retain their existing install path.
func preflightInstallSlot(ctx context.Context, c ServiceConfig, command func(context.Context, string, ...string) ([]byte, error)) (*slotReconciliation, error) {
	if c.Role != "worker" || c.HostID == "" {
		return nil, nil
	}
	key, err := readFleetPublisher(filepath.Join(c.State, "fleet-publisher.pub"))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	state, err := (installManager{c.Detection.OS, command}).inspect(ctx, installUnit{path: c.Service, label: "local.mycelium." + c.Role})
	if err != nil {
		return nil, err
	}
	if !state.active {
		return nil, nil
	} // No live image to reconcile on a stopped service.
	client, err := fleetClient(c)
	if err != nil {
		return nil, err
	}
	client = fleetNoRedirect(client)
	defer client.HTTP.CloseIdleConnections()
	var status HostMaintenanceStatus
	if err = client.Call(ctx, "GET", "/api/hosts/"+url.PathEscape(c.HostID)+"/maintenance", nil, &status); err != nil {
		return nil, err
	}
	var running *FleetRuntimeProof
	for _, w := range status.Workers {
		if w.Machine.ID == c.Name && w.Fresh && w.Machine.Fleet != nil && w.Machine.Fleet.Session == w.Machine.Session && fleetProofReady(w.Machine.Fleet) {
			running = w.Machine.Fleet
		}
	}
	if running == nil {
		return nil, fmt.Errorf("fleet_existing_service_unavailable")
	}
	identity, err := EnrollmentServiceHealth(ctx, c, command)
	if err != nil || !strings.HasPrefix(identity, strconv.Itoa(running.PID)+"/") {
		return nil, fmt.Errorf("fleet_existing_service_unavailable")
	}
	slot, err := readReleaseSlot(c.Binary)
	if err != nil {
		return nil, err
	}
	if slot.SHA256 == running.SHA256 {
		slot.Version = running.Version
		return &slotReconciliation{Running: *running, Slot: slot, RollbackTarget: "installed_slot"}, nil
	}
	var approved EnrollmentRelease
	if err = client.Call(ctx, "GET", "/api/invites/release", nil, &approved); err != nil {
		return nil, slotRefusal(*running, slot)
	}
	slot, err = qualifyReleaseSlot(c, *running, slot, approved, key)
	if err != nil {
		return nil, err
	}
	return &slotReconciliation{Running: *running, Slot: slot, RollbackTarget: "installed_slot"}, nil
}

// Atomic slot publication changes file identity. Reuse a hash only while local
// identity, size and modification time agree; install preflight never uses this
// observation cache. No file I/O occurs under a queue lock.
var releaseSlotCache sync.Map

type releaseSlotObservation struct {
	info os.FileInfo
	slot ReleaseSlotIdentity
}

func observeReleaseSlot(path string) (ReleaseSlotIdentity, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return ReleaseSlotIdentity{}, err
	}
	if value, ok := releaseSlotCache.Load(path); ok {
		old := value.(releaseSlotObservation)
		if os.SameFile(old.info, info) && old.info.Size() == info.Size() && old.info.ModTime().Equal(info.ModTime()) {
			return old.slot, nil
		}
	}
	slot, err := readReleaseSlot(path)
	if err == nil {
		releaseSlotCache.Store(path, releaseSlotObservation{info, slot})
	}
	return slot, err
}
