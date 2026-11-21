package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type quietWorkerSlot struct {
	Session         string    `json:"session"`
	ServiceIdentity string    `json:"service_identity"`
	ProcessBirth    string    `json:"process_birth"`
	MaintenanceMode string    `json:"maintenance_mode"`
	DrainOwner      string    `json:"drain_owner,omitempty"`
	MaintenanceAt   time.Time `json:"maintenance_at"`
}

type installImageProbe func(context.Context, int, *os.File, func(context.Context, string, ...string) ([]byte, error)) error

var (
	errFleetRecoveryNotQuiescent = errors.New("fleet_recovery_not_quiescent: keep the host drained and wait for execution and cleanup to release")
	errFleetRecoveryWorker       = errors.New("fleet_recovery_worker_unproven: verify the fresh enrolled worker session and its active service")
	errFleetRecoverySlot         = errors.New("fleet_recovery_slot_mismatch: cannot match the installed binary to the active service image")
	errFleetRecoveryChanged      = errors.New("fleet_recovery_runtime_changed: recheck the active service, worker session and host drain before retrying")
)

func (r quietWorkerSlot) matches(p HostMaintenance) bool {
	return r.MaintenanceMode == p.Mode && r.DrainOwner == p.DrainOwner && r.MaintenanceAt.Equal(p.UpdatedAt)
}

func checkQuietWorkerQueue(c ServiceConfig, status HostMaintenanceStatus, worker MaintenanceWorker) error {
	m := worker.Machine
	// The queue owns assignment, preparation and release accounting. Requiring
	// its fresh custody verdict prevents an empty or stale inventory from
	// authorizing a stop. Check the drain even on a zero-obligation host.
	if !status.Quiescent || status.Policy.Mode != "drained" && status.Policy.Mode != "disabled" {
		return errFleetRecoveryNotQuiescent
	}
	p := m.Enrollment
	if status.Host != c.HostID || !worker.Fresh || m.ID != c.Name || hostID(&m) != c.HostID || m.Session == "" || m.Recovering || m.Leaving || m.Gone ||
		m.Fleet != nil || p == nil || !p.RunnerSmoke || p.StartGate != startGateProtocol || p.AttemptProtocol != keyedAttemptProtocol || m.Release == nil || m.Release.Version != p.Version {
		return errFleetRecoveryWorker
	}
	return nil
}

// This evidence authorizes a local idle-service stop. Fleet live takeover
// still requires its own runtime proof.
func reconcileQuietWorkerSlot(ctx context.Context, c ServiceConfig, command func(context.Context, string, ...string) ([]byte, error), imageProbe installImageProbe, status HostMaintenanceStatus, worker MaintenanceWorker) (*slotReconciliation, error) {
	if err := checkQuietWorkerQueue(c, status, worker); err != nil {
		return nil, err
	}
	m, policy := worker.Machine, status.Policy
	// The post-check-in receipt binds the exact local service process to the
	// queue's ordinary enrollment session and reported release.
	receipt, err := ReadEnrollmentReceipt(c.State)
	if err != nil || receipt.Session != m.Session || receipt.Version != m.Release.Version || receipt.ProcessID <= 0 {
		return nil, errFleetRecoveryWorker
	}
	identity, err := EnrollmentServiceHealth(ctx, c, command)
	if err != nil || !strings.HasPrefix(identity, strconv.Itoa(receipt.ProcessID)+"/") {
		return nil, errFleetRecoveryWorker
	}
	birth, err := processBirth(receipt.ProcessID)
	if err != nil {
		return nil, errFleetRecoveryWorker
	}
	// Hash the very descriptor the kernel mapping check verified. A release
	// label or an on-disk path alone can name bytes installed after startup.
	if imageProbe == nil {
		imageProbe = verifyInstallServiceImage
	}
	slot, err := readVerifiedReleaseSlot(c.Binary, func(f *os.File) error {
		if err := imageProbe(ctx, receipt.ProcessID, f, command); err != nil {
			return errFleetRecoverySlot
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if err := checkQuietWorkerSpool(c.State, m.Session); err != nil {
		return nil, err
	}
	if after, err := processBirth(receipt.ProcessID); err != nil || after != birth {
		return nil, errFleetRecoveryWorker
	}
	slot.Version = receipt.Version
	return &slotReconciliation{Slot: slot, RollbackTarget: "installed_slot", Recovery: &quietWorkerSlot{
		Session: m.Session, ServiceIdentity: identity, ProcessBirth: birth,
		MaintenanceMode: policy.Mode, DrainOwner: policy.DrainOwner, MaintenanceAt: policy.UpdatedAt.UTC(),
	}}, nil
}

// A durable local record can precede the worker's next queue report. Preserve
// that custody, including unindexed attempts and detached model owners.
func checkQuietWorkerSpool(dir, session string) error {
	var root workerState
	data, err := os.ReadFile(filepath.Join(dir, "state.json"))
	if err != nil || decodeWorkerState(data, &root) != nil || root.Session != session {
		return errFleetRecoveryWorker
	}
	if root.Job != nil || root.Result != nil || root.StorageDirectory != "" || root.PID != 0 || root.LaunchPending || root.Preparation != "" {
		return errFleetRecoveryNotQuiescent
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return errFleetRecoveryNotQuiescent
	}
	children := map[string]bool{}
	for _, entry := range entries {
		name := entry.Name()
		if name != "legacy-state.json" && !(strings.HasPrefix(name, "state-attempt-") && strings.HasSuffix(name, ".json")) {
			continue
		}
		var child workerState
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil || decodeWorkerState(data, &child) != nil || child.Job != nil || child.Result != nil || child.PID != 0 || child.LaunchPending || child.Preparation != "" {
			return errFleetRecoveryNotQuiescent
		}
		children[name] = true
	}
	for _, name := range root.Attempts {
		if !children[name] {
			return errFleetRecoveryNotQuiescent
		}
	}
	entries, err = os.ReadDir(custodianDir(dir))
	if err != nil {
		return errFleetRecoveryNotQuiescent
	}
	modelOwners := make(map[string]bool)
	var snapshots []string
	for _, entry := range entries {
		name := entry.Name()
		id, kind, ok := strings.Cut(strings.TrimPrefix(name, "custodian-"), ".")
		if !ok || !strings.HasPrefix(name, "custodian-") {
			continue
		}
		switch kind {
		case "request.json":
			return errFleetRecoveryNotQuiescent
		case "model-request.json":
			modelOwners[id] = true
		case "snapshot.json":
			snapshots = append(snapshots, id)
		}
	}
	for _, id := range snapshots {
		if !modelOwners[id] {
			return errFleetRecoveryNotQuiescent
		}
	}
	models, err := modelRequests(dir)
	if err != nil || len(models) > 0 {
		return errFleetRecoveryNotQuiescent
	}
	return nil
}
