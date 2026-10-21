package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"time"
)

type enrollmentResumeReceipt struct {
	DrainOwner string    `json:"drain_owner,omitempty"`
	Intent     bool      `json:"intent,omitempty"`
	Host       string    `json:"host"`
	Name       string    `json:"name"`
	UpdatedAt  time.Time `json:"updated_at"`
}

const enrollmentResumeFile = "enrollment-resume.json"

// ReadEnrollmentUpgrade reads the private drain or pre-POST intent fence.
// ResolveEnrollmentUpgrade observes intents before granting resume authority.
func ReadEnrollmentUpgrade(c ServiceConfig) (HostMaintenanceStatus, error) {
	var status HostMaintenanceStatus
	b, err := os.ReadFile(filepath.Join(c.State, enrollmentResumeFile))
	if errors.Is(err, os.ErrNotExist) {
		return status, nil
	}
	if err != nil {
		return status, err
	}
	var r enrollmentResumeReceipt
	if json.Unmarshal(b, &r) != nil || r.Host != c.HostID || r.Name != c.Name || r.UpdatedAt.IsZero() {
		return status, errors.New("invalid enrollment resume receipt; preserve maintenance and inspect installation")
	}
	status.Host = r.Host
	mode := "drained"
	if r.Intent {
		mode = "enabled"
	}
	status.Policy = HostMaintenance{Mode: mode, UpdatedAt: r.UpdatedAt, DrainOwner: r.DrainOwner}
	return status, nil
}

// ResolveEnrollmentUpgrade promotes a durable drain intent after a crash between
// the request and recording its response, using queue-side ownership proof.
// Enabled/disabled queue state retires stale receipts. While drained, completed
// receipts retain both their owner and timestamp CAS fences.
func ResolveEnrollmentUpgrade(ctx context.Context, c ServiceConfig, client Client) (HostMaintenanceStatus, error) {
	receipt, err := ReadEnrollmentUpgrade(c)
	if err != nil || receipt.Policy.Mode == "" {
		return receipt, err
	}
	var observed HostMaintenanceStatus
	if err = client.Call(ctx, "GET", "/api/hosts/"+url.PathEscape(c.HostID)+"/maintenance", nil, &observed); err != nil {
		return receipt, err
	}
	if observed.Policy.Mode == "enabled" || observed.Policy.Mode == "disabled" {
		if err := os.Remove(filepath.Join(c.State, enrollmentResumeFile)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return HostMaintenanceStatus{}, fmt.Errorf("retire stale enrollment resume receipt: %w", err)
		}
		log.Printf("Retired stale enrollment resume receipt: queue confirms host is %s; no drain to resume.", observed.Policy.Mode)
		return HostMaintenanceStatus{}, nil
	}
	if receipt.Policy.DrainOwner == "" || observed.Policy.Mode != "drained" || observed.Policy.DrainOwner != receipt.Policy.DrainOwner || receipt.Policy.Mode == "drained" && !receipt.Policy.UpdatedAt.Equal(observed.Policy.UpdatedAt) {
		return HostMaintenanceStatus{}, enrollmentDrainUnproven(c, observed.Policy)
	}
	if receipt.Policy.Mode == "drained" {
		return observed, nil
	}
	err = saveWorkerJSON(c.State, enrollmentResumeFile, enrollmentResumeReceipt{Host: c.HostID, Name: c.Name, UpdatedAt: observed.Policy.UpdatedAt, DrainOwner: observed.Policy.DrainOwner})
	return observed, err
}

// PrepareEnrollmentUpgrade refuses busy hosts before changing policy. If work
// races with the drain, bounded waiting restores only our exact CAS transition.
// The caller restores installation and resumes owned drains on install/readiness failure.
func PrepareEnrollmentUpgrade(ctx context.Context, c ServiceConfig, client Client, clock Clock, timeout time.Duration, wait func(context.Context, time.Duration) error) (HostMaintenanceStatus, error) {
	if c.State == "" {
		return HostMaintenanceStatus{}, errors.New("upgrade state directory required")
	}
	if clock == nil {
		return HostMaintenanceStatus{}, errors.New("upgrade clock required")
	}
	if wait == nil {
		wait = enrollmentPause
	}
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	path := "/api/hosts/" + url.PathEscape(c.HostID) + "/maintenance"
	var status HostMaintenanceStatus
	if err := client.Call(ctx, "GET", path, nil, &status); err != nil {
		return status, err
	}
	if len(status.Workers) != 1 || status.Workers[0].Machine.ID != c.Name || !status.Workers[0].Fresh {
		return status, errors.New("upgrade requires the exact fresh local hypha and no legacy siblings")
	}
	session := status.Workers[0].Machine.Session
	if c.Enrollment {
		r, err := ReadEnrollmentReceipt(c.State)
		if err != nil || r.Session != session {
			return status, errors.New("upgrade requires the exact locally recorded session")
		}
	}
	var observation EnrollmentObservation
	if err := client.Call(ctx, "GET", "/api/workers/"+url.PathEscape(c.Name)+"/enrollment?session="+url.QueryEscape(session), nil, &observation); err != nil {
		return status, err
	}
	if !observation.Fresh || observation.Machine.Session != session || observation.StartGate != startGateProtocol || observation.AttemptProtocol != keyedAttemptProtocol {
		return status, errors.New("upgrade requires compatible queue protocols before replacing the worker")
	}
	receipt, err := ResolveEnrollmentUpgrade(ctx, c, client)
	if err != nil {
		return status, err
	}
	owned := receipt.Policy.Mode == "drained" && status.Policy.Mode == "drained" && receipt.Policy.UpdatedAt.Equal(status.Policy.UpdatedAt) && receipt.Policy.DrainOwner != "" && receipt.Policy.DrainOwner == status.Policy.DrainOwner
	if status.Policy.Mode == "enabled" {
		if !status.Healthy || len(status.Attempts) > 0 || status.Calls > 0 || status.Preparations > 0 || status.Sidecars > 0 || len(status.PendingReleases) > 0 || len(status.PendingCleanup) > 0 || status.MemoryInUseGB > 0 || status.MemoryDebtGB > 0 {
			return status, errors.New("upgrade refused before drain: local host still has execution custody; rerun after jobs finish")
		}
		// Save a unique intent before crossing the queue mutation boundary.
		owner := ID()
		if err = saveWorkerJSON(c.State, enrollmentResumeFile, enrollmentResumeReceipt{Intent: true, Host: c.HostID, Name: c.Name, UpdatedAt: status.Policy.UpdatedAt, DrainOwner: owner}); err != nil {
			return status, errors.Join(errors.New("cannot persist upgrade drain intent"), err)
		}
		if err = client.Call(ctx, "POST", path, HostMaintenanceRequest{Action: "drain", IfMode: "enabled", IfUpdatedAt: &status.Policy.UpdatedAt, DrainOwner: owner}, &status); err != nil {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cancel()
			recovered, resolveErr := ResolveEnrollmentUpgrade(cleanup, c, client)
			if resolveErr == nil {
				resolveErr = FinishEnrollmentUpgrade(cleanup, c, client, recovered)
			}
			return status, errors.Join(err, resolveErr)
		}
		if status.Policy.DrainOwner != owner {
			return status, enrollmentDrainUnproven(c, status.Policy)
		}
		receipt = status
		owned = true
		if err = saveWorkerJSON(c.State, enrollmentResumeFile, enrollmentResumeReceipt{Host: c.HostID, Name: c.Name, UpdatedAt: status.Policy.UpdatedAt, DrainOwner: status.Policy.DrainOwner}); err != nil {
			return status, errors.Join(errors.New("cannot persist upgrade resume receipt"), undoEnrollmentDrain(ctx, c, client, receipt))
		}
	}
	if !owned {
		receipt = status
		receipt.Policy.UpdatedAt = time.Time{}
	}
	deadline := clock.Now().Add(timeout)
	fail := func(cause error) (HostMaintenanceStatus, error) {
		if owned {
			cause = errors.Join(cause, undoEnrollmentDrain(ctx, c, client, receipt))
		}
		return status, cause
	}
	for {
		if status.Quiescent {
			return receipt, nil
		}
		if !clock.Now().Before(deadline) {
			return fail(errors.New("upgrade drain timed out; installation not started"))
		}
		if err = wait(ctx, time.Second); err != nil {
			return fail(err)
		}
		if err = client.Call(ctx, "GET", path, nil, &status); err != nil {
			return fail(err)
		}
	}
}
func undoEnrollmentDrain(ctx context.Context, c ServiceConfig, client Client, receipt HostMaintenanceStatus) error {
	// Drain timeout/cancellation must not consume the cleanup budget.
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	return FinishEnrollmentUpgrade(cleanup, c, client, receipt)
}
func FinishEnrollmentUpgrade(ctx context.Context, c ServiceConfig, client Client, receipt HostMaintenanceStatus) error {
	if receipt.Policy.Mode != "drained" || receipt.Policy.UpdatedAt.IsZero() {
		return nil
	}
	if receipt.Policy.DrainOwner == "" {
		return enrollmentDrainUnproven(c, receipt.Policy)
	}
	if err := client.Call(ctx, "POST", "/api/hosts/"+url.PathEscape(c.HostID)+"/maintenance", HostMaintenanceRequest{Action: "resume", IfMode: "drained", IfUpdatedAt: &receipt.Policy.UpdatedAt, IfDrainOwner: receipt.Policy.DrainOwner}, nil); err != nil {
		// A failed response may follow a committed resume. Only suggest an
		// operator resume when the queue still confirms a drain.
		var observed HostMaintenanceStatus
		if e := client.Call(ctx, "GET", "/api/hosts/"+url.PathEscape(c.HostID)+"/maintenance", nil, &observed); e != nil {
			observed.Policy.Mode = ""
		}
		return errors.Join(enrollmentDrainUnproven(c, observed.Policy), err)
	}
	err := os.Remove(filepath.Join(c.State, enrollmentResumeFile))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func enrollmentDrainUnproven(c ServiceConfig, policy HostMaintenance) error {
	message := "drain ownership unproven or maintenance changed; host maintenance preserved"
	if policy.Mode == "drained" {
		owner := policy.DrainOwner
		if owner == "" {
			owner = "operator (no onboarding owner recorded)"
		}
		message += fmt.Sprintf("; drain owner: %s; operator can resume with: myce host resume %s", owner, c.HostID)
	}
	return errors.New(message)
}
