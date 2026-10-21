package myce

import (
	"context"
	"errors"
	"net/url"
	"time"
)

// PrepareEnrollmentUpgrade admits no new work and waits for every exact queue
// obligation. It never stops a busy service, edits its spool, or resumes policy.
// A returned drained snapshot is the receipt authorizing our eventual CAS resume.
func PrepareEnrollmentUpgrade(ctx context.Context, c ServiceConfig, client Client, clock Clock, timeout time.Duration, wait func(context.Context, time.Duration) error) (HostMaintenanceStatus, error) {
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
		receipt, err := ReadEnrollmentReceipt(c.State)
		if err != nil || receipt.Session != session {
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
	ownedDrain := status.Policy.Mode == "enabled"
	if ownedDrain {
		if err := client.Call(ctx, "POST", path, HostMaintenanceRequest{Action: "drain", IfMode: "enabled", IfUpdatedAt: &status.Policy.UpdatedAt}, &status); err != nil {
			return status, err
		}
	}
	receipt := status
	if !ownedDrain {
		receipt.Policy.UpdatedAt = time.Time{}
	}
	deadline := clock.Now().Add(timeout)
	for {
		if status.Quiescent {
			return receipt, nil
		}
		if !clock.Now().Before(deadline) {
			return status, errors.New("upgrade incomplete: local host remains maintained while custody settles")
		}
		if err := wait(ctx, time.Second); err != nil {
			return status, err
		}
		if err := client.Call(ctx, "GET", path, nil, &status); err != nil {
			return status, err
		}
	}
}
func FinishEnrollmentUpgrade(ctx context.Context, c ServiceConfig, client Client, receipt HostMaintenanceStatus) error {
	// Only an enabled -> drained transition made by this invocation may resume.
	// Prepare marks pre-existing maintenance by clearing its resume timestamp.
	if receipt.Policy.Mode != "drained" || receipt.Policy.UpdatedAt.IsZero() {
		return nil
	}
	return client.Call(ctx, "POST", "/api/hosts/"+url.PathEscape(c.HostID)+"/maintenance", HostMaintenanceRequest{Action: "resume", IfMode: "drained", IfUpdatedAt: &receipt.Policy.UpdatedAt}, nil)
}
