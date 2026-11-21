package myce

import (
	"context"
	"errors"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const custodianStartupOutputBytes = 8 << 10

// Local operator evidence. Never send subprocess text or launchd's environment
// to the queue. These observations are never used to infer release.
type custodianStartupDiagnostic struct {
	ID              string                 `json:"custodian_id"`
	Execution       *Execution             `json:"execution,omitempty"`
	Bootstrap       custodianManagerProbe  `json:"bootstrap"`
	BootstrapOutput string                 `json:"bootstrap_output,omitempty"`
	Registration    *custodianManagerProbe `json:"registration,omitempty"`
	Snapshot        string                 `json:"snapshot,omitempty"`
	SnapshotPID     int                    `json:"snapshot_pid,omitempty"`
	Stderr          string                 `json:"stderr_tail,omitempty"`
	StderrSource    string                 `json:"stderr_source,omitempty"`
}

type custodianManagerProbe struct {
	Outcome     string `json:"outcome"`
	CommandCode *int   `json:"command_exit_code,omitempty"`
	State       string `json:"state,omitempty"`
	PID         int    `json:"pid,omitempty"`
	Runs        int    `json:"runs,omitempty"`
	ExitCode    *int   `json:"last_exit_code,omitempty"`
	Signal      *int   `json:"last_terminating_signal,omitempty"`
}

func custodianLaunchctlResult(ctx context.Context, err error) custodianManagerProbe {
	var exit *exec.ExitError
	switch {
	case err == nil:
		return custodianManagerProbe{Outcome: "success"}
	case ctx.Err() != nil:
		return custodianManagerProbe{Outcome: "interrupted"}
	case errors.As(err, &exit) && exit.ExitCode() >= 0:
		code := exit.ExitCode()
		return custodianManagerProbe{Outcome: "failed", CommandCode: &code}
	default:
		return custodianManagerProbe{Outcome: "unavailable"}
	}
}

func inspectCustodianRegistration(ctx context.Context, id string) custodianManagerProbe {
	label := custodianLabel(id)
	cmd := exec.CommandContext(ctx, "launchctl", "print", custodianDomain()+"/"+label)
	cmd.Env = withoutMyceToken(os.Environ())
	// launchctl also prints environments and argv. Keep only the top-level
	// lifecycle fields below, including the manager's last process status.
	out := &boundedProbeOutput{limit: 64 << 10}
	cmd.Stdout, cmd.Stderr = out, out
	probe := custodianLaunchctlResult(ctx, cmd.Run())
	if probe.Outcome == "success" {
		probe.Outcome = "registered"
		parseCustodianRegistration(&probe, string(out.Bytes()))
	} else if probe.CommandCode != nil && *probe.CommandCode == 113 && strings.Contains(string(out.Bytes()), `Could not find service "`+label+`"`) {
		probe.Outcome = "not_registered"
	}
	return probe
}

func parseCustodianRegistration(probe *custodianManagerProbe, output string) {
	for line := range strings.SplitSeq(output, "\n") {
		field, value, ok := strings.Cut(line, " = ")
		if !ok {
			continue
		}
		switch field {
		case "\tstate":
			switch value {
			case "running", "not running", "waiting", "spawn scheduled", "exited":
				probe.State = value
			}
		case "\tpid":
			probe.PID, _ = strconv.Atoi(value)
		case "\truns":
			probe.Runs, _ = strconv.Atoi(value)
		case "\tlast exit code":
			if code, err := strconv.Atoi(value); err == nil {
				probe.ExitCode = &code
			}
		case "\tlast terminating signal":
			// launchctl reports a signal name followed by its numeric value.
			if _, code, ok := strings.Cut(value, ": "); ok {
				value = code
			}
			if signal, err := strconv.Atoi(value); err == nil {
				probe.Signal = &signal
			}
		}
	}
}

func saveCustodianStartupDiagnostic(dir string, evidence custodianStartupDiagnostic) bool {
	if err := saveWorkerJSON(dir, custodianName(evidence.ID, "startup"), evidence); err != nil {
		log.Printf("custodian %s startup diagnostic could not be saved", evidence.ID)
		return false
	}
	return true
}

func observeCustodianStartupFailure(dir string, evidence custodianStartupDiagnostic, waitForExit bool) bool {
	ctx, cancel := context.WithTimeout(context.Background(), processPSTimeout)
	defer cancel()
	tick := time.NewTicker(25 * time.Millisecond)
	defer tick.Stop()
	probe := inspectCustodianRegistration(ctx, evidence.ID)
	// The service can publish its prelaunch refusal just before printing its
	// fatal error and exiting. Give launchd time to observe that exit before
	// bootout erases the status. A missing status leaves the exit unknown.
	for waitForExit && probe.Outcome == "registered" && probe.PID > 0 && ctx.Err() == nil {
		select {
		case <-ctx.Done():
		case <-tick.C:
			next := inspectCustodianRegistration(ctx, evidence.ID)
			if next.Outcome != "interrupted" {
				probe = next
			}
		}
	}
	if evidence.Registration == nil || probe.ExitCode != nil || probe.Signal != nil ||
		evidence.Registration.ExitCode == nil && evidence.Registration.Signal == nil {
		evidence.Registration = &probe
	}
	var snapshot custodianSnapshot
	snapshotErr := readCustodianJSON(dir, custodianName(evidence.ID, "snapshot"), &snapshot)
	switch {
	case errors.Is(snapshotErr, os.ErrNotExist):
		evidence.Snapshot = "missing"
	case snapshotErr != nil || snapshot.ID != evidence.ID:
		evidence.Snapshot = "unconfirmed"
	case snapshot.PID > 0:
		evidence.Snapshot = "initialized"
		evidence.SnapshotPID = snapshot.PID
	case snapshot.Finished && snapshot.State.Result != nil:
		evidence.Snapshot = "finished_without_owner_pid"
	default:
		evidence.Snapshot = "present"
	}
	base := filepath.Join(dir, "custodian-"+evidence.ID)
	data, err := readDiagnostic(base+".stderr", custodianStartupOutputBytes, true)
	if err == nil {
		evidence.Stderr, evidence.StderrSource = string(data), "stderr"
	} else if errors.Is(err, os.ErrNotExist) && evidence.Bootstrap.Outcome == "unknown" {
		if data, err = readDiagnostic(base+".log", custodianStartupOutputBytes, true); err == nil {
			evidence.Stderr, evidence.StderrSource = string(data), "legacy_combined"
		}
	}
	return saveCustodianStartupDiagnostic(dir, evidence) && (err == nil || errors.Is(err, os.ErrNotExist))
}

func retireAttemptCustodian(dir, id string, result Result) (keepLogs bool) {
	name := custodianName(id, "startup")
	if result.ExecutionCause != nil && result.ExecutionCause.LaunchRefusal == custodianStartupFailed {
		evidence := custodianStartupDiagnostic{ID: id, Bootstrap: custodianManagerProbe{Outcome: "unknown"}}
		var previous custodianStartupDiagnostic
		if readCustodianJSON(dir, name, &previous) == nil && previous.ID == id {
			evidence = previous
		}
		evidence.Execution = &result.Execution
		keepLogs = !observeCustodianStartupFailure(dir, evidence, true)
	} else {
		_ = os.Remove(filepath.Join(dir, name))
	}
	retireCustodian(id)
	return keepLogs
}
