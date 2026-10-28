package myce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"
)

// DockerRetentionResult describes a container removed or held by explicit reconciliation.
// It excludes private payload evidence.
type DockerRetentionResult struct {
	Container string `json:"container"`
	Attempt   string `json:"attempt"`
	Removed   bool   `json:"removed"`
	Reason    string `json:"reason,omitempty"`
}

// ReconcileRetiredDocker is an explicit, one-time cleanup of completed conversion
// sources. It never loads credentials, adopts custody or changes the live worker.
func ReconcileRetiredDocker(ctx context.Context, config string, clock Clock) ([]DockerRetentionResult, error) {
	return reconcileRetiredDocker(ctx, config, clock, nil)
}

func reconcileRetiredDocker(ctx context.Context, config string, clock Clock, caller func(*dockerAttempt) func(...string) ([]byte, error)) ([]DockerRetentionResult, error) {
	c, err := ReadServiceConfig(config)
	if err != nil {
		return nil, err
	}
	// Decode only conversion provenance; never interpret saved credential bytes.
	var journal struct {
		Version int
		Phase   string
		Config  ServiceConfig
		Sources []ServiceConfig
	}
	data, err := os.ReadFile(machineJournal(c))
	if err != nil {
		return nil, err
	}
	if json.Unmarshal(data, &journal) != nil || journal.Version != 1 || journal.Phase != "complete" || len(journal.Sources) == 0 ||
		c.Role != "worker" || c.Config != config || !filepath.IsAbs(c.State) || c.Name == "" || c.HostID == "" || c.DockerEndpoint == "" ||
		journal.Config.Config != config || journal.Config.Name != c.Name || journal.Config.HostID != c.HostID || journal.Config.State != c.State || journal.Config.Queue != c.Queue || journal.Config.DockerEndpoint != c.DockerEndpoint {
		return nil, errors.New("retention reconciliation requires matching completed worker conversion provenance")
	}
	seenNames, seenSpools := map[string]bool{c.Name: true}, map[string]bool{filepath.Clean(c.State): true}
	for _, source := range journal.Sources {
		if source.Role != "worker" || source.Name == "" || seenNames[source.Name] || !filepath.IsAbs(source.State) || seenSpools[filepath.Clean(source.State)] ||
			source.HostID != c.HostID || source.Queue != c.Queue || source.DockerEndpoint != c.DockerEndpoint {
			return nil, errors.New("retired worker does not match conversion ownership")
		}
		seenNames[source.Name], seenSpools[filepath.Clean(source.State)] = true, true
	}
	retention := DefaultContainerRetention
	if c.ContainerRetention != nil {
		retention = *c.ContainerRetention
	}
	if retention < 0 || clock == nil {
		return nil, errors.New("retention reconciliation requires a clock and nonnegative retention")
	}
	now := clock.Now()
	var report []DockerRetentionResult
	var failures []error
	for i, source := range journal.Sources {
		results, err := reconcileRetiredDockerSource(ctx, source, now, retention, caller)
		report = append(report, results...)
		if err != nil {
			failures = append(failures, fmt.Errorf("retired source %d: %w", i+1, err))
		}
	}
	return report, errors.Join(failures...)
}

func reconcileRetiredDockerSource(ctx context.Context, source ServiceConfig, now time.Time, retention time.Duration, caller func(*dockerAttempt) func(...string) ([]byte, error)) ([]DockerRetentionResult, error) {
	d := &dockerAttempt{ctx: ctx, state: dockerExecution{Endpoint: source.DockerEndpoint}, binary: "docker", timeout: 2 * time.Second, env: dockerClientEnvironment()}
	call := d.call
	if caller != nil {
		call = caller(d)
	}
	objects, inventoryErr := inspectOwnedDocker(ctx, source.DockerEndpoint, "", func(_ context.Context, _ string, args ...string) ([]byte, error) {
		if args[0] == "ps" {
			args = append(args, "--filter", "label=myce.worker="+source.Name)
		}
		return call(args...)
	})
	var report []DockerRetentionResult
	allowed := map[string]bool{}
	candidates := map[string]bool{}
	for _, c := range objects {
		reason := ""
		switch {
		case inventoryErr != nil:
			reason = "incomplete Docker inventory"
		case c.Config.Labels["myce.worker"] != source.Name:
			reason = "worker ownership label mismatch"
		case dockerLive(c) || c.HostConfig.RestartPolicy == nil || c.HostConfig.RestartPolicy.Name != "no":
			reason = "stopped state or no-restart policy unconfirmed"
		default:
			reason = retiredDockerHold(source.State, source.Name, c.Config.Labels["myce.attempt"], now, retention)
		}
		report = append(report, DockerRetentionResult{Container: c.ID, Attempt: c.Config.Labels["myce.attempt"], Reason: reason})
		allowed[c.ID] = reason == ""
		if reason == "" {
			candidates[c.Config.Labels["myce.attempt"]] = true
		}
	}
	if inventoryErr != nil {
		return report, inventoryErr
	}
	// The retired spool must still be offline. Its lock file may be created,
	// but the spool directory must already exist. Never lock the successor.
	lock, err := os.OpenFile(filepath.Join(source.State, "worker.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err == nil {
		defer lock.Close()
		err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
		}
	}
	removed := map[string]bool{}
	attemptErrors := map[string]error{}
	var failures []error
	if err == nil {
		identities := make([]string, 0, len(candidates))
		for identity := range candidates {
			identities = append(identities, identity)
		}
		slices.Sort(identities)
		for _, identity := range identities {
			attemptErrors[identity] = pruneStoppedDockerPaths(ctx, source.State, source.Name, "", now, retention, []string{filepath.Join(source.State, identity+".docker-ack.json")}, func(attempt *dockerAttempt) func(...string) ([]byte, error) {
				attemptCall := attempt.call
				if caller != nil {
					attemptCall = caller(attempt)
				}
				return func(args ...string) ([]byte, error) {
					if attempt.state.Endpoint != source.DockerEndpoint {
						return nil, errors.New("custody endpoint differs from conversion")
					}
					if args[0] == "rm" {
						for _, id := range args[1:] {
							if !allowed[id] {
								return nil, errors.New("container outside verified reconciliation inventory")
							}
						}
					}
					out, err := attemptCall(args...)
					if args[0] == "rm" {
						// Docker prints successfully removed IDs on stdout even when
						// another member fails. Keep custody for retry on any error.
						confirmed := strings.Fields(string(out))
						for _, id := range args[1:] {
							if err == nil || slices.Contains(confirmed, id) {
								removed[id] = true
							}
						}
					}
					return out, err
				}
			})
			if attemptErrors[identity] != nil {
				failures = append(failures, attemptErrors[identity])
			}
		}
	} else {
		failures = append(failures, fmt.Errorf("retired spool unavailable or in use: %w", err))
	}
	for i := range report {
		r := &report[i]
		r.Removed = removed[r.Container]
		if !r.Removed && r.Reason == "" {
			switch {
			case err != nil:
				r.Reason = "retired spool unavailable or in use"
			case attemptErrors[r.Attempt] != nil:
				r.Reason = attemptErrors[r.Attempt].Error()
			default:
				r.Reason = "current attempt custody still references this container"
			}
		}
	}
	slices.SortFunc(report, func(a, b DockerRetentionResult) int { return strings.Compare(a.Container, b.Container) })
	return report, errors.Join(failures...)
}

// Explain routine holds before invoking the unchanged collector. That collector
// rechecks custody, labels, stopped state and private exit evidence before rm.
func retiredDockerHold(dir, worker, identity string, now time.Time, retention time.Duration) string {
	if identity == "" || identity == "." || identity == ".." || filepath.Base(identity) != identity {
		return "invalid attempt ownership label"
	}
	var ack dockerAcknowledgement
	data, err := os.ReadFile(filepath.Join(dir, identity+".docker-ack.json"))
	if err != nil || json.Unmarshal(data, &ack) != nil || ack.Worker != worker || ack.At.IsZero() {
		return "matching release acknowledgement unavailable in retired spool"
	}
	if now.Before(ack.At.Add(retention)) {
		return "acknowledgement retention window has not elapsed"
	}
	var st dockerExecution
	data, err = os.ReadFile(filepath.Join(dir, identity+".docker.json"))
	if err != nil || json.Unmarshal(data, &st) != nil || st.Identity != identity || st.Endpoint == "" || !st.Released || st.Pending || st.OperatorConfirmed {
		return "released Docker custody unconfirmed in retired spool"
	}
	return ""
}
