package myce

import (
	"encoding/json"
	"io"
	"strings"
	"sync"
	"time"
	"unsafe"
)

func (d *dockerAttempt) rpcBudget(operation string) time.Duration {
	d.latencyMu.Lock()
	defer d.latencyMu.Unlock()
	if d.timeout > 0 {
		return d.timeout
	} // injected/explicit I/O budget
	return max(d.latency[operation].budget(), d.observationBudget[operation])
}
func (d *dockerAttempt) rpcCadence(operation string) time.Duration {
	d.latencyMu.Lock()
	defer d.latencyMu.Unlock()
	return d.latency[operation].cadence()
}

// Draining a completed CLI's buffered output needs the same local variability
// allowance as its response, rather than just the mean between observations.
// Only completed samples count here: outage backoff must not delay pipe cleanup.
func (d *dockerAttempt) rpcDrainBudget(operation string) time.Duration {
	d.latencyMu.Lock()
	defer d.latencyMu.Unlock()
	return d.latency[operation].budget()
}
func (d *dockerAttempt) noteRPCLatency(operation string, elapsed time.Duration) {
	d.latencyMu.Lock()
	defer d.latencyMu.Unlock()
	if d.latency == nil {
		d.latency = map[string]readinessLatency{}
	}
	v := d.latency[operation]
	v.observe(elapsed)
	d.latency[operation] = v
	delete(d.observationBudget, operation)
}
func (d *dockerAttempt) backoffRPC(operation string, budget time.Duration) {
	d.latencyMu.Lock()
	defer d.latencyMu.Unlock()
	if d.observationBudget == nil {
		d.observationBudget = map[string]time.Duration{}
	}
	d.observationBudget[operation] = scaleReadinessDuration(budget, 2)
}

// Docker's JSON pull stream identifies each layer and separates downloading
// from extracting. Only a larger byte counter or a new phase renews silence;
// repeated status/keepalive messages cannot hide a stalled transfer. Managed
// launches remain --pull=never; this observer does not authorize a download.
type dockerProgressOutput struct {
	mu       sync.Mutex
	output   io.Writer
	progress *readinessProgress
	layers   map[string]struct {
		status  string
		current int64
	}
	line     diagnosticLine
	releases []func()
}

func (w *dockerProgressOutput) Write(b []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n, err := w.output.Write(b)
	w.line.write(b[:n], w.record)
	return n, err
}

func (w *dockerProgressOutput) close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.line.clear()
	for _, release := range w.releases {
		release()
	}
	w.releases = nil
	w.layers = nil
}
func (w *dockerProgressOutput) record(line []byte) {
	var v struct {
		ID       string                         `json:"id"`
		Status   string                         `json:"status"`
		Progress struct{ Current, Total int64 } `json:"progressDetail"`
	}
	if json.Unmarshal(line, &v) != nil || v.ID == "" || v.Progress.Current < 0 {
		return
	}
	if !contains([]string{"Downloading", "Extracting", "Pull complete", "Already exists", "Download complete", "Verifying Checksum"}, v.Status) {
		return
	}
	if w.layers == nil {
		w.layers = map[string]struct {
			status  string
			current int64
		}{}
	}
	old, exists := w.layers[v.ID]
	if !exists {
		// Layer identities survive individual lines; reserve their retained
		// representation separately from transient JSON/parser memory.
		needed := uint64(len(v.ID)+len(v.Status)) + uint64(unsafe.Sizeof(old)+unsafe.Sizeof(v.ID))
		grant, release := w.line.tail.reserveBytes(needed)
		if uint64(grant) < needed {
			release()
			return
		}
		w.releases = append(w.releases, release)
	}
	if !exists || dockerProgressPhase(v.Status) > dockerProgressPhase(old.status) || v.Status == old.status && v.Progress.Current > old.current {
		w.layers[v.ID] = struct {
			status  string
			current int64
		}{v.Status, v.Progress.Current}
		w.progress.advance()
	}
}

func dockerProgressPhase(status string) int {
	switch status {
	case "Downloading":
		return 1
	case "Verifying Checksum", "Download complete":
		return 2
	case "Extracting":
		return 3
	case "Pull complete", "Already exists":
		return 4
	}
	return 0
}

func dockerObservation(args []string) bool {
	return len(args) > 0 && (args[0] == "ps" || args[0] == "inspect" || args[0] == "network" && len(args) > 1 && (args[1] == "ls" || args[1] == "inspect"))
}

func (d *dockerAttempt) initializationFile(operation string, args []string) string {
	identity := operation
	if operation == "start" && len(args) > 1 {
		d.latencyMu.Lock()
		identity = d.initializations[args[1]]
		d.latencyMu.Unlock()
		// An unknown/restored ID must not inherit an unrelated image's timings.
		if identity == "" {
			identity = args[1]
		}
	} else {
		stable := []string{operation}
		for i := 0; i < len(args); i++ {
			arg := args[i]
			if strings.Contains(arg, "@sha256:") {
				stable = append(stable, args[i:]...)
				break
			}
			switch arg {
			case "--env", "-e", "--memory", "--memory-swap", "--cpus", "--user", "-u", "--entrypoint", "--workdir", "-w":
				if i+1 < len(args) {
					value := args[i+1]
					i++
					if (arg == "--env" || arg == "-e") && strings.HasPrefix(value, "MYCE_") {
						continue
					}
					stable = append(stable, arg, value)
				}
			}
		}
		identity = strings.Join(stable, "\x00")
	}

	return readinessFile(d.dir, []string{d.state.Endpoint, operation, identity})
}
