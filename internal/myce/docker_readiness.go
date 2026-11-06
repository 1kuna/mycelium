package myce

import (
	"encoding/json"
	"io"
	"strings"
	"sync"
	"time"
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
	line     []byte
	dropping bool
}

func (w *dockerProgressOutput) Write(b []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n, err := w.output.Write(b)
	for _, c := range b[:n] {
		if c == '\n' || c == '\r' {
			if !w.dropping {
				w.record(w.line)
			}
			w.line, w.dropping = w.line[:0], false
		} else if len(w.line) < dockerEventLineLimit && !w.dropping {
			w.line = append(w.line, c)
		} else {
			w.line, w.dropping = w.line[:0], true
		}
	}
	return n, err
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
		for i, arg := range args {
			if strings.Contains(arg, "@sha256:") {
				identity = strings.Join(args[i:], "\x00")
				break
			}
		}
	}
	return readinessFile(d.dir, []string{d.state.Endpoint, operation, identity})
}
