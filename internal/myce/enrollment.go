package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type EnrollmentProof struct {
	Version         string `json:"version"`
	StartGate       string `json:"start_gate"`
	AttemptProtocol string `json:"attempt_protocol"`
	RunnerSmoke     bool   `json:"runner_smoke"`
}
type EnrollmentReceipt struct {
	ProcessID         int             `json:"process_id"`
	Session           string          `json:"session"`
	Version           string          `json:"version"`
	Devices           []MachineDevice `json:"devices"`
	LogicalCPUs       int             `json:"logical_cpus"`
	SystemMemoryBytes uint64          `json:"system_memory_bytes"`
}
type EnrollmentObservation struct {
	Machine         Machine         `json:"machine"`
	Fresh           bool            `json:"fresh"`
	StartGate       string          `json:"start_gate"`
	AttemptProtocol string          `json:"attempt_protocol"`
	Policy          HostMaintenance `json:"policy"`
}
type EnrollmentResult struct {
	State   string
	Machine Machine
}

func (a API) registerEnrollment(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/workers/{id}/enrollment", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		out, err := a.Queue.enrollmentObservation(r.PathValue("id"), r.URL.Query().Get("session"))
		respondOperation(w, out, err, "status_unavailable")
	})
}

func (q *Queue) enrollmentObservation(id, session string) (EnrollmentObservation, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	m := q.s.Machines[id]
	if m == nil || m.Session != session || session == "" {
		return EnrollmentObservation{}, ErrStale
	}
	if err := q.db.Ping(); err != nil {
		return EnrollmentObservation{}, err
	}
	status := q.hostMaintenanceStatus(hostID(m))
	for _, worker := range status.Workers {
		if worker.Machine.ID == id {
			return EnrollmentObservation{worker.Machine, worker.Fresh, startGateProtocol, keyedAttemptProtocol, status.Policy}, nil
		}
	}
	return EnrollmentObservation{}, ErrStale
}

func enrollmentRunnerSmoke(ctx context.Context, dir string, clock Clock) error {
	// Exercise the ordinary recorded process-group runner, its gate, and cleanup.
	// No application recipe, GPU benchmark or fleet credential is needed to join.
	env := []string{}
	for _, v := range os.Environ() {
		k, _, _ := strings.Cut(v, "=")
		if k != "MYCE_TOKEN" && k != "CUDA_VISIBLE_DEVICES" && k != "ZE_AFFINITY_MASK" && k != "ONEAPI_DEVICE_SELECTOR" {
			env = append(env, v)
		}
	}
	env = append(env, "CUDA_VISIBLE_DEVICES=", "ZE_AFFINITY_MASK=2147483647", "ONEAPI_DEVICE_SELECTOR=!*:*", "MYCE_DEVICE=cpu", "MYCE_PLATFORM=cpu")
	return runRecordedCommand(ctx, []string{"sh", "-c", `test "${MYCE_TOKEN+x}" != x && test "$MYCE_DEVICE" = cpu && n=0 && while test "$n" -lt 32; do n=$((n+1)); done && test "$n" -eq 32`}, dir, env, io.Discard, commandScope{StateDir: dir, Clock: clock}, "enrollment-command-"+ID()+".json")
}
func (w *Worker) prepareEnrollment(ctx context.Context, s *workerStore) error {
	if !w.Enrollment {
		return nil
	}
	if err := cleanupRecordedProcesses(s.dir, "enrollment-command-*.json", w.Clock); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := enrollmentRunnerSmoke(ctx, s.dir, w.Clock); err != nil {
		return errors.New("enrollment runner smoke failed; preserve spool and inspect the service")
	}
	w.Machine.Enrollment = &EnrollmentProof{BuildVersion, startGateProtocol, keyedAttemptProtocol, true}
	return nil
}
func saveEnrollmentReceipt(dir string, m Machine) error {
	if m.Enrollment == nil {
		return nil
	}
	r := EnrollmentReceipt{ProcessID: os.Getpid(), Session: m.Session, Version: m.Enrollment.Version, Devices: m.Devices}
	if p := m.CapabilityProfile; p != nil {
		r.LogicalCPUs, r.SystemMemoryBytes = p.LogicalCPUs, p.SystemMemoryBytes
	}
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if previous, err := os.ReadFile(filepath.Join(dir, "enrollment.json")); err == nil && bytes.Equal(previous, data) {
		return nil
	}
	return saveWorkerJSON(dir, "enrollment.json", r)
}
func ReadEnrollmentReceipt(dir string) (EnrollmentReceipt, error) {
	var r EnrollmentReceipt
	b, err := os.ReadFile(filepath.Join(dir, "enrollment.json"))
	if err != nil {
		return r, err
	}
	if json.Unmarshal(b, &r) != nil || r.Session == "" || r.Version == "" {
		return r, errors.New("enrollment receipt incomplete")
	}
	return r, nil
}
func EvaluateEnrollment(receipt EnrollmentReceipt, o EnrollmentObservation) (string, error) {
	return evaluateEnrollmentVersion(receipt, o, BuildVersion)
}
func evaluateEnrollmentVersion(receipt EnrollmentReceipt, o EnrollmentObservation, version string) (string, error) {
	incomplete := func(reason string) (string, error) { return "", fmt.Errorf("enrollment incomplete: %s", reason) }
	m := o.Machine
	p := m.Enrollment
	if !o.Fresh || m.Gone || m.Leaving || m.Recovering || m.Session == "" || m.Session != receipt.Session {
		return incomplete("exact session is not fresh and reconciled")
	}
	if receipt.Version != version || o.StartGate != startGateProtocol || o.AttemptProtocol != keyedAttemptProtocol || p == nil || p.StartGate != startGateProtocol || p.AttemptProtocol != keyedAttemptProtocol || p.Version != receipt.Version {
		return incomplete("release or worker protocol mismatch; upgrade the queue first")
	}
	if !p.RunnerSmoke {
		return incomplete("runner smoke missing")
	}
	if m.InventoryReason != "" || m.MemoryError != "" || m.CPUCapacityReason != "" || m.HostMemoryError != "" || m.CheckpointUnavailable || m.HostMemoryPolicyMismatch || m.DockerCleanup != "" {
		return incomplete("components, storage, cleanup or host policy are unavailable")
	}
	if len(m.Devices) != len(receipt.Devices) {
		return incomplete("expected device inventory missing")
	}
	for _, expected := range receipt.Devices {
		found := false
		for _, device := range m.Devices {
			if device.ID == expected.ID && device.Identity == expected.Identity && device.Platform == expected.Platform {
				found = device.Reason == "" && device.BudgetGB > 0 && device.TotalMemoryGB > 0
				break
			}
		}
		if !found {
			return incomplete("expected device unavailable")
		}
	}
	profile := m.CapabilityProfile
	if profile == nil || receipt.LogicalCPUs <= 0 || receipt.SystemMemoryBytes == 0 || profile.LogicalCPUs != receipt.LogicalCPUs || profile.SystemMemoryBytes != receipt.SystemMemoryBytes || profile.OS == "" || profile.Arch == "" || !m.HostMemoryMeasured || m.HostAvailableMemoryGB <= 0 {
		return incomplete("CPU or RAM facts unavailable")
	}
	if o.Policy.CPUBudgetCores == nil || o.Policy.MemoryBudget == nil || m.CPUBudgetCores != *o.Policy.CPUBudgetCores || m.HostMemoryBudgetGB != o.Policy.MemoryBudget.GiB {
		return incomplete("CPU or RAM policy unacknowledged")
	}
	switch o.Policy.Mode {
	case "disabled", "drained":
		return o.Policy.Mode, nil
	case "enabled":
	default:
		return incomplete("host policy unacknowledged")
	}
	if a := profile.Availability; a != nil && !a.Available {
		if a.Mode == "daily" || a.Signals.Laptop {
			return "polite_waiting", nil
		}
		return incomplete("participation unavailable")
	}
	if m.AdmissionReason != "" {
		return incomplete("admission withheld")
	}
	return "ready", nil
}

// EnrollmentServiceHealth returns a stable process/restart identity, never
// stdout from a service or its environment. Readiness requires 30 stable seconds.
func EnrollmentServiceHealth(ctx context.Context, c ServiceConfig, command func(context.Context, string, ...string) ([]byte, error)) (string, error) {
	if command == nil {
		return "", errors.New("service manager unavailable")
	}
	var b []byte
	var err error
	if c.Detection.OS == "linux" {
		b, err = command(ctx, "systemctl", "--user", "show", filepath.Base(c.Service), "--property=ActiveState,SubState,MainPID,NRestarts,Result", "--no-pager")
	} else {
		u := installUnit{path: c.Service, label: "local.mycelium.worker"}
		b, err = command(ctx, "launchctl", "print", u.target())
	}
	if err != nil {
		return "", errors.New("service manager unhealthy")
	}
	fields := map[string]string{}
	for _, line := range strings.Split(string(b), "\n") {
		// launchctl prints a service block whose direct children have one tab.
		// Nested resource/coalition states are unrelated to the service state.
		if c.Detection.OS != "linux" && (!strings.HasPrefix(line, "\t") || strings.HasPrefix(line, "\t\t")) {
			continue
		}
		k, v, ok := strings.Cut(strings.TrimSpace(line), "=")
		if ok {
			fields[strings.TrimSpace(k)] = strings.TrimSpace(v)
		}
	}
	if c.Detection.OS == "linux" {
		pid, _ := strconv.Atoi(fields["MainPID"])
		_, restartsErr := strconv.ParseUint(fields["NRestarts"], 10, 64)
		if fields["ActiveState"] != "active" || fields["SubState"] != "running" || pid <= 0 || restartsErr != nil || fields["Result"] != "success" {
			return "", errors.New("service is stopped, restarting or unhealthy")
		}
		return fields["MainPID"] + "/" + fields["NRestarts"], nil
	}
	pid, _ := strconv.Atoi(fields["pid"])
	_, runsErr := strconv.ParseUint(fields["runs"], 10, 64)
	if fields["state"] != "running" || pid <= 0 || runsErr != nil {
		return "", errors.New("LaunchAgent is not stably running in the GUI login session")
	}
	return fields["pid"] + "/" + fields["runs"], nil
}

type EnrollmentWait struct {
	// Version defaults to this binary; recovery explicitly verifies the restored release.
	Version   string
	Clock     Clock
	Timeout   time.Duration
	StableFor time.Duration
	Wait      func(context.Context, time.Duration) error
	Command   func(context.Context, string, ...string) ([]byte, error)
	Observe   func(context.Context) (EnrollmentObservation, error)
}

func enrollmentPause(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
func WaitEnrollment(ctx context.Context, c ServiceConfig, client Client, opt EnrollmentWait) (EnrollmentResult, error) {
	if opt.Clock == nil {
		return EnrollmentResult{}, errors.New("enrollment clock required")
	}
	if opt.Wait == nil {
		opt.Wait = enrollmentPause
	}
	if opt.Timeout <= 0 {
		opt.Timeout = time.Minute
	}
	if opt.StableFor <= 0 {
		opt.StableFor = 30 * time.Second
	}
	if opt.Version == "" {
		opt.Version = BuildVersion
	}
	deadline := opt.Clock.Now().Add(opt.Timeout)
	var stableSince time.Time
	var previous string
	last := "waiting for exact queue registration"
	for {
		if err := ctx.Err(); err != nil {
			return EnrollmentResult{}, err
		}
		receipt, err := ReadEnrollmentReceipt(c.State)
		var observation EnrollmentObservation
		var state, health string
		if err == nil {
			if opt.Observe != nil {
				observation, err = opt.Observe(ctx)
			} else {
				err = client.Call(ctx, "GET", "/api/workers/"+url.PathEscape(c.Name)+"/enrollment?session="+url.QueryEscape(receipt.Session), nil, &observation)
			}
		}
		var api *HTTPError
		if errors.As(err, &api) && api.Status == http.StatusUpgradeRequired {
			return EnrollmentResult{}, errors.New("enrollment incomplete: HTTP 426; upgrade queue before the hypha")
		}
		if err == nil {
			state, err = evaluateEnrollmentVersion(receipt, observation, opt.Version)
			if err == nil {
				err = expectedEnrollmentDevices(c, observation.Machine.Devices)
			}
		}
		if err == nil {
			health, err = EnrollmentServiceHealth(ctx, c, opt.Command)
		}
		if err == nil {
			pid, _, _ := strings.Cut(health, "/")
			if receipt.ProcessID <= 0 || pid != strconv.Itoa(receipt.ProcessID) {
				err = errors.New("enrollment receipt does not belong to the current service process")
			}
		}
		if err == nil {
			key := receipt.Session + "/" + state + "/" + health
			if key != previous {
				previous = key
				stableSince = opt.Clock.Now()
			}
			if opt.Clock.Now().Sub(stableSince) >= opt.StableFor {
				return EnrollmentResult{state, observation.Machine}, nil
			}
			last = "service stability check pending"
		} else {
			previous = ""
			stableSince = time.Time{}
			last = "queue, components or service not healthy"
		}
		if !opt.Clock.Now().Before(deadline) {
			return EnrollmentResult{}, fmt.Errorf("enrollment incomplete: %s", last)
		}
		if err := opt.Wait(ctx, time.Second); err != nil {
			return EnrollmentResult{}, err
		}
	}
}
func (r EnrollmentResult) Summary(c ServiceConfig) string {
	m := r.Machine
	p := m.CapabilityProfile
	devices := []string{}
	for _, d := range m.Devices {
		devices = append(devices, fmt.Sprintf("%s %s (%s, %s)", d.Platform, d.ID, d.Identity, d.MemoryMode))
	}
	if len(devices) == 0 {
		devices = append(devices, "CPU only")
	}
	participation := "dedicated"
	if p.Availability != nil {
		participation = p.Availability.Summary()
	}
	return fmt.Sprintf("%s · %s/%s · %s · CPU %d logical, allowance %d (0=detected minus reserve) · RAM %.1f GiB, allowance %.1f GiB (0=uncapped) · service healthy · %s · NAS · %s · %s", m.ID, p.OS, p.Arch, strings.Join(devices, ", "), p.LogicalCPUs, m.CPUBudgetCores, float64(p.SystemMemoryBytes)/(1<<30), m.HostMemoryBudgetGB, r.State, participation, m.MemoryEnforcement)
}

// Configured selectors are the independent expectation. A receipt describes
// what the worker observed and cannot establish that a configured card exists.
func expectedEnrollmentDevices(c ServiceConfig, devices []MachineDevice) error {
	selectors := []string{}
	for _, d := range c.MachineDevices {
		selectors = append(selectors, d.Selector)
	}
	if len(selectors) == 0 {
		var err error
		selectors, err = deviceAllowance(c.Device)
		if err != nil {
			return err
		}
	}
	for _, selector := range selectors {
		found := false
		for _, d := range devices {
			if selector == d.Selector || selector == d.Identity || selector == d.ID {
				found = true
				break
			}
		}
		if !found {
			return errors.New("enrollment incomplete: configured device_selector_unmatched")
		}
	}
	return nil
}
