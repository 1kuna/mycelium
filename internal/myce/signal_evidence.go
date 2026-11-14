package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"mycelium/internal/probeguard"
)

type SignalCapture struct {
	PID                   int
	Birth                 string
	StartedAt, ObservedAt time.Time
	Directory             string
	Cgroup                string // Exact attempt/member cgroup, from worker custody.
}

// Publish the immutable exit before optional I/O. A copied exit receives the
// metadata so readers never race a mutation of a published teardown fact.
func collectPublishedExit(exit *memberExit, publish func(*memberExit) error, collect func(*memberExit)) error {
	first := publish(exit)
	next := *exit
	collect(&next)
	if next.Evidence == nil {
		return first
	}
	return errors.Join(first, publish(&next))
}

type SignalEvidence struct {
	StartedAt     time.Time      `json:"started_at"`
	Attribution   string         `json:"attribution"`
	ExitStatus    int            `json:"exit_status"`
	CoreDumped    bool           `json:"kernel_core_dumped,omitempty"`
	PID           int            `json:"pid"`
	Signal        string         `json:"signal"`
	ObservedAt    time.Time      `json:"observed_at"`
	FaultAddress  string         `json:"fault_address,omitempty"`
	Libraries     []FaultLibrary `json:"libraries,omitempty"`
	CoreCapture   string         `json:"core_capture"`
	CorePointer   string         `json:"core_pointer,omitempty"`
	CoreBytes     int64          `json:"core_bytes,omitempty"`
	CoreTruncated bool           `json:"core_truncated,omitempty"`
	Unavailable   []string       `json:"unavailable,omitempty"`
	CaptureNS     int64          `json:"capture_ns"`
}
type FaultLibrary struct {
	Name    string `json:"name"`
	Address string `json:"address"`
	Offset  string `json:"offset"`
	Device  string `json:"device"`
	Inode   string `json:"inode"`
}

var workerFaultHost = localFaultHost
var workerSignalEvidence = localSignalEvidence

func crashSignal(s syscall.Signal) string {
	switch s {
	case syscall.SIGSEGV:
		return "SIGSEGV"
	case syscall.SIGBUS:
		return "SIGBUS"
	case syscall.SIGABRT:
		return "SIGABRT"
	}
	return ""
}
func (w Worker) captureSignalEvidencePID(pid int, dir string, e *memberExit, candidate bool, scopes ...SignalCapture) {
	if e == nil {
		return
	}
	possible := candidate && crashSignal(syscall.Signal(e.Status-128)) != ""
	if e.Signal == "" && !possible {
		return
	}
	source := w.SignalEvidenceSource
	if source == nil {
		source = workerSignalEvidence
	}
	request := SignalCapture{PID: pid, ObservedAt: e.ObservedAt, Directory: dir, StartedAt: e.ObservedAt}
	if len(scopes) > 0 {
		request = scopes[0]
		request.ObservedAt = e.ObservedAt
	}
	var evidence *SignalEvidence
	if request.PID > 0 || request.Cgroup != "" {
		evidence = source(request)
	}
	if evidence == nil {
		evidence = &SignalEvidence{CoreCapture: "unavailable", Unavailable: []string{"payload_pid"}}
	}
	if evidence.PID == 0 {
		evidence.PID = request.PID
	}
	evidence.StartedAt, evidence.ObservedAt, evidence.ExitStatus, evidence.CoreDumped = request.StartedAt, e.ObservedAt, e.Status, e.CoreDumped
	if e.Signal != "" {
		evidence.Signal = e.Signal
	}
	if evidence.Signal == "" {
		evidence.Signal = "unknown"
		evidence.Unavailable = append(evidence.Unavailable, "shell_status_is_not_signal_proof")
	}
	e.Evidence = evidence
}

type faultOutput struct {
	bytes.Buffer
	truncated bool
}

func (b *faultOutput) Write(p []byte) (int, error) {
	n := len(p)
	remaining := faultSectionBytes - b.Len()
	if len(p) > remaining {
		b.truncated = true
		p = p[:remaining]
	}
	_, _ = b.Buffer.Write(p)
	return n, nil // Drain bounded output; never retain the rest.
}
func faultJournal(ctx context.Context, args ...string) ([]byte, bool, error) {
	cmd := exec.CommandContext(ctx, "journalctl", args...)
	// Host diagnostics never inherit a job's env or service credentials.
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "LANG=C"}
	var out faultOutput
	cmd.Stdout = &out
	err := cmd.Run()
	return out.Bytes(), out.truncated, err
}

func localSignalEvidence(request SignalCapture) *SignalEvidence {
	pid, at, dir := request.PID, request.ObservedAt, request.Directory
	started := time.Now()
	e := &SignalEvidence{CoreCapture: "unavailable", Unavailable: []string{"native_stack_and_registers_require_operator_core_capture"}}
	if runtime.GOOS != "linux" {
		e.Unavailable = append(e.Unavailable, "native_signal_capture_unsupported")
		return e
	}
	probeguard.Check()
	pattern, patternErr := boundedFaultRead("/proc/sys/kernel/core_pattern")
	limits, limitsErr := boundedFaultRead("/proc/self/limits")
	if patternErr == nil && limitsErr == nil {
		e.CoreCapture = coreCaptureState(string(pattern), string(limits))
		if e.CoreCapture == "disabled_soft_limit" {
			e.Unavailable = append(e.Unavailable, "core_capture_requires_operator_enablement")
		}
	}
	// Reuse the fault monitor's two-second journal I/O budget. This runs only
	// for a fatal signal, off q.mu and process.mu, and never enables a core limit.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	kernel := collectSignalJournal(ctx, e, request, faultJournal)
	if patternErr == nil {
		_, crashed := kernelSignalRecord(kernel, pid)
		patternText := strings.TrimSpace(string(pattern))
		stamp := at
		if !crashed.IsZero() {
			stamp = crashed
		}
		if !strings.Contains(patternText, "%t") || !crashed.IsZero() {
			if path, ok := corePatternPath(patternText, pid, stamp, dir); ok && pid > 0 {
				if st, err := os.Stat(path); err == nil && st.Mode().IsRegular() && !st.ModTime().Before(request.StartedAt) {
					e.CorePointer, e.CoreBytes, e.CoreCapture = path, st.Size(), "file_core_recorded"
				}
			}
		}
	}

	if e.FaultAddress == "" {
		e.Unavailable = append(e.Unavailable, "fault_address")
	}
	if len(e.Libraries) == 0 {
		e.Unavailable = append(e.Unavailable, "loaded_libraries")
	}
	e.CaptureNS = time.Since(started).Nanoseconds()
	return e
}
func collectSignalJournal(ctx context.Context, e *SignalEvidence, request SignalCapture, read func(context.Context, ...string) ([]byte, bool, error)) []byte {
	pid, at := request.PID, request.ObservedAt
	since := "--since=@" + strconv.FormatInt(request.StartedAt.Unix(), 10)
	until := "--until=@" + strconv.FormatInt(at.Unix()+1, 10)
	kernel, truncated, err := read(ctx, "-k", "-b", "--reverse", "--all", "--no-pager", "--quiet", "--output=json", since, until)
	if err == nil && pid > 0 {
		e.FaultAddress = kernelFaultAddress(kernel, pid)
		if e.FaultAddress != "" {
			e.Signal = "SIGSEGV"
		}
	} else {
		e.Unavailable = append(e.Unavailable, "kernel_signal_record")
	}
	if truncated {
		e.Unavailable = append(e.Unavailable, "kernel_signal_record_truncated")
	}
	// Request only specific metadata: COREDUMP_ENVIRON, CMDLINE, stack text and
	// memory bytes are deliberately excluded. PID + boot + event time fence reuse.
	coreArgs := []string{"-b", "--reverse", "--all", "--no-pager", "--quiet", "--output=json", since, until, "--output-fields=COREDUMP_PID,COREDUMP_SIGNAL,COREDUMP_TIMESTAMP,COREDUMP_FILENAME,COREDUMP_TRUNCATED,COREDUMP_PROC_MAPS,COREDUMP_CGROUP,MYCE_CORE_CAPTURE,MYCE_ATTEMPT"}
	if pid > 0 {
		coreArgs = append(coreArgs, "COREDUMP_PID="+strconv.Itoa(pid))
	} else if request.Cgroup != "" {
		coreArgs = append(coreArgs, "COREDUMP_CGROUP="+request.Cgroup)
	}
	core, truncated, err := read(ctx, coreArgs...)
	if err == nil {
		applyCoreMetadataFor(e, core, request)
	} else {
		e.Unavailable = append(e.Unavailable, "core_metadata")
	}
	if e.PID > 0 && e.FaultAddress == "" {
		e.FaultAddress = kernelFaultAddress(kernel, e.PID)
	}
	if truncated {
		e.Unavailable = append(e.Unavailable, "core_metadata_truncated")
	}
	return kernel
}

func coreCaptureState(pattern, limits string) string {
	p := strings.TrimSpace(pattern)
	// Linux ignores RLIMIT_CORE for pipe handlers. Whether the configured
	// handler accepts this process remains unknown until its record is readable.
	if strings.HasPrefix(p, "|") {
		if strings.Contains(p, "systemd-coredump") {
			return "systemd_coredump_configured_no_record"
		}
		return "pipe_handler_configured_no_record"
	}
	for _, line := range strings.Split(limits, "\n") {
		f := strings.Fields(line)
		if len(f) >= 5 && strings.Join(f[:4], " ") == "Max core file size" && f[4] == "0" {
			return "disabled_soft_limit"
		}
	}
	if p != "" {
		return "file_pattern_configured_no_record"
	}
	return "unavailable"
}

func kernelFaultAddress(data []byte, pid int) string {
	address, _ := kernelSignalRecord(data, pid)
	return address
}
func kernelSignalRecord(data []byte, pid int) (string, time.Time) {
	re := regexp.MustCompile(`\[` + strconv.Itoa(pid) + `\]: (?:segfault|general protection fault).*? at ([0-9a-fA-Fx]+)\b`)
	for _, line := range bytes.Split(data, []byte("\n")) {
		var entry struct {
			Message  string `json:"MESSAGE"`
			Realtime string `json:"__REALTIME_TIMESTAMP"`
		}
		if json.Unmarshal(line, &entry) == nil {
			if m := re.FindStringSubmatch(entry.Message); m != nil {
				micros, _ := strconv.ParseInt(entry.Realtime, 10, 64)
				var at time.Time
				if micros > 0 {
					at = time.UnixMicro(micros)
				}
				return m[1], at
			}
		}
	}
	return "", time.Time{}
}
func applyCoreMetadataFor(e *SignalEvidence, data []byte, request SignalCapture) {
	for _, line := range bytes.Split(data, []byte("\n")) {
		var entry struct {
			PID       string `json:"COREDUMP_PID"`
			Signal    string `json:"COREDUMP_SIGNAL"`
			Timestamp string `json:"COREDUMP_TIMESTAMP"`
			Filename  string `json:"COREDUMP_FILENAME"`
			Truncated string `json:"COREDUMP_TRUNCATED"`
			Maps      string `json:"COREDUMP_PROC_MAPS"`
			Cgroup    string `json:"COREDUMP_CGROUP"`
			Capture   string `json:"MYCE_CORE_CAPTURE"`
			Attempt   string `json:"MYCE_ATTEMPT"`
		}
		if json.Unmarshal(line, &entry) != nil || request.PID > 0 && entry.PID != strconv.Itoa(request.PID) || request.PID == 0 && (request.Cgroup == "" || entry.Cgroup != request.Cgroup) {
			continue
		}
		micros, err := strconv.ParseInt(entry.Timestamp, 10, 64)
		if err != nil {
			continue
		}
		at := time.UnixMicro(micros)
		// The kernel's %t is whole seconds, even though systemd serializes
		// microseconds. Do not discard a crash in its launch second. Matching
		// is current boot + retained payload PID + execution window; systemd
		// exports no /proc/PID/stat birth value to independently compare.
		if at.Before(request.StartedAt.Truncate(time.Second)) || at.After(request.ObservedAt) {
			continue
		}
		signal, err := strconv.Atoi(entry.Signal)
		if err != nil || crashSignal(syscall.Signal(signal)) == "" {
			continue
		}
		if e.Signal != "" && e.Signal != crashSignal(syscall.Signal(signal)) {
			continue
		}
		e.PID, _ = strconv.Atoi(entry.PID)
		if request.PID > 0 {
			e.Attribution = "payload_pid"
		} else {
			e.Attribution = "attempt_cgroup"
		}
		e.Signal = crashSignal(syscall.Signal(signal))
		if entry.Capture == "suppressed_ephemeral" && regexp.MustCompile(`^attempt-[0-9a-f]{32}$`).MatchString(entry.Attempt) && strings.Contains(entry.Cgroup+"/", "/"+entry.Attempt+"/") {
			e.CoreCapture = "suppressed_ephemeral"
			e.CorePointer, e.CoreBytes, e.CoreTruncated, e.Libraries = "", 0, false, nil
			return
		}
		e.Libraries = parseFaultMaps(entry.Maps)
		if filepath.IsAbs(entry.Filename) && !strings.ContainsAny(entry.Filename, "\n\x00") {
			e.CorePointer, e.CoreCapture, e.CoreTruncated = entry.Filename, "systemd_core_recorded", entry.Truncated == "1"
		}
		return // journal queries are newest-first; keep the first matching record.
	}
}
func parseFaultMaps(data string) []FaultLibrary {
	if len(data) > faultSectionBytes {
		data = data[:faultSectionBytes]
	}
	var out []FaultLibrary
	seen := map[string]bool{}
	for _, line := range strings.Split(data, "\n") {
		f := strings.Fields(line)
		if len(f) < 6 || !strings.Contains(f[1], "x") || !strings.HasPrefix(f[5], "/") {
			continue
		}
		key := fmt.Sprint(f[3:6])
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, FaultLibrary{Name: filepath.Base(f[5]), Address: f[0], Offset: f[2], Device: f[3], Inode: f[4]})
	}
	return out
}

func signalCgroup(path string) string {
	if value, ok := strings.CutPrefix(path, "/sys/fs/cgroup"); ok && strings.HasPrefix(value, "/") {
		return value
	}
	return ""
}
func commandNamespaceLauncher(cmd *exec.Cmd) bool {
	for i, arg := range cmd.Args {
		if arg == "_native-exec" && i+1 < len(cmd.Args) && cmd.Args[i+1] == "namespace-monitor" {
			return true
		}
	}
	return false
}
