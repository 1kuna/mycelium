package myce

import (
	"bytes"
	"context"
	"encoding/json"
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

type SignalEvidence struct {
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
func (w Worker) captureSignalEvidence(cmd *exec.Cmd, e *memberExit) {
	if cmd.Process != nil {
		w.captureSignalEvidencePID(cmd.Process.Pid, cmd.Dir, e, false)
	}
}
func (w Worker) captureSignalEvidencePID(pid int, dir string, e *memberExit, candidate bool) {
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
	var evidence *SignalEvidence
	if pid > 0 {
		evidence = source(pid, e.ObservedAt, dir)
	}
	if evidence == nil {
		evidence = &SignalEvidence{CoreCapture: "unavailable", Unavailable: []string{"payload_pid"}}
	}
	evidence.PID, evidence.ObservedAt, evidence.ExitStatus, evidence.CoreDumped = pid, e.ObservedAt, e.Status, e.CoreDumped
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

func localSignalEvidence(pid int, at time.Time, dir string) *SignalEvidence {
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
		if path, ok := corePatternPath(strings.TrimSpace(string(pattern)), pid, at, dir); ok {
			if st, err := os.Stat(path); err == nil && st.Mode().IsRegular() && st.ModTime().After(at.Add(-time.Second)) {
				e.CorePointer, e.CoreBytes, e.CoreCapture = path, st.Size(), "file_core_recorded"
			}
		}
	}
	// Reuse the fault monitor's two-second journal I/O budget. This runs only
	// for a fatal signal, off q.mu and process.mu, and never enables a core limit.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	since := "--since=@" + strconv.FormatInt(at.Add(-time.Second).Unix(), 10)
	until := "--until=@" + strconv.FormatInt(at.Add(time.Second).Unix()+1, 10)
	kernel, truncated, err := faultJournal(ctx, "-k", "-b", "--no-pager", "--quiet", "--output=json", since, until)
	if err == nil {
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
	core, truncated, err := faultJournal(ctx, "-b", "--no-pager", "--quiet", "--output=json", since, until, "COREDUMP_PID="+strconv.Itoa(pid), "--output-fields=COREDUMP_PID,COREDUMP_SIGNAL,COREDUMP_TIMESTAMP,COREDUMP_FILENAME,COREDUMP_TRUNCATED,COREDUMP_PROC_MAPS")
	if err == nil {
		applyCoreMetadata(e, core, pid, at)
	} else {
		e.Unavailable = append(e.Unavailable, "core_metadata")
	}
	if truncated {
		e.Unavailable = append(e.Unavailable, "core_metadata_truncated")
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
func coreCaptureState(pattern, limits string) string {
	for _, line := range strings.Split(limits, "\n") {
		f := strings.Fields(line)
		if len(f) >= 5 && strings.Join(f[:4], " ") == "Max core file size" && f[4] == "0" {
			return "disabled_soft_limit"
		}
	}
	p := strings.TrimSpace(pattern)
	if strings.HasPrefix(p, "|") {
		if strings.Contains(p, "systemd-coredump") {
			return "systemd_coredump_configured_no_record"
		}
		return "pipe_handler_configured_no_record"
	}
	if p != "" {
		return "file_pattern_configured_no_record"
	}
	return "unavailable"
}
func kernelFaultAddress(data []byte, pid int) string {
	re := regexp.MustCompile(`\[` + strconv.Itoa(pid) + `\]: (?:segfault|general protection fault).*? at ([0-9a-fA-Fx]+)\b`)
	for _, line := range bytes.Split(data, []byte("\n")) {
		var entry struct {
			Message string `json:"MESSAGE"`
		}
		if json.Unmarshal(line, &entry) == nil {
			if m := re.FindStringSubmatch(entry.Message); m != nil {
				return m[1]
			}
		}
	}
	return ""
}
func applyCoreMetadata(e *SignalEvidence, data []byte, pid int, at time.Time) {
	for _, line := range bytes.Split(data, []byte("\n")) {
		var entry struct {
			PID       string `json:"COREDUMP_PID"`
			Signal    string `json:"COREDUMP_SIGNAL"`
			Timestamp string `json:"COREDUMP_TIMESTAMP"`
			Filename  string `json:"COREDUMP_FILENAME"`
			Truncated string `json:"COREDUMP_TRUNCATED"`
			Maps      string `json:"COREDUMP_PROC_MAPS"`
		}
		if json.Unmarshal(line, &entry) != nil || entry.PID != strconv.Itoa(pid) {
			continue
		}
		micros, err := strconv.ParseInt(entry.Timestamp, 10, 64)
		if err != nil {
			continue
		}
		delta := time.UnixMicro(micros).Sub(at)
		if delta < -time.Second || delta > time.Second {
			continue
		}
		signal, err := strconv.Atoi(entry.Signal)
		if err != nil || crashSignal(syscall.Signal(signal)) == "" {
			continue
		}
		if e.Signal != "" && e.Signal != crashSignal(syscall.Signal(signal)) {
			continue
		}
		e.Signal = crashSignal(syscall.Signal(signal))
		e.Libraries = parseFaultMaps(entry.Maps)
		if filepath.IsAbs(entry.Filename) && !strings.ContainsAny(entry.Filename, "\n\x00") {
			e.CorePointer, e.CoreCapture, e.CoreTruncated = entry.Filename, "systemd_core_recorded", entry.Truncated == "1"
		}
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
