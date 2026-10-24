package myce

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"runtime"
	"strings"

	"golang.org/x/sys/unix"
	"mycelium/internal/probeguard"
)

// journalctl owns journal enumeration/cursors; no sudo or privileged helper.
// A readable kmsg ring is the ordinary-account fallback.
func localDeviceFaultSource() DeviceFaultSource {
	if runtime.GOOS != "linux" {
		return nil
	}
	var cursor, previousBoot string
	return func(ctx context.Context) (KernelLogSnapshot, error) {
		probeguard.Check()
		data, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
		if err != nil {
			return KernelLogSnapshot{}, err
		}
		snapshot := KernelLogSnapshot{BootID: strings.TrimSpace(string(data))}
		if snapshot.BootID != previousBoot {
			cursor = ""
			previousBoot = snapshot.BootID
		}
		args := []string{"-k", "-b", "--no-pager", "--output=json", "--quiet"}
		if cursor != "" {
			args = append(args, "--after-cursor="+cursor)
		}
		command := exec.CommandContext(ctx, "journalctl", args...)
		command.Env = withoutMyceToken(os.Environ())
		output, err := command.Output()
		if err == nil {
			lines, next, parseErr := parseKernelJournal(output)
			if parseErr != nil {
				cursor = ""
				return snapshot, parseErr
			}
			snapshot.Lines = lines
			if next != "" {
				cursor = next
			}
			return snapshot, nil
		}
		// A rotated cursor can fail; the next scan retries the current boot.
		cursor = ""
		fd, err := unix.Open("/dev/kmsg", unix.O_RDONLY|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
		if err != nil {
			return snapshot, errors.New("kernel log not readable")
		}
		defer unix.Close(fd)
		buf := make([]byte, 64*1024)
		for ctx.Err() == nil {
			n, err := unix.Read(fd, buf)
			if errors.Is(err, unix.EAGAIN) {
				return snapshot, nil
			}
			if errors.Is(err, unix.EPIPE) {
				continue
			} // Ring overwrite; continue from oldest retained record.
			if err != nil {
				return snapshot, err
			}
			if n == 0 {
				return snapshot, nil
			}
			header, message, ok := strings.Cut(string(buf[:n]), ";")
			fields := strings.Split(header, ",")
			if ok && len(fields) >= 3 {
				snapshot.Lines = append(snapshot.Lines, KernelLogLine{ID: fields[2], Message: strings.TrimSpace(strings.SplitN(message, "\n", 2)[0])})
			}
		}
		return snapshot, ctx.Err()
	}
}

// Parse journal JSON independently of native access so real fault messages can
// be replayed on any laptop. Commit the cursor only after a complete parse.
func parseKernelJournal(data []byte) ([]KernelLogLine, string, error) {
	var lines []KernelLogLine
	cursor := ""
	scan := bufio.NewScanner(bytes.NewReader(data))
	scan.Buffer(make([]byte, 4096), 1024*1024)
	for scan.Scan() {
		var entry struct {
			Cursor     string          `json:"__CURSOR"`
			Time       string          `json:"__MONOTONIC_TIMESTAMP"`
			SourceTime string          `json:"_SOURCE_MONOTONIC_TIMESTAMP"`
			Message    json.RawMessage `json:"MESSAGE"`
		}
		if err := json.Unmarshal(scan.Bytes(), &entry); err != nil {
			return nil, "", err
		}
		var message string
		if err := json.Unmarshal(entry.Message, &message); err != nil {
			// journal JSON encodes non-UTF8 messages as a byte array.
			var raw []byte
			if err := json.Unmarshal(entry.Message, &raw); err != nil {
				return nil, "", err
			}
			message = string(raw)
		}
		if entry.SourceTime != "" {
			entry.Time = entry.SourceTime
		}
		lines = append(lines, KernelLogLine{ID: entry.Time, Message: message})
		cursor = entry.Cursor
	}
	return lines, cursor, scan.Err()
}
