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
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
	"mycelium/internal/probeguard"
)

// journalctl owns journal enumeration/cursors; no sudo or privileged helper.
// A readable kmsg ring is the ordinary-account fallback. Without a cursor the
// read starts at since, or at the last record already read, never at the start
// of the boot: a 313k-line boot journal took 9.4 s against the 2 s budget (#595).
func localDeviceFaultSource() DeviceFaultSource {
	if runtime.GOOS != "linux" {
		return nil
	}
	journal := func(ctx context.Context, args []string) ([]byte, error) {
		command := exec.CommandContext(ctx, "journalctl", args...)
		command.Env = withoutMyceToken(os.Environ())
		return command.Output()
	}
	bootID := func() ([]byte, error) {
		probeguard.Check() // Runs first on every scan, before journal or kmsg access.
		return os.ReadFile("/proc/sys/kernel/random/boot_id")
	}
	return kernelLogSource(bootID, journal, readKmsg)
}

func kernelLogSource(bootID func() ([]byte, error), journal func(context.Context, []string) ([]byte, error), kmsg func(context.Context, time.Time) ([]KernelLogLine, error)) DeviceFaultSource {
	var cursor, previousBoot string
	var last time.Time
	return func(ctx context.Context, since time.Time) (KernelLogSnapshot, error) {
		data, err := bootID()
		if err != nil {
			return KernelLogSnapshot{}, err
		}
		snapshot := KernelLogSnapshot{BootID: strings.TrimSpace(string(data))}
		if snapshot.BootID != previousBoot {
			cursor, last = "", time.Time{}
			previousBoot = snapshot.BootID
		}
		if last.After(since) {
			since = last
		}
		output, err := journal(ctx, kernelJournalArgs(cursor, since))
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
			if len(lines) > 0 && lines[len(lines)-1].At.After(last) {
				last = lines[len(lines)-1].At
			}
			return snapshot, nil
		}
		// A rotated cursor can fail; the next scan resumes from the last record time.
		cursor = ""
		snapshot.Lines, err = kmsg(ctx, since)
		return snapshot, err
	}
}

func readKmsg(ctx context.Context, since time.Time) ([]KernelLogLine, error) {
	fd, err := unix.Open("/dev/kmsg", unix.O_RDONLY|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, errors.New("kernel log not readable")
	}
	defer unix.Close(fd)
	// kmsg stamps records in monotonic microseconds; anchor them to wall time.
	var mono unix.Timespec
	_ = unix.ClockGettime(unix.CLOCK_MONOTONIC, &mono)
	boot := time.Now().Add(-time.Duration(mono.Nano()))
	var lines []KernelLogLine
	buf := make([]byte, 64*1024)
	for ctx.Err() == nil {
		n, err := unix.Read(fd, buf)
		if errors.Is(err, unix.EAGAIN) || err == nil && n == 0 {
			return lines, nil
		}
		if errors.Is(err, unix.EPIPE) {
			continue
		} // Ring overwrite; continue from oldest retained record.
		if err != nil {
			return lines, err
		}
		header, message, ok := strings.Cut(string(buf[:n]), ";")
		fields := strings.Split(header, ",")
		if ok && len(fields) >= 3 {
			micros, _ := strconv.ParseInt(fields[2], 10, 64)
			if at := boot.Add(time.Duration(micros) * time.Microsecond); !at.Before(since) {
				lines = append(lines, KernelLogLine{ID: fields[2], At: at, Message: strings.TrimSpace(strings.SplitN(message, "\n", 2)[0])})
			}
		}
	}
	return lines, ctx.Err()
}

func kernelJournalArgs(cursor string, since time.Time) []string {
	args := []string{"-k", "-b", "--no-pager", "--output=json", "--quiet"}
	if cursor != "" {
		return append(args, "--after-cursor="+cursor)
	}
	return append(args, "--since=@"+strconv.FormatInt(since.Unix(), 10))
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
			Realtime   string          `json:"__REALTIME_TIMESTAMP"`
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
		line := KernelLogLine{ID: entry.Time, Message: message}
		if micros, err := strconv.ParseInt(entry.Realtime, 10, 64); err == nil {
			line.At = time.UnixMicro(micros)
		}
		lines = append(lines, line)
		cursor = entry.Cursor
	}
	return lines, cursor, scan.Err()
}
