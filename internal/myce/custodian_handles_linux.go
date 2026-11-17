package myce

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// This opt-in inspector uses existing operator sudo authority, not a new host
// grant. Only the read-only helper runs elevated; recovery and its locks remain
// in the ordinary worker account. No token/configuration is read by the helper.
func privilegedCustodianStartupFence(ctx context.Context, dir, id string) error {
	return fenceCustodianStartupWithHandles(ctx, dir, id, privilegedCustodianHandlesEmpty)
}

func privilegedCustodianHandlesEmpty(ctx context.Context, dir, id string) error {
	binary, err := os.Executable()
	if err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, "sudo", "-n", "--", binary, "_inspect-custodian-handles", "--state-dir", dir, "--custodian-id", id, "--owner-uid", strconv.Itoa(os.Geteuid()))
	cmd.Env = withoutMyceToken(os.Environ())
	cmd.WaitDelay = time.Second
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("privileged custodian handle inspection failed; custody retained: %s", strings.TrimSpace(string(output)))
	}
	if string(output) != "custodian handles clear: "+id+"\n" {
		return errors.New("invalid privileged handle inspection response; custody retained")
	}
	return nil
}

type custodianHandleFile struct {
	dev   uint64
	inode uint64
}

// InspectCustodianHandles is a root-only, read-only kernel metadata probe for
// exactly the request/log files owned by the specified ordinary worker account.
// It does not load configuration, lock/write state, signal, launch or use a queue.
func InspectCustodianHandles(ctx context.Context, dir, id string, uid int) error {
	if os.Geteuid() != 0 {
		return errors.New("custodian handle inspector requires existing root inspection authority")
	}
	if uid <= 0 || !filepath.IsAbs(dir) || !validCustodianID(id) {
		return errors.New("exact ordinary owner, absolute spool and custodian ID required")
	}
	custody := custodianDir(dir)
	info, err := os.Lstat(custody)
	if err != nil {
		return errors.New("custody directory unavailable for handle inspection")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.IsDir() || stat.Uid != uint32(uid) {
		return errors.New("custody directory owner/type mismatch")
	}
	paths := []string{filepath.Join(custody, custodianName(id, "request")), filepath.Join(custody, "custodian-"+id+".log")}
	targets := make([]custodianHandleFile, 0, len(paths))
	for _, path := range paths {
		info, err := os.Lstat(path)
		if err != nil {
			return errors.New("custodian handle target unavailable")
		}
		st, ok := info.Sys().(*syscall.Stat_t)
		if !ok || !info.Mode().IsRegular() || st.Uid != uint32(uid) {
			return errors.New("custodian handle target owner/type mismatch")
		}
		targets = append(targets, custodianHandleFile{uint64(st.Dev), st.Ino})
	}
	if err := inspectCustodianProcHandles(ctx, "/proc", targets); err != nil {
		return err
	}
	// A rename/replacement during inspection cannot certify different files.
	for i, path := range paths {
		info, err := os.Lstat(path)
		if err != nil {
			return errors.New("custodian handle target changed during inspection")
		}
		st, ok := info.Sys().(*syscall.Stat_t)
		if !ok || !info.Mode().IsRegular() || uint64(st.Dev) != targets[i].dev || st.Ino != targets[i].inode {
			return errors.New("custodian handle target changed during inspection")
		}
	}
	return ctx.Err()
}

func inspectCustodianProcHandles(ctx context.Context, root string, targets []custodianHandleFile) error {
	entries, err := os.ReadDir(root)
	if err != nil {
		return errors.New("kernel process inventory unavailable; custody retained")
	}
	inspected := 0
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 0 {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		proc := filepath.Join(root, entry.Name())
		fds, err := os.ReadDir(filepath.Join(proc, "fd"))
		if errors.Is(err, os.ErrNotExist) {
			if _, gone := os.Stat(proc); errors.Is(gone, os.ErrNotExist) {
				continue
			}
		}
		if err != nil {
			return fmt.Errorf("PID %d descriptors unavailable; custody retained", pid)
		}
		for _, fd := range fds {
			if err := ctx.Err(); err != nil {
				return err
			}
			info, err := os.Stat(filepath.Join(proc, "fd", fd.Name()))
			if errors.Is(err, os.ErrNotExist) {
				continue
			} // FD closed or process exited.
			if err != nil {
				return fmt.Errorf("PID %d descriptor %s inspection failed; custody retained", pid, fd.Name())
			}
			st, ok := info.Sys().(*syscall.Stat_t)
			if !ok {
				return fmt.Errorf("PID %d descriptor identity unavailable", pid)
			}
			for _, target := range targets {
				if uint64(st.Dev) == target.dev && st.Ino == target.inode {
					return fmt.Errorf("PID %d descriptor %s holds custodian file; custody retained", pid, fd.Name())
				}
			}
		}
		maps, err := os.Open(filepath.Join(proc, "maps"))
		if errors.Is(err, os.ErrNotExist) {
			if _, gone := os.Stat(proc); errors.Is(gone, os.ErrNotExist) {
				continue
			}
		}
		if err != nil {
			return fmt.Errorf("PID %d mappings unavailable; custody retained", pid)
		}
		scan := bufio.NewScanner(maps)
		// Maps contain arbitrary user filenames; don't truncate a long mapping and
		// then call the rest of its inventory empty.
		scan.Buffer(make([]byte, 4096), 1<<20)
		mapErr := inspectCustodianMaps(ctx, pid, scan, targets)
		closeErr := maps.Close()
		if mapErr != nil {
			return mapErr
		}
		if closeErr != nil {
			return fmt.Errorf("PID %d mapping inspection failed", pid)
		}
		inspected++
	}
	if inspected == 0 {
		return errors.New("empty kernel process inventory; custody retained")
	}
	return ctx.Err()
}

func inspectCustodianMaps(ctx context.Context, pid int, scan *bufio.Scanner, targets []custodianHandleFile) error {
	for scan.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		fields := strings.Fields(scan.Text())
		if len(fields) < 5 {
			return fmt.Errorf("PID %d malformed mapping inventory", pid)
		}
		major, minor, ok := strings.Cut(fields[3], ":")
		a, e1 := strconv.ParseUint(major, 16, 32)
		b, e2 := strconv.ParseUint(minor, 16, 32)
		inode, e3 := strconv.ParseUint(fields[4], 10, 64)
		if !ok || e1 != nil || e2 != nil || e3 != nil {
			return fmt.Errorf("PID %d malformed mapping identity", pid)
		}
		if inode == 0 {
			continue
		}
		for _, target := range targets {
			if uint64(unix.Major(target.dev)) == a && uint64(unix.Minor(target.dev)) == b && target.inode == inode {
				return fmt.Errorf("PID %d mapping holds custodian file; custody retained", pid)
			}
		}
	}
	if scan.Err() != nil {
		return fmt.Errorf("PID %d incomplete mapping inventory; custody retained", pid)
	}
	return nil
}
