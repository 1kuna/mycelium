package myce

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

func fenceCustodianStartup(ctx context.Context, dir, id string) error {
	return fenceCustodianStartupWithHandles(ctx, dir, id, func(ctx context.Context, dir, id string) error {
		for _, name := range []string{custodianName(id, "request"), "custodian-" + id + ".log"} {
			if err := nativeScratchEmpty(ctx, filepath.Join(custodianDir(dir), name)); err != nil {
				return err
			}
		}
		return nil
	})
}

func fenceCustodianStartupWithHandles(ctx context.Context, dir, id string, handles func(context.Context, string, string) error) error {
	if err := custodianManagerAbsent(ctx, id); err != nil {
		return err
	}
	return fenceCustodianProcesses(ctx, dir, id, handles)
}

func custodianManagerAbsent(ctx context.Context, id string) error {
	// Not-found is required, not merely inactive. A loaded scope/job can still
	// contain or dispatch a helper. Inspector errors never prove absence.
	cmd := exec.CommandContext(ctx, "systemctl", "--user", "show", custodianUnit(id), "--property=LoadState,ActiveState,SubState,Job,ControlGroup")
	cmd.Env = withoutMyceToken(os.Environ())
	var out, diagnostic bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &diagnostic
	err := cmd.Run()
	var exit *exec.ExitError
	if err != nil && (!errors.As(err, &exit) || exit.ExitCode() != 1) || diagnostic.Len() != 0 || ctx.Err() != nil {
		return errors.New("custodian manager inspection failed; custody retained")
	}
	return absentCustodianScope(out.String())
}

func fenceCustodianProcesses(ctx context.Context, dir, id string, handles func(context.Context, string, string) error) error {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return err
	}
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid == os.Getpid() {
			continue
		}
		path := filepath.Join("/proc", entry.Name())
		info, err := os.Stat(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return errors.New("custodian process inventory incomplete; custody retained")
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return errors.New("custodian process ownership unknown")
		}
		if stat.Uid != uint32(os.Geteuid()) {
			continue
		}
		b, err := os.ReadFile(filepath.Join(path, "cmdline"))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return errors.New("custodian process arguments unreadable; custody retained")
		}
		if bytes.Contains(b, []byte(id)) {
			return errors.New("custodian launcher/helper remains live; custody retained")
		}
	}
	// The handle inspector is still mandatory; callers may explicitly choose
	// the privileged kernel inspector when account-wide lsof is incomplete.
	return handles(ctx, dir, id)
}

func absentCustodianScope(output string) error {
	expected := map[string]string{"LoadState": "not-found", "ActiveState": "inactive", "SubState": "dead", "Job": "", "ControlGroup": ""}
	for _, line := range strings.Split(strings.TrimSuffix(output, "\n"), "\n") {
		key, value, ok := strings.Cut(line, "=")
		want, exists := expected[key]
		if !ok || !exists || value != want {
			return errors.New("custodian scope/job absent state unconfirmed; custody retained")
		}
		delete(expected, key)
	}
	if len(expected) != 0 {
		return errors.New("custodian manager inventory incomplete; custody retained")
	}
	return nil
}
