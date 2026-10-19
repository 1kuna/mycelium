package myce

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// InstallFlags uses the same table as parsing, overlay and validation.
func (c ServiceConfig) InstallFlags() map[string]string {
	hadRetention := c.ContainerRetention != nil
	f := flag.NewFlagSet("saved-install", flag.ContinueOnError)
	RegisterInstallFlags(f, &c)
	out := map[string]string{}
	f.VisitAll(func(v *flag.Flag) {
		if v.Name != "container-retention" || hadRetention {
			out[v.Name] = v.Value.String()
		}
	})
	return out
}

func ValidateInstallIdentity(old, next ServiceConfig) error {
	if old.Role != next.Role || old.Service != next.Service || old.Binary != next.Binary || old.State != next.State || old.Config != next.Config || old.Credentials != next.Credentials || old.HostStateDir != next.HostStateDir || old.HostID != next.HostID || old.WorkerUser != next.WorkerUser || old.WorkerHome != next.WorkerHome || old.Device != next.Device || old.Detection.OS != next.Detection.OS || old.Detection.Arch != next.Detection.Arch {
		return errors.New("installed role, account, host, device, unit and state identity cannot change; preserve custody and install separately")
	}
	return nil
}

func prepareClosedWorkerInstall(c ServiceConfig) error {
	var s HostIntent
	_, err := os.Lstat(filepath.Join(c.HostStateDir, hostStateFile))
	if errors.Is(err, os.ErrNotExist) {
		s, err = InitializeHostIntent(c.HostStateDir, c.HostID, RealClock{})
	} else if err == nil {
		s, err = ReadHostIntent(c.HostStateDir)
	}
	if err != nil {
		return err
	}
	if s.Host != c.HostID || s.Desired != "closed" {
		return errors.New("close the installed host before installing or upgrading workers")
	}
	// Neither an install nor a failed install may allocate/renew a host revision,
	// open a sibling device, or treat old custody as released.
	return nil
}

// The manager supplies the effective list, including system and runtime drop-ins.
// Consumer files remain untouched. Refuse overrides of the generated execution
// and fencing contract; unrelated settings (e.g. resource limits) are preserved.
func validateInstallDropIns(ctx context.Context, c ServiceConfig, command func(context.Context, string, ...string) ([]byte, error)) error {
	for _, unit := range []string{filepath.Base(c.Service), "myce-host-reconcile.service", "myce-host-reconcile.timer"} {
		b, err := command(ctx, "systemctl", "--user", "show", unit, "--property=DropInPaths", "--value")
		if err != nil {
			return errors.New("cannot inspect effective service drop-ins")
		}
		for _, path := range strings.Fields(string(b)) {
			if !filepath.IsAbs(path) || strings.Contains(path, "\\") {
				return errors.New("cannot inspect effective service drop-ins")
			}
			st, err := os.Lstat(path)
			if err != nil || !st.Mode().IsRegular() || st.Size() > 1<<20 {
				return errors.New("cannot inspect effective service drop-ins")
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return errors.New("cannot inspect effective service drop-ins")
			}
			for _, line := range strings.Split(string(data), "\n") {
				key, _, ok := strings.Cut(strings.TrimSpace(line), "=")
				if !ok {
					continue
				}
				key = strings.TrimSpace(key)
				if strings.HasPrefix(key, "Exec") || strings.HasPrefix(key, "Condition") || strings.HasPrefix(key, "Assert") || strings.HasPrefix(key, "Environment") || strings.HasPrefix(key, "On") || key == "Unit" || key == "User" || key == "Group" || key == "WorkingDirectory" || key == "RootDirectory" || key == "RootImage" || key == "KillMode" || key == "SendSIGKILL" || key == "Delegate" || key == "Restart" || key == "TimeoutStopSec" {
					return errors.New("effective service drop-in conflicts with installed startup gates; preserve it and resolve before activation")
				}
			}
		}
	}
	return nil
}

// installFiles is a per-invocation fault seam, never a global filesystem hook.
type installFiles struct {
	before func(operation, path string) error
	rename func(source, destination string) error
}

func (f installFiles) check(op, path string) error {
	if f.before != nil {
		return f.before(op, path)
	}
	return nil
}

type installedFile struct {
	path, backup string
	existed      bool
	data         []byte
	mode         os.FileMode
}
type installTransaction struct {
	files     installFiles
	published []installedFile
	originals map[string]installedFile
	manager   installManager
	units     []installUnit
}

func (tx *installTransaction) put(path string, data []byte, mode os.FileMode) error {
	old, ok := tx.originals[path]
	if !ok {
		return errors.New("artifact missing from install snapshot")
	}
	if old.existed {
		trash := filepath.Join(filepath.Dir(path), ".Trash")
		if err := os.MkdirAll(trash, 0700); err != nil {
			return err
		}
		old.backup = filepath.Join(trash, ID()+"-"+filepath.Base(path))
		if err := writeInstallFile(old.backup, old.data, old.mode, installFiles{}, nil); err != nil {
			return err
		}
	}
	// Register immediately after rename, including failures syncing its directory.
	return writeInstallFile(path, data, mode, tx.files, func() { tx.published = append(tx.published, old) })
}
func writeInstallFile(path string, data []byte, mode os.FileMode, files installFiles, published func()) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".myce-install-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err = f.Chmod(mode); err == nil {
		err = files.check("write", path)
	}
	if err == nil {
		_, err = f.Write(data)
	}
	if err == nil {
		err = files.check("fsync", path)
	}
	if err == nil {
		err = f.Sync()
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		return err
	}
	if err = files.check("rename", path); err != nil {
		return err
	}
	rename := files.rename
	if rename == nil {
		rename = os.Rename
	}
	if err = rename(f.Name(), path); err != nil {
		return err
	}
	if published != nil {
		published()
	}
	if err = files.check("directory-fsync", path); err != nil {
		return err
	}
	return syncWorkerDir(filepath.Dir(path))
}
func (tx *installTransaction) restoreFiles() error {
	var result error
	for i := len(tx.published) - 1; i >= 0; i-- {
		p := tx.published[i]
		err := tx.files.check("restore", p.path)
		if err == nil {
			if p.existed {
				err = writeInstallFile(p.path, p.data, p.mode, installFiles{before: func(op, path string) error { return tx.files.check("restore-"+op, path) }}, nil)
			} else {
				err = tx.files.check("restore-remove", p.path)
				if err == nil {
					err = os.Remove(p.path)
				}
				if errors.Is(err, os.ErrNotExist) {
					err = nil
				}
				if err == nil {
					err = tx.files.check("restore-directory-fsync", p.path)
				}
				if err == nil {
					err = syncWorkerDir(filepath.Dir(p.path))
				}
			}
		}
		if err != nil {
			result = errors.Join(result, fmt.Errorf("restore %s: %w", filepath.Base(p.path), err))
		}
	}
	return result
}

// installedHostWorker refreshes installation bindings without rotating a session
// or dropping saved executions. It is safe while CLOSED; it admits no work.
func installedHostWorker(c ServiceConfig) (string, HostWorker, error) {
	state, err := filepath.Abs(c.State)
	if err != nil {
		return "", HostWorker{}, err
	}
	sum := sha256.Sum256([]byte(state))
	path := filepath.Join(c.HostStateDir, "worker-"+hex.EncodeToString(sum[:16])+".json")
	var w HostWorker
	if b, e := os.ReadFile(path); e == nil {
		if e = json.Unmarshal(b, &w); e != nil {
			return "", w, e
		}
		if w.Host != c.HostID || w.Name != c.Name || w.State != c.State {
			return "", w, ErrHostIdentity
		}
	} else if !errors.Is(e, os.ErrNotExist) {
		return "", w, e
	}
	if b, e := os.ReadFile(filepath.Join(c.State, "state.json")); e == nil {
		var st workerState
		if e = decodeWorkerState(b, &st); e != nil {
			return "", w, e
		}
		if w.Session == "" {
			w.Session = st.Session
		}
	} else if !errors.Is(e, os.ErrNotExist) {
		return "", w, e
	}
	w.Host, w.Name, w.State, w.Config, w.Unit, w.Queue, w.Environments = c.HostID, c.Name, c.State, c.Config, filepath.Base(c.Service), c.Queue, c.EnvironmentCache
	w.Command = []string{c.Binary, "service", "--config", c.Config}
	return path, w, nil
}

func RegisterInstalledWorker(c ServiceConfig) error {
	unlock, err := hostOperationLock(c.HostStateDir, "intake.lock", true)
	if err != nil {
		return err
	}
	defer unlock()
	s, err := ReadHostIntent(c.HostStateDir)
	if err != nil {
		return err
	}
	if s.Host != c.HostID {
		return ErrHostIdentity
	}
	path, w, err := installedHostWorker(c)
	if err != nil {
		return err
	}
	return saveWorkerJSON(filepath.Dir(path), filepath.Base(path), w)
}
