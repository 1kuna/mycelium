package myce

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// InstallFlags is the saved half of the CLI's presence-based upgrade overlay.
// Identity fields without flags stay in the original ServiceConfig.
func (c ServiceConfig) InstallFlags() map[string]string {
	values := map[string]any{
		"allow-deviceless": c.AllowDeviceless, "device": c.Device, "docker-host": c.DockerEndpoint,
		"host-state-dir": c.HostStateDir, "joining-file": c.JoiningFile, "nas": c.NAS, "queue": c.Queue, "listen": c.Listen, "name": c.Name,
		"gpu": c.Detection.Platform, "memory-mode": c.Detection.MemoryMode, "memory-gb": c.Detection.MemoryGB, "reserve-memory-gb": c.Detection.ReserveGB,
		"host-memory-budget-gb": c.HostMemoryBudgetGB, "memory-ceiling-slack-gb": c.MemoryCeilingSlackGB, "memory-swap-max-bytes": c.MemorySwapMaxBytes,
		"host-id": c.HostID, "reserve-host-memory-gb": c.ReserveHostMemoryGB, "state-dir": c.State, "checkpoint-root": c.Checkpoints,
		"model-root": c.ModelRoot, "environment-root": c.EnvironmentRoot, "model-cache": c.ModelCache, "model-cache-bytes": c.ModelCacheBytes,
		"environment-cache": c.EnvironmentCache, "environment-cache-bytes": c.EnvironmentCacheBytes, "heartbeat": c.Heartbeat, "start-disabled": c.StartDisabled,
		"require-memory-ceilings": c.RequireMemoryCeilings, "require-mounted-nas": c.RequireMountedNAS, "db": c.DB, "project-config": c.ProjectConfig, "host-config": c.HostConfig,
		"log-retention": c.LogRetention, "upload-spool-dir": c.UploadSpoolDir, "upload-spool-bytes": c.UploadSpoolBytes, "restart-grace": c.RestartGrace,
		"production-loss-timeout": c.ProductionLossTimeout, "deadline-loss-timeout": c.DeadlineLossTimeout, "experiment-loss-timeout": c.ExperimentLossTimeout,
		"checkpoints": c.Checkpoints, "environment-file": c.EnvironmentFile, "private-devices": c.PrivateDevices, "no-new-privileges": c.NoNewPrivileges,
		"nice": c.Nice, "cpu-quota": c.CPUQuota, "memory-max": c.MemoryMax, "tasks-max": c.TasksMax, "model-config": c.ModelCatalog, "environment-config": c.EnvironmentCatalog,
	}
	if c.ContainerRetention != nil {
		values["container-retention"] = *c.ContainerRetention
	}
	out := make(map[string]string, len(values))
	for name, value := range values {
		out[name] = fmt.Sprint(value)
	}
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
}
type installTransaction struct {
	files     installFiles
	published []installedFile
}

func (tx *installTransaction) put(path string, data []byte, mode os.FileMode) error {
	old := installedFile{path: path}
	if st, err := os.Lstat(path); err == nil {
		if !st.Mode().IsRegular() {
			return errors.New("refusing non-regular installed artifact")
		}
		old.existed = true
		previous, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		trash := filepath.Join(filepath.Dir(path), ".Trash")
		if err = os.MkdirAll(trash, 0700); err != nil {
			return err
		}
		old.backup = filepath.Join(trash, ID()+"-"+filepath.Base(path))
		if err = writeInstallFile(old.backup, previous, st.Mode().Perm(), installFiles{}, nil); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
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
func (tx *installTransaction) restore() error {
	var result error
	for i := len(tx.published) - 1; i >= 0; i-- {
		p := tx.published[i]
		if p.existed {
			data, err := os.ReadFile(p.backup)
			if err == nil {
				var st os.FileInfo
				st, err = os.Stat(p.backup)
				if err == nil {
					err = writeInstallFile(p.path, data, st.Mode().Perm(), installFiles{}, nil)
				}
			}
			result = errors.Join(result, err)
		} else {
			result = errors.Join(result, os.Remove(p.path), syncWorkerDir(filepath.Dir(p.path)))
		}
	}
	return result
}
