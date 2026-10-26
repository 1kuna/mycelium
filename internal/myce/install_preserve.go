package myce

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
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
	if old.Role != next.Role || old.Service != next.Service || old.Binary != next.Binary || old.State != next.State || old.Config != next.Config || old.Credentials != next.Credentials || old.HostID != next.HostID || old.WorkerUser != next.WorkerUser || old.WorkerHome != next.WorkerHome || old.Device != next.Device || old.Detection.OS != next.Detection.OS || old.Detection.Arch != next.Detection.Arch {
		return errors.New("installed role, account, host, device, unit and state identity cannot change; preserve custody and install separately")
	}
	return nil
}

func validateInstallDropIns(ctx context.Context, c ServiceConfig, command func(context.Context, string, ...string) ([]byte, error)) error {
	for _, unit := range []string{filepath.Base(c.Service)} {
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
				if strings.HasPrefix(key, "Exec") || strings.HasPrefix(key, "Condition") || strings.HasPrefix(key, "Assert") || strings.HasPrefix(key, "Environment") || key == "User" || key == "Group" || key == "WorkingDirectory" || key == "RootDirectory" || key == "RootImage" || key == "KillMode" || key == "SendSIGKILL" || key == "Delegate" || key == "Restart" || key == "TimeoutStopSec" {
					return errors.New("effective service drop-in conflicts with installed service contract; preserve it and resolve before activation")
				}
			}
		}
	}
	return nil
}

// installFiles is a per-invocation fault seam, never a global filesystem hook.
type installFiles struct {
	retain            func(*installTransaction)
	machineConversion bool
	// Persist ownership before publication, including across a process crash.
	publication func(path string, data []byte, mode os.FileMode, removed bool) error

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
	retired   []string
	units     []installUnit
}

func (tx *installTransaction) put(path string, data []byte, mode os.FileMode) error {
	old, ok := tx.originals[path]
	if !ok {
		return errors.New("artifact missing from install snapshot")
	}
	if tx.files.publication != nil {
		if err := tx.files.publication(path, data, mode, false); err != nil {
			return err
		}
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
		if tx.fileMatches(p.path) {
			continue
		}
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

// StateRoots uses the runtime registries, including their effective cache defaults.
func (c ServiceConfig) StateRoots() ([]string, error) {
	if c.Role == "worker" {
		return (Worker{StateDir: c.State, CacheRoot: c.ModelCache, EnvironmentRoot: c.EnvironmentCache}).StateRoots()
	}
	if c.Role == "queue" {
		return append([]string{c.State}, QueueStateRoots(c.DB, c.UploadSpoolDir, "", "", true)...), nil
	}
	return []string{c.State}, nil
}

// retire preserves obsolete owned artifacts beside the existing install backups.
// It shares the publication journal, so failures use the same exact restore path.
func (tx *installTransaction) retire(path string) error {
	old, ok := tx.originals[path]
	if !ok {
		return errors.New("artifact missing from install snapshot")
	}
	if !old.existed {
		return nil
	}
	if tx.files.publication != nil {
		if err := tx.files.publication(path, nil, 0, true); err != nil {
			return err
		}
	}
	trash := filepath.Join(filepath.Dir(path), ".Trash")
	if err := os.MkdirAll(trash, 0700); err != nil {
		return err
	}
	old.backup = filepath.Join(trash, ID()+"-"+filepath.Base(path))
	if err := tx.files.check("retire", path); err != nil {
		return err
	}
	if err := os.Rename(path, old.backup); err != nil {
		return err
	}
	tx.published = append(tx.published, old)
	return syncWorkerDir(filepath.Dir(path))
}

// pruneBackups keeps the immediately replaced artifact for each installed path.
// Only committed transactions prune; current files and this transaction's exact
// backups are never candidates. This is diagnostic retention, not rollback policy.
func (tx *installTransaction) pruneBackups() error {
	var result error
	keep := map[string]string{}
	for _, p := range tx.published {
		if p.backup != "" {
			keep[p.path] = p.backup
		}
	}
	for path := range tx.originals {
		trash := filepath.Join(filepath.Dir(path), ".Trash")
		info, err := os.Lstat(trash)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			result = errors.Join(result, err)
			continue
		}
		if !info.IsDir() {
			result = errors.Join(result, errors.New("install backup root must be a real directory"))
			continue
		}
		entries, err := os.ReadDir(trash)
		if err != nil {
			result = errors.Join(result, err)
			continue
		}
		var backups []string
		var latest time.Time
		retained := keep[path]
		for _, entry := range entries {
			prefix, ok := strings.CutSuffix(entry.Name(), "-"+filepath.Base(path))
			if !ok || !validAttemptIdentity(prefix) {
				continue
			}
			info, err := entry.Info()
			if err != nil {
				result = errors.Join(result, err)
				continue
			}
			if !info.Mode().IsRegular() {
				continue
			}
			backup := filepath.Join(trash, entry.Name())
			backups = append(backups, backup)
			if keep[path] == "" && (retained == "" || info.ModTime().After(latest)) {
				retained, latest = backup, info.ModTime()
			}
		}
		for _, backup := range backups {
			if backup != retained {
				result = errors.Join(result, os.Remove(backup))
			}
		}
	}
	return result
}
