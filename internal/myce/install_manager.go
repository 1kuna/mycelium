package myce

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// installManager adapts the existing injected command runner. The transaction
// owns ordering; only manager operations differ between systemd and launchd.
type installManager struct {
	os      string
	command func(context.Context, string, ...string) ([]byte, error)
}
type installUnitState struct {
	enabled        string
	loaded, active bool
}
type installUnit struct {
	path  string
	label string
	state installUnitState
}

func (u installUnit) name() string { return filepath.Base(u.path) }
func (u installUnit) target() string {
	label := u.label
	if label == "" {
		label = strings.TrimSuffix(u.name(), ".plist")
	}
	return "gui/" + strconv.Itoa(os.Getuid()) + "/" + label
}
func installExit(err error, codes ...int) bool {
	var status interface{ ExitCode() int }
	if !errors.As(err, &status) {
		return false
	}
	for _, code := range codes {
		if status.ExitCode() == code {
			return true
		}
	}
	return false
}
func (m installManager) inspect(ctx context.Context, u installUnit) (installUnitState, error) {
	var s installUnitState
	if m.os == "linux" {
		b, err := m.command(ctx, "systemctl", "--user", "show", u.name(), "--property=UnitFileState", "--value")
		if err != nil {
			return s, err
		}
		s.enabled = strings.TrimSpace(string(b))
		// UnitFileState is empty for a definition absent from the manager. Unlike
		// is-enabled's overloaded exit status, a failed show is never a disabled unit.
		if s.enabled == "" {
			s.enabled = "not-found"
		}

		switch s.enabled {
		case "enabled", "enabled-runtime", "disabled", "static", "indirect", "not-found":
		default:
			return s, errors.New("unit enablement state cannot be safely restored; no install changes made")
		}
		_, err = m.command(ctx, "systemctl", "--user", "is-active", "--quiet", u.name())
		if err != nil && !installExit(err, 3, 4) {
			return s, err
		}
		s.active = err == nil
		return s, nil
	}
	b, err := m.command(ctx, "launchctl", "print", u.target())
	if installExit(err, 113) {
		return s, nil
	} // launchctl's service-not-found status
	if err != nil {
		return s, err
	}
	s.loaded = true
	for _, line := range strings.Split(string(b), "\n") {
		key, value, ok := strings.Cut(strings.TrimSpace(line), " = ")
		if ok && (key == "state" && value == "running" || key == "pid" && value != "0") {
			s.active = true
		}
	}
	return s, nil
}
func (m installManager) reload(ctx context.Context, units []installUnit) error {
	if m.os == "linux" {
		_, err := m.command(ctx, "systemctl", "--user", "daemon-reload")
		return err
	}
	// launchd has no global reload. Unload cached definitions after restoring
	// disk; the registration phase bootstraps exactly the previously loaded jobs.
	var result error
	for _, u := range units {
		s, err := m.inspect(ctx, u)
		if err == nil && s.loaded {
			err = m.stop(ctx, u)
		}
		if err != nil {
			result = errors.Join(result, fmt.Errorf("%s reload: %w", u.name(), err))
		}
	}
	return result
}
func (m installManager) registration(ctx context.Context, u installUnit, want installUnitState) error {
	current, err := m.inspect(ctx, u)
	if err != nil {
		return err
	}
	if m.os == "darwin" {
		if current.loaded == want.loaded {
			return nil
		}
		if want.loaded {
			_, err = m.command(ctx, "launchctl", "bootstrap", "gui/"+strconv.Itoa(os.Getuid()), u.path)
		} else {
			err = m.stop(ctx, u)
		}
		return err
	}
	if current.enabled == want.enabled {
		return nil
	}
	// Drop links from the failed install first, including runtime enablement.
	if current.enabled != "not-found" {
		if _, err = m.command(ctx, "systemctl", "--user", "disable", u.name()); err != nil {
			return err
		}
	}
	switch want.enabled {
	case "enabled", "enabled-runtime":
		args := []string{"--user", "enable"}
		if want.enabled == "enabled-runtime" {
			args = append(args, "--runtime")
		}
		_, err = m.command(ctx, "systemctl", append(args, u.name())...)

	}
	// Static/indirect/absent states come from the restored definition; final
	// inspection must prove the exact state, rather than assuming disable did it.
	return err
}
func (m installManager) activity(ctx context.Context, u installUnit, active bool, restart bool) error {
	current, err := m.inspect(ctx, u)
	if err != nil {
		return err
	}
	if current.active == active && !(restart && active) {
		return nil
	}
	if m.os == "linux" {
		action := "stop"
		if active {
			action = "restart"
		}
		_, err = m.command(ctx, "systemctl", "--user", action, u.name())
	} else if active {
		_, err = m.command(ctx, "launchctl", "kickstart", "-k", u.target())
	} else if current.loaded && current.active {
		_, err = m.command(ctx, "launchctl", "kill", "SIGTERM", u.target())
	}
	return err
}

func installUnitPaths(c ServiceConfig) []string {
	paths := []string{c.Service}
	if c.Detection.OS == "linux" && c.Role == "worker" {
		for _, name := range []string{"myce-host-reconcile.timer", "myce-host-reconcile.service"} {
			paths = append(paths, filepath.Join(filepath.Dir(c.Service), name))
		}
	}
	return paths
}
func snapshotInstall(ctx context.Context, c ServiceConfig, files installFiles, command func(context.Context, string, ...string) ([]byte, error), extraPaths ...string) (*installTransaction, error) {
	tx := &installTransaction{files: files, manager: installManager{c.Detection.OS, command}, originals: map[string]installedFile{}}
	paths := []string{c.Binary, c.Config, c.Credentials}
	paths = append(paths, extraPaths...)
	if c.Detection.OS == "linux" && c.Role == "worker" {
		paths = append(paths, c.GPUEnvironmentFile())
	}
	for _, path := range installUnitPaths(c) {
		u := installUnit{path: path}
		if c.Detection.OS == "darwin" {
			u.label = "local.mycelium." + c.Role
		} // ServiceText fixes Label independently of the plist filename.
		state, err := tx.manager.inspect(ctx, u)
		if err != nil {
			return nil, fmt.Errorf("snapshot %s: %w", u.name(), err)
		}
		u.state = state
		tx.units = append(tx.units, u)
		paths = append(paths, path)
	}
	retired, err := obsoleteStartDropIns(ctx, c, command)
	if err != nil {
		return nil, err
	}
	tx.retired = retired
	paths = append(paths, retired...)
	// Snapshot every possible destination before the first write, including
	// absent files. put cannot introduce a destination outside this snapshot.
	for _, path := range paths {
		if path == "" {
			continue
		}
		if _, ok := tx.originals[path]; ok {
			continue
		}
		f := installedFile{path: path}
		st, err := os.Lstat(path)
		if err == nil {
			if !st.Mode().IsRegular() {
				return nil, errors.New("refusing non-regular installed artifact")
			}
			f.data, err = os.ReadFile(path)
			f.existed, f.mode = true, st.Mode().Perm()
		}
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		tx.originals[path] = f
	}
	return tx, nil
}

// restore is the one recovery path on both platforms: files, manager reload,
// registration, activity, then independent verification of the whole snapshot.
func (tx *installTransaction) restore(ctx context.Context) error {
	recovery, cancel := SetupTimeout(context.WithoutCancel(ctx))
	defer cancel()
	result := tx.restoreFiles()
	reloadErr := tx.manager.reload(recovery, tx.units)
	result = errors.Join(result, reloadErr)

	for _, u := range tx.units {
		if err := tx.manager.registration(recovery, u, u.state); err != nil {
			result = errors.Join(result, fmt.Errorf("%s registration: %w", u.name(), err))
		}
	}
	for _, u := range tx.units {
		if err := tx.manager.activity(recovery, u, result == nil && u.state.active, true); err != nil {
			result = errors.Join(result, fmt.Errorf("%s activity: %w", u.name(), err))
		}
	}
	differences := tx.differences(recovery)
	if result == nil && len(differences) == 0 {
		return nil
	}
	// A failed recovery never opens admission or leaves an attempted activation
	// running on uncertain artifacts. Queue policy controls admission independently.
	for _, u := range tx.units {
		if err := tx.manager.stop(recovery, u); err != nil {
			result = errors.Join(result, fmt.Errorf("%s keep stopped: %w", u.name(), err))
		}
	}

	differences = tx.differences(recovery)
	detail := "none (recovery operation failed)"
	if len(differences) > 0 {
		detail = strings.Join(differences, "; ")
	}
	return fmt.Errorf("install rollback incomplete; keep affected services stopped; snapshot differences: %s: %w", detail, errors.Join(result, errors.New("snapshot restoration failed")))
}
func (tx *installTransaction) differences(ctx context.Context) []string {
	var differences []string
	for _, u := range tx.units {
		fields := []string{}
		if !tx.fileMatches(u.path) {
			fields = append(fields, "file")
		}
		state, err := tx.manager.inspect(ctx, u)
		if err != nil {
			fields = append(fields, "manager state unknown")
		} else {
			if state.enabled != u.state.enabled {
				fields = append(fields, "enabled")
			}
			if state.loaded != u.state.loaded {
				fields = append(fields, "loaded")
			}
			if state.active != u.state.active {
				fields = append(fields, "active")
			}
		}
		if len(fields) > 0 {
			differences = append(differences, u.name()+": "+strings.Join(fields, ","))
		}
	}
	// Binary/config/credential restoration is also checked, with sanitized names.
	for _, p := range tx.published {
		isUnit := false
		for _, u := range tx.units {
			isUnit = isUnit || u.path == p.path
		}
		if !isUnit && !tx.fileMatches(p.path) {
			differences = append(differences, filepath.Base(p.path)+": file")
		}
	}
	return differences
}
func (tx *installTransaction) fileMatches(path string) bool {
	want := tx.originals[path]
	st, err := os.Lstat(path)
	if !want.existed {
		return errors.Is(err, os.ErrNotExist)
	}
	if err != nil || !st.Mode().IsRegular() || st.Mode().Perm() != want.mode {
		return false
	}
	data, err := os.ReadFile(path)
	return err == nil && bytes.Equal(data, want.data)
}

// Recovery must attempt exclusion even when state inspection itself failed.
func (m installManager) stop(ctx context.Context, u installUnit) error {
	if m.os == "linux" {
		_, err := m.command(ctx, "systemctl", "--user", "stop", u.name())
		if installExit(err, 5) {
			return nil
		}
		return err
	}
	_, err := m.command(ctx, "launchctl", "bootout", u.target())
	if installExit(err, 113) {
		return nil
	}
	if err != nil {
		return err
	}
	// bootout acknowledges the request before launchd necessarily removes the
	// label. Reusing it sooner races bootstrap (EIO), including during rollback.
	wait, cancel := SetupTimeout(ctx)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		state, err := m.inspect(wait, u)
		if err != nil {
			return err
		}
		if !state.loaded {
			return nil
		}
		select {
		case <-wait.Done():
			return wait.Err()
		case <-ticker.C:
		}
	}
}

func obsoleteStartDropIns(ctx context.Context, c ServiceConfig, command func(context.Context, string, ...string) ([]byte, error)) ([]string, error) {
	if c.Detection.OS != "linux" || c.Role != "worker" {
		return nil, nil
	}
	paths, err := filepath.Glob(filepath.Join(filepath.Dir(c.Service), "myce-*.service.d", "*.conf"))
	if err != nil {
		return nil, err
	}
	effective, err := command(ctx, "systemctl", "--user", "show", filepath.Base(c.Service), "--property=DropInPaths", "--value")
	if err != nil {
		return nil, errors.New("cannot inspect effective service drop-ins")
	}
	paths = append(paths, strings.Fields(string(effective))...)
	seen := map[string]bool{}
	var retired []string
	for _, path := range paths {
		if seen[path] {
			continue
		}
		seen[path] = true
		if !filepath.IsAbs(path) {
			return nil, errors.New("cannot inspect effective service drop-ins")
		}
		st, err := os.Lstat(path)
		if err != nil || !st.Mode().IsRegular() || st.Size() > 1<<20 {
			return nil, errors.New("cannot inspect effective service drop-ins")
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		if bytes.Contains(data, []byte("_host-start-check")) {
			retired = append(retired, path)
		}
	}
	return retired, nil
}
func (tx *installTransaction) retireObsolete(ctx context.Context) error {
	for _, u := range tx.units[1:] {
		if u.state.enabled == "not-found" && !u.state.active && !tx.originals[u.path].existed {
			continue
		}
		if err := tx.manager.activity(ctx, u, false, false); err != nil {
			return err
		}
		if u.state.enabled != "not-found" {
			if _, err := tx.manager.command(ctx, "systemctl", "--user", "disable", u.name()); err != nil {
				return err
			}
		}
		if err := tx.retire(u.path); err != nil {
			return err
		}
	}
	for _, path := range tx.retired {
		if err := tx.retire(path); err != nil {
			return err
		}
	}
	return nil
}
