package myce

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
)

// InstallFlags uses the same table as parsing, overlay and validation.
func (c ServiceConfig) InstallFlags() map[string]string {
	f := flag.NewFlagSet("saved-install", flag.ContinueOnError)
	RegisterInstallFlags(f, &c)
	out := map[string]string{}
	f.VisitAll(func(v *flag.Flag) { out[v.Name] = v.Value.String() })
	return out
}

func ValidateInstallIdentity(old, next ServiceConfig) error {
	if old.Role != next.Role || old.Service != next.Service || old.Binary != next.Binary || old.State != next.State || old.Config != next.Config || old.Credentials != next.Credentials || old.HostID != next.HostID || old.WorkerUser != next.WorkerUser || old.WorkerHome != next.WorkerHome || old.Device != next.Device || old.Detection.OS != next.Detection.OS || old.Detection.Arch != next.Detection.Arch {
		return errors.New("installed role, account, host, device, unit and state identity cannot change; preserve custody and install separately")
	}
	return nil
}

func validateInstallDropIns(ctx context.Context, c ServiceConfig, command func(context.Context, string, ...string) ([]byte, error), retired ...string) error {
	starts := []string{""} // The base unit's command remains until an empty reset.
	overridden := false
	for _, unit := range []string{filepath.Base(c.Service)} {
		b, err := command(ctx, "systemctl", "--user", "show", unit, "--property=DropInPaths", "--value")
		if err != nil {
			return errors.New("cannot inspect effective service drop-ins")
		}
		for _, path := range strings.Fields(string(b)) {
			if slices.Contains(retired, path) {
				continue
			}
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
			// Unit-file continuations are part of the same assignment.
			text := strings.ReplaceAll(string(data), "\r\n", "\n")
			text = strings.ReplaceAll(text, "\\\n", " ")
			section := ""
			for _, line := range strings.Split(text, "\n") {
				line = strings.TrimSpace(line)
				if strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
					continue
				}
				if strings.HasPrefix(line, "[") {
					section = line
					continue
				}
				key, value, ok := strings.Cut(strings.TrimSpace(line), "=")
				if !ok {
					continue
				}
				key = strings.TrimSpace(key)
				if c.Role == "queue" && key == "ExecStart" {
					if section != "[Service]" {
						continue
					}
					overridden = true
					value = strings.TrimSpace(value)
					if value == "" {
						starts = nil
					} else {
						starts = append(starts, value)
					}
					continue
				}
				if key == "Environment" || key == "PassEnvironment" || key == "UnsetEnvironment" {
					if err := validateInstallEnvironment(c, key, value); err != nil {
						return err
					}
					continue
				}
				if strings.HasPrefix(key, "Exec") || strings.HasPrefix(key, "Condition") || strings.HasPrefix(key, "Assert") || strings.HasPrefix(key, "Environment") || key == "User" || key == "Group" || key == "WorkingDirectory" || key == "RootDirectory" || key == "RootImage" || key == "KillMode" || key == "SendSIGKILL" || key == "Delegate" || key == "Restart" || key == "TimeoutStopSec" || key == "Type" || key == "NotifyAccess" {
					return errors.New("effective service drop-in conflicts with installed service contract; preserve it and resolve before activation")
				}
			}
		}
	}
	if overridden {
		if len(starts) != 1 {
			return &InstallExecStartOverrideError{Flags: []string{"ExecStart"}}
		}
		return compareQueueExecStart(c, starts[0])
	}
	return nil
}

var ErrQueueExecStartOverride = errors.New("queue_execstart_override")

// InstallExecStartOverrideError deliberately reports names, never values: an
// operator's command may contain credentials or private paths.
type InstallExecStartOverrideError struct {
	Flags []string
}

func (e *InstallExecStartOverrideError) Error() string {
	return "queue_execstart_override: effective ExecStart hides or differs from generated settings: " + strings.Join(e.Flags, ", ") + "; preserve the override and resolve before activation, or use --save-settings-only to prepare retirement without activation"
}
func (e *InstallExecStartOverrideError) Unwrap() error { return ErrQueueExecStartOverride }

func compareQueueExecStart(c ServiceConfig, start string) error {
	refuse := func(flags ...string) error { return &InstallExecStartOverrideError{Flags: flags} }
	// Only ':' has the generated unit's ordinary execution semantics. Other
	// executable prefixes change identity, argv or error handling.
	literal := strings.HasPrefix(start, ":")
	start = strings.TrimPrefix(start, ":")
	words, err := installEnvironmentWords(start)
	if err != nil || len(words) < 2 {
		return refuse("ExecStart")
	}
	for i, word := range words {
		if !literal && strings.Contains(word, "$") {
			return refuse("ExecStart") // Environment expansion cannot prove equivalence.
		}
		withoutEscapes := strings.ReplaceAll(word, "%%", "")
		if strings.Contains(withoutEscapes, "%") {
			return refuse("ExecStart") // Unknown systemd specifier.
		}
		words[i] = strings.ReplaceAll(word, "%%", "%")
	}
	if words[0] != c.Binary {
		return refuse("executable")
	}
	args := words[1:]
	if args[0] == "service" {
		wrapper := flag.NewFlagSet("service", flag.ContinueOnError)
		wrapper.SetOutput(io.Discard)
		path := wrapper.String("config", "", "")
		if wrapper.Parse(args[1:]) != nil || *path != c.Config {
			return refuse("--config")
		}
		args = wrapper.Args()
		if len(args) == 0 {
			args = c.Args()
		}
		// serviceCommand always appends this, even with an explicit runtime argv.
		args = append(args, "--service-config", c.Config)
	}
	if len(args) == 0 || args[0] != "queue" {
		return refuse("ExecStart")
	}
	// Defaults of queue's runtime flags, not installer defaults. Unknown flags
	// are retained and compared conservatively, rather than silently discarded.
	defaults := map[string]string{
		"listen": "127.0.0.1:7331", "db": "myce-state/myce.db", "checkpoints": "./checkpoints",
		"file-retention": DefaultFileRetention.String(), "log-retention": DefaultLogRetention.String(),
		"file-store-bytes": strconv.FormatInt(100<<30, 10), "file-store-production-reserve": "0",
		"upload-spool-bytes": strconv.FormatInt(DefaultUploadSpoolBytes, 10), "upload-spool-dir": "",
		"restart-grace": RestartGrace.String(), "production-infrastructure-retries": strconv.Itoa(DefaultProductionInfrastructureRetries),
		"production-loss-timeout": "0s", "deadline-loss-timeout": "0s", "experiment-loss-timeout": "0s",
		"model-config": "", "environment-config": "", "project-config": "", "host-config": "",
		"discovery-listen": "", "discovery": "true",
	}
	parse := func(argv []string) (map[string]string, error) {
		values := make(map[string]string, len(defaults))
		for name, value := range defaults {
			values[name] = value
		}
		for len(argv) > 0 {
			word := argv[0]
			argv = argv[1:]
			if !strings.HasPrefix(word, "-") {
				return nil, refuse("ExecStart")
			}
			name, value, equal := strings.Cut(strings.TrimPrefix(strings.TrimPrefix(word, "-"), "-"), "=")
			if name == "" {
				return nil, refuse("ExecStart")
			}
			if !equal {
				if name == "discovery" {
					value = "true"
				} else {
					if len(argv) == 0 {
						return nil, refuse("--" + name)
					}
					value, argv = argv[0], argv[1:]
				}
			}
			values[name] = value
		}
		if path, ok := values["service-config"]; ok {
			if path != c.Config {
				return nil, refuse("--service-config")
			}
			// ListenJoining uses saved DiscoveryListen only without an explicit bind.
			if values["discovery-listen"] == "" {
				values["discovery-listen"] = c.DiscoveryListen
			}
			delete(values, "service-config")
		}
		// The install table supplies the same duration/integer/boolean types as
		// the runtime flags; Set canonicalizes 60s/1m0s and numeric spellings.
		f := flag.NewFlagSet("queue-override", flag.ContinueOnError)
		var config ServiceConfig
		RegisterInstallFlags(f, &config)
		f.Bool("discovery", true, "")
		for name, value := range values {
			if v := f.Lookup(name); v != nil {
				if err := v.Value.Set(value); err != nil {
					return nil, refuse("--" + name)
				}
				values[name] = v.Value.String()
			}
		}
		return values, nil
	}
	actual, err := parse(args[1:])
	if err != nil {
		return err
	}
	want, err := parse(c.Args()[1:])
	if err != nil {
		return err
	}
	var differing []string
	for name, value := range want {
		if other, ok := actual[name]; !ok || other != value {
			differing = append(differing, "--"+name)
		}
	}
	for name := range actual {
		if _, ok := want[name]; !ok {
			differing = append(differing, "--"+name)
		}
	}
	if len(differing) > 0 {
		sort.Strings(differing)
		return refuse(differing...)
	}
	return nil
}

// Preparation publishes only settings and the base unit. It never changes the
// binary, credentials or manager state, including on failure.
func saveQueueInstallSettings(ctx context.Context, c ServiceConfig, tx *installTransaction) (result error) {
	defer func() {
		if result != nil {
			result = errors.Join(result, tx.restoreFiles())
		}
	}()
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	if err = tx.put(c.Config, data, 0600); err != nil {
		return err
	}
	if err = tx.put(c.Service, []byte(c.ServiceText()), 0600); err != nil {
		return err
	}
	if err := tx.pruneBackups(); err != nil {
		log.Printf("install backup retention: %v", err)
	}
	log.Print("queue settings and generated unit saved only; service NOT reloaded or activated; the existing ExecStart override remains effective until retired")
	return nil
}

// Credential sources belong to the generated unit, including EnvironmentFile's
// token path. Extra inline environment assignments may belong to the operator.
func validateInstallEnvironment(c ServiceConfig, directive, value string) error {
	owned := map[string]bool{"MYCE_TOKEN": true, "MYCE_QUEUE": true, "MYCE_JOINING_FILE": true}
	if c.Path != "" {
		owned["PATH"] = true
	}
	if c.Role == "worker" {
		// This is the generated unit's runtime file, not the credential file.
		// Only assignments actually present there are owned by that source.
		path := c.GPUEnvironmentFile()
		st, err := os.Stat(path)
		var data string
		if errors.Is(err, os.ErrNotExist) {
			data = gpuRuntimeEnvironment(os.Environ()) // First install generates this file.
		} else {
			if err != nil || !st.Mode().IsRegular() || st.Size() > 1<<20 {
				return errors.New("cannot inspect generated service runtime environment")
			}
			b, err := os.ReadFile(path)
			if err != nil {
				return errors.New("cannot inspect generated service runtime environment")
			}
			data = string(b)
		}
		for _, line := range strings.Split(data, "\n") {
			name, _, assignment := strings.Cut(strings.TrimSpace(line), "=")
			if assignment && !strings.HasPrefix(name, "#") {
				owned[name] = true
			}
		}
	}
	// Include inline assignments if the generated service gains any later.
	for _, line := range strings.Split(c.ServiceText(), "\n") {
		if value, ok := strings.CutPrefix(line, "Environment="); ok {
			words, err := installEnvironmentWords(value)
			if err != nil {
				return errors.New("cannot inspect generated service environment")
			}
			for _, word := range words {
				name, _, _ := strings.Cut(word, "=")
				owned[name] = true
			}
		}
	}
	words, err := installEnvironmentWords(value)
	if err != nil || len(words) == 0 {
		return errors.New("cannot inspect service drop-in environment assignment")
	}
	for _, word := range words {
		name, _, assignment := strings.Cut(word, "=")
		if owned[name] || directive == "Environment" && !assignment {
			return errors.New("service drop-in overrides generated service environment")
		}
	}
	return nil
}

// systemd Environment= is a whitespace-separated list with single/double quotes
// and C escapes, not shell expansion. Never include assignment values in errors.
func installEnvironmentWords(value string) ([]string, error) {
	var words []string
	for value = strings.TrimSpace(value); value != ""; value = strings.TrimSpace(value) {
		var word strings.Builder
		var quote byte
		for value != "" {
			ch := value[0]
			if quote == 0 && (ch == ' ' || ch == '\t') {
				break
			}
			if ch == '\'' || ch == '"' {
				if quote == 0 {
					quote = ch
					value = value[1:]
					continue
				}
				if quote == ch {
					quote = 0
					value = value[1:]
					continue
				}
			}
			if ch != '\\' {
				word.WriteByte(ch)
				value = value[1:]
				continue
			}
			if strings.HasPrefix(value, `\s`) {
				word.WriteByte(' ')
				value = value[2:]
				continue
			}
			decodeQuote := byte('"')
			if strings.HasPrefix(value, `\'`) {
				decodeQuote = '\''
			}
			r, _, rest, err := strconv.UnquoteChar(value, decodeQuote)
			if err != nil {
				return nil, errors.New("invalid unit environment quoting")
			}
			word.WriteRune(r)
			value = rest
		}
		if quote != 0 {
			return nil, errors.New("unterminated unit environment quoting")
		}
		words = append(words, word.String())
	}
	return words, nil
}

// installFiles is a per-invocation fault seam, never a global filesystem hook.
type installFiles struct {
	storageFS *onboardingStorageFS // Per-invocation storage fault seam.
	retain    func(*installTransaction)
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
