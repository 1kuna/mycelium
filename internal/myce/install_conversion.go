package myce

// Conversion uses SQLite's own WAL recovery on a private working copy and the
// existing atomic file publisher. A durable marker fences runtime access across
// the multi-file install; recovery restores verified originals before retrying.
import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"time"
)

type LocalConversionOptions struct {
	Config   string
	Home     string
	Clock    Clock
	Probe    MachineProbe
	DryRun   bool
	boundary func(string) error
}
type conversionPath struct {
	Path     string                  `json:"path"`
	File     PersistedConversionFile `json:"original"`
	Modified time.Time               `json:"modified"`
}
type installConversionJournal struct {
	Root        string            `json:"root"`
	Files       []conversionPath  `json:"files"`
	Publish     map[string]string `json:"publish"` // active path -> staged relative path; empty means retired
	RetiredDirs map[string]uint32 `json:"retired_directories"`
	Created     []string          `json:"created_directories"`
}
type LocalConversionReport struct {
	PersistedConversionManifest
	Workers    int      `json:"workers"`
	Changes    int      `json:"changed_files,omitempty"`
	Actions    []string `json:"actions,omitempty"`
	Backup     string   `json:"backup,omitempty"`
	Settlement string   `json:"settlement"`
}

type conversionLayout struct {
	configs  []ServiceConfig
	host, id string
	files    []conversionPath
	dirs     map[string]string // staged directory -> existing directory
	spools   map[string]string
}

func readConversionLayout(o LocalConversionOptions) (l conversionLayout, err error) {
	l.dirs = map[string]string{}
	l.spools = map[string]string{}
	root := filepath.Join(o.Home, ".local", "share", "mycelium")
	if o.Config != "" {
		root = filepath.Dir(filepath.Dir(o.Config))
	}
	paths, err := filepath.Glob(filepath.Join(root, "*", "config.json"))
	if err != nil {
		return l, err
	}
	if len(paths) == 0 {
		return l, errors.New("no installed configurations; select an existing installation with --config")
	}
	seen := map[string]bool{}
	add := func(path, rel string) error {
		if seen[path] {
			return nil
		}
		info, e := os.Lstat(path)
		if e != nil {
			return e
		}
		if !info.Mode().IsRegular() {
			return errors.New("conversion requires regular owned files")
		}
		if st, ok := info.Sys().(*syscall.Stat_t); !ok || st.Uid != uint32(os.Geteuid()) {
			return ErrHostIdentity
		}
		// Refuse symlinked parents too; no record can redirect writes through a link.
		for p := filepath.Dir(path); p != "/"; p = filepath.Dir(p) {
			st, e := os.Lstat(p)
			if e != nil {
				return e
			}
			if !st.IsDir() {
				return errors.New("conversion refuses symlinked directories")
			}
		}
		h, e := conversionFileHash(path)
		if e != nil {
			return e
		}
		l.files = append(l.files, conversionPath{path, PersistedConversionFile{rel, h, info.Size(), uint32(info.Mode().Perm())}, info.ModTime()})
		seen[path] = true
		return nil
	}
	walk := func(dir, rel string) error {
		l.dirs[rel] = dir
		return filepath.WalkDir(dir, func(path string, d fs.DirEntry, e error) error {
			if e != nil {
				return e
			}
			if d.IsDir() {
				if path != dir && (d.Name() == ".host-conversion" || d.Name() == ".Trash" || d.Name() == "model-cache" || d.Name() == "environment-cache") {
					return filepath.SkipDir
				}
				return nil
			}
			r, e := filepath.Rel(dir, path)
			if e != nil {
				return e
			}
			return add(path, filepath.ToSlash(filepath.Join(rel, r)))
		})
	}
	for _, path := range paths {
		c, e := ReadServiceConfig(path)
		if e != nil {
			return l, e
		}
		if c.Config != path || (c.Role != "worker" && c.Role != "queue") || c.HostID == "" || !filepath.IsAbs(c.HostStateDir) {
			return l, errors.New("incomplete installed identity")
		}
		if l.host == "" {
			l.host = c.HostStateDir
			l.id = c.HostID
		}
		if c.HostStateDir != l.host || c.HostID != l.id {
			return l, errors.New("installation configs disagree on host identity")
		}
		if c.Detection.OS != o.Probe.OS {
			return l, errors.New("installed service manager does not match this host")
		}
		idx := len(l.configs)
		l.configs = append(l.configs, c)
		for label, p := range map[string]string{"config.json": c.Config, "myce": c.Binary, "unit": c.Service} {
			if e = add(p, fmt.Sprintf("install/%d/%s", idx, label)); e != nil {
				return l, e
			}
		}
		if c.EnvironmentFile == "" && c.JoiningFile == "" && c.Credentials != "" {
			if e = add(c.Credentials, fmt.Sprintf("install/%d/credentials.json", idx)); e != nil {
				return l, e
			}
		}
		if c.Role == "queue" {
			if l.dirs["queue-state"] != "" {
				return l, errors.New("only one local queue is supported")
			}
			l.dirs["queue-state"] = c.State
			for _, suffix := range []string{"", "-wal", "-shm", ".lock", ".uploads.lock"} {
				if e = add(c.DB+suffix, "queue.db"+suffix); e != nil && !errors.Is(e, os.ErrNotExist) {
					return l, e
				}
			}
		} else {
			rel := fmt.Sprintf("workers/%d", idx)
			l.spools[c.State] = rel
			if e = walk(c.State, rel); e != nil {
				return l, e
			}
			// Build custody can live outside the spool. Only its known state files are converted.
			if c.EnvironmentCache != "" {
				e = filepath.WalkDir(c.EnvironmentCache, func(p string, d fs.DirEntry, e error) error {
					if errors.Is(e, os.ErrNotExist) {
						return nil
					}
					if e != nil {
						return e
					}
					if !d.IsDir() && d.Name() == "build-state.json" {
						r, _ := filepath.Rel(c.EnvironmentCache, p)
						return add(p, fmt.Sprintf("workers/env-%d/%s", idx, filepath.ToSlash(r)))
					}
					return nil
				})
				if e != nil {
					return l, e
				}
			}
		}
	}
	if e := walk(l.host, "host"); e != nil {
		return l, e
	}
	sort.Slice(l.files, func(i, j int) bool { return l.files[i].File.Path < l.files[j].File.Path })
	return l, nil
}

// checkConversionWriters does not create files, including during dry-run. Native
// queue/worker/controller flocks and SQLite's reserved/WAL writer byte locks are
// held through publication. No service stop/start is performed by conversion.
func checkConversionWriters(ctx context.Context, l conversionLayout, p MachineProbe) (func(), error) {
	var held []*os.File
	release := func() {
		for _, f := range held {
			_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
			_ = f.Close()
		}
	}
	fail := func(err error) (func(), error) { release(); return nil, err }
	if p.Command == nil {
		return fail(errors.New("service manager unavailable"))
	}
	units := []string{"myce-host-reconcile.service", "myce-host-reconcile.timer"}
	for _, c := range l.configs {
		units = append(units, filepath.Base(c.Service))
	}
	if p.OS != "linux" {
		return fail(errors.New("local installation conversion currently requires systemd user units"))
	}
	for _, unit := range units {
		b, e := p.Command(ctx, "systemctl", "--user", "show", unit, "--property=LoadState,ActiveState", "--value")
		if e != nil {
			return fail(errors.New("cannot inspect installed service manager"))
		}
		inactive := false
		for _, s := range strings.Fields(string(b)) {
			if s == "active" || s == "activating" || s == "deactivating" || s == "reloading" {
				return fail(errors.New("stop queue, workers and reconciliation units before conversion"))
			}
			if s == "inactive" || s == "failed" {
				inactive = true
			}
		}
		if !inactive {
			return fail(errors.New("service manager did not establish inactive units"))
		}
	}
	for _, src := range l.files {
		path := src.Path
		sqliteByte := int64(-1)
		if src.File.Path == "queue.db" {
			sqliteByte = 1073741825
		} // SQLite RESERVED_BYTE
		if src.File.Path == "queue.db-shm" {
			sqliteByte = 120
		} // WAL_WRITE_LOCK
		if !strings.HasSuffix(path, ".lock") && !strings.HasSuffix(src.File.Path, "/config.json") && sqliteByte < 0 {
			continue
		}
		f, e := os.OpenFile(path, os.O_RDWR, 0)
		if e != nil {
			return fail(e)
		}
		held = append(held, f)
		if sqliteByte >= 0 {
			// Shared byte locks exclude SQLite's exclusive writer locks while
			// allowing its read-only WAL recovery to take compatible shared locks.
			lock := syscall.Flock_t{Type: syscall.F_RDLCK, Whence: 0, Start: sqliteByte, Len: 1}
			command := syscall.F_SETLK
			// Linux OFD locks survive this process opening and closing other handles
			// while hashing/copying SQLite files, and conflict with SQLite POSIX locks.
			if runtime.GOOS == "linux" {
				command = 37 /* F_OFD_SETLK */
			}
			if e = syscall.FcntlFlock(f.Fd(), command, &lock); e != nil {
				return fail(errors.New("live SQLite writer; conversion refused"))
			}
		} else if e = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
			return fail(errors.New("live installation writer; conversion refused"))
		}
	}
	return release, nil
}

func ConvertLocalInstallation(ctx context.Context, o LocalConversionOptions) (r LocalConversionReport, err error) {
	if os.Geteuid() == 0 || o.Clock == nil || o.Home == "" {
		return r, ErrHostIdentity
	}
	l, err := readConversionLayout(o)
	if err != nil {
		return r, err
	}
	release, err := checkConversionWriters(ctx, l, o.Probe)
	if err != nil {
		return r, err
	}
	defer release()
	// Refresh hashes and inventory under the writer fences.
	l, err = readConversionLayout(o)
	if err != nil {
		return r, err
	}
	marker := filepath.Join(l.host, ".host-conversion", "IN_PROGRESS")
	if b, e := os.ReadFile(marker); e == nil {
		if o.DryRun {
			return r, errors.New("interrupted conversion; rerun without --dry-run to restore the verified backup")
		}
		var j installConversionJournal
		if json.Unmarshal(b, &j) != nil {
			return r, errors.New("invalid conversion recovery journal")
		}
		if e = restoreInstallConversion(j); e != nil {
			return r, e
		}
		if e = os.Remove(marker); e != nil {
			return r, e
		}
		if e = syncWorkerDir(filepath.Dir(marker)); e != nil {
			return r, e
		}
		// Recovery restores original input first, then a new invocation retries.
		r.Phase = "restored"
		r.Backup = j.Root
		return r, nil
	} else if !errors.Is(e, os.ErrNotExist) {
		return r, e
	}
	if s, e := ReadHostIntent(l.host); e == nil {
		current := true
		for _, src := range l.files {
			if conversionWorkerRecord(src.File.Path) && conversionWorkerState(filepath.Base(src.File.Path)) {
				b, e := os.ReadFile(src.Path)
				var st workerState
				if e != nil {
					return r, e
				}
				if e = decodeWorkerState(b, &st); e != nil {
					return r, e
				}
			}
		}
		for _, src := range l.files {
			if src.File.Path == "queue.db" {
				db, e := conversionDB(src.Path, true)
				if e != nil {
					return r, e
				}
				var v int
				e = db.QueryRow("PRAGMA user_version").Scan(&v)
				db.Close()
				if e != nil {
					return r, e
				}
				current = current && v == persistedSchema
			}
		}
		if current {
			r.Phase = "unchanged"
			r.Schema = persistedSchema
			r.Host = s.Host
			r.Settlement = s.HostSettlement()
			return r, nil
		}
	}
	r.PersistedConversionManifest = PersistedConversionManifest{Schema: persistedSchema, Host: l.id, SourceFormat: "7285a88", Phase: "dry-run", Blockers: []string{}}
	for _, src := range l.files {
		r.Files = append(r.Files, src.File)
	}
	if o.DryRun {
		for _, src := range l.files {
			if src.File.Path == "queue.db" {
				db, e := conversionDB(src.Path, true)
				if e != nil {
					return r, e
				}
				var b []byte
				e = db.QueryRow("SELECT data FROM queue WHERE id=1").Scan(&b)
				db.Close()
				if e != nil {
					return r, e
				}
				var s state
				if e = json.Unmarshal(b, &s); e != nil {
					return r, e
				}
				r.Jobs = len(s.Jobs)
				for _, j := range s.Jobs {
					r.Copies += len(j.Copies)
				}
				r.Releases = len(s.HostReleases)
				r.Slots = len(s.ModelSlots)
			}
			if strings.HasSuffix(src.File.Path, ".result.json") {
				r.Reports++
			}
			if conversionWorkerRecord(src.File.Path) && filepath.Base(src.File.Path) == "state.json" {
				r.Workers++
			}
		}
		r.Actions = []string{"checkpoint saved SQLite WAL", "verify exact installation backup", "convert queue and saved worker formats in place", "preserve config, unit and Docker bindings", "publish CLOSED; reconcile saved custody before open"}
		r.Settlement = "evaluated during validated conversion"
		return r, nil
	}
	root := filepath.Join(l.host, ".host-conversion")
	if err = os.MkdirAll(root, 0700); err != nil {
		return r, err
	}
	info, e := os.Lstat(root)
	if e != nil {
		return r, e
	}
	owner, ok := info.Sys().(*syscall.Stat_t)
	if !info.IsDir() || !ok || owner.Uid != uint32(os.Geteuid()) || info.Mode().Perm()&0077 != 0 {
		return r, errors.New("conversion backup directory must be private and owned by the worker")
	}
	run, err := os.MkdirTemp(root, "run-")
	if err != nil {
		return r, err
	}
	r.Backup = run
	j := installConversionJournal{Root: run, Files: l.files, Publish: map[string]string{}, RetiredDirs: map[string]uint32{}}
	// Directory modes are part of rollback for retired per-unit control trees.
	err = filepath.WalkDir(filepath.Join(l.host, "units"), func(p string, d fs.DirEntry, e error) error {
		if errors.Is(e, os.ErrNotExist) {
			return nil
		}
		if e != nil {
			return e
		}
		if d.IsDir() {
			st, e := d.Info()
			if e != nil {
				return e
			}
			j.RetiredDirs[p] = uint32(st.Mode().Perm())
		}
		return nil
	})
	if err != nil {
		return r, err
	}
	step := func(phase string) error {
		if e := ctx.Err(); e != nil {
			return e
		}
		if o.boundary != nil {
			return o.boundary(phase)
		}
		return nil
	}
	for _, src := range l.files {
		if err = copyConversionExact(src.Path, filepath.Join(run, "originals", src.File.Path), src); err != nil {
			return r, err
		}
	}
	if err = verifyInstallBackup(j); err != nil {
		return r, err
	}
	if err = atomicHostJSON(run, "backup.json", j, nil); err != nil {
		return r, err
	}
	if err = step("backed-up"); err != nil {
		return r, err
	}
	stage := filepath.Join(run, "stage")
	for _, src := range l.files {
		if err = copyConversionExact(filepath.Join(run, "originals", src.File.Path), filepath.Join(stage, src.File.Path), src); err != nil {
			return r, err
		}
	}
	// Checkpoint only the verified private copy. Even WAL/shm source bytes remain
	// exactly recoverable; SQLite, rather than a custom WAL reader, merges pages.
	if _, e := os.Stat(filepath.Join(stage, "queue.db")); e == nil {
		db, e := conversionDB(filepath.Join(stage, "queue.db"), false)
		if e != nil {
			return r, e
		}
		var busy, log, done int
		e = db.QueryRow("PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &log, &done)
		ce := db.Close()
		if e != nil || ce != nil || busy != 0 {
			return r, errors.Join(e, ce, errors.New("WAL checkpoint failed"))
		}
	}
	if err = step("checkpointed"); err != nil {
		return r, err
	}
	options := persistedConversionOptions{Host: l.id, Clock: o.Clock, spools: l.spools}
	if err = convertPersistedTree(stage, options, &r.PersistedConversionManifest); err != nil {
		return r, err
	}
	if len(r.Blockers) > 0 {
		return r, errors.New("conversion blocked by unsafe persisted identities")
	}
	// A registry must still name one of the installed configs and units.
	registryPaths, e := filepath.Glob(filepath.Join(stage, "host", "worker-*.json"))
	if e != nil {
		return r, e
	}
	for _, path := range registryPaths {
		b, e := os.ReadFile(path)
		if e != nil {
			return r, e
		}
		var w HostWorker
		if e = json.Unmarshal(b, &w); e != nil {
			return r, e
		}
		matched := false
		for _, c := range l.configs {
			matched = matched || c.Role == "worker" && w.Name == c.Name && w.State == c.State && w.Config == c.Config && w.Unit == filepath.Base(c.Service) && w.Queue == c.Queue && w.Environments == c.EnvironmentCache
		}
		if !matched {
			return r, errors.New("worker registry does not match installed bindings")
		}
	}
	if err = step("converted"); err != nil {
		return r, err
	}
	// Only explicit installation credential paths may change format. Checkpoint
	// and job payloads named credentials.json/config.json are opaque bytes.
	for i, c := range l.configs {
		if c.Role == "worker" {
			r.Workers++
		}
		if c.EnvironmentFile == "" && c.JoiningFile == "" {
			p := filepath.Join(stage, fmt.Sprintf("install/%d/credentials.json", i))
			b, e := os.ReadFile(p)
			if e != nil {
				return r, e
			}
			var old struct {
				Token string `json:"token"`
			}
			if json.Unmarshal(b, &old) == nil && old.Token != "" {
				if e = writePersistedFile(p, []byte("MYCE_TOKEN="+fmt.Sprintf("%q", old.Token)+"\nMYCE_QUEUE="+fmt.Sprintf("%q", c.Queue)+"\n")); e != nil {
					return r, e
				}
			}
		}
	}
	intent, err := ReadHostIntent(filepath.Join(stage, "host"))
	if err != nil {
		return r, err
	}
	custody := len(intent.Pending) > 0 || r.Releases > 0 || r.Slots > 0
	if _, e := os.Stat(filepath.Join(stage, "queue.db")); e == nil {
		db, e := conversionDB(filepath.Join(stage, "queue.db"), true)
		if e != nil {
			return r, e
		}
		var b []byte
		e = db.QueryRow("SELECT data FROM queue WHERE id=1").Scan(&b)
		ce := db.Close()
		if e != nil || ce != nil {
			return r, errors.Join(e, ce)
		}
		var saved state
		if e = json.Unmarshal(b, &saved); e != nil {
			return r, e
		}
		for _, job := range saved.Jobs {
			custody = custody || job.Machine != "" && !job.ReleaseConfirmed
			for _, copy := range job.Copies {
				custody = custody || copy.Machine != "" && !copy.ReleaseConfirmed
			}
		}
	}
	for _, src := range l.files {
		if conversionWorkerRecord(src.File.Path) && (filepath.Base(src.File.Path) == "state.json" || strings.HasSuffix(src.File.Path, ".docker.json") || strings.HasSuffix(src.File.Path, ".result.json") || strings.HasPrefix(filepath.Base(src.File.Path), "engine-") || strings.HasPrefix(filepath.Base(src.File.Path), "attempt-")) {
			b, e := os.ReadFile(filepath.Join(stage, src.File.Path))
			if e != nil {
				return r, e
			}
			var raw map[string]json.RawMessage
			if json.Unmarshal(b, &raw) != nil {
				return r, errors.New("invalid worker custody")
			}
			custody = custody || len(raw["job"]) > 0 || len(raw["owner"]) > 0 || len(raw["identity"]) > 0 || len(raw["pid"]) > 0 || len(raw["launch_pending"]) > 0
		}
	}
	if !custody {
		now := o.Clock.Now()
		intent.LastClose.Settlement = "confirmed"
		intent.LastClose.SettledAt = &now
		intent.Holds = nil
		if err = atomicHostJSON(filepath.Join(stage, "host"), hostStateFile, intent, nil); err != nil {
			return r, err
		}
	}
	r.Settlement = intent.HostSettlement()
	// Map staged output back to the original paths. Configs, units, spool roots
	// and Docker endpoints never move. Removed legacy control files stay backed up.
	originals := map[string]conversionPath{}
	for _, src := range l.files {
		originals[src.Path] = src
		j.Publish[src.Path] = ""
	}
	for _, src := range l.files {
		p := filepath.Join(stage, src.File.Path)
		if _, e := os.Stat(p); e == nil {
			j.Publish[src.Path] = src.File.Path
		}
	}
	err = filepath.WalkDir(filepath.Join(stage, "host"), func(p string, d fs.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(filepath.Join(stage, "host"), p)
		j.Publish[filepath.Join(l.host, rel)] = filepath.ToSlash(filepath.Join("host", rel))
		return nil
	})
	if err != nil {
		return r, err
	}
	for path, rel := range j.Publish {
		if strings.HasSuffix(path, ".lock") && rel != "" {
			delete(j.Publish, path)
			continue
		}
		src, exists := originals[path]
		if rel != "" {
			h, e := conversionFileHash(filepath.Join(stage, rel))
			if e != nil {
				return r, e
			}
			if exists && h == src.File.SHA256 {
				delete(j.Publish, path)
				continue
			}
		}
		if !exists {
			for d := filepath.Dir(path); d != l.host; d = filepath.Dir(d) {
				if _, e := os.Stat(d); errors.Is(e, os.ErrNotExist) {
					j.Created = append(j.Created, d)
				} else {
					break
				}
			}
		}
	}
	r.Changes = len(j.Publish)
	if err = step("validated"); err != nil {
		return r, err
	}
	// Verify complete inputs again before the first active-path write.
	for _, src := range l.files {
		if err = verifyConversionSource(src.Path, HostConversionSource{SHA256: src.File.SHA256, Bytes: src.File.Bytes, Mode: src.File.Mode}); err != nil {
			return r, err
		}
	}
	defer func() {
		if err != nil {
			restoreErr := restoreInstallConversion(j)
			if restoreErr == nil {
				restoreErr = os.Remove(marker)
				if errors.Is(restoreErr, os.ErrNotExist) {
					restoreErr = nil
				}
				if restoreErr == nil {
					restoreErr = syncWorkerDir(root)
				}
			}
			err = errors.Join(err, restoreErr)
		}
	}()
	if err = atomicHostJSON(root, "IN_PROGRESS", j, nil); err != nil {
		return r, err
	}
	if err = step("journaled"); err != nil {
		return r, err
	}
	var paths []string
	for path := range j.Publish {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for index, path := range paths {
		rel := j.Publish[path]
		if rel == "" {
			err = os.Remove(path)
			if errors.Is(err, os.ErrNotExist) {
				err = nil
			}
		} else {
			src := originals[path]
			if src.Path == "" {
				src.File.Mode = 0600
			}
			err = copyConversionExact(filepath.Join(stage, rel), path, src)
		}
		if err != nil {
			return r, err
		}
		if err = syncWorkerDir(filepath.Dir(path)); err != nil {
			return r, err
		}
		if err = step(fmt.Sprintf("publish:%d", index)); err != nil {
			return r, err
		}
	}
	for _, dir := range conversionDirectories(j.RetiredDirs) {
		if err = os.Remove(dir); err != nil && !errors.Is(err, os.ErrNotExist) {
			return r, err
		}
		if err = syncWorkerDir(filepath.Dir(dir)); err != nil {
			return r, err
		}
	}
	// Check the active bytes against the validated stage while still fenced.
	for _, path := range paths {
		rel := j.Publish[path]
		if rel == "" {
			continue
		}
		a, e := conversionFileHash(path)
		if e != nil {
			return r, e
		}
		b, e := conversionFileHash(filepath.Join(stage, rel))
		if e != nil {
			return r, e
		}
		if a != b {
			return r, errors.New("published conversion checksum mismatch")
		}
	}
	if err = step("published"); err != nil {
		return r, err
	}
	// This single durable marker removal commits the fenced multi-file publish.
	r.Phase = "complete"
	if err = atomicHostJSON(run, "manifest.json", r, nil); err != nil {
		return r, err
	}
	if err = os.Remove(marker); err != nil {
		return r, err
	}
	err = syncWorkerDir(root)
	return r, err
}
func conversionDB(path string, readonly bool) (*sql.DB, error) {
	u := url.URL{Scheme: "file", Path: path}
	if readonly {
		u.RawQuery = "mode=ro&immutable=1"
		if info, e := os.Stat(path + "-wal"); e == nil && info.Size() > 0 {
			u.RawQuery = "mode=ro&readonly_shm=1"
		}
	}
	db, e := sql.Open("sqlite", u.String())
	if e == nil {
		db.SetMaxOpenConns(1)
	}
	return db, e
}
func copyConversionExact(from, to string, src conversionPath) error {
	mode := fs.FileMode(src.File.Mode)
	if mode == 0 {
		mode = 0600
	}
	if e := copyConversionOriginal(from, to, mode); e != nil {
		return e
	}
	if e := os.Chmod(to, mode); e != nil {
		return e
	}
	if !src.Modified.IsZero() {
		if e := os.Chtimes(to, src.Modified, src.Modified); e != nil {
			return e
		}
	}
	f, e := os.OpenFile(to, os.O_RDWR, 0)
	if e != nil {
		return e
	}
	return errors.Join(f.Sync(), f.Close())
}
func verifyInstallBackup(j installConversionJournal) error {
	for _, src := range j.Files {
		if e := verifyConversionSource(filepath.Join(j.Root, "originals", src.File.Path), HostConversionSource{SHA256: src.File.SHA256, Bytes: src.File.Bytes, Mode: src.File.Mode}); e != nil {
			return e
		}
	}
	return nil
}
func restoreInstallConversion(j installConversionJournal) error {
	if e := verifyInstallBackup(j); e != nil {
		return e
	}
	for dir, mode := range j.RetiredDirs {
		if e := os.MkdirAll(dir, fs.FileMode(mode)); e != nil {
			return e
		}
		if e := os.Chmod(dir, fs.FileMode(mode)); e != nil {
			return e
		}
	}
	originals := map[string]conversionPath{}
	for _, src := range j.Files {
		originals[src.Path] = src
	}
	paths := make([]string, 0, len(j.Publish))
	for path := range j.Publish {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for _, path := range paths {
		if src, ok := originals[path]; ok {
			if e := copyConversionExact(filepath.Join(j.Root, "originals", src.File.Path), path, src); e != nil {
				return e
			}
		} else {
			if e := os.Remove(path); e != nil && !errors.Is(e, os.ErrNotExist) {
				return e
			}
		}
		if e := syncWorkerDir(filepath.Dir(path)); e != nil && !errors.Is(e, os.ErrNotExist) {
			return e
		}
	}
	sort.Slice(j.Created, func(i, k int) bool { return len(j.Created[i]) > len(j.Created[k]) })
	for _, dir := range j.Created {
		if e := os.Remove(dir); e != nil && !errors.Is(e, os.ErrNotExist) {
			return e
		}
	}
	return nil
}

func conversionDirectories(dirs map[string]uint32) []string {
	paths := make([]string, 0, len(dirs))
	for path := range dirs {
		paths = append(paths, path)
	}
	sort.Slice(paths, func(i, j int) bool { return len(paths[i]) > len(paths[j]) })
	return paths
}
