package myce

// Offline control conversion is deliberately separate from runtime decoding.
// Formats below are pinned to 8bc2a83. Nothing here treats an old receipt as
// current-boot release evidence or infers chronology between independent counters.
import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

type HostConversionOptions struct {
	Dir            string
	Host           string
	QueueDatabases map[string]string // Stopped, WAL-consistent local copies; never contacted over the network.
	Clock          Clock
	Apply          bool
	boundary       func(string) error
}
type HostConversionSource struct {
	Path     string `json:"path"`
	SHA256   string `json:"sha256"`
	Bytes    int64  `json:"bytes"`
	Mode     uint32 `json:"mode"`
	Retire   bool   `json:"retire"`
	Original string `json:"original"`
}
type HostConversionManifest struct {
	Schema     int                    `json:"schema_version"`
	Host       string                 `json:"host_id"`
	Identities []HostWorker           `json:"worker_identities"`
	Spools     []string               `json:"spool_directories"`
	Queues     map[string]string      `json:"queue_databases"`
	Phase      string                 `json:"phase"`
	Workers    int                    `json:"worker_count"`
	Receipts   int                    `json:"receipt_count"`
	Pending    int                    `json:"pending_count"`
	LowerBound uint64                 `json:"revision_lower_bound"`
	Conflicts  []string               `json:"conflicts"`
	Sources    []HostConversionSource `json:"sources"`
	Intent     HostIntent             `json:"target"`
}

type hostReceipt struct {
	Generation       uint64       `json:"generation"`
	Workers          []HostWorker `json:"workers"`
	EnableGeneration uint64       `json:"enable_generation,omitempty"`
}

type forgottenQueues struct {
	Generation uint64            `json:"generation"`
	Queues     map[string]uint64 `json:"queues"`
}

type legacyControlFlag struct {
	Generation   uint64          `json:"generation"`
	Who          string          `json:"who"`
	When         json.RawMessage `json:"when"`
	Reason       string          `json:"reason"`
	GraceSeconds float64         `json:"grace_seconds"`
}

func conversionHash(b []byte) string  { sum := sha256.Sum256(b); return hex.EncodeToString(sum[:]) }
func conversionDir(dir string) string { return filepath.Join(dir, ".host-conversion") }

// ConvertHostControl never stops processes. The caller stops the old controllers,
// queue and workers first. Their existing flock files enforce offline access.
func ConvertHostControl(o HostConversionOptions) (HostConversionManifest, error) {
	var m HostConversionManifest
	if o.Clock == nil || o.Host == "" || o.Dir == "" {
		return m, errors.New("conversion requires explicit host identity and clock")
	}
	dir, err := filepath.Abs(o.Dir)
	if err != nil {
		return m, err
	}
	o.Dir = dir
	queuePaths := map[string]string{}
	for endpoint, path := range o.QueueDatabases {
		u, e := url.Parse(endpoint)
		if e != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || path == "" {
			return m, errors.New("invalid offline queue database mapping")
		}
		absolute, e := filepath.Abs(path)
		if e != nil {
			return m, e
		}
		queuePaths[endpoint] = absolute
	}
	o.QueueDatabases = queuePaths
	if unitParent(dir) != "" {
		return m, errors.New("convert the whole host, not a worker unit")
	}
	if _, err = os.Stat(dir); err != nil {
		return m, err
	}
	unlock, err := hostOperationLock(dir, "intake.lock", false)
	if err != nil {
		return m, err
	}
	defer unlock()
	unlock, err = hostOperationLock(dir, "control.lock", false)
	if err != nil {
		return m, err
	}
	defer unlock()
	unlock, err = hostOperationLock(dir, "launch.lock", false)
	if err != nil {
		return m, err
	}
	defer unlock()
	root := conversionDir(dir)
	if st, e := os.Lstat(root); e == nil && (!st.IsDir() || st.Mode()&os.ModeSymlink != 0) {
		return m, errors.New("conversion storage must be a real private directory on the host-state filesystem")
	} else if e != nil && !errors.Is(e, os.ErrNotExist) {
		return m, e
	}
	if b, e := os.ReadFile(filepath.Join(root, "manifest.json")); e == nil {
		if e = json.Unmarshal(b, &m); e != nil || m.Schema != 1 || m.Host != o.Host {
			return m, errors.New("conversion journal identity/schema mismatch")
		}
		if err = validateHostIntent(m.Intent); err != nil {
			return m, err
		}

		if m.Phase == "complete" {
			for _, src := range m.Sources {
				if err = verifyConversionSource(filepath.Join(root, src.Original), src); err != nil {
					return m, err
				}
			}
			// Complete journal is durable; marker removal is the final activation step.
			if o.Apply {
				if e := os.Remove(filepath.Join(root, "IN_PROGRESS")); e != nil && !errors.Is(e, os.ErrNotExist) {
					return m, e
				}
				if e := syncWorkerDir(root); e != nil {
					return m, e
				}
			}
			s, e := ReadHostIntent(dir)
			if e != nil || s.Host != m.Host || s.Revision < m.Intent.Revision {
				return m, errors.New("completed conversion target missing or changed identity")
			}
			return m, nil
		}
		if !o.Apply {
			return m, nil
		}
		o.QueueDatabases = m.Queues
		return resumeHostConversion(o, m)
	} else if !errors.Is(e, os.ErrNotExist) {
		return m, e
	}
	if _, e := os.Stat(filepath.Join(dir, hostStateFile)); !errors.Is(e, os.ErrNotExist) {
		return m, errors.New("host state exists without this conversion journal; refusing replacement")
	}
	m, err = inventoryHostConversion(o)
	if err != nil {
		return m, err
	}
	if len(m.Conflicts) > 0 {
		return m, errors.New("conversion conflicts require private inventory repair")
	}
	if !o.Apply {
		return m, nil
	}
	if err = os.MkdirAll(root, 0700); err != nil {
		return m, err
	}
	if err = os.Chmod(root, 0700); err != nil {
		return m, err
	}
	// An early marker keeps the new runtime fenced even if the process dies while
	// preserving originals, before the full journal can be committed.
	if err = atomicHostJSON(root, "IN_PROGRESS", struct{ Host string }{o.Host}, nil); err != nil {
		return m, err
	}
	if err = syncWorkerDir(dir); err != nil {
		return m, err
	}
	m.Phase = "inventory"
	if err = atomicHostJSON(root, "manifest.json", m, nil); err != nil {
		return m, err
	}
	if err = conversionBoundary(o, "inventory"); err != nil {
		return m, err
	}

	return resumeHostConversion(o, m)
}

func conversionBoundary(o HostConversionOptions, phase string) error {
	if o.boundary != nil {
		return o.boundary(phase)
	}
	return nil
}
func verifyConversionSource(path string, src HostConversionSource) error {
	st, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !st.Mode().IsRegular() || uint32(st.Mode().Perm()) != src.Mode || st.Size() != src.Bytes {
		return errors.New("conversion source type/mode/size changed")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if conversionHash(b) != src.SHA256 {
		return errors.New("conversion source hash changed")
	}
	return nil
}
func copyConversionOriginal(source, target string, mode fs.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(target), 0700); err != nil {
		return err
	}
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	// Copy to a sibling temporary file, then replace. An interrupted partial copy
	// is never accepted as an original on resume.
	out, err := os.CreateTemp(filepath.Dir(target), ".copy-")
	if err != nil {
		return err
	}
	tmp := out.Name()
	defer os.Remove(tmp)
	_, err = io.Copy(out, in)
	if err == nil {
		err = out.Chmod(mode)
	}
	if err == nil {
		err = out.Sync()
	}
	err = errors.Join(err, out.Close())
	if err != nil {
		return err
	}
	if err = os.Rename(tmp, target); err != nil {
		return err
	}
	return syncWorkerDir(filepath.Dir(target))
}
func resumeHostConversion(o HostConversionOptions, m HostConversionManifest) (HostConversionManifest, error) {
	root := conversionDir(o.Dir)
	// Retain the stopped sources' locks across copy, validation and publication.
	var unlocks []func()
	defer func() {
		for _, u := range unlocks {
			u()
		}
	}()
	lockPaths := map[string]bool{}
	for _, dir := range m.Spools {
		lockPaths[filepath.Join(dir, "worker.lock")] = true
	}
	for _, path := range o.QueueDatabases {
		lockPaths[path+".lock"] = true
	}
	entries, _ := os.ReadDir(filepath.Join(o.Dir, "units"))
	for _, entry := range entries {
		if entry.IsDir() {
			for _, name := range []string{"control.lock", "launch.lock"} {
				lockPaths[filepath.Join(o.Dir, "units", entry.Name(), name)] = true
			}
		}
	}
	for path := range lockPaths {
		u, e := hostOperationLock(filepath.Dir(path), filepath.Base(path), false)
		if e != nil {
			return m, e
		}
		unlocks = append(unlocks, u)
	}
	if m.Phase == "inventory" {
		for _, src := range m.Sources {
			target := filepath.Join(root, src.Original)
			if _, err := os.Stat(target); err == nil {
				if err = verifyConversionSource(target, src); err != nil {
					return m, err
				}
				continue
			}
			if err := verifyConversionSource(src.Path, src); err != nil {
				return m, err
			}
			if err := copyConversionOriginal(src.Path, target, fs.FileMode(src.Mode)); err != nil {
				return m, err
			}
			if err := conversionBoundary(o, "original"); err != nil {
				return m, err
			}
		}
		if err := syncWorkerDir(root); err != nil {
			return m, err
		}
		m.Phase = "preserved"
		if err := atomicHostJSON(root, "manifest.json", m, nil); err != nil {
			return m, err
		}
		if err := conversionBoundary(o, "preserved"); err != nil {
			return m, err
		}
	}
	// Verify all originals and all still-active sources before any publication.
	for _, src := range m.Sources {
		if err := verifyConversionSource(filepath.Join(root, src.Original), src); err != nil {
			return m, err
		}
		if _, err := os.Stat(src.Path); err == nil {
			if err = verifyConversionSource(src.Path, src); err != nil {
				return m, err
			}
		} else if !src.Retire || !errors.Is(err, os.ErrNotExist) {
			return m, err
		}
	}
	if m.Phase == "preserved" {
		if err := validateHostIntent(m.Intent); err != nil {
			return m, err
		}
		if err := atomicHostJSON(root, "staged.json", m.Intent, nil); err != nil {
			return m, err
		}
		m.Phase = "staged"
		if err := atomicHostJSON(root, "manifest.json", m, nil); err != nil {
			return m, err
		}
		if err := conversionBoundary(o, "staged"); err != nil {
			return m, err
		}
	}
	if m.Phase == "staged" {
		for i, src := range m.Sources {
			if src.Retire {
				if _, err := os.Stat(src.Path); errors.Is(err, os.ErrNotExist) {
					continue
				} else if err != nil {
					return m, err
				}
				retired := filepath.Join(root, "retired", fmt.Sprintf("%06d", i))
				if err := os.MkdirAll(filepath.Dir(retired), 0700); err != nil {
					return m, err
				}
				if err := os.Rename(src.Path, retired); err != nil {
					return m, err
				}
				if err := syncWorkerDir(filepath.Dir(src.Path)); err != nil {
					return m, err
				}
				if err := syncWorkerDir(filepath.Dir(retired)); err != nil {
					return m, err
				}
				if err := conversionBoundary(o, "retired-source"); err != nil {
					return m, err
				}
			}
		}
		// The unit directories themselves are now inert. Keep them, including their
		// original locks, under private storage instead of deleting any source.
		units := filepath.Join(o.Dir, "units")
		if _, err := os.Stat(units); err == nil {
			if err = os.Rename(units, filepath.Join(root, "retired-units")); err != nil {
				return m, err
			}
			if err = syncWorkerDir(o.Dir); err != nil {
				return m, err
			}
			if err = syncWorkerDir(root); err != nil {
				return m, err
			}
		}
		// Copy unit-only registries to the shared lookup path, checking duplicates.
		for _, src := range m.Sources {
			if filepath.Base(filepath.Dir(filepath.Dir(src.Path))) == "units" && strings.HasPrefix(filepath.Base(src.Path), "worker-") {
				b, err := os.ReadFile(filepath.Join(root, src.Original))
				if err != nil {
					return m, err
				}
				var w HostWorker
				if err = json.Unmarshal(b, &w); err != nil {
					return m, err
				}
				if err = AdoptHostWorker(o.Dir, w); err != nil {
					return m, err
				}
			}
		}
		m.Phase = "retired"
		if err := atomicHostJSON(root, "manifest.json", m, nil); err != nil {
			return m, err
		}
		if err := conversionBoundary(o, "retired"); err != nil {
			return m, err
		}
	}
	if m.Phase == "retired" {
		// Regenerate validated staging on resume (rename may already have occurred).
		if err := atomicHostJSON(root, "staged.json", m.Intent, nil); err != nil {
			return m, err
		}
		if err := os.Rename(filepath.Join(root, "staged.json"), filepath.Join(o.Dir, hostStateFile)); err != nil {
			return m, err
		}
		if err := syncWorkerDir(o.Dir); err != nil {
			return m, err
		}
		if err := syncWorkerDir(root); err != nil {
			return m, err
		}
		if err := conversionBoundary(o, "published"); err != nil {
			return m, err
		}
		m.Phase = "published"
		if err := atomicHostJSON(root, "manifest.json", m, nil); err != nil {
			return m, err
		}
	}
	if m.Phase != "published" {
		return m, errors.New("unsupported conversion phase")
	}
	if paths, err := legacyHostPaths(o.Dir); err != nil || len(paths) > 0 {
		return m, ErrHostState
	}
	m.Phase = "complete"
	if err := atomicHostJSON(root, "manifest.json", m, nil); err != nil {
		return m, err
	}
	if err := os.Remove(filepath.Join(root, "IN_PROGRESS")); err != nil && !errors.Is(err, os.ErrNotExist) {
		return m, err
	}
	if err := syncWorkerDir(root); err != nil {
		return m, err
	}
	if err := conversionBoundary(o, "unfenced"); err != nil {
		return m, err
	}
	return m, nil
}

func inventoryHostConversion(o HostConversionOptions) (HostConversionManifest, error) {
	m := HostConversionManifest{Schema: 1, Host: o.Host, Phase: "dry-run", Queues: o.QueueDatabases, Conflicts: []string{}, Sources: []HostConversionSource{}}
	now := o.Clock.Now()
	m.Intent = HostIntent{Schema: 1, Host: o.Host, Desired: "open", Reason: "offline conversion; fresh reconciliation required", AcceptedAt: now, Pending: []HostObligation{}, Holds: []string{}}
	seen := map[string]bool{}
	add := func(path string, retire bool) ([]byte, error) {
		path, err := filepath.Abs(path)
		if err != nil {
			return nil, err
		}
		if seen[path] {
			return os.ReadFile(path)
		}
		st, err := os.Lstat(path)
		if err != nil {
			return nil, err
		}
		if !st.Mode().IsRegular() {
			return nil, errors.New("conversion refuses non-regular source")
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		index := len(m.Sources)
		m.Sources = append(m.Sources, HostConversionSource{Path: path, SHA256: conversionHash(b), Bytes: st.Size(), Mode: uint32(st.Mode().Perm()), Retire: retire, Original: fmt.Sprintf("originals/%06d", index)})
		seen[path] = true
		return b, nil
	}
	hold := func(code string) {
		if !slices.Contains(m.Intent.Holds, code) {
			m.Intent.Holds = append(m.Intent.Holds, code)
		}
		m.Intent.Desired = "closed"
	}
	bound := func(n uint64) { m.LowerBound = max(m.LowerBound, n) }
	counters := func(b []byte) error {
		var fields map[string]json.RawMessage
		if json.Unmarshal(b, &fields) != nil {
			return nil
		}
		for _, key := range []string{"generation", "enable_generation"} {
			if value, ok := fields[key]; ok {
				var n uint64
				if err := json.Unmarshal(value, &n); err != nil {
					return errors.New("saved control counter unsupported or exhausted")
				}
				bound(n)
			}
		}
		return nil
	}

	pending := func(queue string, r Result) {
		if queue == "" || r.ID == "" || r.Attempt <= 0 || r.Machine == "" || r.Session == "" {
			hold("ambiguous_execution")
			return
		}
		r.ReleasePending = true
		r.ReleaseOnly = true
		p := HostObligation{Queue: queue, Result: r}
		for _, old := range m.Intent.Pending {
			if old.Queue == queue && old.Result.ID == r.ID && old.Result.Attempt == r.Attempt && old.Result.Machine == r.Machine && old.Result.Session == r.Session {
				return
			}
		}
		m.Intent.Pending = append(m.Intent.Pending, p)
		hold("pending_release")
	}
	var workers []HostWorker
	dirs := []string{o.Dir}
	entries, err := os.ReadDir(filepath.Join(o.Dir, "units"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return m, err
	}
	var unlocks []func()
	defer func() {
		for _, u := range unlocks {
			u()
		}
	}()
	for _, entry := range entries {
		if !entry.IsDir() || !ValidWorkerUnit(entry.Name()) {
			m.Conflicts = append(m.Conflicts, "unknown_unit_mapping")
			continue
		}
		dir := filepath.Join(o.Dir, "units", entry.Name())
		dirs = append(dirs, dir)
		for _, name := range []string{"control.lock", "launch.lock"} {
			u, e := hostOperationLock(dir, name, false)
			if e != nil {
				return m, e
			}
			unlocks = append(unlocks, u)
		}
	}
	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return m, err
		}
		type controlSource struct {
			name, path string
			archived   bool
		}
		var sources []controlSource
		for _, entry := range entries {
			path := filepath.Join(dir, entry.Name())
			if entry.Name() != ".Trash" {
				sources = append(sources, controlSource{name: entry.Name(), path: path})
				continue
			}
			// 8bc2a83 archives controls as ID-name under .Trash. They no
			// longer authorize runtime actions, but their counters and exact
			// historical obligations still constrain offline conversion.
			if err = filepath.WalkDir(path, func(path string, d fs.DirEntry, e error) error {
				if e != nil {
					return e
				}
				if d.IsDir() {
					return nil
				}
				name := ""
				for _, suffix := range []string{"DISABLED", "control-generation.json", "disable-receipt.json", "enable-pending.json", "forgotten-queues.json"} {
					if d.Name() == suffix || strings.HasSuffix(d.Name(), "-"+suffix) {
						name = suffix
						break
					}
				}
				sources = append(sources, controlSource{name: name, path: path, archived: true})
				return nil
			}); err != nil {
				return m, err
			}
		}
		for _, source := range sources {
			name, path := source.name, source.path
			retire := !source.archived || dir != o.Dir
			switch {
			case name == "DISABLED":
				b, e := add(path, retire)
				if e != nil {
					return m, e
				}
				hold("disabled_flag")
				if err := counters(b); err != nil {
					return m, err
				}
				var f legacyControlFlag
				if json.Unmarshal(b, &f) != nil {
					hold("malformed_flag")
				} else {
					bound(f.Generation)
				}
			case name == "control-generation.json":
				b, e := add(path, retire)
				if e != nil {
					return m, e
				}
				var n uint64
				if json.Unmarshal(b, &n) != nil {
					return m, errors.New("unreadable counter; revision lower bound unknown")
				}
				bound(n)
			case name == "forgotten-queues.json":
				b, e := add(path, retire)
				if e != nil {
					return m, e
				}
				var f forgottenQueues
				if json.Unmarshal(b, &f) != nil {
					return m, errors.New("unreadable queue counters")
				}
				bound(f.Generation)
				for _, n := range f.Queues {
					bound(n)
				}
				hold("forgotten_queue_custody")
			case name == "disable-receipt.json" || name == "enable-pending.json":
				b, e := add(path, retire)
				if e != nil {
					return m, e
				}
				m.Receipts++
				if err := counters(b); err != nil {
					return m, err
				}
				hold("historical_receipt")
				var r hostReceipt
				if json.Unmarshal(b, &r) != nil {
					hold("malformed_receipt")
					continue
				}
				bound(r.Generation)
				bound(r.EnableGeneration)
				if r.Generation == 0 || r.Workers == nil {
					hold("incomplete_receipt")
				}
				workers = append(workers, r.Workers...)
			case strings.HasPrefix(name, "worker-") && strings.HasSuffix(name, ".json"):
				b, e := add(path, dir != o.Dir)
				if e != nil {
					return m, e
				}
				var w HostWorker
				if json.Unmarshal(b, &w) != nil {
					return m, errors.New("unreadable worker host mapping")
				}
				workers = append(workers, w)
			case source.archived:
				if _, err := add(path, dir != o.Dir); err != nil {
					return m, err
				}
			case dir != o.Dir && name != "control.lock" && name != "launch.lock" && name != "reconcile.lock" && name != ".Trash":
				m.Conflicts = append(m.Conflicts, "unsupported_unit_record")
			}
		}
	}
	byState := map[string]HostWorker{}
	for _, w := range workers {
		bound(w.Generation)
		if w.Host != o.Host || w.Name == "" || !filepath.IsAbs(w.State) {
			m.Conflicts = append(m.Conflicts, "unknown_host_mapping")
			continue
		}
		if old, ok := byState[w.State]; ok && (old.Host != w.Host || old.Name != w.Name || old.Queue != w.Queue) {
			m.Conflicts = append(m.Conflicts, "conflicting_worker_mapping")
			continue
		}
		byState[w.State] = w
		for _, r := range w.Executions {
			if r.Machine != w.Name || w.Session != "" && r.Session != w.Session {
				hold("ambiguous_receipt_execution")
				continue
			}
			pending(w.Queue, r)
		}
	}
	m.Workers = len(byState)
	if len(byState) == 0 {
		hold("missing_worker_inventory")
	}
	stateDirs := make([]string, 0, len(byState))
	for dir := range byState {
		stateDirs = append(stateDirs, dir)
	}
	slices.Sort(stateDirs)
	m.Spools = stateDirs
	for _, dir := range stateDirs {
		w := byState[dir]
		m.Identities = append(m.Identities, HostWorker{Host: w.Host, Name: w.Name, State: dir, Session: w.Session, Queue: w.Queue, Unit: w.Unit})
	}
	for _, dir := range stateDirs {
		w := byState[dir]
		lock, e := hostOperationLock(dir, "worker.lock", false)
		if e != nil {
			return m, e
		}
		unlocks = append(unlocks, lock)
		// Preserve the spool as bytes; do not reinterpret checkpoints or rewrite
		// executions. S12b owns general persisted custody/checkpoint conversion.
		e = filepath.WalkDir(dir, func(path string, d fs.DirEntry, e error) error {
			if e != nil {
				return e
			}
			if d.IsDir() {
				return nil
			}
			if strings.HasSuffix(path, ".lock") {
				return nil
			}
			_, err := add(path, false)
			return err
		})
		if e != nil {
			return m, e
		}
		b, e := os.ReadFile(filepath.Join(dir, "state.json"))
		if errors.Is(e, os.ErrNotExist) {
			hold("missing_worker_spool")
			continue
		}
		if e != nil {
			return m, e
		}
		var st workerState
		if json.Unmarshal(b, &st) != nil {
			hold("malformed_worker_spool")
			continue
		}
		if st.Job != nil {
			pending(w.Queue, Result{Machine: w.Name, Session: st.Session, Execution: Execution{ID: st.Job.ID, Attempt: st.Job.Attempt}})
		}
		paths, _ := filepath.Glob(filepath.Join(dir, "*.docker.json"))
		for _, path := range paths {
			b, e := os.ReadFile(path)
			if e != nil {
				return m, e
			}
			var d dockerExecution
			if json.Unmarshal(b, &d) != nil || d.Owner == nil {
				hold("ambiguous_daemon_custody")
				continue
			}
			if d.Owner.Machine != w.Name {
				m.Conflicts = append(m.Conflicts, "conflicting_daemon_mapping")
				continue
			}
			pending(w.Queue, *d.Owner)
		}
		if st.PID != 0 || st.LaunchPending || len(st.Groups) > 0 {
			hold("unverified_native_custody")
		}
	}
	endpoints := make([]string, 0, len(o.QueueDatabases))
	for endpoint := range o.QueueDatabases {
		endpoints = append(endpoints, endpoint)
	}
	slices.Sort(endpoints)
	for _, endpoint := range endpoints {
		path := o.QueueDatabases[endpoint]
		path, e := filepath.Abs(path)
		if e != nil {
			return m, e
		}
		u, e := hostOperationLock(filepath.Dir(path), filepath.Base(path)+".lock", false)
		if e != nil {
			return m, e
		}
		unlocks = append(unlocks, u)
		// SQLite reads a private byte copy so WAL read marks cannot mutate source.
		snapshot, e := os.MkdirTemp(o.Dir, ".host-inventory-")
		if e != nil {
			return m, e
		}
		var b []byte
		e = func() error {
			defer os.RemoveAll(snapshot) // Only this disposable byte copy is removed.
			copyPath := filepath.Join(snapshot, "queue.db")
			for _, suffix := range []string{"", "-wal", "-shm"} {
				if _, err := os.Stat(path + suffix); errors.Is(err, os.ErrNotExist) && suffix != "" {
					continue
				} else if err != nil {
					return err
				}
				data, err := add(path+suffix, false)
				if err != nil {
					return err
				}
				if err = os.WriteFile(copyPath+suffix, data, 0600); err != nil {
					return err
				}
			}
			uri := url.URL{Scheme: "file", Path: copyPath}
			db, err := sql.Open("sqlite", uri.String()+"?mode=ro")
			if err != nil {
				return err
			}
			err = db.QueryRow("SELECT data FROM queue WHERE id=1").Scan(&b)
			return errors.Join(err, db.Close())
		}()
		if e != nil {
			return m, e
		}
		var s state
		if e = json.Unmarshal(b, &s); e != nil {
			return m, e
		}
		bound(s.HostControls[o.Host].Generation)
		if c, ok := s.HostControls[o.Host]; ok && (!c.Enabled || !c.Cleared) {
			hold("queue_control_pending")
		}
		if s.DisabledHosts[o.Host] != "" {
			hold("queue_closed")
		}
		for _, c := range s.MachineControls {
			if c.Host == o.Host {
				bound(c.Generation)
				if !c.Enabled || !c.Cleared {
					hold("queue_control_pending")
				}
			}
		}
		for _, r := range s.HostReleases {
			machine := s.Machines[r.Machine]
			if machine == nil {
				m.Conflicts = append(m.Conflicts, "unknown_queue_host_mapping")
				continue
			}
			if r.Host != "" && r.Host != hostID(machine) {
				m.Conflicts = append(m.Conflicts, "conflicting_release_host_mapping")
				continue
			}
			if hostID(machine) == o.Host {
				pending(endpoint, Result{Machine: r.Machine, Session: r.Session, Execution: Execution{ID: r.Job, Attempt: r.Attempt}})
				if r.Preparation {
					for i := range m.Intent.Pending {
						p := &m.Intent.Pending[i]
						if p.Queue == endpoint && p.Engine == nil && p.Result.ID == r.Job && p.Result.Machine == r.Machine && p.Result.Session == r.Session && p.Result.Attempt == r.Attempt {
							p.Preparation = true
						}
					}
				}
			}
		}
		for _, slot := range s.ModelSlots {
			if slot == nil {
				hold("ambiguous_engine_custody")
				continue
			}
			machine := s.Machines[slot.Machine]
			if machine == nil {
				m.Conflicts = append(m.Conflicts, "unknown_engine_host_mapping")
				continue
			}
			if hostID(machine) != o.Host || slot.State == "unloaded" {
				continue
			}
			hold("pending_engine_release")
			if slot.Machine == "" || slot.Session == "" || slot.Model == "" || slot.Attempt == "" {
				hold("ambiguous_engine_custody")
				continue
			}
			m.Intent.Pending = append(m.Intent.Pending, HostObligation{Queue: endpoint, Engine: &HostEngineExecution{Machine: slot.Machine, Session: slot.Session, Model: slot.Model, Attempt: slot.Attempt}})
		}
		for _, j := range s.Jobs {
			for _, c := range j.Copies {
				machine := s.Machines[c.Machine]
				if machine == nil {
					hold("unknown_queue_execution_mapping")
					continue
				}
				if hostID(machine) == o.Host && (c.Recoverable || j.State == "running" || j.State == "stopping") {
					pending(endpoint, Result{Machine: c.Machine, Session: c.Session, Execution: Execution{ID: j.ID, Attempt: c.Attempt}})
				}
			}
		}
	}
	// Every referenced queue must be mapped to a preserved offline database;
	// otherwise its saved counters are unknown and no safe revision can be allocated.
	for _, w := range byState {
		if w.Queue != "" {
			if _, ok := o.QueueDatabases[w.Queue]; !ok {
				m.Conflicts = append(m.Conflicts, "queue_database_mapping_missing")
			}
		}
	}
	if m.LowerBound == ^uint64(0) {
		return m, errors.New("host revision exhausted")
	}
	m.Intent.HostRevision = HostRevision{Revision: m.LowerBound + 1, Operation: ID()}
	if m.Intent.Desired == "closed" {
		m.Intent.StopDeadline = &now
		m.Intent.PreviousClose = m.Intent.Revision
	}
	m.Pending = len(m.Intent.Pending)
	if err = validateHostIntent(m.Intent); err != nil {
		return m, err
	}
	return m, nil
}
