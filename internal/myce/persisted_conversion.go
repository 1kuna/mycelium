package myce

// Offline conversion uses an immutable source tree and one published target
// directory. No multi-file in-place transaction or dual runtime decoder exists.
import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"unicode"
	"unicode/utf8"
)

const persistedSchema = 3

var errPersistedFormat = errors.New("unsupported persisted format: stop services and run myce state convert on an offline copy")

type PersistedConversionOptions struct {
	Source, Target, Host string
	Clock                Clock
	Apply                bool
	boundary             func(string) error
}
type PersistedConversionFile struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
	Mode   uint32 `json:"mode"`
}
type PersistedConversionManifest struct {
	Host           string                    `json:"host_id"`
	Tables         map[string]int            `json:"table_records,omitempty"`
	Schema         int                       `json:"schema_version"`
	SourceFormat   string                    `json:"source_format"`
	Phase          string                    `json:"phase"`
	Files          []PersistedConversionFile `json:"files"`
	ConvertedFiles []PersistedConversionFile `json:"converted_files,omitempty"`
	Jobs           int                       `json:"jobs"`
	Copies         int                       `json:"copies"`
	Releases       int                       `json:"releases"`
	Slots          int                       `json:"slots"`
	Reports        int                       `json:"reports"`
	Blockers       []string                  `json:"blockers"`
}

// ConvertPersistedState requires a stopped local bundle: queue.db (optional),
// workers/<name>/state.json (optional), host/ (optional), checkpoints/ and payloads.
// The source remains the backup; target also retains hash-verifiable originals.
func ConvertPersistedState(o PersistedConversionOptions) (m PersistedConversionManifest, err error) {
	m = PersistedConversionManifest{Schema: persistedSchema, Host: o.Host, SourceFormat: "7285a88", Phase: "dry-run", Blockers: []string{}}
	if o.Clock == nil || o.Host == "" || o.Source == "" || o.Target == "" {
		return m, errors.New("conversion requires source, target, host identity and clock")
	}
	o.Source, err = filepath.Abs(o.Source)
	if err != nil {
		return m, err
	}
	o.Target, err = filepath.Abs(o.Target)
	if err != nil {
		return m, err
	}
	if info, e := os.Lstat(o.Source); e != nil || !info.IsDir() {
		return m, errors.New("source must be a real directory")
	}
	o.Source, err = filepath.EvalSymlinks(o.Source)
	if err != nil {
		return m, err
	}
	parent, e := filepath.EvalSymlinks(filepath.Dir(o.Target))
	if e != nil {
		return m, e
	}
	o.Target = filepath.Join(parent, filepath.Base(o.Target))
	if o.Source == o.Target || strings.HasPrefix(o.Target, o.Source+string(os.PathSeparator)) || strings.HasPrefix(o.Source, o.Target+string(os.PathSeparator)) {
		return m, errors.New("source and target must be disjoint directories")
	}
	// Serialize concurrent converters, including the completed-target readback.
	lock, e := os.OpenFile(o.Target+".conversion.lock", os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return m, e
	}
	defer lock.Close()
	if e = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		return m, errors.New("target conversion is in use")
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	if _, e := os.Lstat(o.Target); e == nil {
		b, e := os.ReadFile(filepath.Join(o.Target, ".conversion", "manifest.json"))
		if e != nil {
			return m, errors.New("target exists without a completed conversion")
		}
		if json.Unmarshal(b, &m) != nil || m.Schema != persistedSchema || m.Host != o.Host || m.Phase != "complete" {
			return m, errors.New("target conversion is incomplete")
		}
		if e = verifyPersistedSource(o.Source, m, false); e != nil {
			return m, e
		}
		if e = verifyPersistedOriginals(o.Target, m); e != nil {
			return m, e
		}
		if len(m.ConvertedFiles) == 0 {
			return m, errors.New("target lacks validated output hashes")
		}
		output := m
		output.Files = m.ConvertedFiles
		if e = verifyPersistedSource(o.Target, output, true); e != nil {
			return m, errors.New("completed target changed; do not reconvert an active target")
		}
		intent, e := ReadHostIntent(filepath.Join(o.Target, "host"))
		if e != nil || intent.Host != o.Host || intent.Desired != "closed" {
			return m, errors.New("completed target is no longer CLOSED")
		}
		// Repeat the publication barrier if an earlier rename was visible but
		// syncing its parent failed. Readback alone is not durable publication.
		return m, syncWorkerDir(filepath.Dir(o.Target))
	} else if !errors.Is(e, os.ErrNotExist) {
		return m, e
	}
	// Hold the same locks used by the old queue/worker before inventory and copy.
	var locks []*os.File
	defer func() {
		for _, f := range locks {
			_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
			_ = f.Close()
		}
	}()
	err = filepath.WalkDir(o.Source, func(path string, d fs.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.Type()&os.ModeSymlink != 0 {
			return errors.New("conversion refuses symlinks")
		}
		if d.IsDir() {
			if d.Name() == ".conversion" {
				return errors.New("source is already converted")
			}
			return nil
		}
		info, e := d.Info()
		if e != nil {
			return e
		}
		if !info.Mode().IsRegular() {
			return errors.New("conversion requires regular source files")
		}
		if strings.HasSuffix(d.Name(), ".lock") {
			f, e := os.OpenFile(path, os.O_RDWR, 0)
			if e != nil {
				return e
			}
			if e = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
				f.Close()
				return errors.New("conversion source is in use")
			}
			locks = append(locks, f)
		}
		return nil
	})
	if err != nil {
		return m, err
	}
	err = filepath.WalkDir(o.Source, func(path string, d fs.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.IsDir() {
			return nil
		}
		info, e := d.Info()
		if e != nil {
			return e
		}
		hash, e := conversionFileHash(path)
		if e != nil {
			return e
		}
		rel, e := filepath.Rel(o.Source, path)
		if e != nil {
			return e
		}
		m.Files = append(m.Files, PersistedConversionFile{filepath.ToSlash(rel), hash, info.Size(), uint32(info.Mode().Perm())})
		return nil
	})
	if err != nil {
		return m, err
	}
	for _, f := range m.Files {
		if f.Path == "host/.host-conversion/IN_PROGRESS" {
			return m, errors.New("source has an unfinished host conversion; recover it before bundle conversion")
		}
	}
	// Staging lives beside target, so the only activation rename is same-filesystem.
	if len(m.Files) == 0 {
		return m, errors.New("empty conversion source")
	}
	stage, err := os.MkdirTemp(filepath.Dir(o.Target), ".myce-convert-")
	if err != nil {
		return m, err
	}
	// A failed stage is retained privately for diagnosis; the source is untouched.
	for _, f := range m.Files {
		if err = copyConversionOriginal(filepath.Join(o.Source, f.Path), filepath.Join(stage, f.Path), fs.FileMode(f.Mode)); err != nil {
			return m, err
		}
	}
	for _, f := range m.Files {
		if err = copyConversionOriginal(filepath.Join(o.Source, f.Path), filepath.Join(stage, ".conversion", "originals", f.Path), 0600); err != nil {
			return m, err
		}
	}
	if err = verifyPersistedSource(stage, m, true); err != nil {
		return m, err
	}
	if err = convertPersistedTree(stage, o, &m); err != nil {
		return m, err
	}
	if err = verifyPersistedOriginals(stage, m); err != nil {
		return m, err
	}
	// Include additions, deletions, type/mode changes and WAL in the recheck.
	if err = verifyPersistedSource(o.Source, m, false); err != nil {
		return m, err
	}
	if len(m.Blockers) > 0 {
		return m, errors.New("conversion blocked: ambiguous persisted records; source and unpublished target remain inert")
	}
	if err = filepath.WalkDir(stage, func(path string, d fs.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.IsDir() {
			if path == filepath.Join(stage, ".conversion") {
				return filepath.SkipDir
			}
			return nil
		}
		info, e := d.Info()
		if e != nil {
			return e
		}
		h, e := conversionFileHash(path)
		if e != nil {
			return e
		}
		rel, e := filepath.Rel(stage, path)
		if e != nil {
			return e
		}
		m.ConvertedFiles = append(m.ConvertedFiles, PersistedConversionFile{filepath.ToSlash(rel), h, info.Size(), uint32(info.Mode().Perm())})
		return nil
	}); err != nil {
		return m, err
	}
	if !o.Apply {
		return m, nil
	}
	m.Phase = "complete"
	if err = atomicHostJSON(filepath.Join(stage, ".conversion"), "manifest.json", m, nil); err != nil {
		return m, err
	}
	if o.boundary != nil {
		if err = o.boundary("validated"); err != nil {
			return m, err
		}
	}
	if err = verifyPersistedSource(o.Source, m, false); err != nil {
		return m, err
	}
	// No replacing an existing target, including a concurrent converter's target.
	if _, e := os.Lstat(o.Target); !errors.Is(e, os.ErrNotExist) {
		return m, errors.New("target appeared during conversion")
	}
	var dirs []string
	if err = filepath.WalkDir(stage, func(path string, d fs.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.IsDir() {
			dirs = append(dirs, path)
		}
		return nil
	}); err != nil {
		return m, err
	}
	for i := len(dirs) - 1; i >= 0; i-- {
		if err = syncWorkerDir(dirs[i]); err != nil {
			return m, err
		}
	}
	if err = os.Rename(stage, o.Target); err != nil {
		return m, err
	}
	if err = syncWorkerDir(filepath.Dir(o.Target)); err != nil {
		return m, err
	}
	if o.boundary != nil {
		err = o.boundary("published")
	}
	return m, err
}

// Compare a complete inventory, not only the originally listed names.
func verifyPersistedSource(root string, m PersistedConversionManifest, allowBackup bool) error {
	var files []PersistedConversionFile
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.IsDir() {
			if allowBackup && path == filepath.Join(root, ".conversion") {
				return filepath.SkipDir
			}
			return nil
		}
		info, e := d.Info()
		if e != nil {
			return e
		}
		if !info.Mode().IsRegular() {
			return errors.New("conversion source type changed")
		}
		h, e := conversionFileHash(path)
		if e != nil {
			return e
		}
		rel, e := filepath.Rel(root, path)
		if e != nil {
			return e
		}
		files = append(files, PersistedConversionFile{filepath.ToSlash(rel), h, info.Size(), uint32(info.Mode().Perm())})
		return nil
	})
	if err != nil {
		return err
	}
	// Staging clamps unsafe write bits; the recorded source mode remains evidence.
	expected := append([]PersistedConversionFile(nil), m.Files...)
	for i := range expected {
		expected[i].Mode &^= 0022
	}
	if !reflect.DeepEqual(files, m.Files) && !(allowBackup && reflect.DeepEqual(files, expected)) {
		return errors.New("conversion source changed")
	}
	return nil
}

func verifyPersistedOriginals(root string, m PersistedConversionManifest) error {
	for _, f := range m.Files {
		if !fs.ValidPath(f.Path) {
			return errors.New("invalid conversion manifest path")
		}
		h, e := conversionFileHash(filepath.Join(root, ".conversion", "originals", f.Path))
		if e != nil || h != f.SHA256 {
			return errors.New("conversion original hash mismatch")
		}
	}
	return nil
}
func convertPersistedTree(root string, o PersistedConversionOptions, m *PersistedConversionManifest) error {
	hosts := map[string]uint64{o.Host: 0}
	revisions := map[string]HostRevision{}
	var lower uint64
	// Saved counters bound the new revision; they never establish release.
	invalidCounter := false
	var counters func(any)
	counters = func(v any) {
		switch x := v.(type) {
		case map[string]any:
			for k, v := range x {
				if k == "revision" || k == "generation" || k == "enable_generation" {
					if n, ok := v.(json.Number); ok {
						u, e := strconv.ParseUint(string(n), 10, 64)
						if e == nil {
							lower = max(lower, u)
						} else {
							invalidCounter = true
						}
					} else {
						invalidCounter = true
					}
				}
				counters(v)
			}
		case []any:
			for _, v := range x {
				counters(v)
			}
		}
	}
	counterErr := filepath.WalkDir(filepath.Join(root, "host"), func(path string, d fs.DirEntry, e error) error {
		if errors.Is(e, os.ErrNotExist) {
			return nil
		}
		if e != nil {
			return e
		}
		if d.IsDir() || !strings.HasSuffix(d.Name(), ".json") {
			return nil
		}
		b, e := os.ReadFile(path)
		if e != nil {
			return e
		}
		var v any
		dec := json.NewDecoder(strings.NewReader(string(b)))
		dec.UseNumber()
		if dec.Decode(&v) != nil {
			return errors.New("malformed host conversion input")
		}
		if strings.HasSuffix(d.Name(), "control-generation.json") {
			n, ok := v.(json.Number)
			if !ok {
				return errors.New("invalid host counter")
			}
			u, e := strconv.ParseUint(string(n), 10, 64)
			if e != nil {
				return e
			}
			lower = max(lower, u)
		}
		if strings.HasSuffix(d.Name(), "forgotten-queues.json") {
			var saved forgottenQueues
			if json.Unmarshal(b, &saved) != nil {
				return errors.New("invalid saved queue counters")
			}
			for _, n := range saved.Queues {
				lower = max(lower, n)
			}
		}
		counters(v)
		if invalidCounter {
			return errors.New("invalid host revision lower bound")
		}
		return nil
	})
	if counterErr != nil && !errors.Is(counterErr, os.ErrNotExist) {
		return counterErr
	}
	queuePath := filepath.Join(root, "queue.db")
	if _, err := os.Stat(queuePath); err == nil {
		u := url.URL{Scheme: "file", Path: queuePath}
		db, e := sql.Open("sqlite", u.String())
		if e != nil {
			return e
		}
		defer db.Close()
		if _, e = db.Exec("PRAGMA synchronous=FULL"); e != nil {
			return e
		}
		var check string
		if e = db.QueryRow("PRAGMA integrity_check").Scan(&check); e != nil || check != "ok" {
			return errors.New("source SQLite integrity check failed")
		}
		var data []byte
		if e = db.QueryRow("SELECT data FROM queue WHERE id=1").Scan(&data); e != nil {
			return e
		}
		var raw map[string]json.RawMessage
		var s state
		history := map[string]json.RawMessage{}
		var version int
		if e = db.QueryRow("PRAGMA user_version").Scan(&version); e != nil {
			return e
		}
		if version != 0 {
			return errPersistedFormat
		}
		var known struct {
			state
			HostControls    json.RawMessage `json:"host_controls"`
			MachineControls json.RawMessage `json:"machine_controls"`
			DisabledHosts   json.RawMessage `json:"disabled_hosts"`
		}
		if json.Unmarshal(data, &raw) != nil {
			return errors.New("unsupported queue state")
		}
		if e = retireReleaseFields(raw, history, &lower); e != nil {
			return e
		}
		currentData, e := json.Marshal(raw)
		if e != nil {
			return e
		}
		dec := json.NewDecoder(bytes.NewReader(currentData))
		dec.DisallowUnknownFields()
		if dec.Decode(&known) != nil {
			return errors.New("unsupported queue state")
		}
		s = known.state
		if s.Schema != 0 || s.Jobs == nil || s.Machines == nil {
			return errors.New("unsupported queue state")
		}
		if v := raw["schema_version"]; v != nil && string(v) != "3" {
			return errPersistedFormat
		}
		for key, machine := range s.Machines {
			if machine == nil || machine.ID == "" || machine.ID != key || machine.HostID == "" {
				m.Blockers = append(m.Blockers, "machine_identity")
				continue
			}
			hosts[hostID(machine)] = lower
		}
		for key, j := range s.Jobs {
			m.Jobs++
			if j == nil || j.ID == "" || j.ID != key {
				m.Blockers = append(m.Blockers, "job_identity")
				continue
			}
			if j.State != "queued" && j.State != "running" && j.State != "stopping" && !terminal(j.State) {
				m.Blockers = append(m.Blockers, "job_state")
			}
			if !validCheckpointGenerations(j) {
				m.Blockers = append(m.Blockers, "checkpoint_generation")
			}
			// The old writer used these exact cancellation markers. Translate
			// exclusion once, without inferring release from absent custody rows.
			if j.CancelledAt == nil && (j.Reason == "cancel requested" || j.State == "cancelled") {
				var jobs map[string]map[string]json.RawMessage
				if e = json.Unmarshal(raw["jobs"], &jobs); e != nil {
					return e
				}
				jobs[j.ID]["cancelled_at"], _ = json.Marshal(o.Clock.Now())
				raw["jobs"], _ = json.Marshal(jobs)
			}
			if j.Machine != "" && (j.Session == "" || j.Attempt <= 0) || (j.State == "running" || j.State == "stopping") && j.Machine == "" {
				m.Blockers = append(m.Blockers, "job_execution_identity")
			}
			m.Copies += len(j.Copies)
			if j.Attempt > 0 && j.Machine != "" && len(j.Copies) == 0 {
				if j.Session == "" {
					m.Blockers = append(m.Blockers, "copy_session")
				} else { // Only this exact durable binding can reconstruct the single recorded copy.
					var jobs map[string]map[string]json.RawMessage
					_ = json.Unmarshal(raw["jobs"], &jobs)
					b, _ := json.Marshal([]Copy{{Machine: j.Machine, Session: j.Session, Attempt: j.Attempt}})
					jobs[j.ID]["copies"] = b
					raw["jobs"], _ = json.Marshal(jobs)
					m.Copies++
				}
			}
			for _, c := range j.Copies {
				if c.Machine == "" || c.Session == "" || c.Attempt <= 0 {
					m.Blockers = append(m.Blockers, "copy_identity")
				}
			}
		}
		for h := range s.Hosts {
			hosts[h] = lower
		}
		m.Releases = len(s.HostReleases)
		for _, r := range s.HostReleases {
			if r.Host != "" {
				hosts[r.Host] = lower
			}
			if r.Machine == "" || r.Session == "" || r.Job == "" || r.Attempt <= 0 && !(r.Preparation && r.Job == "preparation" && r.Attempt == 0) {
				m.Blockers = append(m.Blockers, "release_identity")
			}
		}
		m.Slots = len(s.ModelSlots)
		for _, slot := range s.ModelSlots {
			if slot == nil || slot.Machine == "" || slot.Session == "" || slot.Model == "" || s.Machines[slot.Machine] == nil || slot.Attempt == "" || slot.State == "loading" && slot.LoadUntil == nil {
				m.Blockers = append(m.Blockers, "engine_identity_or_deadline")
			}
		}
		for _, key := range []string{"host_controls", "machine_controls", "disabled_hosts"} {
			if b := raw[key]; b != nil {
				history[key] = b
				delete(raw, key)
			}
		}
		// Independent old counters only provide a lower bound; never order them.
		for _, key := range []string{"host_controls", "machine_controls"} {
			var controls map[string]struct {
				Generation uint64 `json:"generation"`
				Host       string `json:"host"`
			}
			if len(history[key]) > 0 {
				if e = json.Unmarshal(history[key], &controls); e != nil {
					return errPersistedFormat
				}
			}
			for h, c := range controls {
				if key == "machine_controls" {
					if c.Host != "" {
						h = c.Host
					} else if machine := s.Machines[h]; machine != nil {
						h = machine.HostID
					} else {
						m.Blockers = append(m.Blockers, "control_host_mapping")
						continue
					}
				}
				if h == "" {
					m.Blockers = append(m.Blockers, "control_host_mapping")
					continue
				}
				if _, ok := hosts[h]; !ok {
					hosts[h] = lower
				}
			}
			for _, c := range controls {
				for h := range hosts {
					hosts[h] = max(hosts[h], c.Generation)
				}
			}
		}
		var disabled map[string]string
		if len(history["disabled_hosts"]) > 0 {
			if e = json.Unmarshal(history["disabled_hosts"], &disabled); e != nil {
				return errPersistedFormat
			}
		}
		for h := range disabled {
			if h == "" {
				m.Blockers = append(m.Blockers, "control_host_mapping")
			} else {
				if _, ok := hosts[h]; !ok {
					hosts[h] = lower
				}
			}
		}
		if len(history) > 0 {
			raw["conversion_history"], _ = json.Marshal(history)
		}
		m.Tables = map[string]int{}
		rows, e := db.Query("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
		if e != nil {
			return e
		}
		var tables []string
		for rows.Next() {
			var name string
			if e = rows.Scan(&name); e != nil {
				rows.Close()
				return e
			}
			tables = append(tables, name)
		}
		if e = rows.Close(); e != nil {
			return e
		}
		for _, name := range tables {
			var count int
			if e = db.QueryRow(`SELECT count(*) FROM "` + strings.ReplaceAll(name, `"`, `""`) + `"`).Scan(&count); e != nil {
				return e
			}
			m.Tables[name] = count
		}
		if b := raw["host_intents"]; len(b) > 0 {
			history["host_intents"] = b
			raw["conversion_history"], _ = json.Marshal(history)
		}
		intents := s.HostIntents
		if intents == nil {
			intents = map[string]QueueHostIntent{}
		}
		for h, old := range intents {
			if _, ok := hosts[h]; !ok {
				hosts[h] = max(lower, old.Revision)
			}
		}
		for h, rev := range hosts {
			rev = max(lower, max(rev, intents[h].Revision))
			if rev == ^uint64(0) {
				return errors.New("revision exhausted")
			}
			rev++
			hosts[h] = rev
			revisions[h] = HostRevision{Revision: rev, Operation: ID()}
			intents[h] = QueueHostIntent{HostRevision: revisions[h], Desired: "closed", AcceptedAt: o.Clock.Now(), Reason: "offline conversion; reconcile before explicit open"}
		}
		raw["host_intents"], _ = json.Marshal(intents)
		raw["schema_version"] = json.RawMessage("3")
		data, e = json.Marshal(raw)
		if e != nil {
			return e
		}
		tx, e := db.Begin()
		if e != nil {
			return e
		}
		if _, e = tx.Exec("UPDATE queue SET data=? WHERE id=1", data); e != nil {
			tx.Rollback()
			return e
		}
		if _, e = tx.Exec("PRAGMA user_version=3"); e != nil {
			tx.Rollback()
			return e
		}
		if e = tx.Commit(); e != nil {
			return e
		}
		if _, e = db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); e != nil {
			return e
		}
		if e = db.Close(); e != nil {
			return e
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	// Explicitly retire JSON credentials in copied install directories.
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.IsDir() && d.Name() == ".conversion" {
			return filepath.SkipDir
		}
		if d.IsDir() || d.Name() != "credentials.json" {
			return nil
		}
		b, e := os.ReadFile(path)
		if e != nil {
			return e
		}
		var old struct {
			Token string `json:"token"`
		}
		if json.Unmarshal(b, &old) != nil {
			m.Blockers = append(m.Blockers, "credential_format")
			return nil
		}
		if old.Token == "" || !utf8.ValidString(old.Token) || strings.ContainsFunc(old.Token, unicode.IsControl) {
			m.Blockers = append(m.Blockers, "credential_format")
			return nil
		}
		config, e := os.ReadFile(filepath.Join(filepath.Dir(path), "config.json"))
		if e != nil {
			m.Blockers = append(m.Blockers, "credential_queue_binding")
			return nil
		}
		var c ServiceConfig
		if json.Unmarshal(config, &c) != nil || c.Queue == "" || !utf8.ValidString(c.Queue) || strings.ContainsFunc(c.Queue, unicode.IsControl) {
			m.Blockers = append(m.Blockers, "credential_queue_binding")
			return nil
		}
		text := []byte("MYCE_TOKEN=" + strconv.Quote(old.Token) + "\nMYCE_QUEUE=" + strconv.Quote(c.Queue) + "\n")
		if e = writePersistedFile(path, text); e != nil {
			return e
		}
		token, queue, e := ReadFleetEnvironment(path)
		if e != nil || token != old.Token || queue != c.Queue {
			m.Blockers = append(m.Blockers, "credential_format")
		}
		return nil
	})
	if err != nil {
		return err
	}
	// Worker and report formats retain every payload field, offset and identity.
	err = filepath.WalkDir(filepath.Join(root, "workers"), func(path string, d fs.DirEntry, e error) error {
		if errors.Is(e, os.ErrNotExist) {
			return nil
		}
		if e != nil {
			return e
		}
		if d.IsDir() {
			return nil
		}
		if d.Name() == "state.json" || d.Name() == "operator-stop.json" || strings.HasPrefix(d.Name(), "engine-") && strings.HasSuffix(d.Name(), ".json") && !strings.HasSuffix(d.Name(), ".docker.json") || strings.HasPrefix(d.Name(), "attempt-") && strings.HasSuffix(d.Name(), ".json") && !strings.HasSuffix(d.Name(), ".docker.json") && !strings.HasSuffix(d.Name(), ".result.json") || d.Name() == "build-state.json" {
			b, e := os.ReadFile(path)
			if e != nil {
				return e
			}
			var st workerState
			var raw map[string]json.RawMessage
			if json.Unmarshal(b, &st) != nil || json.Unmarshal(b, &raw) != nil || (d.Name() == "state.json" || d.Name() == "operator-stop.json") && st.Session == "" {
				m.Blockers = append(m.Blockers, "worker_session")
				return nil
			}
			if (st.PID > 0 || st.LaunchPending) && st.Identity == "" {
				m.Blockers = append(m.Blockers, "worker_process_identity")
			}
			if st.Job != nil && (st.Job.ID == "" || st.Job.Attempt <= 0) {
				m.Blockers = append(m.Blockers, "worker_attempt")
			}
			if st.Result != nil && (st.Job == nil || st.Result.ID != st.Job.ID || st.Result.Attempt != st.Job.Attempt || st.Result.Session != st.Session || st.Result.Machine == "") {
				m.Blockers = append(m.Blockers, "worker_result_identity")
			}
			if st.Schema != 0 {
				m.Blockers = append(m.Blockers, "worker_schema")
			}
			raw["schema_version"] = json.RawMessage("3")
			b, e = json.Marshal(raw)
			if e != nil {
				return e
			}
			var current workerState
			if e = decodeWorkerState(b, &current); e != nil {
				m.Blockers = append(m.Blockers, "worker_format")
			}
			return atomicHostJSON(filepath.Dir(path), d.Name(), raw, nil)
		}
		if strings.HasSuffix(d.Name(), ".result.json") {
			b, e := os.ReadFile(path)
			if e != nil {
				return e
			}
			var r Result
			if json.Unmarshal(b, &r) != nil || r.ID == "" || r.Machine == "" || r.Session == "" || r.Attempt <= 0 {
				m.Blockers = append(m.Blockers, "report_identity")
			}
			m.Reports++
		}
		if strings.HasSuffix(d.Name(), ".docker.json") {
			b, e := os.ReadFile(path)
			if e != nil {
				return e
			}
			var st dockerExecution
			if json.Unmarshal(b, &st) != nil || st.Identity == "" || filepath.Base(st.Identity) != st.Identity || d.Name() != st.Identity+".docker.json" || st.Endpoint == "" || st.Owner == nil || st.Owner.ID == "" || st.Owner.Session == "" || st.Owner.Machine == "" || st.Owner.Attempt <= 0 {
				m.Blockers = append(m.Blockers, "opaque_daemon_owner")
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	// Checkpoint bytes are never edited. A nonempty attempt needs an explicit
	// positive generation; mtime and arbitrary save files cannot rank progress.
	err = filepath.WalkDir(filepath.Join(root, "checkpoints"), func(path string, d fs.DirEntry, e error) error {
		if errors.Is(e, os.ErrNotExist) {
			return nil
		}
		if e != nil {
			return e
		}
		if d.IsDir() && filepath.Dir(path) == filepath.Join(root, "checkpoints") {
			if _, e := scanCheckpointProgress(path, true); e != nil {
				m.Blockers = append(m.Blockers, "checkpoint_generation")
			}
		}
		if d.IsDir() && strings.HasPrefix(d.Name(), "attempt-") {
			_, e := checkpointProgress(path)
			if e != nil {
				m.Blockers = append(m.Blockers, "checkpoint_generation")
			}
			return filepath.SkipDir
		}
		return nil
	})
	if err != nil {
		return err
	}
	// Retired receipts are inert backup material, never current release proof.
	hostDir := filepath.Join(root, "host")
	if err = os.MkdirAll(hostDir, 0700); err != nil {
		return err
	}
	pending, registries, extraRetired, err := convertBundleHostWorkers(root, o, m)
	if err != nil {
		return err
	}
	paths, err := legacyHostPaths(hostDir)
	if err != nil {
		return err
	}
	paths = append(paths, extraRetired...)
	for _, path := range paths {
		rel, _ := filepath.Rel(hostDir, path)
		dst := filepath.Join(root, ".conversion", "retired-host", rel)
		if err = os.MkdirAll(filepath.Dir(dst), 0700); err != nil {
			return err
		}
		if err = os.Rename(path, dst); err != nil {
			return err
		}
	}
	revision := hosts[o.Host]
	if revision == 0 {
		revision = 1
	}
	now := o.Clock.Now()
	id := revisions[o.Host]
	if id.Revision == 0 {
		if lower == ^uint64(0) {
			return errors.New("revision exhausted")
		}
		revision = max(revision, lower+1)
		id = HostRevision{Revision: revision, Operation: ID()}
	}
	intent := HostIntent{Schema: hostStateVersion, Host: o.Host, HostRevision: id, Desired: "closed", Reason: "offline conversion; fresh reconciliation and explicit open required", AcceptedAt: now, StopDeadline: &now, PreviousClose: revision, LastClose: &HostClose{HostRevision: id, Settlement: "pending"}, Pending: []HostObligation{}, Holds: []string{"diagnostic:offline_conversion_release_unverified"}}
	if b, e := os.ReadFile(filepath.Join(hostDir, hostStateFile)); e == nil {
		var previous HostIntent
		if json.Unmarshal(b, &previous) != nil {
			return errPersistedFormat
		}
		intent.MemoryAllowanceBytes = previous.MemoryAllowanceBytes
		intent.Pending = previous.Pending
		intent.Holds = append(intent.Holds, previous.Holds...)
	}
	intent.Pending = mergeObligations(intent.Pending, pending)
	for _, w := range registries {
		if err = AdoptHostWorker(hostDir, w); err != nil {
			return err
		}
	}
	if err = validateHostIntent(intent); err != nil {
		return err
	}
	sort.Strings(m.Blockers)
	return atomicHostJSON(hostDir, hostStateFile, intent, nil)
}

func writePersistedFile(path string, b []byte) error {
	f, e := os.OpenFile(path, os.O_WRONLY|os.O_TRUNC, 0600)
	if e != nil {
		return e
	}
	_, e = f.Write(b)
	if e == nil {
		e = f.Chmod(0600)
	}
	if e == nil {
		e = f.Sync()
	}
	return errors.Join(e, f.Close())
}

// Rebind registry custody using the exact saved session, never an original
// absolute path. Old commands/configs remain inert until the installer registers
// current configuration. No path named by a source record is opened here.
func convertBundleHostWorkers(root string, o PersistedConversionOptions, m *PersistedConversionManifest) ([]HostObligation, map[string]HostWorker, []string, error) {
	hostDir := filepath.Join(root, "host")
	workers := map[string]HostWorker{}
	registries := map[string]HostWorker{}
	var pending []HostObligation
	var retired []string
	add := func(w HostWorker) {
		u, e := url.Parse(w.Queue)
		if w.Host != o.Host || w.Name == "" || w.Session == "" || e != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" {
			m.Blockers = append(m.Blockers, "host_worker_mapping")
			return
		}
		if old, ok := workers[w.Name]; ok && (old.Session != w.Session || old.Queue != w.Queue || old.State != w.State) {
			m.Blockers = append(m.Blockers, "host_worker_conflict")
			return
		}
		for _, r := range w.Executions {
			if r.Machine != w.Name || r.Session != w.Session || r.ID == "" || r.Attempt <= 0 {
				m.Blockers = append(m.Blockers, "host_execution_identity")
				continue
			}
			pending = mergeObligations(pending, []HostObligation{{Queue: w.Queue, Result: r}})
		}
		workers[w.Name] = w
	}
	err := filepath.WalkDir(hostDir, func(path string, d fs.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.IsDir() {
			if d.Name() == ".host-conversion" || d.Name() == ".Trash" {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasPrefix(d.Name(), "worker-") && strings.HasSuffix(d.Name(), ".json") {
			b, e := os.ReadFile(path)
			if e != nil {
				return e
			}
			var w HostWorker
			if json.Unmarshal(b, &w) != nil {
				return errors.New("malformed host worker")
			}
			add(w)
			if filepath.Dir(path) == hostDir {
				retired = append(retired, path)
			}
		}
		if d.Name() == "disable-receipt.json" || d.Name() == "enable-pending.json" {
			b, e := os.ReadFile(path)
			if e != nil {
				return e
			}
			var receipt hostReceipt
			if json.Unmarshal(b, &receipt) != nil {
				return errors.New("malformed host receipt")
			}
			if receipt.Generation == 0 || receipt.Workers == nil {
				m.Blockers = append(m.Blockers, "host_receipt_format")
			}
			for _, w := range receipt.Workers {
				add(w.HostWorker)
			}
		}
		return nil
	})
	if err != nil {
		return nil, nil, nil, err
	}
	for _, w := range workers {
		matches := []string{}
		for _, f := range m.Files {
			if !strings.HasPrefix(f.Path, "workers/") || filepath.Base(f.Path) != "state.json" {
				continue
			}
			b, e := os.ReadFile(filepath.Join(root, f.Path))
			if e != nil {
				return nil, nil, nil, e
			}
			var st workerState
			if decodeWorkerState(b, &st) == nil && st.Session == w.Session {
				matches = append(matches, filepath.Dir(f.Path))
				if st.Job != nil && st.Job.Machine != "" && st.Job.Machine != w.Name {
					m.Blockers = append(m.Blockers, "host_spool_identity")
				}
			}
		}
		if len(matches) != 1 {
			m.Blockers = append(m.Blockers, "host_spool_mapping")
			continue
		}
		w.State = filepath.Join(o.Target, matches[0])
		w.Config = ""
		w.Command = nil
		w.Environments = ""
		// Missing current installed config deliberately prevents a rest/ready PASS.
		// Installer/current worker registration must restore these exact bindings.
		registries[w.Name] = w
	}
	return pending, registries, retired, nil
}

// These are the exact retired fields written by 7285a88. Preserve their values
// as history before strict current decoding; they no longer control admission.
func retireReleaseFields(raw, history map[string]json.RawMessage, lower *uint64) error {
	for _, section := range []string{"machines", "hosts"} {
		if len(raw[section]) == 0 {
			continue
		}
		var records map[string]map[string]json.RawMessage
		if json.Unmarshal(raw[section], &records) != nil {
			return errors.New("invalid release state section")
		}
		retired := map[string]map[string]json.RawMessage{}
		for id, record := range records {
			old := map[string]json.RawMessage{}
			fields := []string{"enable_pending", "host_memory_budget_generation"}
			if section == "hosts" {
				fields = []string{"production_idle_since"}
			}
			for _, key := range fields {
				if b, ok := record[key]; ok {
					old[key] = b
					delete(record, key)
					if key == "host_memory_budget_generation" {
						var n uint64
						if json.Unmarshal(b, &n) != nil {
							return errors.New("invalid memory generation")
						}
						*lower = max(*lower, n)
					}
				}
			}
			if section == "hosts" && len(record["policy"]) > 0 {
				var policy map[string]json.RawMessage
				if json.Unmarshal(record["policy"], &policy) != nil {
					return errors.New("invalid release host policy")
				}
				if b, ok := policy["production_idle"]; ok {
					old["production_idle"] = b
					delete(policy, "production_idle")
					record["policy"], _ = json.Marshal(policy)
				}
			}
			if len(old) > 0 {
				retired[id] = old
			}
		}
		if len(retired) > 0 {
			history["retired_"+section+"_fields"], _ = json.Marshal(retired)
			raw[section], _ = json.Marshal(records)
		}
	}
	return nil
}
