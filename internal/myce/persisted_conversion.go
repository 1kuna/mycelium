package myce

// Offline format transforms operate only on the verified private working copy.
// install_conversion.go owns in-place publication and exact rollback.
import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const persistedSchema = 3

var errPersistedFormat = errors.New("unsupported persisted format: stop services and run myce host convert --local")

type persistedConversionOptions struct {
	Host   string
	Clock  Clock
	spools map[string]string
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

func convertPersistedTree(root string, o persistedConversionOptions, m *PersistedConversionManifest) error {
	o.Clock = conversionTimestamp{o.Clock.Now()}
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
	var err error
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
		rel, _ := filepath.Rel(root, path)
		if !conversionWorkerRecord(filepath.ToSlash(rel)) {
			return nil
		}
		if conversionWorkerState(d.Name()) {
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

// Preserve installed bindings and all saved execution sessions. Only the known
// bound spool determines the current session; source paths are never followed.
func convertBundleHostWorkers(root string, o persistedConversionOptions, m *PersistedConversionManifest) ([]HostObligation, map[string]HostWorker, []string, error) {
	hostDir := filepath.Join(root, "host")
	workers := map[string]HostWorker{}
	registries := map[string]HostWorker{}
	var pending []HostObligation
	var retired []string
	add := func(w HostWorker) {
		u, e := url.Parse(w.Queue)
		if w.Host != o.Host || w.Name == "" || e != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" {
			m.Blockers = append(m.Blockers, "host_worker_mapping")
			return
		}
		if old, ok := workers[w.Name]; ok && (old.Queue != w.Queue || old.State != w.State) {
			m.Blockers = append(m.Blockers, "host_worker_conflict")
			return
		}
		for _, r := range w.Executions {
			if r.Machine != w.Name || r.Session == "" || r.ID == "" || r.Attempt <= 0 {
				m.Blockers = append(m.Blockers, "host_execution_identity")
				continue
			}
			pending = mergeObligations(pending, []HostObligation{{Queue: w.Queue, Result: r}})
		}
		if old, ok := workers[w.Name]; ok {
			// Receipt snapshots may omit bindings; keep the registered installation.
			if w.Config == "" {
				w.Config = old.Config
			}
			if w.Unit == "" {
				w.Unit = old.Unit
			}
			if len(w.Command) == 0 {
				w.Command = old.Command
			}
			if w.Environments == "" {
				w.Environments = old.Environments
			}
			if w.Config != old.Config && old.Config != "" || w.Unit != old.Unit && old.Unit != "" {
				m.Blockers = append(m.Blockers, "host_worker_binding_conflict")
				return
			}
			if w.Session == "" {
				w.Session = old.Session
			}
			w.Executions = append(w.Executions, old.Executions...)
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
		// RC registerHost omitted Session. Its saved state path is authoritative;
		// a session-bearing receipt can disambiguate but never replace a binding.
		matches := []string{}
		for _, f := range m.Files {
			if !conversionWorkerRecord(f.Path) || filepath.Base(f.Path) != "state.json" {
				continue
			}
			b, e := os.ReadFile(filepath.Join(root, f.Path))
			if e != nil {
				return nil, nil, nil, e
			}
			var st workerState
			if decodeWorkerState(b, &st) != nil {
				continue
			}
			if f.Path == o.spools[w.State]+"/state.json" || o.spools == nil && (w.Session == "" || st.Session == w.Session) {
				// Receipts can describe a previous execution session. Keep that
				// exact obligation; the bound spool supplies the current session.
				matches = append(matches, st.Session)
				if st.Job != nil {
					pending = mergeObligations(pending, []HostObligation{{Queue: w.Queue, Result: Result{Machine: w.Name, Session: st.Session, Execution: Execution{ID: st.Job.ID, Attempt: st.Job.Attempt}}}})
				}
			}
		}
		if len(matches) != 1 {
			m.Blockers = append(m.Blockers, "host_spool_mapping")
			continue
		}
		w.Session = matches[0]
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

// One conversion revision has one accepted timestamp in both local and queue state.
type conversionTimestamp struct{ at time.Time }

func (c conversionTimestamp) Now() time.Time { return c.at }

// Runtime custody is stored directly in each spool, or at the explicitly
// inventoried environment build paths. Nested checkpoint/job payloads are opaque.
func conversionWorkerRecord(rel string) bool {
	parts := strings.Split(rel, "/")
	return len(parts) == 3 && parts[0] == "workers" || len(parts) > 3 && strings.HasPrefix(parts[1], "env-") && filepath.Base(rel) == "build-state.json"
}

func conversionWorkerState(name string) bool {
	return name == "state.json" || name == "operator-stop.json" || strings.HasPrefix(name, "engine-") && strings.HasSuffix(name, ".json") && !strings.HasSuffix(name, ".docker.json") || strings.HasPrefix(name, "attempt-") && strings.HasSuffix(name, ".json") && !strings.HasSuffix(name, ".docker.json") && !strings.HasSuffix(name, ".result.json") || name == "build-state.json"
}
