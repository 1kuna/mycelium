package myce

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"
)

type localConversionFixture struct {
	o             LocalConversionOptions
	queue, worker ServiceConfig
	home          string
}

func localReleaseInstall(t *testing.T, idle bool) localConversionFixture {
	return localReleaseInstallKind(t, idle, "release-7285a88")
}
func localReleaseInstallKind(t *testing.T, idle bool, kind string) localConversionFixture {
	t.Helper()
	home, e := filepath.EvalSymlinks(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	d := Detection{OS: "linux", Arch: "arm64", Platform: "fake", MemoryMode: "fixed", MemoryGB: 8}
	q, e := PlanInstall("queue", home, "", home, "http://fixture-queue.invalid", "fixture-queue", d)
	if e != nil {
		t.Fatal(e)
	}
	w, e := PlanInstall("worker", home, "", home, q.Queue, "fixture-worker", d)
	if e != nil {
		t.Fatal(e)
	}
	if e = w.WorkerInstance("gpu0"); e != nil {
		t.Fatal(e)
	}
	for _, c := range []*ServiceConfig{&q, &w} {
		c.HostID = "fixture-host"
		c.Version = 0
		c.AllowDeviceless = true
	}
	if idle {
		kind = "release-7285a88-idle"
	}
	source := releaseFixtureKind(t, kind)
	for _, pair := range [][2]string{{"queue.db", q.DB}, {"queue.db.lock", q.DB + ".lock"}, {"queue.db.uploads.lock", q.DB + ".uploads.lock"}} {
		if e = copyConversionOriginal(filepath.Join(source, pair[0]), pair[1], 0600); e != nil {
			t.Fatal(e)
		}
	}
	for _, pair := range [][2]string{{"workers/one", w.State}, {"host", w.HostStateDir}, {"checkpoints", w.Checkpoints}} {
		if e = filepath.WalkDir(filepath.Join(source, pair[0]), func(p string, d fs.DirEntry, e error) error {
			if e != nil {
				return e
			}
			rel, _ := filepath.Rel(filepath.Join(source, pair[0]), p)
			dest := filepath.Join(pair[1], rel)
			if d.IsDir() {
				return os.MkdirAll(dest, 0700)
			}
			return copyConversionOriginal(p, dest, 0600)
		}); e != nil {
			t.Fatal(e)
		}
	}
	// Bind known synthetic placeholders to a temp HOME, without adding fields to
	// the actual RC writer's JSON (in particular its missing registry Session).
	e = filepath.WalkDir(w.HostStateDir, func(p string, d fs.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.IsDir() {
			return nil
		}
		b, e := os.ReadFile(p)
		if e != nil {
			return e
		}
		for from, to := range map[string]string{"/fixture/worker-state": w.State, "/fixture/config.json": w.Config, "/fixture/environment-cache": w.EnvironmentCache} {
			b = bytes.ReplaceAll(b, []byte(from), []byte(to))
		}
		return os.WriteFile(p, b, 0600)
	})
	if e != nil {
		t.Fatal(e)
	}
	for _, c := range []ServiceConfig{q, w} {
		for _, p := range []string{c.Config, c.Binary, c.Credentials, c.Service} {
			if e = os.MkdirAll(filepath.Dir(p), 0700); e != nil {
				t.Fatal(e)
			}
		}
		b, _ := json.Marshal(c)
		if e = os.WriteFile(c.Config, b, 0600); e != nil {
			t.Fatal(e)
		}
		for p, b := range map[string][]byte{c.Binary: []byte("old synthetic executable"), c.Credentials: []byte(`{"token":"test"}`), c.Service: []byte("old synthetic unit")} {
			if e = os.WriteFile(p, b, 0600); e != nil {
				t.Fatal(e)
			}
		}
	}
	if e = os.WriteFile(filepath.Join(w.State, "worker.lock"), nil, 0600); e != nil {
		t.Fatal(e)
	}
	probe := MachineProbe{OS: "linux", Arch: "arm64", Command: func(context.Context, string, ...string) ([]byte, error) { return []byte("loaded\ninactive\n"), nil }}
	return localConversionFixture{LocalConversionOptions{Home: home, Clock: RealClock{}, Probe: probe}, q, w, home}
}
func fixtureActiveHashes(t *testing.T, f localConversionFixture) map[string]string {
	t.Helper()
	out := map[string]string{}
	if e := filepath.WalkDir(f.home, func(p string, d fs.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.IsDir() {
			if d.Name() == ".host-conversion" {
				return filepath.SkipDir
			}
			return nil
		}
		h, e := conversionFileHash(p)
		out[p] = h
		return e
	}); e != nil {
		t.Fatal(e)
	}
	return out
}

func TestLocalConversionMatrix(t *testing.T) {
	for _, idle := range []bool{true, false} {
		t.Run(map[bool]string{true: "idle", false: "custody"}[idle], func(t *testing.T) {
			f := localReleaseInstall(t, idle)
			before := fixtureActiveHashes(t, f)
			raw, tables := readFixtureDB(t, f.queue.DB)
			f.o.DryRun = true
			r, e := ConvertLocalInstallation(context.Background(), f.o)
			if e != nil {
				t.Fatal(e)
			}
			wantCopies := 206
			if idle {
				wantCopies = 0
			}
			if r.Jobs != 103 || r.Copies != wantCopies {
				t.Fatal(r)
			}
			if !reflect.DeepEqual(before, fixtureActiveHashes(t, f)) {
				t.Fatal("dry-run wrote installation")
			}
			f.o.DryRun = false
			r, e = ConvertLocalInstallation(context.Background(), f.o)
			if e != nil {
				t.Fatal(e, r.Blockers)
			}
			current, afterTables := readFixtureDB(t, f.queue.DB)
			if !reflect.DeepEqual(tables, afterTables) {
				t.Fatal("logs/usage/auxiliary tables changed")
			}
			var history map[string]json.RawMessage
			json.Unmarshal(current["conversion_history"], &history)
			for _, key := range []string{"jobs", "model_slots", "host_releases", "pins"} {
				var a, b any
				json.Unmarshal(raw[key], &a)
				json.Unmarshal(current[key], &b)
				if !reflect.DeepEqual(a, b) {
					t.Fatal("lost custody or job", key)
				}
			}
			s, e := ReadHostIntent(f.worker.HostStateDir)
			if e != nil {
				t.Fatal(e)
			}
			settlement := "pending"
			if idle {
				settlement = "confirmed"
			}
			if s.Desired != "closed" || s.HostSettlement() != settlement {
				t.Fatal(s)
			}
			ws, e := HostWorkers(f.worker.HostStateDir)
			if e != nil || len(ws) != 1 {
				t.Fatal(ws, e)
			}
			if ws[0].Session != "fixture-session" || ws[0].State != f.worker.State || ws[0].Config != f.worker.Config || ws[0].Unit != "myce-worker@gpu0.service" || ws[0].Environments != f.worker.EnvironmentCache || len(ws[0].Command) == 0 {
				t.Fatal("bindings lost", ws)
			}
			if !idle {
				if _, e = changeHostIntent(f.worker.HostStateDir, f.o.Clock, hostIntentChange{Desired: "open", Expected: s.HostRevision}); !errors.Is(e, ErrPreviousCloseUnsettled) {
					t.Fatal("unsettled open accepted", e)
				}
			}
			after := fixtureActiveHashes(t, f)
			again, e := ConvertLocalInstallation(context.Background(), f.o)
			if e != nil || again.Phase != "unchanged" || !reflect.DeepEqual(after, fixtureActiveHashes(t, f)) {
				t.Fatal("rerun changed state", again, e)
			}
		})
	}
}

func TestLocalConversionCheckpointPayloadUntouched(t *testing.T) {
	f := localReleaseInstall(t, false)
	p := filepath.Join(f.worker.Checkpoints, "job-102", "attempt-2")
	for n, b := range map[string]string{"state.json": `{"user":"payload"}`, "credentials.json": `{"token":"synthetic"}`, "config.json": `{"queue":"http://fixture-queue.invalid"}`} {
		if e := os.WriteFile(filepath.Join(p, n), []byte(b), 0600); e != nil {
			t.Fatal(e)
		}
	}
	payload := filepath.Join(f.worker.State, "checkpoints", "job", "attempt-2")
	if e := os.MkdirAll(payload, 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(payload, "state.json"), []byte(`{"user":"payload"}`), 0600); e != nil {
		t.Fatal(e)
	}
	before := sourceHashes(t, f.worker.Checkpoints)
	if _, e := ConvertLocalInstallation(context.Background(), f.o); e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(before, sourceHashes(t, f.worker.Checkpoints)) {
		t.Fatal("checkpoint payload was rewritten")
	}
	b, e := os.ReadFile(filepath.Join(payload, "state.json"))
	if e != nil || string(b) != `{"user":"payload"}` {
		t.Fatal("spool checkpoint rewritten", e)
	}
}

func TestLocalConversionFailureAndCrashRecovery(t *testing.T) {
	// Discover every publication step, then fail each one on fresh release input.
	f := localReleaseInstall(t, false)
	phases := []string{}
	f.o.boundary = func(p string) error { phases = append(phases, p); return nil }
	if _, e := ConvertLocalInstallation(context.Background(), f.o); e != nil {
		t.Fatal(e)
	}
	for _, phase := range phases {
		t.Run(phase, func(t *testing.T) {
			f := localReleaseInstall(t, false)
			before := fixtureActiveHashes(t, f)
			f.o.boundary = func(p string) error {
				if p == phase {
					return errors.New("injected failure")
				}
				return nil
			}
			r, e := ConvertLocalInstallation(context.Background(), f.o)
			if e == nil {
				t.Fatal("fault not fired")
			}
			if !reflect.DeepEqual(before, fixtureActiveHashes(t, f)) {
				t.Fatal("rollback did not restore exact source")
			}
			var j installConversionJournal
			j.Root = r.Backup
			l, e := readConversionLayout(f.o)
			if e != nil {
				t.Fatal(e)
			}
			j.Files = l.files
			if e = verifyInstallBackup(j); e != nil {
				t.Fatal(e)
			}
		})
	}
	// A killed process cannot run defers: exercise every boundary independently.
	for _, phase := range phases {
		t.Run("crash/"+phase, func(t *testing.T) {
			f := localReleaseInstall(t, false)
			before := fixtureActiveHashes(t, f)
			cmd := exec.Command(os.Args[0], "-test.run=^TestLocalConversionCrashChild$")
			cmd.Env = append(os.Environ(), "MYCE_CONVERSION_TEST_HOME="+f.home, "MYCE_CONVERSION_TEST_PHASE="+phase)
			if e := cmd.Run(); e == nil {
				t.Fatal("child did not crash")
			}
			marker := filepath.Join(f.worker.HostStateDir, ".host-conversion", "IN_PROGRESS")
			if _, e := os.Stat(marker); e == nil {
				if _, e = ReadHostIntent(f.worker.HostStateDir); e == nil {
					t.Fatal("partial publication readable")
				}
				r, e := ConvertLocalInstallation(context.Background(), f.o)
				if e != nil || r.Phase != "restored" {
					t.Fatal(r, e)
				}
			}
			if !reflect.DeepEqual(before, fixtureActiveHashes(t, f)) {
				t.Fatal("crash recovery changed originals")
			}
			if _, e := ConvertLocalInstallation(context.Background(), f.o); e != nil {
				t.Fatal("retry", e)
			}
		})
	}

}
func TestLocalConversionCrashChild(t *testing.T) {
	if home := os.Getenv("MYCE_CONVERSION_TEST_HOME"); home != "" {
		p := MachineProbe{OS: "linux", Command: func(context.Context, string, ...string) ([]byte, error) { return []byte("loaded\ninactive\n"), nil }}
		_, _ = ConvertLocalInstallation(context.Background(), LocalConversionOptions{Home: home, Probe: p, Clock: RealClock{}, boundary: func(s string) error {
			if s == os.Getenv("MYCE_CONVERSION_TEST_PHASE") {
				os.Exit(77)
			}
			return nil
		}})
		os.Exit(78)
	}
}

func TestLocalConversionRefusesWriters(t *testing.T) {
	for _, kind := range []string{"unit", "queue-lock", "worker-lock", "sqlite"} {
		t.Run(kind, func(t *testing.T) {
			f := localReleaseInstall(t, false)
			before := fixtureActiveHashes(t, f)
			switch kind {
			case "unit":
				f.o.Probe.Command = func(context.Context, string, ...string) ([]byte, error) { return []byte("loaded\nactive\n"), nil }
			case "queue-lock", "worker-lock":
				p := f.queue.DB + ".lock"
				if kind == "worker-lock" {
					p = filepath.Join(f.worker.State, "worker.lock")
				}
				lock, e := os.OpenFile(p, os.O_RDWR, 0)
				if e != nil {
					t.Fatal(e)
				}
				defer lock.Close()
				if e = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
					t.Fatal(e)
				}
			case "sqlite":
				cmd := exec.Command(os.Args[0], "-test.run=^TestLocalConversionSQLiteWriterChild$")
				cmd.Env = append(os.Environ(), "MYCE_SQLITE_WRITER_TEST_DB="+f.queue.DB)
				stdout, e := cmd.StdoutPipe()
				if e != nil {
					t.Fatal(e)
				}
				if e = cmd.Start(); e != nil {
					t.Fatal(e)
				}
				defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
				b := make([]byte, 1)
				if _, e = stdout.Read(b); e != nil || b[0] != 'R' {
					t.Fatal("writer not ready", e)
				}
			}
			for _, dry := range []bool{true, false} {
				f.o.DryRun = dry
				if _, e := ConvertLocalInstallation(context.Background(), f.o); e == nil {
					t.Fatal("active writer accepted", dry)
				}
			}
			if kind != "sqlite" && !reflect.DeepEqual(before, fixtureActiveHashes(t, f)) {
				t.Fatal("refusal changed files")
			}
		})
	}
}
func TestLocalConversionSQLiteWriterChild(t *testing.T) {
	if p := os.Getenv("MYCE_SQLITE_WRITER_TEST_DB"); p != "" {
		db, e := sql.Open("sqlite", p)
		if e != nil {
			os.Exit(81)
		}
		if _, e = db.Exec("BEGIN IMMEDIATE"); e != nil {
			os.Exit(82)
		}
		os.Stdout.Write([]byte("R"))
		select {}
	}
}

func TestLocalConversionUpgradeAndClosedReconcile(t *testing.T) {
	for _, idle := range []bool{true, false} {
		t.Run(map[bool]string{true: "idle", false: "custody"}[idle], func(t *testing.T) {
			f := localReleaseInstallKind(t, idle, "release-7285a88-native")
			if _, e := ConvertLocalInstallation(context.Background(), f.o); e != nil {
				t.Fatal(e)
			}
			source := filepath.Join(f.home, "new-binary")
			if e := os.WriteFile(source, []byte("new synthetic executable"), 0700); e != nil {
				t.Fatal(e)
			}
			manager := &installMatrixManager{t: t, states: map[string]installUnitState{}, cached: map[string][]byte{}}
			// Both legacy instance names remain unchanged on preserving upgrades.
			for _, c := range []ServiceConfig{f.worker, f.queue} {
				manager.c = c
				if e := Install(context.Background(), c, source, "test", manager.run); e != nil {
					t.Fatal("preserving upgrade", e)
				}
			}
			ws, e := HostWorkers(f.worker.HostStateDir)
			if e != nil || len(ws) != 1 || ws[0].Config != f.worker.Config {
				t.Fatal(ws, e)
			}
			// Starting a CLOSED worker registers its existing session then exits fenced.
			if e = RegisterInstalledWorker(f.worker); e != nil {
				t.Fatal(e)
			}
			q, e := Open(f.queue.DB, f.worker.Checkpoints, RealClock{})
			if e != nil {
				t.Fatal(e)
			}
			defer q.Close()
			server := httptest.NewServer(API{q, "test"}.Handler())
			defer server.Close()
			client := Client{URL: f.queue.Queue, Token: "test", HTTP: &http.Client{Transport: conversionTestTransport{server.URL}}}
			q.localHostQueue = f.queue.Queue
			if e = q.ConfigureLocalHost(f.worker.HostStateDir, f.queue.Queue); e != nil {
				t.Fatal(e)
			}
			s, e := ReadHostIntent(f.worker.HostStateDir)
			if e != nil {
				t.Fatal(e)
			}
			m := machine("fixture-worker", "fake")
			m.HostID = "fixture-host"
			m.Session = "fixture-session"
			if reply := check(t, q, m, nil); reply.Job != nil || !reply.Leave {
				t.Fatal("CLOSED worker admitted work")
			}
			if !idle {
				if _, e = changeHostIntent(f.worker.HostStateDir, RealClock{}, hostIntentChange{Desired: "open", Expected: s.HostRevision}); !errors.Is(e, ErrPreviousCloseUnsettled) {
					t.Fatal(e)
				}
				// Fake service manager reports empty native/engine groups. Real cleanup
				// and reconciliation consume exact saved custody.
				h := HostControl{Dir: f.worker.HostStateDir, Clock: RealClock{}, Service: &fakeHostService{}, Observe: testHostExclusion, Processes: HostProcesses{Owned: func(workerState) (bool, error) { return false, nil }, Signal: func(workerState, syscall.Signal) error { return nil }, Pause: func(time.Duration) {}}}
				if report, e := h.Disable(context.Background(), "converted CLOSED", 0); e != nil || !report.Confirmed {
					t.Fatal(report, e)
				}
				if e = ReconcileHostLocal(context.Background(), f.worker.HostStateDir, RealClock{}, func(string) (Client, error) { return client, nil }); e != nil {
					t.Fatal(e)
				}
			}
			s, e = ReadHostIntent(f.worker.HostStateDir)
			if e != nil || s.HostSettlement() != "confirmed" {
				t.Fatal("not settled", s.HostSettlement(), len(s.Pending), e)
			}
			v := HostInvocation{StateDir: f.worker.HostStateDir, Config: f.worker, ConfigPath: f.worker.Config}
			envelope, e := HostSnapshot(context.Background(), v, RealClock{})
			if e != nil || envelope.Desired.State == nil || *envelope.Desired.State != "closed" {
				t.Fatal(envelope, e)
			}
			opened, e := changeHostIntent(f.worker.HostStateDir, RealClock{}, hostIntentChange{Desired: "open", Expected: s.HostRevision})
			if e != nil {
				t.Fatal(e)
			}
			if e = ReconcileHostLocal(context.Background(), f.worker.HostStateDir, RealClock{}, func(string) (Client, error) { return client, nil }); e != nil {
				t.Fatal(e)
			}
			m.HostRevision = opened.HostRevision
			m.Session = "after-open-session"
			if reply, err := q.Checkin(Checkin{Machine: m, Fresh: true, PreviousSession: "fixture-session"}); err != nil || reply.Job == nil {
				t.Fatal("open did not admit work", reply, err)
			}
		})
	}
}

func TestLocalConversionWAL(t *testing.T) {
	f := localReleaseInstall(t, false)
	db, e := sql.Open("sqlite", f.queue.DB)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = db.Exec("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; UPDATE queue SET data=json_set(data,'$.jobs.waljob',json_extract(data,'$.jobs.job-000')); UPDATE queue SET data=json_set(data,'$.jobs.waljob.id','waljob'); INSERT INTO logs(job,attempt,text) VALUES('job-102',2,'wal-only')"); e != nil {
		t.Fatal(e)
	}
	snapshots := map[string][]byte{}
	for _, s := range []string{"", "-wal", "-shm"} {
		b, e := os.ReadFile(f.queue.DB + s)
		if e != nil {
			t.Fatal(e)
		}
		snapshots[s] = b
	}
	if e = db.Close(); e != nil {
		t.Fatal(e)
	}
	for suffix, b := range snapshots {
		if e = os.WriteFile(f.queue.DB+suffix, b, 0600); e != nil {
			t.Fatal(e)
		}
	}
	before := fixtureActiveHashes(t, f)
	f.o.DryRun = true
	r, e := ConvertLocalInstallation(context.Background(), f.o)
	if e != nil || r.Jobs != 104 {
		t.Fatal("dry WAL count", r.Jobs, e)
	}
	if !reflect.DeepEqual(before, fixtureActiveHashes(t, f)) {
		t.Fatal("dry run modified WAL/SHM")
	}
	f.o.DryRun = false
	r, e = ConvertLocalInstallation(context.Background(), f.o)
	if e != nil || r.Jobs != 104 {
		t.Fatal("lost WAL", r, e)
	}
	raw, tables := readFixtureDB(t, f.queue.DB)
	var jobs map[string]Job
	json.Unmarshal(raw["jobs"], &jobs)
	if jobs["waljob"].ID != "waljob" || len(tables["logs"]) != 2 {
		t.Fatal("WAL records missing")
	}
}

// Route synthetic endpoints without changing their durable identity.
type conversionTestTransport struct{ target string }

func (t conversionTestTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	u, _ := url.Parse(t.target)
	r.URL.Scheme, r.URL.Host = u.Scheme, u.Host
	return http.DefaultTransport.RoundTrip(r)
}

func TestLocalConversionTemplateUnits(t *testing.T) {
	f := localReleaseInstall(t, false)
	second := f.worker
	if e := second.WorkerInstance("gpu1"); e != nil {
		t.Fatal(e)
	}
	second.Name = "fixture-worker-two"
	for _, path := range []string{second.Config, second.Credentials, second.Service, second.Binary} {
		if e := os.MkdirAll(filepath.Dir(path), 0700); e != nil {
			t.Fatal(e)
		}
	}
	b, _ := json.Marshal(second)
	if e := os.WriteFile(second.Config, b, 0600); e != nil {
		t.Fatal(e)
	}
	for _, pair := range [][2]string{{f.worker.Credentials, second.Credentials}, {f.worker.Service, second.Service}, {f.worker.Binary, second.Binary}} {
		if e := copyConversionOriginal(pair[0], pair[1], 0600); e != nil {
			t.Fatal(e)
		}
	}
	replace := func(b []byte) []byte {
		for a, z := range map[string]string{f.worker.State: second.State, f.worker.Config: second.Config, f.worker.EnvironmentCache: second.EnvironmentCache, f.worker.Name: second.Name, "myce-worker@gpu0.service": "myce-worker@gpu1.service"} {
			b = bytes.ReplaceAll(b, []byte(a), []byte(z))
		}
		return b
	}
	if e := filepath.WalkDir(f.worker.State, func(p string, d fs.DirEntry, e error) error {
		if e != nil {
			return e
		}
		rel, _ := filepath.Rel(f.worker.State, p)
		to := filepath.Join(second.State, rel)
		if d.IsDir() {
			return os.MkdirAll(to, 0700)
		}
		b, e := os.ReadFile(p)
		if e != nil {
			return e
		}
		return os.WriteFile(to, replace(b), 0600)
	}); e != nil {
		t.Fatal(e)
	}
	registry, _ := filepath.Glob(filepath.Join(f.worker.HostStateDir, "worker-*.json"))
	b, e := os.ReadFile(registry[0])
	if e != nil {
		t.Fatal(e)
	}
	// Both actual RC registry shapes are discovered inside retired per-unit trees.
	for _, unit := range []string{"myce-worker@gpu0.service", "myce-worker@gpu1.service"} {
		dir := filepath.Join(f.worker.HostStateDir, "units", unit)
		if e = os.MkdirAll(dir, 0750); e != nil {
			t.Fatal(e)
		}
		data := b
		if strings.Contains(unit, "gpu1") {
			data = replace(b)
		}
		if e = os.WriteFile(filepath.Join(dir, "worker-saved.json"), data, 0600); e != nil {
			t.Fatal(e)
		}
	}
	before := fixtureActiveHashes(t, f)
	f.o.boundary = func(p string) error {
		if p == "published" {
			return errors.New("rollback legacy directories")
		}
		return nil
	}
	if _, e = ConvertLocalInstallation(context.Background(), f.o); e == nil {
		t.Fatal("fault not fired")
	}
	if !reflect.DeepEqual(before, fixtureActiveHashes(t, f)) {
		t.Fatal("legacy rollback changed bytes")
	}
	f.o.boundary = nil
	if _, e = ConvertLocalInstallation(context.Background(), f.o); e != nil {
		t.Fatal(e)
	}
	ws, e := HostWorkers(f.worker.HostStateDir)
	if e != nil || len(ws) != 2 {
		t.Fatal(ws, e)
	}
	source := filepath.Join(f.home, "upgrade")
	if e = os.WriteFile(source, []byte("synthetic binary"), 0700); e != nil {
		t.Fatal(e)
	}
	manager := &installMatrixManager{t: t, states: map[string]installUnitState{}, cached: map[string][]byte{}}
	for _, c := range []ServiceConfig{f.worker, second} {
		manager.c = c
		if e = Install(context.Background(), c, source, "test", manager.run); e != nil {
			t.Fatal(e)
		}
		if e = RegisterInstalledWorker(c); e != nil {
			t.Fatal(e)
		}
	}
	units, e := WorkerUnits(f.worker.HostStateDir, "")
	if e != nil || !reflect.DeepEqual(units, []string{"myce-worker@gpu0.service", "myce-worker@gpu1.service"}) {
		t.Fatal(units, e)
	}
}

func TestLocalConversionPreservesDifferentReceiptSession(t *testing.T) {
	f := localReleaseInstall(t, false)
	path := filepath.Join(f.worker.HostStateDir, "enable-pending.json")
	b, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	// A prior session in a saved receipt is historical custody, not a reason to
	// overwrite the current bound spool's session or refuse the installation.
	b = bytes.ReplaceAll(b, []byte("fixture-session"), []byte("prior-session"))
	if e = os.WriteFile(path, b, 0600); e != nil {
		t.Fatal(e)
	}
	if _, e = ConvertLocalInstallation(context.Background(), f.o); e != nil {
		t.Fatal(e)
	}
	ws, e := HostWorkers(f.worker.HostStateDir)
	if e != nil || len(ws) != 1 || ws[0].Session != "fixture-session" {
		t.Fatal(ws, e)
	}
	sessions := map[string]bool{}
	for _, r := range ws[0].Executions {
		sessions[r.Session] = true
	}
	if !sessions["prior-session"] || !sessions["fixture-session"] {
		t.Fatal("execution sessions lost", sessions)
	}
	intent, e := ReadHostIntent(f.worker.HostStateDir)
	if e != nil || intent.HostSettlement() != "pending" {
		t.Fatal(intent, e)
	}
}

func TestLocalConversionTamperedBackupRemainsFenced(t *testing.T) {
	f := localReleaseInstall(t, false)
	cmd := exec.Command(os.Args[0], "-test.run=^TestLocalConversionCrashChild$")
	cmd.Env = append(os.Environ(), "MYCE_CONVERSION_TEST_HOME="+f.home, "MYCE_CONVERSION_TEST_PHASE=published")
	if e := cmd.Run(); e == nil {
		t.Fatal("child did not crash")
	}
	marker := filepath.Join(f.worker.HostStateDir, ".host-conversion", "IN_PROGRESS")
	b, e := os.ReadFile(marker)
	if e != nil {
		t.Fatal(e)
	}
	var j installConversionJournal
	if e = json.Unmarshal(b, &j); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(j.Root, "originals", j.Files[0].File.Path), []byte("damaged"), 0600); e != nil {
		t.Fatal(e)
	}
	if _, e = ConvertLocalInstallation(context.Background(), f.o); e == nil {
		t.Fatal("damaged backup restored")
	}
	if _, e = ReadHostIntent(f.worker.HostStateDir); e == nil {
		t.Fatal("damaged backup unfenced installation")
	}
}
