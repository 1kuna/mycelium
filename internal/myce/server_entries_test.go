package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestEntryPlacementSharedCallsPinsAndFenceChange(t *testing.T) {
	q, clock, a := modelFixture(t)
	q.models.Models[0].Project = "owner"
	q.models.Models[0].AllowedMachines = []string{"one", "two"}
	a.ModelConfigHash = modelConfigHash(q.models)
	b := a
	b.ID, b.Session = "two", "two-session"
	if err := q.ConfigureProjects(map[string]ProjectPolicy{"owner": {Weight: 1, AllowedMachines: []string{"one"}}}); err != nil {
		t.Fatal(err)
	}
	check(t, q, a, nil)
	check(t, q, b, nil)
	call := &ModelCall{Model: "test", Project: "another-project", Priority: "experiment"}
	if err := q.addCall(call); err != nil {
		t.Fatal(err)
	}
	if q.s.ModelSlots[slotKey(a.ID, "test")] == nil || q.s.ModelSlots[slotKey(b.ID, "test")] != nil {
		t.Fatal("entry/owner fences not intersected")
	}
	reportModel(t, q, a, "ready")
	if call.Machine != a.ID {
		t.Fatal("shared model not callable by another project")
	}
	if err := q.finishCall(call.ID); err != nil {
		t.Fatal(err)
	}
	if err := q.Pin(ModelPin{Project: "another-project", Model: "test", Platform: "fake"}); err != nil {
		t.Fatal(err)
	}
	if q.s.Pins[0].Assigned != a.ID {
		t.Fatal("pin escaped owner fence")
	}
	if err := q.SyncModel(SyncRequest{Model: "test", To: "all"}); err != nil {
		t.Fatal(err)
	}
	if plan, _, pins := q.copyPlan(&b); len(plan) > 0 || len(pins) > 0 {
		t.Fatal("precopy escaped owner fence")
	}
	// Policy changes must drain existing copies, even with a pin; elapsed time
	// alone does not release their memory or permit reuse.
	if err := q.ConfigureProjects(map[string]ProjectPolicy{"owner": {Weight: 1, AllowedMachines: []string{"two"}}}); err != nil {
		t.Fatal(err)
	}
	q.Tick()
	slot := q.s.ModelSlots[slotKey(a.ID, "test")]
	if slot.State != "draining" || q.modelMemory(a.ID) != 8 {
		t.Fatal("fence change lost drain/memory custody")
	}
	clock.Advance(time.Second)
	q.Tick()
	if q.s.ModelSlots[slotKey(a.ID, "test")] == nil {
		t.Fatal("clock released unacknowledged copy")
	}
	plan := q.modelPlan(a.ID)
	if len(plan) != 1 || plan[0].Placement.Project != "owner" || !contains(plan[0].Placement.ProjectMachines, "two") {
		t.Fatal("current placement not snapshotted")
	}
	next := &ModelCall{Model: "test", Project: "third-project", Priority: "production"}
	if err := q.addCall(next); err != nil {
		t.Fatal(err)
	}
	if next.Machine == a.ID {
		t.Fatal("excluded ready copy reused")
	}
	reportModel(t, q, a, "unloaded")
	if q.modelMemory(a.ID) != 0 {
		t.Fatal("confirmed release not accounted")
	}
}

func TestEntryPlacementWorkerAndLocalIdentity(t *testing.T) {
	model := Model{Name: "test", Platforms: []string{"fake"}, AllowedMachines: []string{"eligible"}, LocalSource: "approved", SHA256: strings.Repeat("a", 64)}
	m := Machine{ID: "eligible", GPU: "fake"}
	if modelFitsMachine(model, &m) {
		t.Fatal("unapproved source eligible")
	}
	m.ApprovedModels = map[string]string{"approved": model.SHA256}
	m.ModelApprovalStates = map[string]ModelApprovalState{"approved": approvalVerified}
	if !modelFitsMachine(model, &m) {
		t.Fatal("approved source ineligible")
	}
	m.ID = "excluded"
	if modelFitsMachine(model, &m) {
		t.Fatal("entry fence ignored")
	}
	rt := &modelRuntime{models: map[string]*workerModel{}, config: ModelConfig{Models: []Model{model}}, session: "session", machine: "excluded"}
	rt.sync([]ModelSlot{{Attempt: "test-engine", Model: "test", State: "loading", Session: "session", Placement: &ModelPlacement{AllowedMachines: []string{"eligible"}}}})
	if rt.models["test"].state != "failed" {
		t.Fatal("worker accepted excluded load")
	}
}

func TestEntryHeaderlessDefaultsAndAuthentication(t *testing.T) {
	q, _, m := modelFixture(t)
	q.models.Models[0].Project = "owner"
	m.ModelConfigHash = modelConfigHash(q.models)
	var seenProject, seenPriority string
	worker := workerTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q.mu.Lock()
		for _, c := range q.calls {
			seenProject, seenPriority = c.Project, c.Priority
		}
		q.mu.Unlock()
		fmt.Fprint(w, `{"choices":[]}`)
	}))
	defer worker.Close()
	m.ModelURL = worker.URL
	check(t, q, m, nil)
	q.s.ModelSlots[slotKey(m.ID, "test")] = &ModelSlot{Model: "test", Machine: m.ID, Session: m.Session, State: "ready", LastUsed: q.clock.Now()}
	api := API{Queue: q, Token: "test-token"}.Handler()
	request := func(headers http.Header, auth bool) int {
		req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"test"}`))
		req.Header = headers
		if auth {
			req.Header.Set("Authorization", "Bearer test-token")
		}
		out := httptest.NewRecorder()
		api.ServeHTTP(out, req)
		return out.Code
	}
	if code := request(http.Header{}, false); code != 401 {
		t.Fatal("fleet auth bypass", code)
	}
	if code := request(http.Header{}, true); code != 200 || seenProject != "owner" || seenPriority != "experiment" {
		t.Fatal("unsafe headerless defaults", code, seenProject, seenPriority)
	}
	if err := q.ConfigureProjects(map[string]ProjectPolicy{"owner": {Weight: 1, DefaultCallPriority: "production"}}); err != nil {
		t.Fatal(err)
	}
	if code := request(http.Header{}, true); code != 200 || seenPriority != "production" {
		t.Fatal("explicit production default not used", code)
	}
	h := http.Header{}
	h.Set("X-Myce-Project", "other")
	h.Set("X-Myce-Priority", "experiment")
	if code := request(h, true); code != 200 || seenProject != "other" {
		t.Fatal("entry made exclusive", code)
	}
	if err := q.ConfigureProjects(map[string]ProjectPolicy{"owner": {Weight: 1}, "other": {Weight: 1, DefaultCallPriority: "production"}}); err != nil {
		t.Fatal(err)
	}
	h.Set("X-Myce-Project", " other ")
	h.Del("X-Myce-Priority")
	if code := request(h, true); code != 200 || seenProject != "other" || seenPriority != "production" {
		t.Fatal("caller's production default not applied on another owner's entry", code)
	}
	h.Set("X-Myce-Priority", "experiment")
	h.Set("X-Myce-Deadline", "ignored-invalid-deadline")
	if code := request(h, true); code != 200 || seenPriority != "experiment" {
		t.Fatal("non-deadline header compatibility changed", code)
	}
	for _, h := range []http.Header{
		{"X-Myce-Project": {""}}, {"X-Myce-Project": {"  "}}, {"X-Myce-Priority": {""}}, {"X-Myce-Priority": {"invalid"}}, {"X-Myce-Priority": {"deadline"}}, {"X-Myce-Project": {"owner", "other"}},
	} {
		if code := request(h, true); code != 400 {
			t.Fatal("invalid supplied header accepted", code)
		}
	}
	if err := q.ConfigureProjects(map[string]ProjectPolicy{"owner": {Weight: 1, DefaultCallPriority: "deadline"}}); err == nil {
		t.Fatal("invalid policy accepted")
	}
}

// This real subprocess emits its secret on both streams and checks both the
// literal env map and fleet-token exclusion before it can become healthy.
func TestEntrySecretChild(t *testing.T) {
	if os.Getenv("ENTRY_CHILD") != "1" {
		return
	}
	if os.Getenv("MYCE_TOKEN") != "" || os.Getenv("ENTRY_LITERAL") != "literal-value" || os.Getenv("ENTRY_SECRET") != "fake-local-credential-68" {
		os.Exit(9)
	}
	if os.Getenv("ENTRY_CHECK_MODEL") == "1" {
		if _, err := os.Stat(filepath.Join(os.Args[len(os.Args)-2], "config.json")); err != nil {
			os.Exit(10)
		}
	}
	fmt.Fprint(os.Stdout, os.Getenv("ENTRY_SECRET"))
	fmt.Fprint(os.Stderr, os.Getenv("ENTRY_SECRET"))
	if os.Getenv("ENTRY_FAIL") == "1" {
		os.Exit(7)
	}
	if err := http.ListenAndServe("127.0.0.1:"+os.Args[len(os.Args)-1], http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})); err != nil {
		os.Exit(8)
	}
	os.Exit(0)
}
func TestEntrySecretsStayLocalAndEnvReachesProcess(t *testing.T) {
	const secret = "fake-local-credential-68"
	t.Setenv("MYCE_TOKEN", "fake-fleet-credential-68")
	root := t.TempDir()
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "service-credential")
	if err := os.WriteFile(path, []byte(secret+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	model := Model{Name: "test", Engine: "server", Path: "unused", MemoryGB: 1, Platforms: []string{"fake"}, StartCommand: []string{exe, "-test.run=^TestEntrySecretChild$", "--", "{port}"}, HealthURL: "http://127.0.0.1:{port}/health", Logs: "none", Env: map[string]string{"ENTRY_CHILD": "1", "ENTRY_LITERAL": "literal-value"}, SecretEnv: map[string]string{"ENTRY_SECRET": "service-credential"}, secretRoot: root, ReadyTimeout: testWaitTimeout}
	dir := t.TempDir()
	for _, fail := range []bool{false, true} {
		if fail {
			model.Env["ENTRY_FAIL"] = "1"
		}
		engine := NewModelEngine(model, dir, RealClock{})
		ctx, cancel := context.WithTimeout(context.Background(), testWaitTimeout)
		err := engine.Load(ctx, model)
		cancel()
		if fail && err == nil || !fail && err != nil {
			t.Fatalf("load outcome (failure=%v): %v", fail, err)
		}
		if err != nil && strings.Contains(err.Error(), secret) {
			t.Fatal("secret in load error")
		}
		if err := engine.Unload(); err != nil {
			t.Fatal(err)
		}
	}
	// Queue catalog, persisted SQLite state, status and usage contain references
	// only. Private runtime launch fields are omitted from JSON/YAML.
	q, clock, m := modelFixture(t)
	model.SizeBytes = 1
	model.SHA256 = strings.Repeat("0", 64)
	if err := q.ConfigureModels(ModelConfig{Idle: time.Minute, Wait: time.Minute, Models: []Model{model}}); err != nil {
		t.Fatal(err)
	}
	m.ModelConfigHash = modelConfigHash(q.models)
	check(t, q, m, nil)
	clock.Advance(time.Second)
	q.Tick()
	snapshot, err := q.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	usage, err := q.Usage(time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal([]any{q.models, q.s, snapshot, usage})
	yml, _ := yaml.Marshal(model)
	if bytes.Contains(data, []byte(secret)) || bytes.Contains(yml, []byte(secret)) || bytes.Contains(data, []byte(root)) {
		t.Fatal("local secret/path serialized")
	}
	var persisted []byte
	if err := q.db.QueryRow("SELECT data FROM queue WHERE id=1").Scan(&persisted); err != nil {
		t.Fatal(err)
	}
	decoded, decodeErr := readQueueState(q.db, persisted)
	if decodeErr != nil {
		t.Fatal(decodeErr)
	}
	persisted = decoded
	if bytes.Contains(persisted, []byte(secret)) {
		t.Fatal("secret persisted in queue")
	}
	var logs int
	if err := q.db.QueryRow("SELECT count(*) FROM logs").Scan(&logs); err != nil || logs != 0 {
		t.Fatal("unexpected queue logs", err, logs)
	}
	if err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if bytes.Contains(data, []byte(secret)) || strings.HasSuffix(path, ".log") {
			t.Fatal("secret output retained")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []os.FileMode{0644, 0600} {
		if err := os.Chmod(path, mode); err != nil {
			t.Fatal(err)
		}
		if mode == 0600 {
			if err := os.WriteFile(path, []byte("fake-fleet-credential-68"), 0600); err != nil {
				t.Fatal(err)
			}
		}
		if _, _, _, _, err := serverCommand(model, 1234); err == nil || strings.Contains(err.Error(), secret) {
			t.Fatal("unsafe secret accepted or exposed")
		}
	}
}

func TestEntryEnvValidationAndParsingCompatibility(t *testing.T) {
	q, _, _ := modelFixture(t)
	base := q.models.Models[0]
	for _, key := range []string{"MYCE_TOKEN", "MYCE_MACHINE", "CUDA_VISIBLE_DEVICES", "HF_HUB_OFFLINE", "PATH", "bad=name"} {
		m := base
		m.Env = map[string]string{key: "value"}
		if err := validateModelEnv(m); err == nil {
			t.Fatal("reserved env accepted", key)
		}
		m.Env = nil
		m.SecretEnv = map[string]string{key: "credential"}
		m.Logs = "none"
		if err := validateModelEnv(m); err == nil {
			t.Fatal("reserved secret accepted", key)
		}
	}
	m := base
	m.SecretEnv = map[string]string{"KEY": "credential"}
	if validateModelEnv(m) == nil {
		t.Fatal("secret output could be logged")
	}
	m.Logs = "none"
	m.SecretEnv["KEY"] = "../credential"
	if validateModelEnv(m) == nil {
		t.Fatal("secret traversal accepted")
	}
	for _, format := range []string{"yaml", "json"} {
		for _, extra := range []bool{false, true} {
			model := base
			if extra {
				model.Project = "owner"
				model.AllowedMachines = []string{"machine-placeholder"}
				model.Env = map[string]string{"VLLM_WORKER_MULTIPROC_METHOD": "spawn"}
				model.SecretEnv = map[string]string{"SERVICE_KEY": "credential"}
				model.Logs = "none"
			}
			config := ModelConfig{Idle: time.Minute, Wait: time.Minute, Models: []Model{model}}
			var data []byte
			var err error
			if format == "yaml" {
				data, err = yaml.Marshal(config)
			} else {
				data, err = json.Marshal(map[string]any{"models": config.Models, "idle_timeout": "1m", "max_wait": "1m"})
			}
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "models."+format)
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			parsed, err := ReadModels(path)
			if err != nil {
				t.Fatal(format, err)
			}
			got := parsed.Models[0]
			if got.Project != model.Project || len(got.AllowedMachines) != len(model.AllowedMachines) || len(got.Env) != len(model.Env) || len(got.SecretEnv) != len(model.SecretEnv) {
				t.Fatal("new/old fields changed during parsing")
			}
		}
	}
}

func TestEntryWorkerLoadsApprovedSourceWithLocalSecrets(t *testing.T) {
	c, model, original, _ := localApprovalFixture(t)
	secrets := t.TempDir()
	if err := os.Chmod(secrets, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(secrets, "credential"), []byte("fake-local-credential-68"), 0600); err != nil {
		t.Fatal(err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	model.StartCommand = []string{exe, "-test.run=^TestEntrySecretChild$", "--", "{model_path}", "{port}"}
	model.Env = map[string]string{"ENTRY_CHILD": "1", "ENTRY_LITERAL": "literal-value", "ENTRY_CHECK_MODEL": "1"}
	model.SecretEnv = map[string]string{"ENTRY_SECRET": "credential"}
	model.Logs = "none"
	model.HealthURL = "http://127.0.0.1:{port}/health"
	dir := t.TempDir()
	worker := Worker{SecretRoot: secrets, ModelApprovalRoot: c.ApprovalRoot, Machine: Machine{ID: "eligible", Session: "entry-session", GPU: "fake"}, Client: Client{Token: "fake-worker-token"}, Clock: RealClock{}, ModelConfig: ModelConfig{Models: []Model{model}, Idle: time.Minute, Wait: testWaitTimeout}, MemorySource: func(context.Context) (float64, error) { return 8, nil }}
	rt, err := startModels(worker, dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := rt.close(); err != nil {
			t.Error(err)
		}
	})
	approved, states := rt.cache.modelApprovals([]Model{model})
	rt.setLocalApprovals(approved, states)
	rt.sync([]ModelSlot{{Attempt: "test-engine", Model: model.Name, State: "loading", Session: "entry-session", Placement: &ModelPlacement{AllowedMachines: []string{"eligible"}}}})
	await(t, func() bool { reports, _ := rt.report(); return len(reports) == 1 && reports[0].State == "ready" })
	path, err := rt.cache.Prepare(context.Background(), model)
	if err != nil || path != original {
		t.Fatal("worker did not retain in-place source", err)
	}
	if names, _, err := rt.cache.Report([]Model{model}); err != nil || !contains(names, model.Name) {
		t.Fatal("local readiness missing", err)
	}
	// A fresh fence in the load plan drains an already-ready worker copy.
	rt.sync([]ModelSlot{{Attempt: "test-engine", Model: model.Name, State: "ready", Session: "entry-session", Placement: &ModelPlacement{AllowedMachines: []string{"excluded"}}}})
	await(t, func() bool { reports, _ := rt.report(); return len(reports) == 1 && reports[0].State == "unloaded" })
}

func TestEntrySimDryExample(t *testing.T) {
	data, err := os.ReadFile("../../docs/examples/sim-server-entries.yaml")
	if err != nil {
		t.Fatal(err)
	}
	sum := strings.Repeat("a", 64)
	files := []ModelFile{{Path: "weights-placeholder.bin", SizeBytes: 1, SHA256: sum}}
	size, identity, err := ModelFilesIdentity(files)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, prefix := range []string{"CHAT", "GLM", "CHANDRA"} {
		text = strings.NewReplacer(prefix+"_GPU_GIB_PLACEHOLDER", "1", prefix+"_HOST_GIB_PLACEHOLDER", "1", prefix+"_SIZE_PLACEHOLDER", fmt.Sprint(size), prefix+"_MANIFEST_SHA256_PLACEHOLDER", identity, prefix+"_FILE_PLACEHOLDER", files[0].Path, prefix+"_FILE_SIZE_PLACEHOLDER", "1", prefix+"_FILE_SHA256_PLACEHOLDER", sum).Replace(text)
	}
	path := filepath.Join(t.TempDir(), "models.yaml")
	if err := os.WriteFile(path, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	config, err := ReadModels(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(config.Models) != 3 {
		t.Fatal("three dry entries required")
	}
	for _, model := range config.Models {
		if model.Project != "sim" || model.Logs != "none" || len(model.AllowedMachines) != 1 || model.LocalSource == "" || model.Path != "" || model.Env["VLLM_WORKER_MULTIPROC_METHOD"] != "spawn" {
			t.Fatal("incomplete dry entry")
		}
	}
}
