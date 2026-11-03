package myce

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestResolveInlineEnvironmentValidationError(t *testing.T) {
	q, _ := fixture(t)
	s := cpuResolveSpec()
	root := t.TempDir()
	s.Environment = Environment{LockFile: filepath.Join(root, "x-requirements.txt"), Python: "3.12"}
	if err := os.WriteFile(s.Environment.LockFile, []byte("# locked fixture\n"), 0600); err != nil {
		t.Fatal(err)
	}
	r, err := q.Resolve(ResolveRequest{Spec: s})
	if err != nil {
		t.Fatal(err)
	}
	f := resolveFinding(r, "environment")
	if f == nil || f.Problem != "lock_file must be uv.lock, pyproject.toml, or requirements.txt" || f.PathDiagnostic != nil || !strings.Contains(f.Fix, "Correct the environment declaration") {
		t.Fatalf("inline validation error hidden: %+v", r.Findings)
	}
	// A missing correctly named file still reports the queue-side path diagnostic.
	s.Environment.LockFile = filepath.Join(root, "requirements.txt")
	r, err = q.Resolve(ResolveRequest{Spec: s})
	if err != nil {
		t.Fatal(err)
	}
	f = resolveFinding(r, "environment.lock_file")
	if f == nil || f.PathDiagnostic == nil || f.Side != "queue" || f.PathDiagnostic.Path != s.Environment.LockFile || f.PathDiagnostic.ResolutionBase != "queue-working-directory" || !strings.Contains(f.Problem, "not the submitter") {
		t.Fatalf("queue path diagnostic lost: %+v", r.Findings)
	}
}

func cpuResolveSpec() Spec {
	return Spec{Name: "resolve", Project: "test", Priority: "experiment", Run: RunSpec{Command: []string{"sh", "run.sh"}}, Needs: ResourceNeeds{CPUCores: 1, MemoryGB: 1, Platform: Platform{Required: []string{"cpu"}}}}
}

func TestResolveNASSharedInputReadsRemainUnverified(t *testing.T) {
	q, _ := fixture(t)
	s := cpuResolveSpec()
	s.SharedInputs = map[string]SharedInput{"movie": {NAS: "media/movie", SHA256: strings.Repeat("a", 64), SizeBytes: 10}}
	r, err := q.Resolve(ResolveRequest{Spec: s})
	if err != nil {
		t.Fatal(err)
	}
	notes := strings.Join(r.Notes, " ")
	for _, want := range []string{"shared_inputs.movie.nas", "worker NAS reads are unverified", "--checkpoint-root", "does not check their existence or bytes", "profile local:", "myce file put"} {
		if !strings.Contains(notes, want) {
			t.Fatalf("resolve omitted %q: %s", want, notes)
		}
	}
	if len(r.Findings) != 0 {
		t.Fatalf("unknown NAS visibility became a proven missing input: %+v", r.Findings)
	}
}

func resolveFinding(r ResolveReport, field string) *ResolveFinding {
	for i := range r.Findings {
		if r.Findings[i].Field == field {
			return &r.Findings[i]
		}
	}
	return nil
}

// A locked CPU job with a staged input resolves on two dev workers whose
// local state roots differ, through the real API, without changing the queue.
// Resolve is not a permit: submission still verifies the staged bytes.
func TestResolveLockedCPUJobWithStagedInputOnTwoRoots(t *testing.T) {
	root := t.TempDir()
	key := strings.Repeat("c", 64)
	catalog := EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"tools": {"cpu": {Key: key, Backend: "uv", LockFile: "requirements.txt", Python: "3.12", Files: map[string][]byte{"requirements.txt": []byte("# locked fixture\n")}}}}}
	d, err := StartDev(DevConfig{Workers: 2, GPU: "cpu", Address: "127.0.0.1:0", Database: filepath.Join(root, "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"), FileStore: filepath.Join(root, "store"), Token: "dev-resolve-token", Interval: 10 * time.Millisecond, EnvironmentConfig: catalog, Environments: []string{key}})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	client := Client{URL: d.URL, Token: "dev-resolve-token"}
	s := cpuResolveSpec()
	s.Environment = Environment{Name: "tools"}
	s.Files = FileManifest{"run.sh": {SHA256: strings.Repeat("d", 64), SizeBytes: 12}, "data.txt": {SHA256: strings.Repeat("e", 64), SizeBytes: 5}}
	var r ResolveReport
	await(t, func() bool {
		r = ResolveReport{}
		if err := client.Call(context.Background(), "POST", "/api/resolve", ResolveRequest{Spec: s}, &r); err != nil {
			t.Fatal(err)
		}
		return len(r.Targets) == 2
	})
	for _, target := range r.Targets {
		if !target.Eligible || !strings.Contains(target.Reason, "environment prepared") || target.Target.Platform != "cpu" {
			t.Fatalf("target not eligible with prepared environment: %+v", r)
		}
	}
	if len(r.Findings) != 0 || !strings.Contains(strings.Join(r.Resolved, " "), "environment tools: queue library recipes for cpu") {
		t.Fatalf("staged locked job reported gaps: %+v", r)
	}
	d.Queue.mu.Lock()
	jobs, reservations := len(d.Queue.s.Jobs), len(d.Queue.s.Reservations)
	d.Queue.mu.Unlock()
	if jobs+reservations != 0 {
		t.Fatalf("resolve created queue state: jobs=%d reservations=%d", jobs, reservations)
	}
	var h *HTTPError
	if _, err := client.Submit(context.Background(), s); !errors.As(err, &h) || h.Code != "job_files_invalid" {
		t.Fatalf("submission accepted unstaged bytes after resolve: %v", err)
	}
}

// The field case: a shared interactive host refused an unprepared environment
// with only "shared host profile prevents execution".
func TestResolveExplainsSharedHostRefusal(t *testing.T) {
	q, c := fixture(t)
	key := strings.Repeat("b", 64)
	q.environments = EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"tools": {"cpu": {Key: key, Backend: "uv", LockFile: "requirements.txt", Files: map[string][]byte{"requirements.txt": []byte("# pinned\n")}}}}}
	m := machine("shared", "cpu")
	m.LastSeen = c.Now()
	m.SharedHost = &SharedHostProfile{InteractiveReserveGB: 1}
	m.EnvironmentBuilder = true
	q.s.Machines[m.ID] = &m
	s := cpuResolveSpec()
	s.Environment = Environment{Name: "tools"}
	r, err := q.Resolve(ResolveRequest{Spec: s})
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Targets) != 1 || r.Targets[0].Eligible || !strings.Contains(r.Targets[0].Reason, "environment not prepared on this shared host") || !strings.Contains(r.Targets[0].Reason, "Run without a managed environment") {
		t.Fatalf("shared host refusal not explained: %+v", r.Targets)
	}
	s.RequireTarget = true
	_, err = q.Submit(s)
	if err == nil || !strings.Contains(err.Error(), "shared hosts never build environments") {
		t.Fatalf("submit refusal lacks the shared-host condition: %v", err)
	}
	s.Environment = Environment{}
	s.RequireTarget = false
	r, err = q.Resolve(ResolveRequest{Spec: s})
	if err != nil || !r.Targets[0].Eligible {
		t.Fatalf("the suggested fix does not resolve: %+v %v", r, err)
	}
}

// A pin that only exists because the data sits on one host is flagged with
// the staging alternative; an authored host requirement is respected.
func TestResolveFlagsDataLocalityPin(t *testing.T) {
	q, c := fixture(t)
	for _, id := range []string{"data-host", "other-host"} {
		m := machine(id, "cpu")
		m.LastSeen = c.Now()
		q.s.Machines[id] = &m
	}
	s := cpuResolveSpec()
	s.AllowedMachines = []string{"data-host"}
	s.Run.Command = []string{"sh", "-c", "wc -l /srv/local-data/clips.csv"}
	r, err := q.Resolve(ResolveRequest{Spec: s})
	if err != nil {
		t.Fatal(err)
	}
	pin := resolveFinding(r, "allowed_machines")
	if pin == nil || pin.Side != "submitter" || !strings.Contains(pin.Problem, "other-host") || !strings.Contains(pin.Fix, "--files-from") || !strings.Contains(pin.Fix, "shared_inputs") {
		t.Fatalf("data-locality pin not flagged with staging: %+v", r.Findings)
	}
	path := resolveFinding(r, "run.command[2]")
	if path == nil || path.Side != "worker" || !strings.Contains(path.Problem, "/srv/local-data/clips.csv") {
		t.Fatalf("local data path not named: %+v", r.Findings)
	}
	eligible := slices.IndexFunc(r.Targets, func(t ResolveTarget) bool { return t.Eligible })
	if eligible < 0 || r.Targets[eligible].Machine != "data-host" {
		t.Fatalf("pinned eligibility: %+v", r.Targets)
	}
	r, err = q.Resolve(ResolveRequest{Spec: s, RequirementReasons: map[string]string{"host": "approved parity"}})
	if err != nil || resolveFinding(r, "allowed_machines") != nil {
		t.Fatalf("authored host requirement flagged: %+v %v", r.Findings, err)
	}
}

// Review regression: resolve must resolve a registered extension as submit
// does, not report every target as lacking a qualified variant.
func TestResolveRegisteredExtensionJob(t *testing.T) {
	q, _, _, s := observationFixture(t)
	r, err := q.Resolve(ResolveRequest{Spec: s})
	if err != nil || len(r.Targets) != 1 || !r.Targets[0].Eligible {
		t.Fatalf("qualified extension job not eligible: %+v %v", r, err)
	}
	s.RequireTarget = true
	if _, err := q.Submit(s); err != nil {
		t.Fatalf("submit disagrees with resolve: %v", err)
	}
}
