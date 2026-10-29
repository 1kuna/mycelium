package myce

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func portabilityRow(t *testing.T, r PortabilityReport, target ExecutionTarget) PortabilityTarget {
	t.Helper()
	for _, row := range r.Targets {
		if row.Target == target {
			return row
		}
	}
	t.Fatalf("missing target %+v in %+v", target, r)
	return PortabilityTarget{}
}
func TestPortabilityLiveInventoryAndCoverage(t *testing.T) {
	q, c := fixture(t)
	e := compositionExtension(t, true)
	e.RequirementReasons = map[string]string{"backend:cuda": "CUDA-only library (authored); /private/source secret=value"}
	cuda := ExecutionTarget{"linux", "arm64", "cuda"}
	metal := ExecutionTarget{"darwin", "arm64", "metal"}
	// Declared but absent, incomplete environment must remain distinct from the
	// observed Metal platform without a variant.
	e.Variants[0].Target = cuda
	e.Variants[0].Environments["main"] = EnvironmentRecipe{Backend: "pixi", LockFile: "env/pixi.lock", Build: []string{"sh", "{extension}/build.sh"}}
	e.Files = nil
	m := machine("example", "metal")
	m.LastSeen = c.Now()
	m.CapabilityProfile.OS = "darwin"
	m.CapabilityProfile.Arch = "arm64"
	q.s.Machines[m.ID] = &m
	before := identityHash(q.s)
	r, err := q.Portability(PortabilityRequest{Recipe: &e})
	if err != nil {
		t.Fatal(err)
	}
	if identityHash(q.s) != before {
		t.Fatal("advice changed queue state")
	}
	observed := portabilityRow(t, r, metal)
	absent := portabilityRow(t, r, cuda)
	if !observed.Observed || observed.Declared || absent.Observed || !absent.Declared || absent.VariantDigest == "" {
		t.Fatalf("coverage conflated: %+v %+v", observed, absent)
	}
	if absent.Environments[0].Inputs != "missing" || absent.Environments[0].LockCoverage != "unknown: missing inputs" {
		t.Fatalf("missing inputs: %+v", absent.Environments)
	}
	if r.Requirements[0].ReasonSource != "authored" {
		t.Fatal(r.Requirements)
	}
	text, _ := json.Marshal(r)
	for _, private := range []string{"/private/source", "secret=value", "CUDA-only library", "build.sh"} {
		if bytes.Contains(text, []byte(private)) {
			t.Fatalf("private metadata leaked: %s", private)
		}
	}
	if !bytes.Contains(text, []byte("mps (queue backend metal)")) || !bytes.Contains(text, []byte("osx-arm64")) || !bytes.Contains(text, []byte("linux-aarch64")) {
		t.Fatal("missing per-target guidance")
	}
	// Changing only the inventory creates a new row; no fleet allowlist.
	unknown := machine("novel", "xpu")
	unknown.LastSeen = c.Now()
	unknown.CapabilityProfile.OS = "freebsd"
	unknown.CapabilityProfile.Arch = "riscv64"
	q.s.Machines[unknown.ID] = &unknown
	r, err = q.Portability(PortabilityRequest{Recipe: &e})
	if err != nil {
		t.Fatal(err)
	}
	novel := portabilityRow(t, r, ExecutionTarget{"freebsd", "riscv64", "xpu"})
	if !strings.Contains(novel.Runtime, "runtime port required") || novel.Hosts[0].Admission == "can admit now" {
		t.Fatalf("unsupported runtime: %+v", novel)
	}
	c.Advance(q.machineLossTimeout(m.ID) + time.Second)
	r, err = q.Portability(PortabilityRequest{Recipe: &e})
	if err != nil {
		t.Fatal(err)
	}
	h := portabilityRow(t, r, metal).Hosts[0]
	if h.Freshness != "stale" || h.Admission != "unknown: inventory stale" {
		t.Fatalf("stale evidence: %+v", h)
	}
}

func TestPortabilityQualificationBuildAndBusyCapacity(t *testing.T) {
	q, c := fixture(t)
	e := compositionExtension(t, true)
	m := machine("worker", "fake")
	m.LastSeen = c.Now()
	m.CapabilityProfile.OS = e.Variants[0].Target.OS
	m.CapabilityProfile.Arch = e.Variants[0].Target.Arch
	m.ExtensionFingerprint = "fixture-fingerprint"
	er := newExtensionReport(e, &m, "extension:"+e.Name)
	er.Setup = true
	er.Qualified = true
	er.CheckID = "manual-qualification"
	m.ExtensionReports = []ExtensionReport{er}
	recipe := e.Variants[0].Environments["main"]
	m.Environments = []string{recipe.Key}
	m.EnvironmentReports = []EnvironmentReport{{Key: recipe.Key, State: "ready", Error: "/private/error"}}
	q.s.Machines[m.ID] = &m
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	r, err := q.Portability(PortabilityRequest{Extension: extensionKey(e), Project: "test"})
	if err != nil {
		t.Fatal(err)
	}
	row := portabilityRow(t, r, e.Variants[0].Target)
	h := row.Hosts[0]
	if h.Qualification != "qualified" || h.Prepared != "prepared" || h.Builds["main"] != "ready" || h.CheckDigest == "" || h.EnvironmentIdentity == "" {
		t.Fatalf("identity/build evidence: %+v", h)
	}
	authored, err := q.Portability(PortabilityRequest{Recipe: &e, Project: "test"})
	if err != nil {
		t.Fatal(err)
	}
	if portabilityRow(t, authored, e.Variants[0].Target).Hosts[0].Qualification == "qualified" {
		t.Fatal("caller metadata manufactured qualification")
	}
	// Occupancy cannot remove declared coverage or native qualification.
	m.FreeMemoryGB = 0
	r, err = q.Portability(PortabilityRequest{Extension: extensionKey(e), Project: "test"})
	if err != nil {
		t.Fatal(err)
	}
	row = portabilityRow(t, r, e.Variants[0].Target)
	h = row.Hosts[0]
	if !row.Declared || h.Qualification != "qualified" || h.Admission == "can admit now" {
		t.Fatalf("busy != unsupported: %+v", row)
	}
	m.ExtensionFingerprint = "changed"
	r, err = q.Portability(PortabilityRequest{Extension: extensionKey(e), Project: "test"})
	if err != nil {
		t.Fatal(err)
	}
	if portabilityRow(t, r, e.Variants[0].Target).Hosts[0].Qualification == "qualified" {
		t.Fatal("stale fingerprint qualified")
	}
}

func TestPortabilityLocksAndCPUChecks(t *testing.T) {
	target := ExecutionTarget{"linux", "arm64", "cpu"}
	recipe := EnvironmentRecipe{Backend: "pixi", LockFile: "env/pixi.lock", Files: map[string][]byte{"extension/env/pixi.lock": []byte("version: 6\nenvironments:\n  default:\n    packages:\n      linux-64: []\n"), "extension/env/pixi.toml": []byte("[workspace]\n")}}
	env := portabilityEnvironment("main", recipe, nil, target)
	if env.Inputs != "present" || env.LockCoverage != "missing target" {
		t.Fatalf("wrong target: %+v", env)
	}
	recipe.Files["extension/env/pixi.lock"] = []byte("version: 6\nenvironments:\n  default:\n    packages:\n      linux-aarch64: []\n")
	env = portabilityEnvironment("main", recipe, nil, target)
	if env.LockCoverage != "declared: native frozen install required" {
		t.Fatalf("lock is not execution proof: %+v", env)
	}
	q, _ := fixture(t)
	r, err := q.Portability(PortabilityRequest{Recipe: &Extension{Variants: []ExtensionVariant{{Target: target, Needs: ResourceNeeds{CPUCores: 3, MemoryGB: 2}, Check: []string{"cpu-check"}}}}})
	if err != nil {
		t.Fatal(err)
	}
	row := portabilityRow(t, r, target)
	if row.Needs.CPUCores != 3 || row.Needs.MemoryGB != 2 || !strings.Contains(strings.Join(row.Actions, " "), "own resource needs and CPU checks") {
		t.Fatal(row)
	}
	if r.Inventory != "unknown" || len(row.Hosts) != 0 {
		t.Fatal("empty inventory implies support")
	}
}

func TestPortabilityAPIReadOnlyAndPrivate(t *testing.T) {
	q, clock := fixture(t)
	m := machine("private-api-worker", "cpu")
	m.LastSeen = clock.Now()
	q.s.Machines[m.ID] = &m
	server := httptest.NewServer(API{Queue: q, Token: "private-test-token"}.Handler())
	defer server.Close()
	resp, err := http.Get(server.URL + "/api/portability")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatal("unauthenticated advice")
	}
	client := Client{URL: server.URL, Token: "private-test-token"}
	request := PortabilityRequest{Spec: &Spec{Environment: Environment{LockFile: "/private/never-read-this"}, Needs: ResourceNeeds{MemoryGB: 1, Platform: Platform{Required: []string{"cpu"}}}}}
	before := identityHash(q.s)
	var r PortabilityReport
	if err := client.Call(context.Background(), "POST", "/api/portability", request, &r); err != nil {
		t.Fatal(err)
	}
	if identityHash(q.s) != before {
		t.Fatal("API advice mutated state")
	}
	var out bytes.Buffer
	if err := WritePortability(&out, r, false); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "Inventory: fresh") {
		t.Fatal(out.String())
	}
}

// The real dev API can observe mixed synthetic check-ins on a GPU-free laptop.
// Recovering prevents these metadata-only fixtures from accepting payloads.
func TestPortabilityDevMixedInventory(t *testing.T) {
	root := t.TempDir()
	d, err := StartDev(DevConfig{Workers: 1, GPU: "cpu", Address: "127.0.0.1:0", Database: filepath.Join(root, "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"), Token: "dev-test-token", Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	client := Client{URL: d.URL, Token: "dev-test-token"}
	targets := []ExecutionTarget{{"linux", "amd64", "xpu"}, {"linux", "arm64", "cuda"}, {"darwin", "arm64", "metal"}}
	for i, target := range targets {
		m := machine(string(rune('a'+i)), target.Platform)
		m.CapabilityProfile.OS = target.OS
		m.CapabilityProfile.Arch = target.Arch
		var reply Reply
		if err := client.Call(context.Background(), "POST", "/api/workers/checkin", Checkin{Machine: m, Recovering: true}, &reply); err != nil {
			t.Fatal(err)
		}
	}
	var r PortabilityReport
	if err := client.Call(context.Background(), "GET", "/api/portability", nil, &r); err != nil {
		t.Fatal(err)
	}
	for _, target := range targets {
		if !portabilityRow(t, r, target).Observed {
			t.Fatal(target)
		}
	}
}

func TestPortabilityPlainCatalogPreparation(t *testing.T) {
	q, c := fixture(t)
	key := strings.Repeat("b", 64)
	q.environments = EnvironmentCatalog{Environments: map[string]map[string]EnvironmentRecipe{"tools": {"cpu": {Key: key, Backend: "uv", LockFile: "requirements.txt", Files: map[string][]byte{"requirements.txt": []byte("# pinned fixture\n")}}}}}
	m := machine("cpu-worker", "cpu")
	m.LastSeen = c.Now()
	m.Environments = []string{key}
	m.HostMemoryMeasured = true
	m.FreeHostMemoryGB = 16
	m.HostAvailableMemoryGB = 16
	if _, err := q.Checkin(Checkin{Machine: m}); err != nil {
		t.Fatal(err)
	}
	s := Spec{Project: "test", Run: RunSpec{Command: []string{"echo", "advice must not execute this"}}, Targets: []ExecutionTarget{{"linux", "amd64", "cpu"}}, Needs: ResourceNeeds{CPUCores: 1, MemoryGB: 1, Platform: Platform{Required: []string{"cpu"}}}, Environment: Environment{Name: "tools"}}
	report, err := q.Portability(PortabilityRequest{Spec: &s})
	if err != nil {
		t.Fatal(err)
	}
	row := portabilityRow(t, report, s.Targets[0])
	h := row.Hosts[0]
	if h.Prepared != "prepared" || h.Builds["catalog"] != "prepared" || len(row.Environments) != 1 || row.Environments[0].Inputs != "present" {
		t.Fatalf("plain catalog projection: %+v", row)
	}
	if h.Admission != "can admit now" {
		t.Fatalf("current plain admission: %+v", h)
	}
}
