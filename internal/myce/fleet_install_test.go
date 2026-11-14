package myce

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type fleetInstallFixture struct {
	t              *testing.T
	c              ServiceConfig
	i              FleetUpdateInstruction
	deps           fleetInstallDeps
	old, next      []byte
	key            ed25519.PrivateKey
	version        string
	mu             sync.Mutex
	authorizations int
	fetches        int
	acks           []string
	denyAt         int
	denyCode       string
	failureCodes   []string
	badArtifact    bool
	restarts       int
	launches       int
	restartError   bool
	changeDR       bool
	managerPID     int
	runtimePID     int
	runtimeVersion string
	helperPresent  bool
	healthFailure  bool
}

func newFleetInstallFixture(t *testing.T, goos string) *fleetInstallFixture {
	t.Helper()
	f := &fleetInstallFixture{t: t, old: []byte("old installed executable"), next: []byte("approved new executable"), version: "v1.0.0", managerPID: 123, runtimePID: 123, runtimeVersion: "v1.0.0"}
	home := t.TempDir()
	var err error
	f.c, err = PlanInstall("worker", home, "", home, "http://queue.invalid", "fixture", Detection{OS: goos, Arch: "arm64", Platform: "fake", MemoryMode: "fixed", MemoryGB: 2})
	if err != nil {
		t.Fatal(err)
	}
	f.c.HostID, f.c.AllowDeviceless = "fixture-host", true
	if goos == "darwin" {
		f.old = append([]byte{0xcf, 0xfa, 0xed, 0xfe}, f.old...)
		f.next = append([]byte{0xcf, 0xfa, 0xed, 0xfe}, f.next...)
	}
	roots, err := f.c.StateRoots()
	if err != nil {
		t.Fatal(err)
	}
	if err = PrepareStateRoots(roots...); err != nil {
		t.Fatal(err)
	}
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	f.key = key
	f.i = FleetUpdateInstruction{Operation: "fixture-operation", Revision: 1, Host: f.c.HostID, Machine: f.c.Name, Session: "old-session", PublisherKey: pub}
	f.sign("v1.0.1")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if r.Header.Get("Authorization") != "Bearer fixture-private-token" {
			t.Error("missing saved credentials")
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/api/invites/release":
			_ = json.NewEncoder(w).Encode(f.i.Release)
		case "/api/hosts/fixture-host/maintenance":
			_ = json.NewEncoder(w).Encode(HostMaintenanceStatus{Workers: []MaintenanceWorker{{Fresh: true, Machine: Machine{ID: f.c.Name, Session: f.i.Session, Fleet: &FleetRuntimeProof{Version: f.runtimeVersion, SHA256: fleetBytesHash(f.old), Session: f.i.Session, PID: f.runtimePID, ServiceHealthy: true, RunnerSmoke: true, StartGate: StartGateProtocol, AttemptProtocol: AttemptProtocol}}}}})
		case "/api/fleet/authorize":
			f.authorizations++
			if r.URL.Query().Get("revision") != strconv.Itoa(f.i.Revision) || r.URL.Query().Get("operation") != f.i.Operation || r.URL.Query().Get("host") != f.i.Host || r.URL.Query().Get("machine") != f.i.Machine || r.URL.Query().Get("session") != f.i.Session {
				t.Error("authorization lost instruction fence")
			}
			if f.authorizations == f.denyAt || f.denyAt < 0 {
				if f.denyCode != "" {
					respond(w, nil, fleetError(f.denyCode))
					return
				}
				http.Error(w, "changed maintenance owner", http.StatusConflict)
				return
			}
			_ = json.NewEncoder(w).Encode(FleetRuntimeProof{Version: f.runtimeVersion, SHA256: fleetBytesHash(f.old), Session: f.i.Session, PID: f.runtimePID, ServiceHealthy: true, RunnerSmoke: true, StartGate: StartGateProtocol, AttemptProtocol: AttemptProtocol})
		case "/api/fleet/artifact":
			f.fetches++
			if f.authorizations < 2 || r.URL.Query().Get("operation") != f.i.Operation || r.URL.Query().Get("target") != "myce-"+f.c.Detection.OS+"-arm64" {
				t.Error("artifact requested without authorization or target fence")
			}
			if f.badArtifact {
				_, _ = w.Write(bytes.Repeat([]byte("x"), len(f.next)))
			} else {
				_, _ = w.Write(f.next)
			}
		case "/api/fleet/ack":
			var ack struct {
				Operation, Host, Machine, Session, Phase, Failure string
				Revision                                          int
			}
			if json.NewDecoder(r.Body).Decode(&ack) != nil || ack.Revision != f.i.Revision || ack.Operation != f.i.Operation || ack.Session != f.i.Session || ack.Host != f.i.Host || ack.Machine != f.i.Machine {
				t.Error("acknowledgement lost instruction fence")
			}
			if strings.Contains(ack.Failure, "fixture-private-token") {
				t.Error("failure disclosed credentials")
			}
			f.acks = append(f.acks, ack.Phase)
			if ack.Phase == "failed" {
				f.failureCodes = append(f.failureCodes, ack.Failure)
			}
		case "/api/fleet/instruction":
			_ = json.NewEncoder(w).Encode(f.i)
		default:
			t.Errorf("unexpected request %s", r.URL)
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	f.c.Queue = server.URL
	config, _ := json.Marshal(f.c)
	for path, data := range map[string][]byte{
		f.c.Config: config, f.c.Binary: f.old, f.c.Service: []byte(f.c.ServiceText()),
		f.c.Credentials: []byte("MYCE_TOKEN=fixture-private-token\nMYCE_QUEUE=" + server.URL + "\n"),
		filepath.Join(f.c.State, "fleet-publisher.pub"): pub,
		filepath.Join(f.c.State, "identity-kept"):       []byte("unique identity"),
		filepath.Join(f.c.State, "spool-kept"):          []byte("buffered result"),
	} {
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		mode := os.FileMode(0600)
		if path == f.c.Binary {
			mode = 0700
		}
		if err := os.WriteFile(path, data, mode); err != nil {
			t.Fatal(err)
		}
	}
	clock := &fakeClock{now: time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC)}
	f.deps = fleetInstallDeps{command: f.command, versionCommand: func(ctx context.Context, path string) ([]byte, []byte, error) {
		data, err := f.command(ctx, path, "version", "--json")
		return data, nil, err
	}, client: fleetClient, health: EnrollmentWait{Clock: clock, Wait: func(_ context.Context, duration time.Duration) error { clock.Advance(duration); return nil }}}
	return f
}

func (f *fleetInstallFixture) sign(version string) {
	f.t.Helper()
	m := ReleaseManifest{Version: version, StartGate: StartGateProtocol, AttemptProtocol: AttemptProtocol}
	for _, target := range ReleaseTargets {
		m.Artifacts = append(m.Artifacts, ReleaseArtifact{OS: target.OS, Arch: target.Arch, File: "myce-" + target.OS + "-" + target.Arch, Size: int64(len(f.next)), SHA256: fleetBytesHash(f.next), NativeSensing: target.NativeSensing})
	}
	data, _ := json.Marshal(m)
	envelope, _ := json.Marshal(SignedRelease{Manifest: data, Signature: ed25519.Sign(f.key, data)})
	f.i.Release = EnrollmentRelease{Manifest: m, SignedRelease: envelope, PublisherKey: f.i.PublisherKey}
}

func (f *fleetInstallFixture) command(_ context.Context, name string, args ...string) ([]byte, error) {
	f.t.Helper()
	if strings.Contains(strings.Join(args, " "), "fixture-private-token") {
		f.t.Fatal("credential in command arguments")
	}
	if name == f.c.Binary || strings.HasPrefix(filepath.Base(name), ".myce-fleet-") {
		version := f.version
		if name != f.c.Binary {
			version = f.i.Release.Manifest.Version
		}
		return json.Marshal(VersionInfo{Version: version, OS: f.c.Detection.OS, Arch: f.c.Detection.Arch, StartGate: StartGateProtocol, AttemptProtocol: AttemptProtocol, NativeSensing: true})
	}
	if name == "systemd-run" || name == "launchctl" && len(args) > 0 && args[0] == "submit" {
		r, err := readFleetReceipt(filepath.Join(f.c.State, fleetUpdateReceiptFile))
		if err != nil || r.Phase != "prepared" && r.Phase != "installed" {
			f.t.Fatal("helper launched without durable receipt", err)
		}
		joined := strings.Join(args, " ")
		if !strings.Contains(joined, f.c.Binary+" fleet-apply --config "+f.c.Config) || strings.Contains(joined, "--token") {
			f.t.Fatal("helper did not use installed binary/saved authority", joined)
		}
		f.launches++
		return nil, nil
	}
	if name == "codesign" {
		if args[0] == "-dv" {
			return []byte("Identifier=io.github.1kuna.mycelium\nAuthority=Developer ID Application: synthetic\nflags=0x10000(runtime)\n"), nil
		}
		if args[0] == "-dr" {
			requirement := `designated => identifier "io.github.1kuna.mycelium" and anchor apple generic and certificate leaf[subject.OU] = "SYNTHETIC"`
			if f.changeDR && args[len(args)-1] != f.c.Binary {
				requirement += ` and certificate leaf[subject.CN] = "DIFFERENT"`
			}
			return []byte(requirement), nil
		}
		if args[0] == "--verify" {
			return nil, nil
		}
		f.t.Fatal("attempted local signing", args)
	}
	if name == "systemctl" {
		if args[1] == "show" {
			if strings.Contains(strings.Join(args, " "), "MainPID") {
				if f.healthFailure && f.restarts > 0 {
					return nil, errors.New("synthetic failed service")
				}
				return []byte(fmt.Sprintf("ActiveState=active\nSubState=running\nMainPID=%d\nNRestarts=0\nResult=success\n", f.managerPID)), nil
			}
			if strings.Contains(strings.Join(args, " "), "LoadState") {
				if f.helperPresent {
					return []byte("loaded\n"), nil
				}
				return []byte("not-found\n"), nil
			}
			return []byte("enabled\n"), nil
		}
		if args[1] == "is-active" {
			return nil, nil
		}
	}
	if name == "launchctl" && args[0] == "print" {
		if strings.Contains(args[len(args)-1], "/myce-fleet-") && !f.helperPresent {
			return nil, fleetTestExit(113)
		}
		if f.healthFailure && f.restarts > 0 {
			return nil, errors.New("synthetic failed service")
		}
		return []byte(fmt.Sprintf("\tstate = running\n\tpid = %d\n\truns = 1\n", f.managerPID)), nil
	}
	if name == "systemctl" && args[1] == "restart" || name == "launchctl" && args[0] == "kickstart" {
		r, err := readFleetReceipt(filepath.Join(f.c.State, fleetUpdateReceiptFile))
		if err != nil || r.Phase != "restarting" {
			f.t.Fatal("restart preceded durable intent", err)
		}
		if args[len(args)-1] != filepath.Base(f.c.Service) && !strings.HasSuffix(args[len(args)-1], "/local.mycelium.worker") {
			f.t.Fatal("restart targeted another service", args)
		}
		f.restarts++
		if f.restartError {
			return nil, errors.New("fixture-private-token deliberately unsafe manager output")
		}
		f.managerPID++
		return nil, nil
	}
	if name == "launchctl" && args[0] == "remove" {
		return nil, nil
	}
	f.t.Fatalf("unexpected manager command %s %v", name, args)
	return nil, nil
}

func (f *fleetInstallFixture) launch() error {
	return launchFleetUpdate(context.Background(), f.c, f.i, f.deps)
}
func (f *fleetInstallFixture) apply() error {
	return applyFleetUpdate(context.Background(), f.c.Config, filepath.Join(f.c.State, fleetUpdateReceiptFile), f.deps)
}
func (f *fleetInstallFixture) assertBinary(want []byte) {
	f.t.Helper()
	got, err := os.ReadFile(f.c.Binary)
	if err != nil || !bytes.Equal(got, want) {
		f.t.Fatalf("installed bytes changed unexpectedly: %q, %v", got, err)
	}
	staging, _ := filepath.Glob(filepath.Join(filepath.Dir(f.c.Binary), ".myce-*-*"))
	if len(staging) != 0 {
		f.t.Fatal("rebuildable staging retained", staging)
	}
}

func TestFleetInstallPreservesSettingsAndRestartsOnce(t *testing.T) {
	f := newFleetInstallFixture(t, "linux")
	kept := map[string][]byte{}
	for _, path := range []string{f.c.Config, f.c.Credentials, f.c.Service, filepath.Join(f.c.State, "identity-kept"), filepath.Join(f.c.State, "spool-kept"), filepath.Join(f.c.State, "fleet-publisher.pub")} {
		kept[path], _ = os.ReadFile(path)
	}
	if err := f.launch(); err != nil {
		t.Fatal(err)
	}
	if err := f.launch(); err != nil || f.launches != 1 {
		t.Fatal("duplicate instruction relaunched", err, f.launches)
	}
	if err := f.apply(); err != nil {
		t.Fatal(err)
	}
	f.assertBinary(f.next)
	if f.restarts != 1 || !reflect.DeepEqual(f.acks, []string{"restarting"}) || f.authorizations != 3 {
		t.Fatal("incorrect activation", f.restarts, f.acks)
	}
	if err := f.apply(); err != nil || f.restarts != 1 || f.fetches != 1 {
		t.Fatal("resume repeated installation or restart", err)
	}
	for path, want := range kept {
		got, _ := os.ReadFile(path)
		if !bytes.Equal(got, want) {
			t.Errorf("changed saved artifact %s", path)
		}
	}
}

func TestFleetInstallRejectsUntrustedReleaseBeforeFetch(t *testing.T) {
	for _, fault := range []string{"signature", "publisher", "downgrade", "same-version"} {
		t.Run(fault, func(t *testing.T) {
			f := newFleetInstallFixture(t, "linux")
			switch fault {
			case "signature":
				f.i.Release.SignedRelease[len(f.i.Release.SignedRelease)/2] ^= 1
			case "publisher":
				pub, key, _ := ed25519.GenerateKey(rand.Reader)
				f.i.PublisherKey, f.key = pub, key
				f.sign("v1.0.1")
			case "downgrade":
				f.sign("v0.9.0")
			case "same-version":
				f.sign(f.version)
			}
			if err := f.launch(); err == nil || f.fetches != 0 || f.launches != 0 || f.restarts != 0 {
				t.Fatal("unsafe release reached helper", err)
			}
			f.assertBinary(f.old)
		})
	}
}

func TestFleetInstallRequiresCurrentMaintenanceAuthority(t *testing.T) {
	for _, denyAt := range []int{2, 3} {
		t.Run(map[int]string{2: "before-download", 3: "before-publication"}[denyAt], func(t *testing.T) {
			f := newFleetInstallFixture(t, "linux")
			if err := f.launch(); err != nil {
				t.Fatal(err)
			}
			f.denyAt = denyAt
			if err := f.apply(); err == nil || f.restarts != 0 {
				t.Fatal("changed maintenance owner authorized install", err)
			}
			if denyAt == 2 && f.fetches != 0 {
				t.Fatal("artifact requested before authorization")
			}
			f.assertBinary(f.old)
		})
	}
}

func TestFleetInstallRefusesBadHashAndStorageWithoutRollback(t *testing.T) {
	for _, fault := range []string{"hash", "storage", "restart"} {
		t.Run(fault, func(t *testing.T) {
			f := newFleetInstallFixture(t, "linux")
			if err := f.launch(); err != nil {
				t.Fatal(err)
			}
			switch fault {
			case "hash":
				f.badArtifact = true
			case "storage":
				if err := os.Mkdir(filepath.Join(f.c.NAS, "mycelium"), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(f.c.State, filepath.Join(f.c.NAS, "mycelium", "checkpoints")); err != nil {
					t.Fatal(err)
				}
			case "restart":
				f.restartError = true
			}
			err := f.apply()
			if err == nil || strings.Contains(err.Error(), "fixture-private-token") {
				t.Fatal("missing or unsanitized failure", err)
			}
			want := f.old
			if fault == "restart" {
				want = f.next // Fix forward: failure never restores old bytes.
			}
			f.assertBinary(want)
			if fault == "storage" && f.fetches != 0 {
				t.Fatal("download preceded storage preflight")
			}
			before := f.restarts
			if f.apply() == nil || f.restarts != before {
				t.Fatal("failed operation retried restart")
			}
			if fault == "storage" {
				r, err := readFleetReceipt(filepath.Join(f.c.State, fleetUpdateReceiptFile))
				if err != nil {
					t.Fatal(err)
				}
				failure, err := ReadFleetUpdateFailure(f.c)
				if err != nil || failure == nil || failure.Failure != "fleet_storage_preflight_failed" {
					t.Fatalf("storage failure lost: %+v %v", failure, err)
				}
				r.Failure = "private arbitrary error text"
				if err = writeFleetReceipt(f.c, r); err != nil {
					t.Fatal(err)
				}
				failure, err = ReadFleetUpdateFailure(f.c)
				if err != nil || failure == nil || failure.Failure != "fleet_failure_code_unrecognized" {
					t.Fatalf("untrusted failure exposed: %+v %v", failure, err)
				}
			}
		})
	}
}

func TestFleetInstallPublicationAndRestartCrashWindows(t *testing.T) {
	for _, phase := range []string{"prepared", "installed", "restarting"} {
		t.Run(phase, func(t *testing.T) {
			f := newFleetInstallFixture(t, "linux")
			if err := f.launch(); err != nil {
				t.Fatal(err)
			}
			r, _ := readFleetReceipt(filepath.Join(f.c.State, fleetUpdateReceiptFile))
			r.Phase = phase
			if err := writeFleetReceipt(f.c, r); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(f.c.Binary, f.next, 0700); err != nil {
				t.Fatal(err)
			}
			if err := f.apply(); err != nil {
				t.Fatal(err)
			}
			want := 1
			if phase == "restarting" {
				want = 0 // Whether restart happened is now runtime proof's job.
			}
			if f.restarts != want || f.fetches != 0 {
				t.Fatal("crash window repeated bytes/restart or skipped first restart", f.restarts, f.fetches)
			}
			if err := f.apply(); err != nil || f.restarts != want {
				t.Fatal("reconciliation blindly repeated restart", err)
			}
		})
	}
}

func TestFleetInstallDarwinDesignatedRequirement(t *testing.T) {
	for _, change := range []bool{false, true} {
		t.Run(map[bool]string{false: "same-requirement", true: "changed-requirement"}[change], func(t *testing.T) {
			f := newFleetInstallFixture(t, "darwin")
			f.changeDR = change
			if err := f.launch(); err != nil {
				t.Fatal(err)
			}
			err := f.apply()
			if change {
				if err == nil || f.restarts != 0 {
					t.Fatal("Darwin identity change accepted", err)
				}
				f.assertBinary(f.old)
			} else {
				if err != nil || f.restarts != 1 {
					t.Fatal("same identity failed", err)
				}
				f.assertBinary(f.next)
			}
		})
	}
}

func TestFleetInstallSavedJoiningTrustAndRedirectRefusal(t *testing.T) {
	f := newFleetInstallFixture(t, "linux")
	requests := 0
	leak := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		t.Error("redirect received request with saved authority")
	}))
	defer leak.Close()
	queue := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer private-joining-authority" {
			t.Error("did not use saved joining credential")
		}
		http.Redirect(w, r, leak.URL, http.StatusFound)
	}))
	defer queue.Close()
	pin := sha256.Sum256(queue.Certificate().Raw)
	f.c.Queue, f.c.JoiningFile = queue.URL, filepath.Join(filepath.Dir(f.c.Config), "joining.json")
	invitation := Invitation{Version: 1, Fingerprint: hex.EncodeToString(pin[:]), Token: "private-joining-authority", Address: queue.URL}
	if err := WriteInvitation(f.c.JoiningFile, invitation); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MYCE_TOKEN", "ambient-authority-must-be-ignored")
	client, err := fleetClient(f.c)
	if err != nil {
		t.Fatal(err)
	}
	defer client.HTTP.CloseIdleConnections()
	a, err := f.i.Release.Manifest.Select(f.c.Detection.OS, f.c.Detection.Arch)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fetchFleetArtifact(context.Background(), f.c, fleetNoRedirect(client), f.i, a); err == nil || requests != 0 {
		t.Fatal("artifact redirect accepted", err)
	}
	invitation.Fingerprint = strings.Repeat("a", 64)
	if err := WriteInvitation(f.c.JoiningFile, invitation); err != nil {
		t.Fatal(err)
	}
	untrusted, err := fleetClient(f.c)
	if err != nil {
		t.Fatal(err)
	}
	defer untrusted.HTTP.CloseIdleConnections()
	if _, err := fleetAuthorize(context.Background(), fleetNoRedirect(untrusted), f.i, nil); err == nil {
		t.Fatal("saved queue TLS pin ignored")
	}
}

type fleetTestExit int

func (e fleetTestExit) Error() string { return "synthetic manager exit" }
func (e fleetTestExit) ExitCode() int { return int(e) }

// One operation launches once; a later operation (a rerun after a failure)
// launches its own helper.
func TestFleetInstallRerunLaunchesNewOperationOnly(t *testing.T) {
	f := newFleetInstallFixture(t, "linux")
	if err := f.launch(); err != nil {
		t.Fatal(err)
	}
	f.badArtifact = true
	if err := f.apply(); err == nil {
		t.Fatal("bad artifact accepted")
	}
	if err := f.launch(); err != nil || f.launches != 1 {
		t.Fatal("failed operation relaunched", err, f.launches)
	}
	f.i.Operation, f.badArtifact = "fixture-rerun", false
	if err := f.launch(); err != nil || f.launches != 2 {
		t.Fatal("new operation not launched", err, f.launches)
	}
	if err := f.apply(); err != nil || f.restarts != 1 {
		t.Fatal(err, f.restarts)
	}
	f.assertBinary(f.next)
}

func TestFleetInstallSharedBinaryStillRestartsOldWorker(t *testing.T) {
	for _, fault := range []string{"healthy", "storage", "installed-storage"} {
		t.Run(fault, func(t *testing.T) {
			f := newFleetInstallFixture(t, "linux")
			// A colocated queue has already replaced the shared installed pathname;
			// the worker's retained executable mapping and session are still old.
			if err := os.WriteFile(f.c.Binary, f.next, 0700); err != nil {
				t.Fatal(err)
			}
			f.version = f.i.Release.Manifest.Version
			if err := f.launch(); err != nil {
				t.Fatal(err)
			}
			r, err := readFleetReceipt(filepath.Join(f.c.State, fleetUpdateReceiptFile))
			if err != nil || r.BeforeRuntime.SHA256 == r.BeforeSHA256 {
				t.Fatal("disk hash confused with running image", err)
			}
			if fault == "installed-storage" {
				r.Phase = "installed"
				if err := writeFleetReceipt(f.c, r); err != nil {
					t.Fatal(err)
				}
			}
			if fault != "healthy" {
				if err := os.Mkdir(filepath.Join(f.c.NAS, "mycelium"), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(f.c.State, filepath.Join(f.c.NAS, "mycelium", "checkpoints")); err != nil {
					t.Fatal(err)
				}
			}
			err = f.apply()
			if fault != "healthy" {
				if err == nil || err.Error() != "fleet_storage_preflight_failed" || f.restarts != 0 {
					t.Fatal("shared binary bypassed storage preflight", err, f.restarts)
				}
			} else if err != nil || f.restarts != 1 {
				t.Fatal("shared binary skipped worker activation", err, f.restarts)
			}
			if f.fetches != 0 {
				t.Fatal("shared binary fetched again")
			}
			f.assertBinary(f.next)
		})
	}
}

func TestFleetInstallStopsAfterFailedManagerHealth(t *testing.T) {
	f := newFleetInstallFixture(t, "linux")
	if err := f.launch(); err != nil {
		t.Fatal(err)
	}
	f.healthFailure = true
	started := f.deps.health.Clock.Now()
	if err := f.apply(); err == nil || err.Error() != "fleet_health_failed" {
		t.Fatal("failed new service did not stop helper", err)
	}
	if elapsed := f.deps.health.Clock.Now().Sub(started); elapsed != time.Minute {
		t.Fatal("readiness wait not bounded by injected clock", elapsed)
	}
	r, err := readFleetReceipt(filepath.Join(f.c.State, fleetUpdateReceiptFile))
	if err != nil || r.Phase != "failed" || r.Failure != "fleet_health_failed" || f.restarts != 1 || f.acks[len(f.acks)-1] != "failed" {
		t.Fatal("health failure not durable/acknowledged", err)
	}
	f.assertBinary(f.next)
	failure, err := ReadFleetUpdateFailure(f.c)
	if err != nil || failure == nil || failure.Failure != "fleet_health_failed" || failure.Revision != f.i.Revision || failure.Session != f.i.Session {
		t.Fatal("runtime cannot reconcile durable health failure", err)
	}
	if f.apply() == nil || f.restarts != 1 {
		t.Fatal("failed startup automatically retried")
	}
}

func TestFleetInstallActiveApplyLockIsBenign(t *testing.T) {
	f := newFleetInstallFixture(t, "linux")
	if err := f.launch(); err != nil {
		t.Fatal(err)
	}
	unlock, err := commandLock(f.c.State, "fleet-update.lock", false)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	f.i.Operation = "fixture-rerun"
	var refetch *fleetInstructionRefetchError
	if err := f.launch(); !errors.As(err, &refetch) || f.launches != 1 {
		t.Fatal("rerun interrupted active local helper", err)
	}
	r, err := readFleetReceipt(filepath.Join(f.c.State, fleetUpdateReceiptFile))
	if err != nil || r.Instruction.Operation != "fixture-operation" {
		t.Fatal("active receipt replaced", err)
	}
}

func TestFleetInstallFileCacheCapacity(t *testing.T) {
	for _, goos := range []string{"linux", "darwin"} {
		t.Run(goos, func(t *testing.T) {
			f := newFleetInstallFixture(t, goos)
			// Save through the installer's flag overlay, then reinstall with no
			// override before the signed fleet update reads the configuration.
			for _, args := range [][]string{{"--file-cache-bytes=34359738368"}, nil} {
				c, err := ReadServiceConfig(f.c.Config)
				if err != nil {
					t.Fatal(err)
				}
				values := ServiceConfig{}
				flags := flag.NewFlagSet("install", flag.ContinueOnError)
				RegisterInstallFlags(flags, &values)
				if err := flags.Parse(args); err != nil {
					t.Fatal(err)
				}
				c, err = OverlayInstallFlags(flags, c, values)
				if err != nil {
					t.Fatal(err)
				}
				data, err := json.Marshal(c)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(c.Config, data, 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(c.Service, []byte(c.ServiceText()), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := f.launch(); err != nil {
				t.Fatal(err)
			}
			if err := f.apply(); err != nil {
				t.Fatal(err)
			}
			c, err := ReadServiceConfig(f.c.Config)
			if err != nil || c.FileCacheBytes != 32<<30 {
				t.Fatalf("fleet upgrade lost file cache capacity: %d, %v", c.FileCacheBytes, err)
			}
			// launchd regenerates Args at startup; systemd retains the saved unit.
			args := c.Args()
			i := slices.Index(args, "--file-cache-bytes")
			if i < 0 || args[i+1] != "34359738368" {
				t.Fatal("fleet restart arguments lost file cache capacity")
			}
			unit, err := os.ReadFile(c.Service)
			if err != nil || string(unit) != c.ServiceText() {
				t.Fatal("fleet upgrade changed installed service configuration", err)
			}
		})
	}
}

func TestFleetInstallVersionStdoutAndReceiptDiagnostics(t *testing.T) {
	for _, scenario := range []string{"valid", "invalid-json", "nonzero-exit"} {
		t.Run(scenario, func(t *testing.T) {
			f := newFleetInstallFixture(t, "linux")
			info, _ := json.Marshal(VersionInfo{Version: f.i.Release.Manifest.Version, OS: "linux", Arch: "arm64", StartGate: StartGateProtocol, AttemptProtocol: AttemptProtocol})
			if scenario == "invalid-json" {
				info = []byte("invalid JSON")
			}
			exit := "0"
			if scenario == "nonzero-exit" {
				exit = "1"
			}
			probe := filepath.Join(t.TempDir(), "probe")
			script := "#!/bin/sh\nprintf '%s\\n' '" + string(info) + "'\nprintf '%s\\n' 'harmless startup diagnostic' >&2\nexit " + exit + "\n"
			if err := os.WriteFile(probe, []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			f.deps.versionCommand = func(ctx context.Context, _ string) ([]byte, []byte, error) { return fleetVersionCommand(ctx, probe) }
			if err := f.launch(); err != nil {
				t.Fatal(err)
			}
			err := f.apply()
			if scenario == "valid" {
				if err != nil || f.restarts != 1 {
					t.Fatal("stderr broke upgrade", err)
				}
			} else if err == nil || err.Error() != "fleet_candidate_invalid" || f.restarts != 0 {
				t.Fatal("invalid version accepted", err)
			}
			r, err := readFleetReceipt(filepath.Join(f.c.State, fleetUpdateReceiptFile))
			if err != nil || r.VersionStderr != "harmless startup diagnostic\n" {
				t.Fatalf("lost receipt stderr: %q %v", r.VersionStderr, err)
			}
			ack, err := ReadFleetUpdateFailure(f.c)
			if err != nil {
				t.Fatal(err)
			}
			raw, _ := json.Marshal(ack)
			if bytes.Contains(raw, []byte("harmless startup diagnostic")) {
				t.Fatal("local diagnostic leaked into acknowledgement")
			}
		})
	}
}
