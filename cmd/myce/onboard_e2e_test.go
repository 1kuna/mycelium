package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"mycelium/internal/myce"
)

type enrollmentLogBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *enrollmentLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}
func (b *enrollmentLogBuffer) String() string { b.mu.Lock(); defer b.mu.Unlock(); return b.b.String() }

// A fake manager starts only the test's installed binary. No native manager,
// fleet, SSH, sudo or external service is involved.
const enrollmentFakeManager = `#!/bin/sh
set -eu
if test "$(basename "$0")" = codesign; then
  case "$1" in
    --verify) :;;
    -dv) printf 'Identifier=io.github.1kuna.mycelium\nAuthority=Developer ID Application: synthetic\nflags=0x10000(runtime)\n';;
    -dr) echo 'designated => identifier "io.github.1kuna.mycelium" and anchor apple generic and certificate leaf[subject.OU] = "SYNTHETIC"';;
    *) exit 99;;
  esac
  exit
fi
base="$HOME/.local/share/mycelium/worker"
pidfile="$HOME/fake-manager.pid"
start() {
  if test -f "$HOME/fail-next-install"; then rm "$HOME/fail-next-install"; exit 99; fi
  /usr/bin/curl --noproxy '*' --fail --silent --show-error --request POST "$(cat "$HOME/fake-manager.url")/start" > "$pidfile"
  echo start >> "$HOME/fake-manager.log"
}
stop() {
  if test -f "$pidfile"; then
    /usr/bin/curl --noproxy '*' --fail --silent --show-error --request POST "$(cat "$HOME/fake-manager.url")/stop"
    rm -f "$pidfile"
  fi
}
if test "$(basename "$0")" = launchctl; then
  case "$1" in
    print) test -f "$pidfile" || exit 113; pid=$(cat "$pidfile"); kill -0 "$pid" || exit 113; printf 'service = {\n\tstate = running\n\tpid = %s\n\truns = 1\n\tresource coalition = {\n\t\tstate = active\n\t}\n}\n' "$pid";;
    bootstrap) start;;
    bootout) stop;;
    kickstart) stop; start;;
    kill) stop;;
    *) exit 99;;
  esac
  exit
fi
test "$1" = --user && shift
case "$1" in
  show)
    case "$*" in
      *--property=UnitFileState*) if test -f "$base/config.json"; then echo enabled; fi;;
      *--property=DropInPaths*) :;;
      *--property=ActiveState*) test -f "$pidfile"; printf 'ActiveState=active\nSubState=running\nMainPID=%s\nNRestarts=0\nResult=success\n' "$(cat "$pidfile")";;
      *--property=ControlGroup*) echo /fake-admission-only;;
      *) :;;
    esac;;
  is-active) test -f "$pidfile" || exit 3;;
  daemon-reload|disable) :;;
  enable) case "$*" in *--now*) start;; esac;;
  restart) stop; start;;
  stop) stop;;
  *) exit 99;;
esac
`

func TestOneCommandEnrollmentEndToEnd(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("supported enrollment targets only")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	t.Cleanup(cancel)
	dir := t.TempDir()
	nas := filepath.Join(dir, "nas")
	checkpoint := filepath.Join(nas, "mycelium", "checkpoints")
	if err := os.MkdirAll(checkpoint, 0700); err != nil {
		t.Fatal(err)
	}
	q, err := myce.Open(filepath.Join(dir, "queue", "queue.db"), checkpoint, myce.RealClock{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q.Close() })
	identity, err := myce.LoadJoiningIdentity(filepath.Join(dir, "joining-identity"))
	if err != nil {
		t.Fatal(err)
	}
	cert, err := identity.TLS()
	if err != nil {
		t.Fatal(err)
	}
	token := "test-only-fleet-secret-never-printed"
	var redemptions atomic.Int32
	var maintenanceWrites atomic.Int32
	api := (myce.API{Queue: q, Token: token}).HandlerWithJoiningIdentity(identity.Fingerprint())
	queue := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/invites/redeem" {
			redemptions.Add(1)
		}
		if r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/maintenance") {
			maintenanceWrites.Add(1)
		}
		api.ServeHTTP(w, r)
	}))
	queue.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
	queue.Config.ErrorLog = log.New(io.Discard, "", 0)
	queue.StartTLS()
	t.Cleanup(queue.Close)
	// Observe API and runner logs without any synthetic credential disclosure.
	var logs enrollmentLogBuffer
	priorLog := log.Writer()
	log.SetOutput(&logs)
	t.Cleanup(func() { log.SetOutput(priorLog) })
	authority := myce.Invitation{Version: 1, Token: token, Fingerprint: identity.Fingerprint(), Address: queue.URL}
	inviteFile := filepath.Join(dir, "source-invitation.json")
	if err = myce.WriteInvitation(inviteFile, authority); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MYCE_JOINING_FILE", inviteFile)
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	pubDER, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	publicKey := filepath.Join(dir, "release-public.pem")
	if err = os.WriteFile(publicKey, pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER}), 0600); err != nil {
		t.Fatal(err)
	}
	var artifact atomic.Value
	artifact.Store([]byte{})
	var downloadCount atomic.Int32
	var rangeCount atomic.Int32
	var tamper atomic.Bool
	publisher := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		downloadCount.Add(1)
		if r.Header.Get("Range") != "" {
			rangeCount.Add(1)
		}
		data := artifact.Load().([]byte)
		if tamper.Load() {
			data = append([]byte{}, data...)
			data[len(data)-1] ^= 1
		}
		http.ServeContent(w, r, "myce", time.Unix(1, 0), bytes.NewReader(data))
	}))
	t.Cleanup(publisher.Close)
	ca := filepath.Join(dir, "publisher-ca.pem")
	if err = os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: publisher.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	makeCommand := func(version string) string {
		t.Helper()
		binary := filepath.Join(dir, "artifact-"+version)
		build := exec.CommandContext(ctx, "go", "build", "-tags=myce_test", "-ldflags", "-X mycelium/internal/myce.BuildVersion="+version, "-o", binary, ".")
		if output, e := build.CombinedOutput(); e != nil {
			t.Fatalf("build: %v %s", e, output)
		}
		data, e := os.ReadFile(binary)
		if e != nil {
			t.Fatal(e)
		}
		artifact.Store(data)
		hash, e := myce.FileSHA256(binary)
		if e != nil {
			t.Fatal(e)
		}
		m := myce.ReleaseManifest{Version: version, StartGate: myce.StartGateProtocol, AttemptProtocol: myce.AttemptProtocol}
		for _, target := range [][2]string{{"linux", "amd64"}, {"linux", "arm64"}, {"darwin", "arm64"}} {
			a := myce.ReleaseArtifact{OS: target[0], Arch: target[1], File: "myce-" + target[0] + "-" + target[1], SHA256: strings.Repeat("a", 64), Size: 1, NativeSensing: target[0] == "darwin"}
			if target[0] == runtime.GOOS && target[1] == runtime.GOARCH {
				a.SHA256 = hash
				a.Size = int64(len(data))
			}
			m.Artifacts = append(m.Artifacts, a)
		}
		raw, _ := json.Marshal(m)
		signed, _ := json.Marshal(myce.SignedRelease{Manifest: raw, Signature: ed25519.Sign(key, raw)})
		manifest := filepath.Join(dir, "release.json")
		if e = os.WriteFile(manifest, signed, 0600); e != nil {
			t.Fatal(e)
		}
		if e = q.ConfigureEnrollmentRelease(signed, pub, publisher.URL); e != nil {
			t.Fatal(e)
		}
		command := captureJoiningOutput(t, func() error { return inviteCommand(nil) })
		if strings.Count(strings.TrimSpace(command), "\n") != 0 || strings.Contains(command, token) {
			t.Fatal("invite did not print one safe command")
		}
		parts := strings.SplitN(command, "'", 3)
		if len(parts) != 3 {
			t.Fatal("bootstrap encoding missing")
		}
		decoded, e := base64.StdEncoding.DecodeString(parts[1])
		if e != nil || bytes.Contains(decoded, []byte(token)) {
			t.Fatal("fleet credential encoded in command", e)
		}
		outPath := filepath.Join(t.TempDir(), "paste-command")
		fileCommand := captureJoiningOutput(t, func() error { return inviteCommand([]string{"--out", outPath}) })
		written, e := os.ReadFile(outPath)
		if e != nil || string(written) != fileCommand || bytes.Contains(written, []byte(token)) {
			t.Fatal("optional command output differs or exposed credential", e)
		}
		stat, e := os.Stat(outPath)
		if e != nil || stat.Mode().Perm() != 0600 {
			t.Fatal("optional command output not private", e)
		}
		if e = inviteCommand([]string{"--out", outPath}); e == nil {
			t.Fatal("overwrote optional command output")
		}
		return strings.TrimSpace(command)
	}
	newHost := func() string {
		t.Helper()
		host := t.TempDir()
		newEnrollmentTestManager(t, host)
		bin := filepath.Join(host, "bin")
		if err := os.Mkdir(bin, 0700); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"launchctl", "systemctl", "codesign"} {
			if err := os.WriteFile(filepath.Join(bin, name), []byte(enrollmentFakeManager), 0700); err != nil {
				t.Fatal(err)
			}
		}
		for _, name := range []string{"go", "git"} {
			if err := os.WriteFile(filepath.Join(bin, name), []byte("#!/bin/sh\necho forbidden-tool >> \"$HOME/forbidden-tools\"\nexit 99\n"), 0700); err != nil {
				t.Fatal(err)
			}
		}
		return host
	}
	runCommand := func(command, host string, expectSuccess bool, extra ...string) string {
		t.Helper()
		script := command + " --gpu fake --memory-mode fixed --memory-gb 8 --allow-deviceless --nas " + shellQuote(nas) + " --require-mounted-nas=false --enrollment-timeout 80s" + " " + strings.Join(extra, " ")
		cmd := exec.CommandContext(ctx, "sh", "-c", script)
		for _, entry := range os.Environ() {
			k, _, _ := strings.Cut(entry, "=")
			if k != "HOME" && k != "PATH" && k != "MYCE_TOKEN" && k != "MYCE_QUEUE" && k != "MYCE_JOINING_FILE" && k != "CURL_CA_BUNDLE" {
				cmd.Env = append(cmd.Env, entry)
			}
		}
		cmd.Env = append(cmd.Env, "HOME="+host, "PATH="+filepath.Join(host, "bin")+":/usr/bin:/bin:/usr/sbin:/sbin", "CURL_CA_BUNDLE="+ca)
		out, e := cmd.CombinedOutput()
		if strings.Contains(string(out), token) {
			t.Fatal("fleet token printed")
		}
		if (e == nil) != expectSuccess {
			service, _ := os.ReadFile(filepath.Join(host, "fake-service.log"))
			t.Fatalf("bootstrap success=%t err=%v output=%s service=%s", expectSuccess, e, out, service)
		}
		return string(out)
	}
	command := makeCommand("v1.0.1")
	host := newHost()
	// A preexisting prefix proves curl's Range recovery on a clean host.
	var signed myce.SignedRelease
	manifestData, _ := os.ReadFile(filepath.Join(dir, "release.json"))
	_ = json.Unmarshal(manifestData, &signed)
	var manifest myce.ReleaseManifest
	_ = json.Unmarshal(signed.Manifest, &manifest)
	selected, _ := manifest.Select(runtime.GOOS, runtime.GOARCH)
	cache := filepath.Join(host, ".cache", "mycelium", "bootstrap")
	if err = os.MkdirAll(cache, 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(cache, selected.SHA256+".partial"), artifact.Load().([]byte)[:4096], 0600); err != nil {
		t.Fatal(err)
	}
	output := runCommand(command, host, true)
	if !strings.Contains(output, " · ready · ") || !strings.Contains(output, "CPU ") || !strings.Contains(output, "RAM ") || rangeCount.Load() == 0 {
		t.Fatal("missing ready/capabilities/resume", output)
	}
	if _, e := os.Stat(filepath.Join(cache, "invitation.json")); !os.IsNotExist(e) {
		t.Error("bootstrap left a second credential copy", e)
	}
	c, err := myce.ReadServiceConfig(filepath.Join(host, ".local", "share", "mycelium", "worker", "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output, c.Name+" · ") {
		t.Fatal("capability summary omitted the installed hypha name")
	}
	stat, err := os.Stat(c.Config)
	if err != nil {
		t.Fatal(err)
	}
	managerBefore, _ := os.ReadFile(filepath.Join(host, "fake-manager.log"))
	downloadsBefore := downloadCount.Load()
	rerunOutput := runCommand(command, host, true)
	if !strings.Contains(rerunOutput, "installer flags are not reapplied") {
		t.Error("same-version rerun silently ignored installer flags")
	}
	after, _ := os.Stat(c.Config)
	managerAfter, _ := os.ReadFile(filepath.Join(host, "fake-manager.log"))
	if !stat.ModTime().Equal(after.ModTime()) || !bytes.Equal(managerBefore, managerAfter) || downloadCount.Load() != downloadsBefore || redemptions.Load() != 1 {
		t.Fatal("same-version rerun mutated installation or redeemed again")
	}
	if _, err = os.Stat(filepath.Join(host, "forbidden-tools")); !os.IsNotExist(err) {
		t.Fatal("new host used Go or Git")
	}
	// A different host cannot replay the ticket, even with the authentic artifact.
	runCommand(command, newHost(), false)
	// Even authentic executable bytes must match the approved release version.
	wrongManifest := manifest
	wrongManifest.Version = "v9.0.0"
	wrongCommand, err := myce.BootstrapCommand(wrongManifest, publisher.URL, queue.URL, authority.Fingerprint, strings.Repeat("b", 64))
	if err != nil {
		t.Fatal(err)
	}
	beforeWrong := redemptions.Load()
	wrongHost := newHost()
	runCommand(wrongCommand, wrongHost, false)
	if redemptions.Load() != beforeWrong {
		t.Fatal("wrong artifact reached redemption")
	}
	if _, err = os.Stat(filepath.Join(wrongHost, ".local", "share", "mycelium", "worker", "config.json")); !os.IsNotExist(err) {
		t.Fatal("wrong artifact installed")
	}
	beforeTamper := redemptions.Load()
	tamper.Store(true)
	badHost := newHost()
	out := runCommand(command, badHost, false)
	tamper.Store(false)
	if !strings.Contains(out, "checksum refused") || redemptions.Load() != beforeTamper {
		t.Fatal("tampered artifact reached enrollment")
	}
	if _, err = os.Stat(filepath.Join(badHost, ".local", "share", "mycelium", "worker", "config.json")); !os.IsNotExist(err) {
		t.Fatal("tampered artifact installed")
	}
	// Failed upgrades resume their own drain and restore the previous release.
	client, err := authority.Client(queue.URL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.HTTP.CloseIdleConnections)
	newCommand := makeCommand("v1.0.2")
	beforeInterrupted := redemptions.Load()
	oldHash, err := myce.FileSHA256(c.Binary)
	if err != nil {
		t.Fatal(err)
	}
	beforeInvalid := maintenanceWrites.Load()
	invalidOptions := runCommand(newCommand, host, false, "--invalid-installer-option")
	if maintenanceWrites.Load() != beforeInvalid || !strings.Contains(invalidOptions, "invalid-installer-option") {
		t.Fatal("invalid install options reached drain", invalidOptions)
	}
	if hash, e := myce.FileSHA256(c.Binary); e != nil || hash != oldHash {
		t.Fatal("invalid install options changed previous binary", e)
	}
	if err := os.WriteFile(filepath.Join(host, "fail-next-install"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	installerFailure := runCommand(newCommand, host, false)
	var installerStatus myce.HostMaintenanceStatus
	if err = client.Call(ctx, "GET", "/api/hosts/"+c.HostID+"/maintenance", nil, &installerStatus); err != nil || installerStatus.Policy.Mode != "enabled" || !strings.Contains(installerFailure, "host resumed on previous release") {
		t.Error("installer failure abandoned drain or omitted outcome", err, installerFailure)
	}
	readinessFailure := runCommand(newCommand, host, false, "--enrollment-timeout=1s")
	var held myce.HostMaintenanceStatus
	if err = client.Call(ctx, "GET", "/api/hosts/"+c.HostID+"/maintenance", nil, &held); err != nil || held.Policy.Mode != "enabled" {
		t.Error("readiness failure did not resume its self-made drain", err)
	}
	receipt := filepath.Join(c.State, "enrollment-resume.json")
	if hash, e := myce.FileSHA256(c.Binary); e != nil || hash != oldHash {
		t.Error("readiness failure did not roll back previous binary", e)
	}
	if !strings.Contains(readinessFailure, "host resumed on previous release") {
		t.Error("readiness failure omitted recovery outcome", readinessFailure)
	}
	if _, e := os.Stat(receipt); !os.IsNotExist(e) {
		t.Error("successful failure recovery retained resume authority", e)
	}
	output = runCommand(newCommand, host, true)
	if !strings.Contains(output, " · ready · ") || redemptions.Load() != beforeInterrupted {
		t.Fatal("upgrade retry did not become ready without redeeming again")
	}
	if _, e := os.Stat(receipt); !os.IsNotExist(e) {
		t.Fatal("completed upgrade retained resume authority", e)
	}
	// A subsequent replacement must still preserve operator-owned disable.
	if err = client.Call(ctx, "POST", "/api/hosts/"+c.HostID+"/maintenance", myce.HostMaintenanceRequest{Action: "disable"}, nil); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(c.State, "custody-sentinel.bin")
	if err = os.WriteFile(sentinel, []byte("preserved custody"), 0600); err != nil {
		t.Fatal(err)
	}
	beforeUpgrade := redemptions.Load()
	newCommand = makeCommand("v1.0.3")
	output = runCommand(newCommand, host, true)
	if !strings.Contains(output, " · disabled · ") || strings.Contains(output, " · ready · ") || redemptions.Load() != beforeUpgrade {
		t.Fatal("upgrade resumed maintenance or redeemed ticket", output)
	}
	if data, err := os.ReadFile(sentinel); err != nil || string(data) != "preserved custody" {
		t.Fatal("upgrade lost spool custody", err)
	}
	// Replay an authentic older invitation line against a newer installation.
	runCommand(command, host, false)
	runCommand(command+" --allow-downgrade", host, true)
	if strings.Contains(logs.String(), token) {
		t.Fatal("fleet token in queue logs")
	}
	for _, path := range []string{filepath.Join(host, "fake-service.log"), filepath.Join(host, "fake-manager.log")} {
		data, _ := os.ReadFile(path)
		if bytes.Contains(data, []byte(token)) {
			t.Fatal("fleet token in host logs")
		}
	}
	if _, err = myce.ReadInvitation(inviteFile); err != nil {
		t.Fatal("source invitation lost privacy", err)
	}
}
