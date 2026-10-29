package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"mycelium/internal/myce"
)

func TestExtensionCLIComposition(t *testing.T) {
	folder, root := t.TempDir(), t.TempDir()
	description := fmt.Sprintf(`name: fixture.stage
version: '1'
purpose: Wrap an unchanged command
variants:
  - target: {os: %s, arch: %s, platform: fake}
    needs: {memory_gb: 1}
    execution:
      main: {command: [sh, stage.sh]}
    check: [sh, check.sh]
    baselines:
      extension:fixture.stage:
        input: input
        output: output
        measure: exact
        native: synthetic laptop fixture
        variation: exact fixture
`, runtime.GOOS, runtime.GOARCH)
	for name, body := range map[string]string{"extension.yaml": description, "stage.sh": "exit 0\n", "check.sh": "test -z \"$MYCE_TOKEN\" || exit 99\ncat \"$MYCE_CHECK_INPUT\" > \"$MYCE_CHECK_OUTPUT\"\n", "input": "fixture\n", "output": "fixture\n"} {
		if err := os.WriteFile(filepath.Join(folder, name), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	devRoot := t.TempDir()
	d, err := myce.StartDev(myce.DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(devRoot, "queue.db"), Checkpoints: filepath.Join(devRoot, "checkpoints"), HostID: "cli-host", Token: "fixture-not-inherited", Workers: 1, MemoryGB: 8, Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	client := d.Client()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var snapshot myce.Snapshot
		if err = client.Call(context.Background(), "GET", "/api/status", nil, &snapshot); err != nil {
			t.Fatal(err)
		}
		if len(snapshot.Machines) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("worker startup")
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Setenv("MYCE_QUEUE", d.URL)
	t.Setenv("MYCE_TOKEN", "")
	t.Setenv("MYCE_JOINING_FILE", "")
	home := t.TempDir()
	t.Setenv("HOME", home)
	config := filepath.Join(home, ".local", "share", "mycelium", "worker", "config.json")
	if err := os.MkdirAll(filepath.Dir(config), 0700); err != nil {
		t.Fatal(err)
	}
	tokenFile := filepath.Join(home, "token")
	if err := os.WriteFile(tokenFile, []byte("MYCE_TOKEN=fixture-not-inherited\nMYCE_QUEUE="+d.URL+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	workerState := filepath.Join(home, "worker-state")
	saved, _ := json.Marshal(myce.ServiceConfig{Role: "worker", State: workerState, Queue: d.URL, EnvironmentFile: tokenFile})
	if err := os.WriteFile(config, saved, 0600); err != nil {
		t.Fatal(err)
	}
	if err := extensionCommand([]string{"add", "--root", root, folder}); err != nil {
		t.Fatal(err)
	}
	for _, action := range []string{"setup", "check"} {
		stderr, err := os.CreateTemp(t.TempDir(), "stderr")
		if err != nil {
			t.Fatal(err)
		}
		original := os.Stderr
		os.Stderr = stderr
		err = extensionCommand([]string{action, "--root", root, "--gpu", "fake", "--device", "0", "--host", "cli-host", "--environment-cache", t.TempDir(), "--environment-cache-bytes", "1048576", "fixture.stage@1"})
		os.Stderr = original
		stderr.Close()
		if err != nil {
			t.Fatal(err)
		}
		output, err := os.ReadFile(stderr.Name())
		if err != nil {
			t.Fatal(err)
		}
		if action == "check" {
			for _, want := range []string{"WARNING:", root, filepath.Join(workerState, "extensions"), "live worker is not qualified by this check"} {
				if !strings.Contains(string(output), want) {
					t.Errorf("check output missing %q: %s", want, output)
				}
			}
		} else if strings.Contains(string(output), "WARNING:") {
			t.Errorf("setup warned about qualification: %s", output)
		}
	}
	reports := myce.LocalExtensionReports(root, "fake", "")
	if len(reports) != 1 || reports[0].Target == nil || reports[0].Target.OS != runtime.GOOS {
		t.Fatal("variant target lost", reports)
	}
	// No --root must install and qualify where the installed worker reads.
	for _, args := range [][]string{
		{"extension", "add", folder},
		{"extension", "setup", "--gpu", "fake", "fixture.stage@1"},
		{"extension", "check", "--gpu", "fake", "--device", "0", "--host", "cli-host", "--environment-cache", t.TempDir(), "--environment-cache-bytes", "1048576", "fixture.stage@1"},
	} {
		_, stderr, err := captureUsageOutput(t, args)
		if err != nil || strings.Contains(stderr, "WARNING:") {
			t.Fatalf("default worker root %v: %v, %s", args, err, stderr)
		}
	}
	fingerprint, err := myce.ExtensionFingerprint(context.Background(), "fake", "0")
	if err != nil {
		t.Fatal(err)
	}
	reports = myce.LocalExtensionReports(filepath.Join(workerState, "extensions"), "fake", fingerprint)
	if len(reports) != 1 || !reports[0].Qualified {
		t.Fatal("default check did not qualify installed worker root", reports)
	}
}

func TestExtensionCLIInstalledRootDefault(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CACHE_HOME", filepath.Join(home, "cache"))
	folder := filepath.Join(home, "source")
	if err := extensionCommand([]string{"init", "--name", "fixture", folder}); err != nil {
		t.Fatal(err)
	}
	// Without an installed worker, retain the user-cache default.
	if err := extensionCommand([]string{"add", "--local", folder}); err != nil {
		t.Fatal(err)
	}
	if extensions, err := myce.LocalExtensions(myce.DefaultExtensionRoot()); err != nil || len(extensions) != 1 {
		t.Fatal("user cache fallback", extensions, err)
	}
	state := filepath.Join(home, "worker-state")
	config := filepath.Join(home, ".local", "share", "mycelium", "worker", "config.json")
	if err := os.MkdirAll(filepath.Dir(config), 0700); err != nil {
		t.Fatal(err)
	}
	saved, _ := json.Marshal(myce.ServiceConfig{Role: "worker", State: state})
	if err := os.WriteFile(config, saved, 0600); err != nil {
		t.Fatal(err)
	}
	if err := extensionCommand([]string{"add", "--local", folder}); err != nil {
		t.Fatal(err)
	}
	if extensions, err := myce.LocalExtensions(filepath.Join(state, "extensions")); err != nil || len(extensions) != 1 {
		t.Fatal("installed root default", extensions, err)
	}
	// The list command remains offline and reads the same installed library.
	output, _, err := captureUsageOutput(t, []string{"extension", "list"})
	if err != nil || !strings.Contains(output, `"name":"fixture"`) {
		t.Fatal("installed root list", output, err)
	}
	// A custom --config selects a worker outside the conventional install path.
	custom := filepath.Join(home, "custom.json")
	if err := os.Rename(config, custom); err != nil {
		t.Fatal(err)
	}
	output, _, err = captureUsageOutput(t, []string{"extension", "list", "--config", custom})
	if err != nil || !strings.Contains(output, `"name":"fixture"`) {
		t.Fatal("custom installed root list", output, err)
	}
	other := t.TempDir()
	output, _, err = captureUsageOutput(t, []string{"extension", "list", "--config", custom, "--root", other})
	if err != nil || strings.Contains(output, `"name":"fixture"`) {
		t.Fatal("explicit root ignored", output, err)
	}
}

func TestExtensionCheckRootWarning(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	root := t.TempDir()
	var out bytes.Buffer
	warnExtensionCheckRoot(&out, "", root)
	if out.Len() != 0 {
		t.Fatal("warning without installed worker", out.String())
	}
	state := t.TempDir()
	workerRoot := filepath.Join(state, "extensions")
	if err := os.Mkdir(workerRoot, 0700); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(home, ".local", "share", "mycelium", "worker", "config.json")
	if err := os.MkdirAll(filepath.Dir(config), 0700); err != nil {
		t.Fatal(err)
	}
	saved, _ := json.Marshal(myce.ServiceConfig{Role: "worker", State: state})
	if err := os.WriteFile(config, saved, 0600); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(home, "alias")
	if err := os.Symlink(workerRoot, alias); err != nil {
		t.Fatal(err)
	}
	for _, same := range []string{workerRoot, alias} {
		warnExtensionCheckRoot(&out, "", same)
		if out.Len() != 0 {
			t.Fatal("warning for worker root", out.String())
		}
	}
	// Explicit configurations support installations outside the default path.
	custom := filepath.Join(home, "custom.json")
	if err := os.Rename(config, custom); err != nil {
		t.Fatal(err)
	}
	warnExtensionCheckRoot(&out, custom, root)
	for _, want := range []string{root, workerRoot, "live worker is not qualified by this check"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("warning missing %q: %s", want, &out)
		}
	}
}

func TestExtensionCLIInstalledPinnedAuthority(t *testing.T) {
	t.Setenv("MYCE_TOKEN", "")
	t.Setenv("MYCE_QUEUE", "")
	t.Setenv("MYCE_JOINING_FILE", "")
	home := t.TempDir()
	t.Setenv("HOME", home)
	folder := filepath.Join(home, "source")
	if err := extensionCommand([]string{"init", "--name", "fixture", folder}); err != nil {
		t.Fatal(err)
	}
	requests := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.Header.Get("Authorization") != "Bearer fixture-private-authority" {
			t.Error("missing installed authority")
		}
		if r.URL.Path == "/api/extensions" && r.Method == "POST" {
			w.Write([]byte(`{}`))
			return
		}
		// Check must reach the target resolver with the same pinned authority.
		http.Error(w, "fixture-target-stop", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	hash := sha256.Sum256(server.Certificate().Raw)
	joining := filepath.Join(home, "joining.json")
	if err := myce.WriteInvitation(joining, myce.Invitation{Version: 1, Fingerprint: hex.EncodeToString(hash[:]), Token: "fixture-private-authority", Address: server.URL}); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(home, ".local", "share", "mycelium", "queue", "config.json")
	if err := os.MkdirAll(filepath.Dir(config), 0700); err != nil {
		t.Fatal(err)
	}
	saved, _ := json.Marshal(myce.ServiceConfig{Role: "queue", JoiningFile: joining})
	if err := os.WriteFile(config, saved, 0600); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	for _, flags := range [][]string{nil, {"--config", config}, {"--joining-file", joining}} {
		args := append([]string{"add", "--root", root}, flags...)
		if err := extensionCommand(append(args, folder)); err != nil {
			t.Fatal(err)
		}
	}
	if requests != 3 {
		t.Fatalf("registration requests: %d", requests)
	}
	err := extensionCommand([]string{"check", "--root", root, "--host", "fixture-host", "fixture@1"})
	if err == nil || !strings.Contains(err.Error(), "runtime check requires") || requests != 4 {
		t.Fatalf("check authority: %v, requests=%d", err, requests)
	}
	wrong := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("credential sent to wrong certificate") }))
	identity, err := myce.LoadJoiningIdentity(filepath.Join(t.TempDir(), "identity"))
	if err != nil {
		t.Fatal(err)
	}
	cert, err := identity.TLS()
	if err != nil {
		t.Fatal(err)
	}
	wrong.TLS = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	wrong.StartTLS()
	defer wrong.Close()
	if err := extensionCommand([]string{"add", "--root", root, "--queue", wrong.URL, folder}); err == nil {
		t.Fatal("accepted wrong certificate")
	}
	// Local commands accept authority flags without needing readable authority.
	for _, args := range [][]string{
		{"list", "--root", root, "--config", "missing"},
		{"check", "--static", "--joining-file", "missing", folder},
		{"add", "--local", "--root", root, "--config", "missing", folder},
	} {
		if err := extensionCommand(args); err != nil {
			t.Fatal(err)
		}
	}
}
