package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"mycelium/internal/myce"
)

func TestInstalledClientDevResult(t *testing.T) {
	root := t.TempDir()
	binary := os.Getenv("MYCE_DEV_SMOKE_BINARY")
	if binary == "" {
		binary = devTestBinary(t)
	}
	const token = "installed-dev-fixture-token"
	t.Setenv("MYCE_TOKEN", token) // The real command runner must strip this.
	// StartDev is the runtime behind myce dev: real API, queue and CPU runner.
	d, err := myce.StartDev(myce.DevConfig{Address: "127.0.0.1:0", Database: filepath.Join(root, "queue.db"), FileStore: filepath.Join(root, "files"), Checkpoints: filepath.Join(root, "checkpoints"), Token: token, GPU: "cpu", Workers: 1, Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	ctx, cancel := context.WithTimeout(context.Background(), devTestTimeout)
	defer cancel()
	c := d.Client()
	input := filepath.Join(root, "input")
	if err := os.WriteFile(input, []byte("verified result"), 0600); err != nil {
		t.Fatal(err)
	}
	ref, err := myce.IdentifyFile(input)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.UploadFile(ctx, input, ref); err != nil {
		t.Fatal(err)
	}
	j, err := c.SubmitAndWait(ctx, myce.Spec{Name: "installed-result", Project: "demo", SubmissionKey: "installed-result", Run: myce.RunSpec{Command: []string{"sh", "-c", `test -z "$MYCE_TOKEN" && cat input > output && echo installed-log`}}, Needs: myce.ResourceNeeds{CPUCores: 1, MemoryGB: 1, Platform: myce.Platform{Required: []string{"cpu"}}}, Files: myce.FileManifest{"input": ref}, Outputs: map[string]string{"output": "results"}})
	if err != nil || j.State != "succeeded" {
		t.Fatalf("dev job: %+v %v", j, err)
	}
	backend, _ := url.Parse(d.URL)
	proxy := httputil.NewSingleHostReverseProxy(backend)
	var mode atomic.Int32
	tls := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			t.Error("missing installed authority")
			w.WriteHeader(401)
			return
		}
		if mode.Load() == 4 && r.URL.Path == "/api/status" {
			pending := *j
			pending.State = "running"
			json.NewEncoder(w).Encode(myce.Snapshot{Jobs: []*myce.Job{&pending}})
			return
		}
		if mode.Load() == 3 {
			w.WriteHeader(410) // Generic expiry, without any #156 dependency.
			io.WriteString(w, "expired")
			return
		}
		if strings.HasPrefix(r.URL.Path, "/api/files/blobs/") && mode.Load() != 0 {
			w.Header().Set("ETag", `"`+j.OutputRefs["output"].SHA256+`"`)
			data := "corrupt! result" // Same length; only SHA-256 can reject it.
			if mode.Load() == 2 {
				data = "short"
			}
			fmt.Fprint(w, data)
			return
		}
		proxy.ServeHTTP(w, r)
	}))
	defer tls.Close()
	home := filepath.Join(root, "home")
	config := filepath.Join(home, ".local", "share", "mycelium", "worker", "config.json")
	if err := os.MkdirAll(filepath.Dir(config), 0700); err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(tls.Certificate().Raw)
	joining := filepath.Join(home, "joining.json")
	if err := myce.WriteInvitation(joining, myce.Invitation{Version: 1, Token: token, Address: tls.URL, Fingerprint: hex.EncodeToString(hash[:])}); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(myce.ServiceConfig{Role: "worker", JoiningFile: joining})
	if err := os.WriteFile(config, data, 0600); err != nil {
		t.Fatal(err)
	}
	// Every consumer process has only HOME; no token, endpoint or joining secret.
	t.Setenv("HOME", home)
	t.Setenv("MYCE_TOKEN", "")
	t.Setenv("MYCE_QUEUE", "")
	t.Setenv("MYCE_JOINING_FILE", "")
	command := func(args ...string) ([]byte, error) {
		t.Helper()
		cmd := exec.CommandContext(ctx, binary, args...)
		output, err := cmd.CombinedOutput()
		if strings.Contains(string(output), token) {
			t.Fatal("credential exposed")
		}
		return output, err
	}
	// Commands without installed-authority resolution must reject --config.
	for _, name := range []string{"cancel", "join"} {
		if output, err := command(name, "--config", config); err == nil || !strings.Contains(string(output), "flag provided but not defined: -config") {
			t.Fatalf("%s silently accepted --config: %v %s", name, err, output)
		}
	}
	for _, args := range [][]string{{"status"}, {"wait", j.ID}, {"logs", j.ID}, {"result", j.ID, "--out", filepath.Join(root, "result")}, {"file", "get", "--size-bytes", "15", j.OutputRefs["output"].SHA256, filepath.Join(root, "raw-output")}} {
		if output, err := command(args...); err != nil {
			t.Fatalf("%s: %v %s", args[0], err, output)
		}
	}
	if data, err := os.ReadFile(filepath.Join(root, "result", "output")); err != nil || string(data) != "verified result" {
		t.Fatalf("verified download: %q %v", data, err)
	}
	python, err := exec.LookPath("python3.11")
	if err != nil {
		python, err = exec.LookPath("python3")
	}
	if err != nil {
		t.Fatal(err)
	}
	script := `from mycelium import Client
from pathlib import Path
import sys
c = Client.from_installed()
assert c.get(sys.argv[1])["state"] == "succeeded"
c.download_result(sys.argv[1], "output", sys.argv[2])
assert Path(sys.argv[2]).read_text() == "verified result"
`
	pythonPath, _ := filepath.Abs(filepath.Join("..", "..", "clients", "python", "src"))
	cmd := exec.CommandContext(ctx, python, "-c", script, j.ID, filepath.Join(root, "python-output"))
	cmd.Env = append(os.Environ(), "PYTHONPATH="+pythonPath)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Python installed authority: %v %s", err, strings.ReplaceAll(string(output), token, "[redacted]"))
	}
	t.Run("profile_observer", func(t *testing.T) {
		testProfileInstalledObserverNotification(t, ctx, binary, root)
	})
	for _, state := range []int32{1, 2, 3, 4} {
		mode.Store(state)
		dest := filepath.Join(root, fmt.Sprint("rejected-", state))
		code := "result_download_failed"
		if state == 3 {
			code = "result_expired"
			err := resultCommand(ctx, []string{j.ID, "--out", dest}, io.Discard)
			var typed *ResultError
			var api *myce.HTTPError
			if !errors.As(err, &typed) || !errors.As(err, &api) || api.Status != 410 || exitCode(err) == 0 {
				t.Fatalf("expiry type lost: %T %v", err, err)
			}
		}
		if state == 4 {
			code = "result_not_accepted"
		}
		if output, err := command("result", j.ID, "--out", dest); err == nil || !strings.HasPrefix(string(output), code+":") {
			t.Fatalf("typed rejection %d: %v %s", state, err, output)
		}
		if _, err := os.Stat(filepath.Join(dest, "output")); !os.IsNotExist(err) {
			t.Fatal("unverified artifact published")
		}
	}
}
