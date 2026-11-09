package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mycelium/internal/myce"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
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
	d, err := myce.StartDev(myce.DevConfig{EnvironmentCacheLimit: 1 << 30, Address: "127.0.0.1:0", Database: filepath.Join(root, "queue.db"), FileStore: filepath.Join(root, "files"), Checkpoints: filepath.Join(root, "checkpoints"), Token: token, GPU: "cpu", Workers: 1, Interval: 10 * time.Millisecond})
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
	var rejectObserver atomic.Bool
	var transient atomic.Bool
	var retryData []byte
	var retryRef myce.FileRef
	var retryReads, retryDownloads atomic.Int32
	var custody atomic.Bool
	var custodyTransfers atomic.Int32
	var mode atomic.Int32
	tls := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if rejectObserver.Load() {
			t.Error("TLS identity failure must prevent authenticated HTTP requests")
			w.WriteHeader(500)
			return
		}
		if r.Header.Get("Authorization") != "Bearer "+token {
			t.Error("missing installed authority")
			w.WriteHeader(401)
			return
		}
		if transient.Load() {
			if strings.HasPrefix(r.URL.Path, "/api/jobs/") {
				if retryReads.Add(1) == 1 {
					w.WriteHeader(503)
					_, _ = io.WriteString(w, `{"code":"queue_stopping"}`)
					return
				}
				_ = json.NewEncoder(w).Encode(myce.Job{ID: "job", State: "succeeded", OutputRefs: myce.FileManifest{"gate-result.txt": retryRef}})
				return
			}
			downloads := retryDownloads.Add(1)
			w.Header().Set("ETag", `"`+retryRef.SHA256+`"`)
			w.Header().Set("Content-Length", fmt.Sprint(len(retryData)))
			if downloads == 1 {
				_, _ = w.Write(retryData[:5])
				return
			}
			_, _ = w.Write(retryData)
			return
		}
		if custody.Load() {
			switch r.URL.Path {
			case "/api/jobs/job/recovery-plan":
				_ = json.NewEncoder(w).Encode(myce.RecoveryPlan{Execution: myce.Execution{ID: "job", Attempt: 1}, Machine: "worker", Session: "session", Outputs: []string{"nested/out.txt"}})
			case "/api/jobs/job":
				json.NewEncoder(w).Encode(myce.Job{ID: "job", State: "succeeded", OutputRefs: myce.FileManifest{"nested/out.txt": {SHA256: strings.Repeat("a", 64), SizeBytes: 1}}})
			default:
				custodyTransfers.Add(1)
				http.Error(w, "unexpected file transfer", 500)
			}
			return
		}
		if mode.Load() == 4 && r.URL.Path == "/api/jobs/"+j.ID {
			pending := *j
			pending.State = "running"
			json.NewEncoder(w).Encode(pending)
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
	for _, name := range []string{"join"} {
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

	mode.Store(0)
	{
		if err = os.Mkdir(filepath.Join(root, "nested"), 0700); err != nil {
			t.Fatal(err)
		}
		input := filepath.Join(root, "nested", "out.txt")
		if err = os.WriteFile(input, []byte("retained output"), 0600); err != nil {
			t.Fatal(err)
		}
		ref, err := myce.IdentifyFile(input)
		if err != nil {
			t.Fatal(err)
		}
		c := d.Client()
		if err = c.UploadFile(ctx, input, ref); err != nil {
			t.Fatal(err)
		}
		j, err := c.SubmitAndWait(ctx, myce.Spec{Name: "recovery", Project: "demo", SubmissionKey: "recovery-cli", Files: myce.FileManifest{"input": ref}, Outputs: map[string]string{"nested/out.txt": "results"}, Run: myce.RunSpec{Command: []string{"sh", "-c", "mkdir nested; cat input > nested/out.txt; exit 1"}}, Needs: myce.ResourceNeeds{CPUCores: 1, MemoryGB: 1, Platform: myce.Platform{Required: []string{"cpu"}}}})
		if err != nil || j.State != "failed" {
			t.Fatalf("failed native attempt: %+v %v", j, err)
		}
		args := []string{"recover", j.ID, "--queue", tls.URL, "--token", c.Token, "--files-from", root, "--key", "recovery", "--by", "owner", "--reason", "recover retained output", "--source", "nas:results/retained"}
		var record myce.RecoveredResult
		out := captureJoiningOutput(t, func() error { return resultCommand(ctx, args, io.Discard) })
		if err = json.Unmarshal([]byte(out), &record); err != nil || record.Kind != "recovered" {
			t.Fatalf("recovery reply: %s %v", out, err)
		}
		again := captureJoiningOutput(t, func() error { return resultCommand(ctx, args, io.Discard) })
		if again != out {
			t.Fatal("CLI replay changed recovery evidence")
		}
		dest := filepath.Join(root, "download")
		var message strings.Builder
		if err = resultCommand(ctx, []string{j.ID, "--queue", tls.URL, "--token", c.Token, "--recovered", record.ResultID, "--out", dest}, &message); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(message.String(), "recovered result") {
			t.Fatal("download did not identify recovery")
		}
		if data, err := os.ReadFile(filepath.Join(dest, "nested", "out.txt")); err != nil || string(data) != "retained output" {
			t.Fatalf("download: %q %v", data, err)
		}
		status := captureJoiningOutput(t, func() error { return run([]string{"status", "--queue", tls.URL, "--token", c.Token, j.ID}) })
		if !strings.Contains(status, "recovered result "+record.ResultID) || !strings.Contains(status, "original state=failed") {
			t.Fatal("status omitted recovered evidence", status)
		}
		if err = resultCommand(ctx, []string{j.ID, "--queue", tls.URL, "--token", c.Token, "--out", filepath.Join(root, "normal")}, io.Discard); err == nil {
			t.Fatal("CLI silently promoted recovery")
		}
		for _, kind := range []string{"symlink", "fifo"} {
			path := filepath.Join(root, kind)
			if kind == "symlink" {
				err = os.Symlink(input, path)
			} else {
				err = syscall.Mkfifo(path, 0600)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err = resultCommand(ctx, []string{"recover", j.ID, "--queue", tls.URL, "--token", c.Token, "--key", "invalid-source", "--by", "owner", "--reason", "source validation", "--file", "nested/out.txt=" + path}, io.Discard); err == nil || !strings.Contains(err.Error(), "regular files") {
				t.Fatalf("special source %s: %v", kind, err)
			}
		}
	}

	{
		root, outside := t.TempDir(), t.TempDir()
		if err := os.WriteFile(filepath.Join(outside, "out.txt"), []byte("outside source root"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, filepath.Join(root, "nested")); err != nil {
			t.Fatal(err)
		}
		custodyTransfers.Store(0)
		custody.Store(true)
		err := resultCommand(context.Background(), []string{"recover", "job", "--queue", tls.URL, "--token", token, "--files-from", root, "--key", "containment", "--by", "owner", "--reason", "retained bytes"}, io.Discard)
		if err == nil || custodyTransfers.Load() != 0 {
			t.Fatalf("source escaped --files-from: err=%v file requests=%d", err, custodyTransfers.Load())
		}
		custody.Store(false)
	}

	{
		root := t.TempDir()
		outside := t.TempDir()
		if err := os.Symlink(outside, filepath.Join(root, "nested")); err != nil {
			t.Fatal(err)
		}
		custodyTransfers.Store(0)
		custody.Store(true)
		err := resultCommand(context.Background(), []string{"job", "--queue", tls.URL, "--token", token, "--out", root}, io.Discard)
		transfers := custodyTransfers.Load()
		if err == nil || !strings.Contains(err.Error(), "directory") || transfers != 0 {
			t.Fatalf("link accepted: %v, transfers %d", err, transfers)
		}
		if files, err := os.ReadDir(outside); err != nil || len(files) != 0 {
			t.Fatal("outside destination changed", err)
		}
		custody.Store(false)
	}

	{
		data := []byte("accepted gate result\n")
		hash := sha256.Sum256(data)
		ref := myce.FileRef{SHA256: hex.EncodeToString(hash[:]), SizeBytes: int64(len(data))}
		retryData, retryRef = data, ref
		transient.Store(true)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		out := t.TempDir()
		var diagnostics strings.Builder
		err := resultCommand(ctx, []string{"job", "--queue", tls.URL, "--token", token, "--out", out}, &diagnostics)
		downloads := int(retryDownloads.Load())
		if err != nil || downloads != 2 {
			t.Fatal(err, downloads, diagnostics.String())
		}
		got, err := os.ReadFile(filepath.Join(out, "gate-result.txt"))
		if err != nil || string(got) != string(data) {
			t.Fatal(string(got), err)
		}
		for _, cause := range []string{"HTTP 503", "unexpected_EOF"} {
			if !strings.Contains(diagnostics.String(), cause) {
				t.Fatal("missing retry cause", cause, diagnostics.String())
			}
		}
		if strings.Contains(diagnostics.String(), "fixture") {
			t.Fatal("credential printed")
		}
		transient.Store(false)
	}

	{
		rejectObserver.Store(true)
		pinned, err := (myce.Invitation{Version: 1, Fingerprint: strings.Repeat("00", 32), Token: "private-fixture-token"}).Client(tls.URL)
		if err != nil {
			t.Fatal(err)
		}
		defer pinned.HTTP.CloseIdleConnections()
		for name, client := range map[string]myce.Client{
			"certificate": {URL: tls.URL, Token: "private-fixture-token"},
			"pin":         pinned,
		} {
			t.Run(name, func(t *testing.T) {
				dir := t.TempDir()
				payload := filepath.Join(dir, "notification.json")
				hook := filepath.Join(dir, "notify")
				if err := os.WriteFile(hook, []byte("#!/bin/sh\ncat > "+shellArg(payload)+"\n"), 0700); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				r := profileReceipt{Key: "key", Project: "test", JobID: "fixture", Status: "queued", Observation: "armed", Delivery: "armed"}
				if err := observeProfile(ctx, client, dir, r, profileObserver{Notify: hook}); err == nil || ctx.Err() != nil {
					t.Fatal("TLS identity failure did not promptly end observation", err, ctx.Err())
				}
				var saved profileReceipt
				if err := readProfileJSON(filepath.Join(dir, "receipt.json"), &saved); err != nil || saved.Status != "queued" || saved.Observation != "not_armed" || saved.ObservationError != "tls_identity" || saved.DeliveryStatus != "delivered" {
					t.Fatal(saved, err)
				}
				var event profileNotification
				if err := readProfileJSON(payload, &event); err != nil || event.Status != "observation_failed" || event.ObservationError != "tls_identity" || event.DeliveryID == "" {
					t.Fatal("TLS failure wake missing or unsanitized", event, err)
				}
				data, err := os.ReadFile(payload)
				if err != nil || strings.Contains(string(data), client.Token) || strings.Contains(string(data), tls.URL) || strings.Contains(string(data), "x509") {
					t.Fatal("notification leaked authority diagnostics", err)
				}
			})
		}
		rejectObserver.Store(false)
	}
}
