package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"mycelium/internal/myce"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestProfileObserverTimeoutAndTransport(t *testing.T) {
	for _, mode := range []string{"timeout", "transport_error", "interrupted"} {
		t.Run(mode, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if mode == "transport_error" {
					w.WriteHeader(401)
					return
				}
				io.WriteString(w, `{"id":"fixture","state":"queued"}`)
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			if mode == "interrupted" {
				cancel()
			}
			dir := t.TempDir()
			hook := filepath.Join(dir, "notify")
			payload := filepath.Join(dir, "notification.json")
			if err := os.WriteFile(hook, []byte("#!/bin/sh\ncat > "+shellArg(payload)+"\n"), 0700); err != nil {
				t.Fatal(err)
			}
			r := profileReceipt{Key: "key", Project: "test", JobID: "fixture", Status: "queued", Observation: "armed", Nonce: "receipt-nonce", Delivery: "armed"}
			if err := observeProfile(ctx, myce.Client{URL: server.URL, Token: "fixture"}, dir, r, profileObserver{Notify: hook}); err == nil {
				t.Fatal("observation should fail")
			}
			var saved profileReceipt
			if err := readProfileJSON(filepath.Join(dir, "receipt.json"), &saved); err != nil || saved.ObservationError != mode || saved.Status != "queued" || saved.Delivery == "armed" {
				t.Fatal(saved, err)
			}
			var notification profileNotification
			if err := readProfileJSON(payload, &notification); err != nil || notification.Status != "observation_failed" || notification.ObservationError != mode || notification.DeliveryID == "" {
				t.Fatal("abnormal end not notified", notification, err)
			}
			terminalID := profileHash([]string{r.Nonce, r.Project, r.Key, r.JobID, "succeeded", ""})
			if notification.DeliveryID == terminalID {
				t.Fatal("failure reused terminal delivery identity")
			}
		})
	}
}

func TestProfileObserverTLSIdentityFailure(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("TLS identity failure must prevent authenticated HTTP requests")
	}))
	defer server.Close()
	pinned, err := (myce.Invitation{Version: 1, Fingerprint: strings.Repeat("00", 32), Token: "private-fixture-token"}).Client(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer pinned.HTTP.CloseIdleConnections()
	for name, client := range map[string]myce.Client{
		"certificate": {URL: server.URL, Token: "private-fixture-token"},
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
			if err != nil || strings.Contains(string(data), client.Token) || strings.Contains(string(data), server.URL) || strings.Contains(string(data), "x509") {
				t.Fatal("notification leaked authority diagnostics", err)
			}
		})
	}
}

// A receiver rejection (as observed with a webhook HTTP 403) is a delivery
// failure, never a failed computation or a confirmed delivered wakeup.
func TestProfileNotifierDeliveryFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"id":"fixture","state":"succeeded"}`)
	}))
	defer server.Close()
	dir := t.TempDir()
	hook := filepath.Join(dir, "notify")
	if err := os.WriteFile(hook, []byte("#!/bin/sh\nexit 7\n"), 0700); err != nil {
		t.Fatal(err)
	}
	receipt := profileReceipt{Key: "key", Project: "test", JobID: "fixture", Status: "running", Observation: "armed", Delivery: "armed"}
	if err := observeProfile(context.Background(), myce.Client{URL: server.URL, Token: "fixture"}, dir, receipt, profileObserver{Notify: hook}); err == nil {
		t.Fatal("delivery rejection hidden")
	}
	var saved profileReceipt
	if err := readProfileJSON(filepath.Join(dir, "receipt.json"), &saved); err != nil || saved.Status != "succeeded" || saved.DeliveryStatus != "failed" || saved.Observation != "not_armed" || saved.Delivery == "armed" {
		t.Fatal("delivery rejection changed computation state", saved, err)
	}
}

func TestProfileObserverRearmDeliveryIdentity(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusUnauthorized) }))
	defer server.Close()
	root := t.TempDir()
	profile := `name: fixture
project: test
class: experiment
source: {git: HEAD}
run: {command: [true]}
targets: [{os: linux, arch: amd64, platform: cpu}]
check: [true]
`
	path := filepath.Join(root, "fixture.yaml")
	if err := os.WriteFile(path, []byte(profile), 0600); err != nil {
		t.Fatal(err)
	}
	key := "rearm"
	dir := filepath.Join(root, ".mycelium", "receipts", profileHash([]string{"test", key}))
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	receipt := profileReceipt{Key: key, Project: "test", JobID: "fixture", Status: "queued", Nonce: rand.Text(), Intent: profileHash(struct {
		Profile string
		Args    []string
	}{profile, []string{"literal"}})}
	if err := saveProfileJSON(filepath.Join(dir, "receipt.json"), receipt); err != nil {
		t.Fatal(err)
	}
	payload := filepath.Join(root, "notification.json")
	hook := filepath.Join(root, "notify")
	if err := os.WriteFile(hook, []byte("#!/bin/sh\nif [ \"${1:-}\" = --check ]; then exit 0; fi\ncat > "+shellArg(payload)+"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	binary := devTestBinary(t)
	ctx, cancel := context.WithTimeout(context.Background(), devTestTimeout)
	defer cancel()
	var ids, arms []string
	for range 2 {
		cmd := exec.CommandContext(ctx, binary, "run", "--queue", server.URL, "--profile", path, "--key", key, "--notify", hook, "--", "literal")
		cmd.Dir = root
		cmd.Env = append(os.Environ(), "MYCE_TOKEN=fixture")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("arm: %v %s", err, out)
		}
		wait(t, ctx, func() bool {
			if readProfileJSON(filepath.Join(dir, "receipt.json"), &receipt) != nil || receipt.ObservationError != "transport_error" || receipt.DeliveryStatus != "delivered" {
				return false
			}
			lock, err := profileLock(filepath.Join(dir, "watch.lock"))
			if err != nil {
				return false
			}
			lock.Close()
			return true
		})
		var notification profileNotification
		data, err := os.ReadFile(payload)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(data, &notification); err != nil || notification.Status != "observation_failed" {
			t.Fatal(notification, err)
		}
		ids = append(ids, notification.DeliveryID)
		arms = append(arms, receipt.ArmNonce)
	}
	if ids[0] == "" || ids[0] == ids[1] || arms[0] == "" || arms[0] == arms[1] {
		t.Fatal("rearmed observation failure was deduplicated", ids, arms)
	}
}

// Reuse TestInstalledClientDevResult's real dev API and pinned installed config.
func testProfileInstalledObserverNotification(t *testing.T, ctx context.Context, binary, root string) {
	t.Helper()
	source := filepath.Join(root, "observer-source")
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "run.sh"), []byte(`test -z "$MYCE_TOKEN" && printf observed > result
`), 0600); err != nil {
		t.Fatal(err)
	}
	profiles := filepath.Join(root, ".mycelium", "profiles")
	if err := os.MkdirAll(profiles, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(profiles, "installed-observer.yaml")
	profile := `name: installed-observer
project: demo
class: experiment
source: {directory: observer-source}
run: {command: [sh, run.sh]}
needs: {cpu_cores: 1, memory_gb: 1}
targets: [{os: ` + runtime.GOOS + `, arch: ` + runtime.GOARCH + `, platform: cpu}]
outputs: {result: results}
check: [test, -s, result]
`
	if err := os.WriteFile(path, []byte(profile), 0600); err != nil {
		t.Fatal(err)
	}
	payload := filepath.Join(root, "installed-notifications")
	hook := filepath.Join(root, "installed-notify")
	script := `#!/bin/sh
test -z "${MYCE_TOKEN+x}" || exit 99
if [ "${1:-}" = --check ]; then exit 0; fi
cat >> ` + shellArg(payload) + `
printf '\n' >> ` + shellArg(payload) + "\n"
	if err := os.WriteFile(hook, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	invoke := func() profileReceipt {
		t.Helper()
		cmd := exec.CommandContext(ctx, binary, "run", "--profile", path, "--key", "installed-observer", "--notify", hook)
		cmd.Dir = root
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("installed profile: %v %s", err, output)
		}
		var receipt profileReceipt
		if err := json.Unmarshal(output, &receipt); err != nil {
			t.Fatal(err)
		}
		return receipt
	}
	dir := filepath.Join(root, ".mycelium", "receipts", profileHash([]string{"demo", "installed-observer"}))
	// This test owns this exact temporary binary/receipt pair. Bound cleanup
	// even when a regression leaves the detached waiter retrying indefinitely.
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		processes, err := exec.CommandContext(cleanupCtx, "ps", "-axo", "pid=,command=").Output()
		if err != nil {
			t.Error("observer cleanup process lookup:", err)
			return
		}
		for _, line := range strings.Split(string(processes), "\n") {
			pidText, command, ok := strings.Cut(strings.TrimSpace(line), " ")
			if !ok || strings.TrimSpace(command) != binary+" wait --receipt "+dir {
				continue
			}
			pid, err := strconv.Atoi(pidText)
			if err != nil {
				t.Error(err)
				continue
			}
			process, err := os.FindProcess(pid)
			if err == nil {
				err = process.Kill()
				process.Release()
			}
			if err != nil && !errors.Is(err, os.ErrProcessDone) {
				t.Error("observer cleanup:", err)
			}
		}
	})
	submitted := invoke()
	receiptPath := filepath.Join(dir, "receipt.json")
	delivered := func() profileReceipt {
		t.Helper()
		var receipt profileReceipt
		wait(t, ctx, func() bool {
			if readProfileJSON(receiptPath, &receipt) != nil || receipt.DeliveryStatus != "delivered" {
				return false
			}
			lock, err := profileLock(filepath.Join(dir, "watch.lock"))
			if err != nil {
				return false
			}
			lock.Close()
			return true
		})
		if receipt.Status != "succeeded" || receipt.Observation != "not_armed" || receipt.Delivery == "armed" {
			t.Fatal("observer did not complete", receipt)
		}
		data, err := os.ReadFile(filepath.Join(dir, "observer.json"))
		var observer profileObserver
		if err != nil || json.Unmarshal(data, &observer) != nil || observer.Config == "" || observer.Joining == "" || strings.Contains(string(data), "installed-dev-fixture-token") {
			t.Fatalf("installed observer authority missing or leaked: %v", err)
		}
		return receipt
	}
	current := delivered()
	// Model a legacy stuck receipt after its owning session has stopped the
	// old waiter and released the lock. The same key must re-arm this job.
	current.Status, current.Observation, current.Delivery, current.DeliveryStatus = "queued", "armed", "armed", ""
	if err := saveProfileJSON(receiptPath, current); err != nil {
		t.Fatal(err)
	}
	if err := saveProfileJSON(filepath.Join(dir, "observer.json"), profileObserver{Queue: "legacy-unused-endpoint", Notify: hook}); err != nil {
		t.Fatal(err)
	}
	if recovered := invoke(); recovered.JobID != submitted.JobID {
		t.Fatal("recovery resubmitted job", recovered)
	}
	delivered()
	data, err := os.ReadFile(payload)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 {
		t.Fatalf("notifier not invoked for initial and recovered observation: %s", data)
	}
	var terminalID string
	for _, line := range lines {
		var event profileNotification
		if err := json.Unmarshal([]byte(line), &event); err != nil || event.JobID != submitted.JobID || event.Status != "succeeded" {
			t.Fatal("bad completion notification", event, err)
		}
		if event.DeliveryID == "" || (terminalID != "" && event.DeliveryID != terminalID) {
			t.Fatal("terminal delivery identity changed across recovery", terminalID, event.DeliveryID)
		}
		terminalID = event.DeliveryID
	}
}
