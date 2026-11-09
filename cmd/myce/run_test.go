package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"io"
	"mycelium/internal/myce"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestRunInstalledPinnedAuthority(t *testing.T) {
	// An explicit plain command retains installed-command behavior even in a
	// project with an adapter; --entry/--adapter select appended entry arguments.
	t.Chdir(t.TempDir())
	if err := os.Mkdir(".mycelium", 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(".mycelium/adapter.yaml", []byte("not a selected adapter"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MYCE_TOKEN", "")
	t.Setenv("MYCE_QUEUE", "")
	t.Setenv("MYCE_JOINING_FILE", "")
	home := t.TempDir()
	t.Setenv("HOME", home)
	var submitted myce.Spec
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer installed-private-token" {
			t.Error("missing installed authority")
		}
		switch r.URL.Path {
		case "/api/jobs":
			if err := json.NewDecoder(r.Body).Decode(&submitted); err != nil {
				t.Error(err)
			}
			io.WriteString(w, `{"id":"pinned-job"}`)
		case "/api/jobs/pinned-job":
			io.WriteString(w, `{"id":"pinned-job","state":"succeeded"}`)
		default:
			io.WriteString(w, `[{"seq":1,"text":"hi\n"}]`)
		}
	}))
	defer server.Close()
	hash := sha256.Sum256(server.Certificate().Raw)
	authority := filepath.Join(home, "joining.json")
	if err := myce.WriteInvitation(authority, myce.Invitation{Version: 1, Fingerprint: hex.EncodeToString(hash[:]), Token: "installed-private-token", Address: server.URL}); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(home, ".local", "share", "mycelium", "worker", "config.json")
	if err := os.MkdirAll(filepath.Dir(config), 0700); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(myce.ServiceConfig{Role: "worker", JoiningFile: authority})
	if err := os.WriteFile(config, data, 0600); err != nil {
		t.Fatal(err)
	}
	var out, diagnostics bytes.Buffer
	if err := runCommand(context.Background(), []string{"--cpu", "2", "--mem", "512M", "--project", "demo", "--class", "experiment", "--", "printf", "%s", "two words", "--flag"}, &out, &diagnostics); err != nil || out.String() != "hi\n" {
		t.Fatalf("pinned run: %v output=%q", err, out.String())
	}
	j := myce.Job{Spec: submitted}
	if j.Spec.Needs.CPUCores != 2 || j.Spec.Needs.HostMemoryGB != .5 || !reflect.DeepEqual(j.Spec.Run.Command, []string{"printf", "%s", "two words", "--flag"}) {
		t.Fatalf("spec: %+v", j.Spec)
	}
	// An explicit endpoint keeps the installed pin, and sends no credential to a
	// different certificate. This is the real TLS client, not a mocked transport.
	requests := 0
	wrong := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests++ }))
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
	client, err := runClient(config, "", wrong.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	defer client.HTTP.CloseIdleConnections()
	if err = client.Call(context.Background(), "GET", "/api/status", nil, nil); err == nil || requests != 0 {
		t.Fatal("installed pin allowed a different certificate or sent credentials")
	}
	if _, err = runClient(config, "", strings.Replace(wrong.URL, "https:", "http:", 1), ""); err == nil {
		t.Fatal("installed trust allowed HTTP downgrade")
	}
}
