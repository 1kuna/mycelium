package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

type storageRetryReport func([]byte)

func (report storageRetryReport) Write(p []byte) (int, error) {
	report(p)
	return len(p), nil
}

func TestProfileStorageFailureCause(t *testing.T) {
	for _, tc := range []struct {
		adapter            bool
		operation, failure string
	}{{false, "preflight", "503"}, {true, "upload", "401"}} {
		adapter, operation, failure := tc.adapter, tc.operation, tc.failure
		t.Run(strings.Join([]string{map[bool]string{false: "profile", true: "entry"}[adapter], operation, failure}, "/"), func(t *testing.T) {
			root := t.TempDir()
			source := filepath.Join(root, "source")
			if err := os.Mkdir(source, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(source, "run.sh"), []byte("true"), 0600); err != nil {
				t.Fatal(err)
			}
			a, err := decodeAdapter([]byte(cpuAdapterYAML("source")))
			if err != nil {
				t.Fatal(err)
			}
			p, _, err := adapterProfile(a, "work")
			if err != nil {
				t.Fatal(err)
			}
			p.Adapter = adapter
			const secret = "fixture-secret-never-print"
			var namedInput atomic.Bool
			var files map[string]struct {
				SHA256    string `json:"sha256"`
				SizeBytes int64  `json:"size_bytes"`
			}
			var reached atomic.Bool
			var server *httptest.Server
			server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if namedInput.Load() {
					switch r.URL.Path {
					case "/api/files/preflight":
						if err := json.NewDecoder(r.Body).Decode(&files); err != nil {
							t.Error(err)
						}
					case "/api/files/input-claims":
						_, _ = io.WriteString(w, `{"id":"fixture-claim"}`)
					default:
						for name, ref := range files {
							if strings.HasSuffix(r.URL.Path, ref.SHA256) {
								if name == "files/dataset.json" || strings.Contains(name, ".myce-inputs/") {
									w.WriteHeader(403)
									_, _ = io.WriteString(w, `{"code":"request_failed","error":"untrusted private detail"}`)
									return
								}
								w.Header().Set("ETag", `"`+ref.SHA256+`"`)
								w.Header().Set("Content-Length", fmt.Sprint(ref.SizeBytes))
								w.WriteHeader(200)
								return
							}
						}
						t.Errorf("unexpected request %s", r.URL.Path)
						w.WriteHeader(400)
					}
					return
				}
				if operation == "upload" && r.URL.Path == "/api/files/preflight" {
					w.WriteHeader(200)
					return
				}
				reached.Store(true)
				status := 503
				if failure == "401" {
					status = 401
				}
				w.WriteHeader(status)
				_ = json.NewEncoder(w).Encode(map[string]string{"code": "request_failed", "error": secret + " https://private.invalid/path"})

			}))
			defer server.Close()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			want := map[string]string{"503": "HTTP 503 code=request_failed", "401": "HTTP 401 code=request_failed"}[failure]
			reports := 0
			report := storageRetryReport(func(p []byte) {
				reports++
				diagnostic := string(p)
				for _, part := range []string{"operation=" + operation, `input="source"`, want, "same intent retained"} {
					if !strings.Contains(diagnostic, part) {
						t.Errorf("retry report missing %q: %s", part, diagnostic)
					}
				}
				for _, bad := range []string{secret, "private.invalid", root, server.URL} {
					if strings.Contains(diagnostic, bad) {
						t.Errorf("unsafe retry report: %s", diagnostic)
					}
				}
				// This fixture inspects a transient cause, not recovery. Its
				// owner interrupts when informed, before any retry pause.
				// Without that owner action, generic outages correctly retry
				// indefinitely; an HTTP status is not a permanent refusal.
				cancel()
			})
			var out bytes.Buffer
			err = runPreparedProfile(ctx, root, p, []byte("fixture"), nil, nil, "storage-cause", "", "", "", server.URL, secret, &out, report)
			if exitCode(err) != 69 {
				t.Fatalf("exit=%d err=%v", exitCode(err), err)
			}
			transient := failure != "401"
			if transient && reports == 0 || !transient && reports != 0 {
				t.Fatalf("transient=%v, retry reports=%d", transient, reports)
			}
			for _, part := range []string{"operation=" + operation, `input="source"`, want, "intent retained", "rerunning the same command resumes from the receipt without duplicating anything"} {
				if !strings.Contains(err.Error(), part) {
					t.Errorf("missing %q: %v", part, err)
				}
			}
			for _, bad := range []string{secret, "private.invalid", root, server.URL} {
				if strings.Contains(err.Error(), bad) {
					t.Errorf("unsafe diagnostic: %v", err)
				}
			}
			var receipt profileReceipt
			if e := json.Unmarshal(out.Bytes(), &receipt); e != nil {
				t.Fatal(e)
			}
			saved := filepath.Join(root, ".mycelium", "receipts", profileHash([]string{p.Project, "storage-cause"}), "receipt.json")
			var retained profileReceipt
			if e := readProfileJSON(saved, &retained); e != nil || retained.Key != receipt.Key || retained.Status != "intent" || len(retained.Uploads) == 0 {
				t.Fatalf("intent not retained: %v", e)
			}
			if !reached.Load() {
				t.Fatal("fake server not reached")
			}
			if !adapter {
				input := filepath.Join(root, "private-local-file")
				if err := os.WriteFile(input, []byte("fixture"), 0600); err != nil {
					t.Fatal(err)
				}
				itemInputs := map[string]string{"dataset.json": input}
				namedInput.Store(true)
				for attempt := 0; attempt < 2; attempt++ {
					var out bytes.Buffer
					err = runPreparedProfile(context.Background(), root, p, []byte("fixture"), nil, itemInputs, "named-input", "", "", "", server.URL, "fixture-token", &out, io.Discard)
					if exitCode(err) != 69 || !strings.Contains(err.Error(), `operation=upload input="dataset.json" HTTP 403`) || strings.Contains(err.Error(), input) {
						t.Fatalf("invocation %d: wrong diagnostic: %v", attempt+1, err)
					}
				}
			}

		})
	}
}
