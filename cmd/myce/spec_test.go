package main

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"mycelium/internal/myce"
)

const validJobYAML = `name: offline
project: example
run:
  command: [echo, ok]
needs:
  cpu_cores: 1
  memory_gb: 1
`

const validExtensionYAML = `name: offline
project: example
kind: render
`

func TestSpecCheck(t *testing.T) {
	for _, tc := range []struct {
		name, body, want string
	}{
		{"yaml", validJobYAML, ""},
		{"json", `{"name":"offline","project":"example","run":{"command":["echo","ok"]},"needs":{"cpu_cores":1,"memory_gb":1}}`, ""},
		{"extension", validExtensionYAML, ""},
		{"unknown", validJobYAML + "typo: true\n", "field typo not found"},
		{"extension cpu override", validExtensionYAML + "needs: {cpu_cores: 1}\n", "extension-owned commands, environments and hardware reservations"},
		{"extension platform override", validExtensionYAML + "needs: {platform: cpu}\n", "extension-owned commands, environments and hardware reservations"},
		{"semantic error", "name: incomplete\n", "name, project and run.command are required"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "job.yaml")
			if err := os.WriteFile(path, []byte(tc.body), 0600); err != nil {
				t.Fatal(err)
			}
			var out, stderr bytes.Buffer
			err := specCommand([]string{"check", path}, &out, &stderr)
			wantExit := 0
			if tc.want != "" {
				wantExit = 1
				if err == nil || !strings.Contains(err.Error(), tc.want) || !strings.Contains(err.Error(), path) {
					t.Fatalf("want file and %q in error, got %v", tc.want, err)
				}
			}
			if exitCode(err) != wantExit {
				t.Fatalf("exit=%d want=%d err=%v", exitCode(err), wantExit, err)
			}
			if tc.want == "" && !strings.Contains(out.String(), "valid (static checks only)") {
				t.Fatalf("missing success message: %q", out.String())
			}
			if tc.want != "" && out.Len() != 0 {
				t.Fatalf("invalid spec printed success: %q", out.String())
			}
		})
	}
}

func TestSpecCheckHelpAndUsage(t *testing.T) {
	for _, args := range [][]string{{"--help"}, {"check", "--help"}} {
		var stderr bytes.Buffer
		if err := specCommand(args, io.Discard, &stderr); err != nil {
			t.Fatal(err)
		}
		for _, text := range []string{"static validity only", "live eligibility, admission or qualification", "no fleet authority", "Exit codes:"} {
			if !strings.Contains(stderr.String(), text) {
				t.Errorf("help missing %q: %s", text, &stderr)
			}
		}
	}
	for _, args := range [][]string{nil, {"other"}, {"check"}, {"check", "a", "b"}, {"check", "--files-from", "inputs", "job.yaml"}} {
		if err := specCommand(args, io.Discard, io.Discard); exitCode(err) != 2 {
			t.Errorf("args=%v exit=%d err=%v", args, exitCode(err), err)
		}
	}
	missing := filepath.Join(t.TempDir(), "missing.yaml")
	if err := specCommand([]string{"check", missing}, io.Discard, io.Discard); exitCode(err) != 1 || !strings.Contains(err.Error(), missing) {
		t.Fatalf("missing file error: %v", err)
	}
}

type specCheckTransport struct{ calls int }

func (s *specCheckTransport) RoundTrip(*http.Request) (*http.Response, error) {
	s.calls++
	return nil, errors.New("offline check attempted HTTP")
}

func TestSpecCheckOfflineNoAuthorityNetworkOrUploads(t *testing.T) {
	transport := &specCheckTransport{}
	previous := http.DefaultTransport
	http.DefaultTransport = transport
	t.Cleanup(func() { http.DefaultTransport = previous })
	t.Setenv("MYCE_TOKEN", "")
	t.Setenv("MYCE_QUEUE", "http://offline.invalid")
	// A broken explicit authority must never be read by the offline command.
	t.Setenv("MYCE_JOINING_FILE", filepath.Join(t.TempDir(), "missing-invitation"))
	path := filepath.Join(t.TempDir(), "job.yaml")
	// Metadata can be statically valid without the referenced input being present.
	body := validJobYAML + "files:\n  input.txt:\n    sha256: " + strings.Repeat("a", 64) + "\n    size_bytes: 1\n"
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"spec", "check", path}); err != nil {
		t.Fatal(err)
	}
	if transport.calls != 0 {
		t.Fatalf("offline validation made %d HTTP requests (including possible uploads)", transport.calls)
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != body {
		t.Fatalf("check changed its input: %v", err)
	}
}

// Resolve names the side that cannot see each missing path, through the real
// dev API: the submitter's input and the queue's inline environment lock.
// An adapter's source warns when large bundled members defeat deduplication.
func TestSpecCheckResolveNamesSideAndSourceBundles(t *testing.T) {
	root := t.TempDir()
	d, err := myce.StartDev(myce.DevConfig{EnvironmentCacheLimit: 1 << 30, Workers: 1, GPU: "cpu", Address: "127.0.0.1:0", Database: filepath.Join(root, "queue.db"), Checkpoints: filepath.Join(root, "checkpoints"), Token: "dev-spec-token", Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	authority := []string{"--queue", d.URL, "--token", "dev-spec-token"}
	inputs := filepath.Join(root, "inputs")
	if err := os.MkdirAll(inputs, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inputs, "data.txt"), []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}
	job := filepath.Join(root, "job.yaml")
	body := validJobYAML + "inputs: {data.txt: data.txt, absent.txt: absent.txt}\nenvironment: {lock_file: not-on-queue/uv.lock, python: '3.12'}\n"
	if err := os.WriteFile(job, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	err = specCommand(append(append([]string{"check", "--resolve", "--files-from", inputs}, authority...), job), &out, io.Discard)
	text := out.String()
	if exitCode(err) != 1 || !strings.Contains(text, "inputs.absent.txt [submitter]") || !strings.Contains(text, `"absent.txt" (base files-from): missing`) {
		t.Fatalf("submitter side not named: %v\n%s", err, text)
	}
	if !strings.Contains(text, "environment.lock_file [queue]") || !strings.Contains(text, "read on the queue host") || !strings.Contains(text, "environment: NAME") {
		t.Fatalf("queue side or library fix not named: %s", text)
	}

	source := filepath.Join(root, "source")
	if err := os.MkdirAll(source, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "run.sh"), []byte("echo ok > result\n"), 0700); err != nil {
		t.Fatal(err)
	}
	bundle, err := os.Create(filepath.Join(source, "tools.tar"))
	if err != nil {
		t.Fatal(err)
	}
	if err := bundle.Truncate(17 << 20); err != nil {
		t.Fatal(err)
	}
	bundle.Close()
	adapter := filepath.Join(root, "adapter.yaml")
	if err := os.WriteFile(adapter, []byte(cpuAdapterYAML(source)), 0600); err != nil {
		t.Fatal(err)
	}
	// The dev worker's first check-in is asynchronous.
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		out.Reset()
		err = specCommand(append(append([]string{"check", "--resolve"}, authority...), adapter), &out, io.Discard)
		if err == nil || time.Now().After(deadline) {
			break
		}
	}
	text = out.String()
	if !strings.Contains(text, "source [submitter]") || !strings.Contains(text, "tools.tar (17 MiB)") || !strings.Contains(text, "shared_inputs") || !strings.Contains(text, "fake-1") {
		t.Fatalf("source bundle warning or eligible target missing: %v %s", err, text)
	}
}
