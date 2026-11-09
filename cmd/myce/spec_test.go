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

}
