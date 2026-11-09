package main

import (
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
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
