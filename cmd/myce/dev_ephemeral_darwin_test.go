package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDevResolveEphemeralRefusal(t *testing.T) {
	binary := devTestBinary(t)
	path := filepath.Join(t.TempDir(), "ephemeral.yaml")
	body := validJobYAML + "advanced: {worker_data: ephemeral}\noutputs: {out.txt: results}\n"
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var out, stderr bytes.Buffer
	err := devCommandSession(ctx, []string{binary, "spec", "check", "--resolve", path}, nil, &out, &stderr)
	if exitCode(err) != 1 ||
		!strings.Contains(out.String(), "worker_data_ephemeral_v2 worker capability required: ephemeral worker data is supported only on Linux") ||
		strings.Contains(out.String(), "native_isolation_unavailable") {
		t.Fatalf("expected an ephemeral placement refusal: %v\n%s\n%s", err, out.String(), stderr.String())
	}
}
