package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSubmitCLISurfacesJobSpecReason(t *testing.T) {
	const reason = "main Docker memory must fit needs.host_memory_gb"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(400)
		_, _ = w.Write([]byte(`{"code":"job_spec_invalid","error":"` + reason + `"}`))
	}))
	defer server.Close()
	t.Setenv("MYCE_TOKEN", "test")
	path := filepath.Join(t.TempDir(), "job.yaml")
	if err := os.WriteFile(path, []byte("name: invalid\nproject: test\n"), 0600); err != nil {
		t.Fatal(err)
	}
	err := run([]string{"submit", "--queue", server.URL, path})
	if err == nil || !strings.Contains(err.Error(), reason) {
		t.Fatalf("CLI lost reason: %v", err)
	}
}
