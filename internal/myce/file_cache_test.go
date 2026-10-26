package myce

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"
)

func TestFileCacheLargeInputUsesProgressBudget(t *testing.T) {
	ref := sharedFixtureFile(t, t.TempDir(), "input", "slow healthy input")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(10 * time.Millisecond)
		w.Header().Set("ETag", fmt.Sprintf("%q", ref.SHA256))
		w.Header().Set("Content-Length", fmt.Sprint(ref.SizeBytes))
		if r.Method != "HEAD" {
			_, _ = w.Write([]byte("slow healthy input"))
		}
	}))
	defer server.Close()
	cache := FileCache{Root: t.TempDir(), Limit: 1 << 20, Clock: RealClock{}, Client: Client{URL: server.URL, HTTP: &http.Client{Timeout: time.Millisecond}}}
	if err := cache.copyTo(context.Background(), ref, filepath.Join(t.TempDir(), "attempt")); err != nil {
		t.Fatal(err)
	}
}
