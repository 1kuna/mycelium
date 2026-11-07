package myce

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
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

// #805 requires preservation of the last verifiable copy. Unlike #720,
// expired or unavailable originals cannot authorize deletion to admit new work.
func TestFileCacheEvictionRetainsUnavailableOriginal(t *testing.T) {
	for _, status := range []int{404, 410, 503} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			q, clock := fixture(t)
			if err := q.ConfigureFileStore(t.TempDir(), 1024, true); err != nil {
				t.Fatal(err)
			}
			api := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
			defer api.Close()
			client := Client{URL: api.URL, Token: "test"}
			root := t.TempDir()
			cache := FileCache{Root: root, Limit: 8, Clock: clock, Client: client}
			src := filepath.Join(t.TempDir(), "src")
			var old FileRef
			for i, data := range []string{"old-data", "new-data"} {
				if err := os.WriteFile(src, []byte(data), 0600); err != nil {
					t.Fatal(err)
				}
				ref, err := IdentifyFile(src)
				if err != nil {
					t.Fatal(err)
				}
				if err := client.UploadFile(context.Background(), src, ref); err != nil {
					t.Fatal(err)
				}
				if i == 0 {
					old = ref
				} else {
					cache.Client.HTTP = &http.Client{Transport: fetchTransport(func(r *http.Request) (*http.Response, error) {
						if strings.Contains(r.URL.Path, old.SHA256) {
							return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(""))}, nil
						}
						return http.DefaultTransport.RoundTrip(r)
					})}
				}
				clock.Advance(time.Second)
				err = cache.copyTo(context.Background(), ref, filepath.Join(t.TempDir(), "private"))
				if i == 0 && err != nil {
					t.Fatal(err)
				}
				if i == 1 && err == nil {
					t.Fatal("new copy evicted an entry whose original is unavailable")
				}
			}
			if _, err := os.Stat(filepath.Join(root, old.SHA256)); err != nil {
				t.Fatal("last copy lost", err)
			}
		})
	}
}
