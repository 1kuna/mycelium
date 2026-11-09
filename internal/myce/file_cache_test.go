package myce

import (
	"context"
	"errors"
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

// #720: an unrelated victim must never require queue availability. Its accepted
// retention may have ended, or its HEAD may fail while the new blob is healthy.
func TestFileCacheEvictionIndependentOfVictimStorage(t *testing.T) {
	for _, status := range []int{503} {
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
							t.Errorf("eviction contacted victim storage")
							return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(""))}, nil
						}
						return http.DefaultTransport.RoundTrip(r)
					})}
				}
				clock.Advance(time.Second)
				if err := cache.copyTo(context.Background(), ref, filepath.Join(t.TempDir(), "private")); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := os.Stat(filepath.Join(root, old.SHA256)); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("victim not evicted", err)
			}
		})
	}
}
