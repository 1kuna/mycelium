package myce

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDownloadFileRelativeSymlinkedCWD(t *testing.T) {
	physical := t.TempDir()
	linked := filepath.Join(t.TempDir(), "linked")
	if err := os.Symlink(physical, linked); err != nil {
		t.Fatal(err)
	}
	t.Chdir(linked)
	body := "verified result\n"
	ref := FileRef{SHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(body))), SizeBytes: int64(len(body))}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"`+ref.SHA256+`"`)
		fmt.Fprint(w, body)
	}))
	defer server.Close()
	c := Client{URL: server.URL, Token: "fixture"}
	if err := c.DownloadFile(context.Background(), ref, "accepted/result.txt"); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(filepath.Join(physical, "accepted/result.txt")); err != nil || string(got) != body {
		t.Fatalf("wrong bytes: %q %v", got, err)
	}
	// Canonicalizing the parent must never canonicalize the destination leaf:
	// otherwise an existing link could overwrite unrelated bytes.
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(outside, []byte("untouched"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, "accepted/link.txt"); err != nil {
		t.Fatal(err)
	}
	if err := c.DownloadFile(context.Background(), ref, "accepted/link.txt"); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("destination link accepted: %v", err)
	}
	if got, err := os.ReadFile(outside); err != nil || string(got) != "untouched" {
		t.Fatal("outside file changed", err)
	}
}
