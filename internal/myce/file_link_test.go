package myce

import (
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
)

// macOS smbfs refuses link() with EOPNOTSUPP (#2152). Publication, bundle
// registration and trash restore copy and verify instead, and never leave a
// partial copy under a blob name.
func TestFileStoreWithoutHardLinks(t *testing.T) {
	q, _, c := retentionFixture(t)
	var refused atomic.Int32
	q.files.link = func(old, new string) error {
		refused.Add(1)
		return &os.LinkError{Op: "link", Old: old, New: new, Err: syscall.EOPNOTSUPP}
	}
	verified := func(ref FileRef, content string) {
		t.Helper()
		if err := verifyFile(t.Context(), q.files.blob(ref), ref); err != nil {
			t.Fatal(err)
		}
		if data, err := os.ReadFile(q.files.blob(ref)); err != nil || string(data) != content {
			t.Fatalf("blob = %q, %v; want %q", data, err, content)
		}
	}
	noTemporaries := func() {
		t.Helper()
		entries, err := os.ReadDir(filepath.Join(q.files.root, "uploads"))
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			if strings.HasPrefix(e.Name(), ".") {
				t.Fatal("temporary copy left behind:", e.Name())
			}
		}
	}

	// Upload publication.
	const content = "published without a hard link"
	_, input := retentionJobInput(t, c, content)
	verified(input, content)
	if _, err := os.Lstat(filepath.Join(q.files.root, "uploads", input.SHA256)); !os.IsNotExist(err) {
		t.Fatal("upload source survived publication", err)
	}
	// A repeat upload of the same bytes deduplicates.
	retentionJobInput(t, c, content)
	verified(input, content)

	// Bundle registration.
	e := fakeExtension(t)
	if err := q.files.freezeExtension(&e); err != nil {
		t.Fatal(err)
	}
	if err := q.files.verify(t.Context(), *e.FilesRef); err != nil {
		t.Fatal(err)
	}

	// Trash move and restore. The restored blob is a new file, never the
	// trash entry renamed back (#2125).
	entry, err := q.files.trashBlob(input.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.files.restoreBlob(input.SHA256); err != nil {
		t.Fatal(err)
	}
	verified(input, content)
	if _, err := os.Stat(entry); err != nil {
		t.Fatal("restore consumed the trash entry", err)
	}
	if refused.Load() < 3 {
		t.Fatal("fallback not exercised; link attempts:", refused.Load())
	}
	noTemporaries()

	// Bytes that do not match their digest never appear under a blob name.
	bad := filepath.Join(q.files.root, "uploads", ".bad")
	if err := os.WriteFile(bad, []byte("not the named bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(bad)
	missing := FileRef{SHA256: strings.Repeat("ab", 32)}
	if err := q.files.linkBlob(bad, q.files.blob(missing), missing.SHA256); err == nil {
		t.Fatal("mismatched copy was published")
	}
	if _, err := os.Lstat(q.files.blob(missing)); !os.IsNotExist(err) {
		t.Fatal("partial or wrong bytes visible under a blob name", err)
	}
	os.Remove(bad)
	noTemporaries()
}
