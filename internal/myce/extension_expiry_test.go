package myce

import (
	"os"
	"strings"
	"testing"
	"time"
)

func TestUnusedExtensionRegistrationExpires(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)}
	root := t.TempDir()
	install := func(version string) Extension {
		e := fakeExtension(t)
		e.Files["extension.yaml"] = []byte(strings.Replace(string(e.Files["extension.yaml"]), "version: '1'", "version: '"+version+"'", 1))
		e, err := extensionFromFiles(e.Files)
		if err != nil {
			t.Fatal(err)
		}
		if err := InstallExtension(root, e); err != nil {
			t.Fatal(err)
		}
		touchExtensionUse(root, e, clock.Now())
		return e
	}
	stale, used, busy := install("1"), install("2"), install("3")
	unlock, err := extensionLock(root, busy, "fake", true)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	clock.Advance(DefaultExtensionRetention - time.Hour)
	touchExtensionUse(root, used, clock.Now())
	if removed := pruneUnusedExtensions(root, clock.Now(), DefaultExtensionRetention); len(removed) != 0 {
		t.Fatalf("pruned inside the window: %v", removed)
	}
	clock.Advance(2 * time.Hour)
	removed := pruneUnusedExtensions(root, clock.Now(), DefaultExtensionRetention)
	if len(removed) != 1 || removed[0] != extensionKey(stale) {
		t.Fatalf("removed %v, want only %s", removed, extensionKey(stale))
	}
	if _, err := os.Stat(extensionFolder(root, stale)); !os.IsNotExist(err) {
		t.Fatalf("stale registration still installed: %v", err)
	}
	reports := LocalExtensionReports(root, "fake", "")
	seen := map[string]bool{}
	for _, r := range reports {
		seen[r.Version] = true
	}
	if seen["1"] || !seen["2"] || !seen["3"] {
		t.Fatalf("reported versions %v; want 2 (recently used) and 3 (in use), not 1", seen)
	}
}
