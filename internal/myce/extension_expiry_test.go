package myce

import (
	"os"
	"path/filepath"
	"slices"
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
	// The heartbeat uses the scan it just pruned. A retired registration must
	// disappear from the selected device and all other devices in that reply.
	retired := install("4")
	old := clock.Now().Add(-DefaultExtensionRetention)
	touchExtensionUse(root, retired, old)
	m := Machine{Device: "0", Devices: []MachineDevice{{ID: "0", Platform: "fake"}, {ID: "1", Platform: "fake"}}}
	collectWorkerExtensionReports(root, clock.Now(), DefaultExtensionRetention, &m)
	for i, reports := range [][]ExtensionReport{m.ExtensionReports, m.Devices[0].ExtensionReports, m.Devices[1].ExtensionReports} {
		seen := map[string]bool{}
		for _, r := range reports {
			seen[r.Version] = true
		}
		if seen["4"] || !seen["2"] || !seen["3"] {
			t.Fatalf("heartbeat view %d reported versions %v; want only 2 and 3", i, seen)
		}
	}
	// Model a quarantine between target reports from a shared library scan.
	// An already-written device receipt can survive the registration rename.
	if err := writeExtensionReports(root, used, "fake", []ExtensionReport{
		{Name: used.Name, Version: used.Version, Digest: used.Digest, Work: "kind:sample", Device: "1", Platform: "fake", Fingerprint: "fixture", Qualified: true},
	}); err != nil {
		t.Fatal(err)
	}
	scanned, err := readLocalExtensions(root, true)
	if err != nil {
		t.Fatal(err)
	}
	target := m.Devices[1].machine(m)
	qualified := func(r ExtensionReport) bool { return r.Version == used.Version && r.Qualified }
	if reports := localExtensionReports(root, "fake", "fixture", scanned, nil, &target); !slices.ContainsFunc(reports, qualified) {
		t.Fatalf("missing the live device receipt before quarantine: %+v", reports)
	}
	m.ExtensionReports = localExtensionReports(root, "fake", "fixture", scanned, nil, &target)
	from := extensionFolder(root, used)
	if err := os.Rename(from, filepath.Join(root, "quarantine")); err != nil {
		t.Fatal(err)
	}
	m.Devices[1].ExtensionReports = localExtensionReports(root, "fake", "fixture", scanned, nil, &target)
	if !slices.ContainsFunc(m.Devices[1].ExtensionReports, qualified) {
		t.Fatal("fixture did not retain the later device's qualification receipt")
	}
	removeVanishedExtensionReports(root, &m, scanned)
	for _, reports := range [][]ExtensionReport{m.ExtensionReports, m.Devices[1].ExtensionReports} {
		if slices.ContainsFunc(reports, func(r ExtensionReport) bool { return r.Version == used.Version }) ||
			!slices.ContainsFunc(reports, func(r ExtensionReport) bool { return r.Version == busy.Version }) {
			t.Fatalf("quarantined registration reported or healthy registration missing: %+v", reports)
		}
	}
}

// A registration installed before use markers existed has an unknown last use.
// It must not be pruned on the upgraded worker's first pass, because weekly
// production work would then re-qualify right before it runs.
func TestUnmarkedExtensionRegistrationGetsFullWindow(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)}
	root := t.TempDir()
	e, err := extensionFromFiles(fakeExtension(t).Files)
	if err != nil {
		t.Fatal(err)
	}
	if err := InstallExtension(root, e); err != nil {
		t.Fatal(err)
	}
	clock.Advance(60 * 24 * time.Hour)
	if removed := pruneUnusedExtensions(root, clock.Now(), DefaultExtensionRetention); len(removed) != 0 {
		t.Fatalf("unmarked registration pruned on first sight: %v", removed)
	}
	clock.Advance(DefaultExtensionRetention - time.Hour)
	if removed := pruneUnusedExtensions(root, clock.Now(), DefaultExtensionRetention); len(removed) != 0 {
		t.Fatalf("unmarked registration pruned inside its window: %v", removed)
	}
	clock.Advance(2 * time.Hour)
	if removed := pruneUnusedExtensions(root, clock.Now(), DefaultExtensionRetention); len(removed) != 1 {
		t.Fatalf("unmarked registration not pruned after a full unused window: %v", removed)
	}
}
