package myce

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEphemeralDarwinWorkerRefusesAssignmentBeforeStaging(t *testing.T) {
	_, clock := fixture(t)
	dir := newTestStateRoot(t)
	// Model a stale assignment with former check-in claims for every
	// ephemeral capability. The worker must requalify before staging.
	m := ephemeralMachine()
	m.Capabilities = append(m.Capabilities, ephemeralLegacyCapability, ephemeralSnapshotsCapability)
	j := Job{ID: "fixture", Attempt: 1, Machine: m.ID, Session: m.Session, Spec: ephemeralSpec()}
	j.Spec.Files = FileManifest{"fixture.txt": {SHA256: strings.Repeat("a", 64), SizeBytes: 16}}
	j.Spec.Run.Command = []string{"/bin/sh", "-c", "cat fixture.txt > out.txt"}
	s, err := newAttemptStore(&workerStore{dir: dir}, j)
	if err != nil {
		t.Fatal(err)
	}
	w := Worker{StateDir: dir, Machine: m, Clock: clock}
	if code, err := w.ephemeralWorkerSupport(); code != ephemeralDarwinCustodyUnqualified || err == nil {
		t.Fatalf("expected unqualified host custody, got %q: %v", code, err)
	}
	p, err := w.start(s, j)
	var refused *recordedStartError
	if p != nil || !errors.As(err, &refused) || err.Error() != ephemeralDarwinCustodyReason || len(refused.facts.Infrastructure) != 1 || !refused.facts.Infrastructure[0].PrelaunchRefused {
		t.Fatalf("expected a recorded refusal before staging, got process=%v error=%v", p != nil, err)
	}
	for _, path := range []string{ephemeralRoot(dir, j), filepath.Join(dir, ephemeralLogName(j))} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("refused assignment has a payload or log root %s: %v", filepath.Base(path), err)
		}
	}
}
