package node

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

func TestReaperKillsTrackedProcessesAndRemovesFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "processes.json")
	refs := []ProcessRef{{PID: 1, Kind: "process", Ref: "one"}, {PID: 2, Kind: "process", Ref: "two"}}
	if err := WriteProcessRefs(path, refs); err != nil {
		t.Fatalf("WriteProcessRefs: %v", err)
	}
	killer := &recordingKiller{}
	reaped, err := NewReaper(path, killer).Reap(context.Background())
	if err != nil {
		t.Fatalf("Reap: %v", err)
	}
	if len(reaped) != 2 || len(killer.refs) != 2 {
		t.Fatalf("reaped=%+v killed=%+v", reaped, killer.refs)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("state file still exists: %v", err)
	}
}

func TestReaperNoFileIsNoop(t *testing.T) {
	killer := &recordingKiller{}
	reaped, err := NewReaper(filepath.Join(t.TempDir(), "missing.json"), killer).Reap(context.Background())
	if err != nil {
		t.Fatalf("Reap: %v", err)
	}
	if len(reaped) != 0 || len(killer.refs) != 0 {
		t.Fatalf("reaped=%+v killed=%+v", reaped, killer.refs)
	}
}

func TestReaperFailsOnInvalidFileAndKillError(t *testing.T) {
	dir := t.TempDir()
	badJSON := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(badJSON, []byte("{"), 0o600); err != nil {
		t.Fatalf("write bad json: %v", err)
	}
	if _, err := NewReaper(badJSON, &recordingKiller{}).Reap(context.Background()); err == nil {
		t.Fatal("expected invalid json error")
	}

	path := filepath.Join(dir, "processes.json")
	if err := WriteProcessRefs(path, []ProcessRef{{PID: 1, Kind: "process", Ref: "one"}}); err != nil {
		t.Fatalf("WriteProcessRefs: %v", err)
	}
	wantErr := errors.New("kill")
	if _, err := NewReaper(path, &recordingKiller{err: wantErr}).Reap(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("kill err = %v", err)
	}
}

func TestBackendProcessKillerDelegatesToBackendStop(t *testing.T) {
	backend := &stopRecorder{}
	err := BackendProcessKiller{Backend: backend}.Kill(context.Background(), ProcessRef{PID: 42, Kind: "process", Ref: "42"})
	if err != nil {
		t.Fatalf("Kill: %v", err)
	}
	if backend.handle.PID != 42 || backend.handle.Ref != "42" {
		t.Fatalf("handle = %+v", backend.handle)
	}
}

type recordingKiller struct {
	refs []ProcessRef
	err  error
}

func (k *recordingKiller) Kill(_ context.Context, ref ProcessRef) error {
	if k.err != nil {
		return k.err
	}
	k.refs = append(k.refs, ref)
	return nil
}

type stopRecorder struct {
	handle ports.Handle
}

func (s *stopRecorder) Name() string {
	return "stop-recorder"
}

func (s *stopRecorder) Launch(context.Context, domain.Preset, string) (ports.Handle, error) {
	panic("not used")
}

func (s *stopRecorder) WaitReady(context.Context, string) error {
	panic("not used")
}

func (s *stopRecorder) Stop(_ context.Context, h ports.Handle) error {
	s.handle = h
	return nil
}

var _ ports.BackendAdapter = (*stopRecorder)(nil)
