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
	if err := WriteProcessRefs(path, []ProcessRef{{PID: 1, Kind: "process", Ref: "one"}, {PID: 2, Kind: "process", Ref: "two"}}); err != nil {
		t.Fatalf("WriteProcessRefs: %v", err)
	}
	wantErr := errors.New("kill")
	killer := &recordingKiller{err: wantErr}
	reaped, err := NewReaper(path, killer).Reap(context.Background())
	if !errors.Is(err, wantErr) {
		t.Fatalf("kill err = %v", err)
	}
	if len(reaped) != 2 || len(killer.refs) != 2 {
		t.Fatalf("reaper did not attempt every ref: reaped=%+v killed=%+v", reaped, killer.refs)
	}
	if _, statErr := os.Stat(path); statErr != nil {
		t.Fatalf("failed reap should preserve refs: %v", statErr)
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

func TestReaperFromRefsDoesNotNeedStateFile(t *testing.T) {
	killer := &recordingKiller{}
	reaped, err := NewReaperFromRefs([]ProcessRef{{PID: 7, Kind: "process", Ref: "7"}}, killer).Reap(context.Background())
	if err != nil {
		t.Fatalf("Reap: %v", err)
	}
	if len(reaped) != 1 || len(killer.refs) != 1 || killer.refs[0].PID != 7 {
		t.Fatalf("reaped=%+v killed=%+v", reaped, killer.refs)
	}
}

func TestStoreProcessRegistryAddsAndRemovesRefs(t *testing.T) {
	store := &processRefStore{}
	registry := StoreProcessRegistry{Store: store, NodeID: "node-a"}
	ref := domain.ProcessRef{PID: 9, Kind: "process", Ref: "9"}
	if err := registry.Add(context.Background(), ref); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := registry.Add(context.Background(), ref); err != nil {
		t.Fatalf("Add duplicate: %v", err)
	}
	if len(store.refs["node-a"]) != 1 {
		t.Fatalf("refs = %+v", store.refs)
	}
	if err := registry.Remove(context.Background(), ref); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if _, ok := store.refs["node-a"]; ok {
		t.Fatalf("refs not deleted: %+v", store.refs)
	}
}

type recordingKiller struct {
	refs []ProcessRef
	err  error
}

func (k *recordingKiller) Kill(_ context.Context, ref ProcessRef) error {
	k.refs = append(k.refs, ref)
	if k.err != nil {
		return k.err
	}
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

type processRefStore struct {
	refs map[string][]domain.ProcessRef
}

func (s *processRefStore) ProcessRefs(_ context.Context, nodeID string) ([]domain.ProcessRef, error) {
	return append([]domain.ProcessRef(nil), s.refs[nodeID]...), nil
}

func (s *processRefStore) SaveProcessRefs(_ context.Context, nodeID string, refs []domain.ProcessRef) error {
	if s.refs == nil {
		s.refs = map[string][]domain.ProcessRef{}
	}
	s.refs[nodeID] = append([]domain.ProcessRef(nil), refs...)
	return nil
}

func (s *processRefStore) DeleteProcessRefs(_ context.Context, nodeID string) error {
	delete(s.refs, nodeID)
	return nil
}
