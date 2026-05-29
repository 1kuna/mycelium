package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"mycelium/internal/ports"
)

type ProcessRef struct {
	PID  int    `json:"pid"`
	Kind string `json:"kind"`
	Ref  string `json:"ref"`
}

type ProcessKiller interface {
	Kill(ctx context.Context, ref ProcessRef) error
}

type Reaper struct {
	path   string
	killer ProcessKiller
}

func NewReaper(path string, killer ProcessKiller) *Reaper {
	return &Reaper{path: path, killer: killer}
}

func (r *Reaper) Reap(ctx context.Context) ([]ProcessRef, error) {
	refs, err := r.readRefs()
	if err != nil {
		return nil, err
	}
	for _, ref := range refs {
		if err := r.killer.Kill(ctx, ref); err != nil {
			return nil, err
		}
	}
	if len(refs) > 0 {
		if err := os.Remove(r.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}
	return refs, nil
}

func WriteProcessRefs(path string, refs []ProcessRef) error {
	data, err := json.MarshalIndent(refs, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o600)
}

func (r *Reaper) readRefs() ([]ProcessRef, error) {
	data, err := os.ReadFile(r.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var refs []ProcessRef
	if err := json.Unmarshal(data, &refs); err != nil {
		return nil, fmt.Errorf("read process refs: %w", err)
	}
	return refs, nil
}

type BackendProcessKiller struct {
	Backend ports.BackendAdapter
}

func (k BackendProcessKiller) Kill(ctx context.Context, ref ProcessRef) error {
	return k.Backend.Stop(ctx, ports.Handle{PID: ref.PID, Kind: ref.Kind, Ref: ref.Ref})
}
