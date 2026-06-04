package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

type ProcessRef = domain.ProcessRef

type ProcessKiller interface {
	Kill(ctx context.Context, ref ProcessRef) error
}

type Reaper struct {
	path   string
	refs   []ProcessRef
	killer ProcessKiller
}

func NewReaper(path string, killer ProcessKiller) *Reaper {
	return &Reaper{path: path, killer: killer}
}

func NewReaperFromRefs(refs []ProcessRef, killer ProcessKiller) *Reaper {
	return &Reaper{refs: append([]ProcessRef(nil), refs...), killer: killer}
}

func (r *Reaper) Reap(ctx context.Context) ([]ProcessRef, error) {
	refs := append([]ProcessRef(nil), r.refs...)
	if refs == nil {
		var err error
		refs, err = r.readRefs()
		if err != nil {
			return nil, err
		}
	}
	var errs []error
	for _, ref := range refs {
		if err := r.killer.Kill(ctx, ref); err != nil {
			errs = append(errs, fmt.Errorf("reap process %d: %w", ref.PID, err))
		}
	}
	if len(errs) > 0 {
		return refs, errors.Join(errs...)
	}
	if len(refs) > 0 && r.path != "" {
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
	return k.Backend.Stop(ctx, ports.Handle{
		PID:       ref.PID,
		PGID:      ref.PGID,
		Kind:      ref.Kind,
		Ref:       ref.Ref,
		Binary:    ref.Binary,
		Args:      append([]string(nil), ref.Args...),
		StartedAt: ref.StartedAt,
	})
}

type ProcessRefStore interface {
	ProcessRefs(ctx context.Context, nodeID string) ([]domain.ProcessRef, error)
	SaveProcessRefs(ctx context.Context, nodeID string, refs []domain.ProcessRef) error
	DeleteProcessRefs(ctx context.Context, nodeID string) error
}

type StoreProcessRegistry struct {
	Store  ProcessRefStore
	NodeID string
}

var storeProcessRegistryMu sync.Mutex

func (r StoreProcessRegistry) Add(ctx context.Context, ref domain.ProcessRef) error {
	if r.Store == nil {
		return fmt.Errorf("process ref store is not configured")
	}
	storeProcessRegistryMu.Lock()
	defer storeProcessRegistryMu.Unlock()
	refs, err := r.Store.ProcessRefs(ctx, r.NodeID)
	if err != nil {
		return err
	}
	for _, existing := range refs {
		if existing.PID == ref.PID {
			return nil
		}
	}
	refs = append(refs, ref)
	return r.Store.SaveProcessRefs(ctx, r.NodeID, refs)
}

func (r StoreProcessRegistry) Remove(ctx context.Context, ref domain.ProcessRef) error {
	if r.Store == nil {
		return fmt.Errorf("process ref store is not configured")
	}
	storeProcessRegistryMu.Lock()
	defer storeProcessRegistryMu.Unlock()
	refs, err := r.Store.ProcessRefs(ctx, r.NodeID)
	if err != nil {
		return err
	}
	out := refs[:0]
	for _, existing := range refs {
		if existing.PID != ref.PID {
			out = append(out, existing)
		}
	}
	if len(out) == 0 {
		return r.Store.DeleteProcessRefs(ctx, r.NodeID)
	}
	return r.Store.SaveProcessRefs(ctx, r.NodeID, out)
}
