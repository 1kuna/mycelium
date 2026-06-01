package gateway

import (
	"sync"
	"time"

	"mycelium/internal/clock"
	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

type StickyTable struct {
	mu      sync.Mutex
	clock   ports.Clock
	ttl     time.Duration
	entries map[string]stickyEntry
}

type stickyEntry struct {
	InstanceID string
	ExpiresAt  time.Time
}

func NewStickyTable(clk ports.Clock, ttl time.Duration) *StickyTable {
	if clk == nil {
		clk = clock.System{}
	}
	if ttl == 0 {
		ttl = 10 * time.Minute
	}
	return &StickyTable{clock: clk, ttl: ttl, entries: map[string]stickyEntry{}}
}

func (t *StickyTable) Get(key string, preset domain.Preset, fleet domain.FleetSnapshot) (domain.ModelInstance, bool) {
	if key == "" || t == nil {
		return domain.ModelInstance{}, false
	}
	t.mu.Lock()
	entry, ok := t.entries[key]
	if !ok || !t.clock.Now().Before(entry.ExpiresAt) {
		delete(t.entries, key)
		t.mu.Unlock()
		return domain.ModelInstance{}, false
	}
	t.mu.Unlock()
	for _, inst := range fleet.Instances {
		if inst.ID == entry.InstanceID && inst.PresetID == preset.ID && inst.State == domain.InstReady {
			return inst, true
		}
	}
	t.mu.Lock()
	delete(t.entries, key)
	t.mu.Unlock()
	return domain.ModelInstance{}, false
}

func (t *StickyTable) Put(key string, inst domain.ModelInstance) {
	if key == "" || t == nil || inst.ID == "" || inst.State != domain.InstReady {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.entries[key] = stickyEntry{InstanceID: inst.ID, ExpiresAt: t.clock.Now().Add(t.ttl)}
}

func (t *StickyTable) Delete(key string) {
	if key == "" || t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.entries, key)
}
