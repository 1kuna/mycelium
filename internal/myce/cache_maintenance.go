package myce

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func (b *cacheBudgets) register(root string, maintain func(context.Context)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.maintenance == nil {
		b.maintenance = map[string]func(context.Context){}
	}
	b.maintenance[root] = maintain
}
func (b *cacheBudgets) maintain(ctx context.Context) {
	b.mu.Lock()
	tasks := make([]func(context.Context), 0, len(b.maintenance))
	for _, f := range b.maintenance {
		tasks = append(tasks, f)
	}
	b.mu.Unlock()
	for _, f := range tasks {
		if ctx.Err() != nil {
			return
		}
		f(ctx)
	}
}
func (c *SharedInputCache) collectAdaptive(ctx context.Context) {
	if err := c.cache.init(); err != nil {
		return
	}
	gate, err := c.cache.tryLock("admission")
	if err != nil || gate == nil {
		return
	}
	defer unlockEnvironment(gate)
	if c.admit(ctx, "", 0) == nil {
		c.refreshCapacityLocked()
	}
}

// Loaded/pinned models and active copies retain the same protection fence as
// foreground admission. Unknown and unverified entries are never collected.
func (c *ModelCache) collectAdaptive(ctx context.Context) {
	if !c.mu.TryLock() {
		return
	}
	defer c.mu.Unlock()
	entries, err := os.ReadDir(c.Root)
	if err != nil {
		return
	}
	type candidate struct {
		key   string
		bytes int64
		used  int64
	}
	var victims []candidate
	used := int64(0)
	for _, e := range entries {
		if ctx.Err() != nil {
			return
		}
		if !strings.HasSuffix(e.Name(), ".model") && !strings.HasSuffix(e.Name(), ".partial") {
			continue
		}
		n, err := cacheEntrySize(filepath.Join(c.Root, e.Name()))
		if err != nil {
			return
		}
		used = cacheSum(used, n)
		if strings.HasSuffix(e.Name(), ".partial") {
			continue
		}
		if _, ok := c.verified.Load(e.Name()); !ok {
			continue
		}
		if c.active[e.Name()] != nil || c.Protected != nil && c.Protected(e.Name()) {
			continue
		}
		st, err := e.Info()
		if err != nil {
			return
		}
		victims = append(victims, candidate{e.Name(), n, st.ModTime().UnixNano()})
	}
	limit, err := c.budgets.limit(c.Root, used, c.Limit, c.Clock.Now())
	if err != nil {
		return
	}
	sort.Slice(victims, func(i, j int) bool {
		hi, hj := c.budgets.recent(c.Root, victims[i].key, c.Clock.Now()), c.budgets.recent(c.Root, victims[j].key, c.Clock.Now())
		if hi != hj {
			return !hi
		}
		return victims[i].used < victims[j].used
	})
	for _, v := range victims {
		if used <= limit || ctx.Err() != nil {
			return
		}
		if c.active[v.key] != nil || c.Protected != nil && c.Protected(v.key) {
			continue
		}
		if c.discardContext(ctx, filepath.Join(c.Root, v.key)) != nil {
			return
		}
		used -= v.bytes
		c.verified.Delete(v.key)
		c.progress.Delete(v.key)
		c.budgets.forget(c.Root, v.key)
	}
}
