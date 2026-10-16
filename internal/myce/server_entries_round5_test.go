package myce

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func entryRound5Ready(t *testing.T) (*Queue, *fakeClock, Machine) {
	t.Helper()
	q, clock, m := modelFixture(t)
	q.models.Models[0].LocalSource = "approved"
	m.ModelConfigHash = modelConfigHash(q.models)
	m.ApprovedModels = map[string]string{"approved": q.models.Models[0].SHA256}
	m.ModelApprovalStates = map[string]ModelApprovalState{"approved": approvalVerified}
	check(t, q, m, nil)
	if err := q.Pin(ModelPin{Project: "test", Model: "test", Machine: m.ID}); err != nil {
		t.Fatal(err)
	}
	reportModel(t, q, m, "ready")
	return q, clock, m
}

func TestEntryUnknownApprovalExpiresAndValidResets(t *testing.T) {
	for _, reset := range []bool{false, true} {
		t.Run(map[bool]string{false: "expires", true: "valid-resets"}[reset], func(t *testing.T) {
			q, clock, m := entryRound5Ready(t)
			m.ApprovedModels = nil
			m.ModelApprovalStates = map[string]ModelApprovalState{"approved": approvalUnknown}
			reportModel(t, q, m, "ready")
			clock.Advance(4 * time.Minute)
			reportModel(t, q, m, "ready")
			if reset {
				m.ApprovedModels = map[string]string{"approved": q.models.Models[0].SHA256}
				m.ModelApprovalStates = map[string]ModelApprovalState{"approved": approvalVerified}
				reportModel(t, q, m, "ready")
				if slot := q.s.ModelSlots[slotKey(m.ID, "test")]; slot.ApprovalUnknownSince != nil {
					t.Fatal("definite valid report did not clear timer")
				}
				m.ApprovedModels = nil
				m.ModelApprovalStates = map[string]ModelApprovalState{"approved": approvalUnknown}
				reportModel(t, q, m, "ready")
				clock.Advance(4 * time.Minute)
				reportModel(t, q, m, "ready")
			}
			slot := q.s.ModelSlots[slotKey(m.ID, "test")]
			if slot == nil || slot.State != "ready" {
				t.Fatal("unknown drained before grace")
			}
			c := addModelCall(t, q, "experiment")
			if c.Machine != m.ID {
				t.Fatal("ready route withheld within grace")
			}
			if err := q.finishCall(c.ID); err != nil {
				t.Fatal(err)
			}
			clock.Advance(time.Minute)
			reportModel(t, q, m, "ready")
			if slot.State != "ready" {
				t.Fatal("unknown drained at, rather than beyond, grace")
			}
			clock.Advance(time.Nanosecond)
			reply := reportModel(t, q, m, "ready")
			if slot.State != "draining" || len(reply.ModelPlan) != 1 || reply.ModelPlan[0].State != "draining" || q.modelMemory(m.ID) != 8 {
				t.Fatal("unknown did not drain beyond grace with custody")
			}
			c = addModelCall(t, q, "experiment")
			if c.Machine != "" {
				t.Fatal("expired source still routed")
			}
			if !strings.Contains(slot.Error, "approval unknown for more than 5m") {
				t.Fatal("expiry reason missing")
			}
			warning := q.nextApprovalWarning
			clock.Advance(time.Second)
			reportModel(t, q, m, "ready")
			if q.nextApprovalWarning != warning {
				t.Fatal("expiry warning was not limited")
			}
			reportModel(t, q, m, "unloaded")
			if q.modelMemory(m.ID) != 0 {
				t.Fatal("confirmed unload retained memory")
			}
		})
	}
}

func TestEntryRevokedApprovalNotMaskedBySlowSibling(t *testing.T) {
	cache, a, _, _ := localApprovalFixture(t)
	other, b, _, _ := localApprovalFixture(t)
	b.LocalSource = "sibling"
	b.Name = "sibling"
	data, err := os.ReadFile(filepath.Join(other.ApprovalRoot, "approved.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cache.ApprovalRoot, "sibling.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	q, clock, m := modelFixture(t)
	a.MemoryGB = 8
	q.models.Models[0] = a
	m.ModelConfigHash = modelConfigHash(q.models)
	m.ApprovedModels, m.ModelApprovalStates = cache.modelApprovals([]Model{a, b})
	check(t, q, m, nil)
	if err := q.Pin(ModelPin{Project: "test", Model: "test", Machine: m.ID}); err != nil {
		t.Fatal(err)
	}
	reportModel(t, q, m, "ready")
	if err := os.Remove(filepath.Join(cache.ApprovalRoot, "approved.json")); err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	cache.localMetadataProbe = func(ctx context.Context, path, root string) ([]localModelMetadata, error) {
		<-release
		return localMetadata(ctx, path, root)
	}
	// Put the slow sibling first: all cheap records must precede all walks.
	for i := 0; i < 3; i++ {
		m.ApprovedModels, m.ModelApprovalStates = cache.modelApprovals([]Model{b, a})
		if m.ModelApprovalStates[a.LocalSource] != approvalInvalid || m.ModelApprovalStates[b.LocalSource] != approvalUnknown {
			t.Fatalf("revoked approval masked by sibling: %v", m.ModelApprovalStates)
		}
		reportModel(t, q, m, "ready")
		slot := q.s.ModelSlots[slotKey(m.ID, "test")]
		if slot == nil || slot.State != "draining" || q.modelMemory(m.ID) != 8 {
			t.Fatal("revoked source did not drain with custody")
		}
		clock.Advance(time.Second)
	}
}

func TestEntryPartialApprovalResultsSurviveSiblingTimeout(t *testing.T) {
	cache, a, _, _ := localApprovalFixture(t)
	bcache, b, bpath, _ := localApprovalFixture(t)
	ccache, c, cpath, _ := localApprovalFixture(t)
	b.LocalSource = "slow"
	b.Name = "slow"
	c.LocalSource = "changed"
	c.Name = "changed"
	for name, source := range map[string]*ModelCache{"slow": bcache, "changed": ccache} {
		data, err := os.ReadFile(filepath.Join(source.ApprovalRoot, "approved.json"))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(cache.ApprovalRoot, name+".json"), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(cpath, "config.json"), []byte("changed"), 0600); err != nil {
		t.Fatal(err)
	}
	release, finished := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }); <-finished })
	cache.localMetadataProbe = func(ctx context.Context, path, root string) ([]localModelMetadata, error) {
		if path == bpath {
			defer close(finished)
			<-release
		}
		return localMetadata(ctx, path, root)
	}
	models := []Model{a, c, b}
	approved, states := cache.modelApprovals(models)
	if states[a.LocalSource] != approvalVerified || approved[a.LocalSource] != a.SHA256 || states[c.LocalSource] != approvalInvalid || states[b.LocalSource] != approvalUnknown {
		t.Fatalf("lost completed results: %v", states)
	}
	// Replaying an older definite valid result would let a stuck sibling extend grace forever.
	approved, states = cache.modelApprovals(models)
	if len(approved) != 0 || states[a.LocalSource] != approvalUnknown {
		t.Fatal("old partial validity was replayed")
	}
	// Cheap revocation is still seen after the directory scanner has already stalled.
	if err := os.Remove(filepath.Join(cache.ApprovalRoot, "approved.json")); err != nil {
		t.Fatal(err)
	}
	_, states = cache.modelApprovals(models)
	if states[a.LocalSource] != approvalInvalid {
		t.Fatal("pending walk hid later revocation")
	}
}

func TestEntryMismatchedRecordPrecedesSlowWalk(t *testing.T) {
	cache, a, _, _ := localApprovalFixture(t)
	bcache, b, _, _ := localApprovalFixture(t)
	b.LocalSource = "slow"
	b.Name = "slow"
	data, err := os.ReadFile(filepath.Join(bcache.ApprovalRoot, "approved.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cache.ApprovalRoot, "slow.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	record := filepath.Join(cache.ApprovalRoot, "approved.json")
	data, err = os.ReadFile(record)
	if err != nil {
		t.Fatal(err)
	}
	var approval localModelApproval
	if err := json.Unmarshal(data, &approval); err != nil {
		t.Fatal(err)
	}
	approval.Host = "mismatched-host-placeholder"
	data, err = json.Marshal(approval)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(record, data, 0600); err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	cache.localMetadataProbe = func(ctx context.Context, path, root string) ([]localModelMetadata, error) {
		<-release
		return localMetadata(ctx, path, root)
	}
	_, states := cache.modelApprovals([]Model{b, a})
	if states[a.LocalSource] != approvalInvalid || states[b.LocalSource] != approvalUnknown {
		t.Fatalf("host mismatch was hidden: %v", states)
	}
}

func TestEntryAlwaysSlowApprovalDrainsAfterGrace(t *testing.T) {
	cache, model, _, _ := localApprovalFixture(t)
	q, clock, m := modelFixture(t)
	cache.Clock = clock
	model.MemoryGB = 8
	q.models.Models[0] = model
	m.ModelConfigHash = modelConfigHash(q.models)
	m.ApprovedModels, m.ModelApprovalStates = cache.modelApprovals(q.models.Models)
	check(t, q, m, nil)
	if err := q.Pin(ModelPin{Project: "test", Model: "test", Machine: m.ID}); err != nil {
		t.Fatal(err)
	}
	reportModel(t, q, m, "ready")
	release, finished := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }); <-finished })
	cache.localMetadataProbe = func(ctx context.Context, path, root string) ([]localModelMetadata, error) {
		defer close(finished)
		<-release
		return localMetadata(ctx, path, root)
	}
	for i, advance := range []time.Duration{0, 4 * time.Minute, time.Minute, time.Nanosecond} {
		clock.Advance(advance)
		m.ApprovedModels, m.ModelApprovalStates = cache.modelApprovals(q.models.Models)
		if m.ModelApprovalStates[model.LocalSource] != approvalUnknown {
			t.Fatal("slow source not unknown")
		}
		reportModel(t, q, m, "ready")
		slot := q.s.ModelSlots[slotKey(m.ID, model.Name)]
		want := "ready"
		if i == 3 {
			want = "draining"
		}
		if slot == nil || slot.State != want || q.modelMemory(m.ID) != 8 {
			t.Fatalf("step %d: slow source state/custody incorrect", i)
		}
		if i == 0 {
			// The production SQLite mutation persists the grace origin for queue recovery.
			var data []byte
			if err := q.db.QueryRow("SELECT data FROM queue WHERE id=1").Scan(&data); err != nil {
				t.Fatal(err)
			}
			var restored state
			if err := json.Unmarshal(data, &restored); err != nil {
				t.Fatal(err)
			}
			saved := restored.ModelSlots[slotKey(m.ID, model.Name)]
			if saved.ApprovalUnknownSince == nil || !saved.ApprovalUnknownSince.Equal(clock.Now()) {
				t.Fatal("unknown grace origin was not persisted")
			}
		}
	}
}
