package gateway

import (
	"testing"
	"time"

	"mycelium/internal/domain"
	"mycelium/test/fixtures"
	"mycelium/test/mocks"
)

func TestStickyTableReturnsOnlyReadyMatchingInstancesBeforeTTL(t *testing.T) {
	clock := mocks.NewFakeClock(time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC))
	table := NewStickyTable(clock, time.Minute)
	preset := fixtures.MakePreset(fixtures.WithPresetID("preset-a"))
	inst := fixtures.MakeInstance(fixtures.WithInstanceID("inst-a"), fixtures.WithInstancePreset(preset.ID))
	table.Put("conversation-a", inst)

	got, ok := table.Get("conversation-a", preset, domain.FleetSnapshot{Instances: []domain.ModelInstance{inst}})
	if !ok || got.ID != inst.ID {
		t.Fatalf("sticky = %+v ok=%v", got, ok)
	}
	otherPreset := fixtures.MakePreset(fixtures.WithPresetID("preset-b"))
	if _, ok := table.Get("conversation-a", otherPreset, domain.FleetSnapshot{Instances: []domain.ModelInstance{inst}}); ok {
		t.Fatal("mismatched preset should not stick")
	}
	table.Put("conversation-a", inst)
	loading := inst
	loading.State = domain.InstLoading
	if _, ok := table.Get("conversation-a", preset, domain.FleetSnapshot{Instances: []domain.ModelInstance{loading}}); ok {
		t.Fatal("loading instance should not stick")
	}
	table.Put("conversation-a", inst)
	clock.Advance(time.Minute)
	if _, ok := table.Get("conversation-a", preset, domain.FleetSnapshot{Instances: []domain.ModelInstance{inst}}); ok {
		t.Fatal("expired sticky entry returned")
	}
	table.Put("", inst)
	if _, ok := table.Get("", preset, domain.FleetSnapshot{Instances: []domain.ModelInstance{inst}}); ok {
		t.Fatal("empty key should not stick")
	}
}
