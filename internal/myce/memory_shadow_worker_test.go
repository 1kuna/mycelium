package myce

import (
	"math"
	"testing"
	"time"
)

func TestMemoryShadowSampler(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	for _, tc := range []struct {
		platform  string
		ram, card *float64
	}{{shadowLinuxCPU, shadowGB(3), nil}, {shadowLinuxDiscrete, shadowGB(4), shadowGB(5)}, {shadowLinuxUnified, shadowGB(8), nil}, {shadowDarwin, shadowGB(3), nil}} {
		ram, card := shadowFootprint(tc.platform, shadowMemoryCounters{shadowGB(3), shadowGB(5), shadowGB(1)})
		equal := func(a, b *float64) bool { return a == nil && b == nil || a != nil && b != nil && *a == *b }
		if !equal(ram, tc.ram) || !equal(card, tc.card) {
			t.Fatalf("incorrect %s footprint", tc.platform)
		}
	}
	ram, card := shadowFootprint(shadowLinuxDiscrete, shadowMemoryCounters{host: shadowGB(3), device: shadowGB(5)})
	if ram != nil || card == nil || *card != 5 {
		t.Fatal("missing driver counter contaminated other pools")
	}
	sampler := shadowFootprintSampler{platform: shadowLinuxUnified}
	first := clock.Now()
	sampler.observe(first, shadowGB(2), &hostMemorySample{gb: 4, at: first}, attemptMemorySamplePeriod)
	clock.Advance(time.Second)
	sampler.observe(clock.Now(), shadowGB(3), &hostMemorySample{gb: 4, at: first}, attemptMemorySamplePeriod)
	clock.Advance(4 * time.Second)
	sampler.observe(clock.Now(), shadowGB(3), &hostMemorySample{gb: 10, at: clock.Now()}, attemptMemorySamplePeriod)
	r := sampler.state.Load()
	if r.RAM.GB != 13 || r.RAM.PeakGB != 13 || math.Abs(r.RAM.Fastest-1.4) > 1e-9 || !r.Card.At.IsZero() {
		t.Fatalf("unexpected paired reading %+v", r)
	}
	clock.Advance(deviceMemorySampleFreshFor)
	sampler.observe(clock.Now(), shadowGB(3), nil, attemptMemorySamplePeriod)
	if r := sampler.state.Load(); !r.RAM.At.IsZero() || r.RAM.PeakGB != 13 || r.RAM.Fastest != 1.4 {
		t.Fatalf("missing device reading renewed RAM evidence %+v", r)
	}
}
