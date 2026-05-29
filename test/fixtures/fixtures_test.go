package fixtures

import (
	"testing"

	"mycelium/internal/domain"
)

func TestMakeNodeDefaultsValid(t *testing.T) {
	n := MakeNode()
	if n.MaxUtil <= 0 || n.MaxUtil > 1 {
		t.Fatalf("MaxUtil out of range: %v", n.MaxUtil)
	}
	if len(n.Accelerators) == 0 {
		t.Fatal("default node must have an accelerator")
	}
}

func TestSparkIsCatastrophicAndUnified(t *testing.T) {
	n := MakeSparkNode()
	if n.OOMSeverity != domain.OOMCatastrophic || !n.UnifiedMemory {
		t.Fatalf("spark fixture wrong: %+v", n)
	}
}

func TestMakeJobOptions(t *testing.T) {
	j := MakeJob(Interactive, HardForInteractive, Latency, WithContext(12000))
	if j.Priority != domain.PriorityInteractive {
		t.Fatalf("priority = %s", j.Priority)
	}
	if j.Preemption != domain.PreemptHardForInteractive {
		t.Fatalf("preemption = %s", j.Preemption)
	}
	if j.SpeedPref != domain.SpeedLatency {
		t.Fatalf("speed = %s", j.SpeedPref)
	}
	if j.ContextRequest != 12000 {
		t.Fatalf("context = %d", j.ContextRequest)
	}
}

func TestMakePresetDefaultsValid(t *testing.T) {
	p := MakePreset()
	if p.EstWeightsMB <= 0 || p.KVPerTokenMB <= 0 {
		t.Fatalf("invalid preset defaults: %+v", p)
	}
}
