package fixtures

import (
	"strings"
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
	if n.DiskTotalMB <= 0 || n.DiskFreeMB <= 0 || n.DiskMinFreeRatio != domain.DefaultDiskMinFreeRatio {
		t.Fatalf("invalid disk defaults: %+v", n)
	}
}

func TestSparkIsCatastrophicAndUnified(t *testing.T) {
	n := MakeSparkNode()
	if n.OOMSeverity != domain.OOMCatastrophic || !n.UnifiedMemory {
		t.Fatalf("spark fixture wrong: %+v", n)
	}
}

func TestMakeJobOptions(t *testing.T) {
	j := MakeJob(Interactive, HardForInteractive, Latency, WithContext(12000), WithConcurrency(3))
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
	if j.ExpectedConcurrency != 3 {
		t.Fatalf("concurrency = %d", j.ExpectedConcurrency)
	}
}

func TestMakePresetDefaultsValid(t *testing.T) {
	p := MakePreset()
	if p.EstWeightsMB <= 0 || p.KVPerTokenMB <= 0 {
		t.Fatalf("invalid preset defaults: %+v", p)
	}
}

func TestAllNodeOptions(t *testing.T) {
	n := Make4090Node(WithNodeID("n1"), WithVRAM(123), WithUsedVRAM(45), WithMaxUtil(0.75), WithDisk(1000, 300), WithDiskMinFreeRatio(0.30), Catastrophic, Maintenance)
	if n.ID != "n1" || n.Accelerators[0].VRAMTotalMB != 123 || n.Accelerators[0].VRAMUsedMB != 45 {
		t.Fatalf("node fields = %+v", n)
	}
	if n.MaxUtil != 0.75 || n.OOMSeverity != domain.OOMCatastrophic || n.Status != domain.NodeMaintenance {
		t.Fatalf("node options = %+v", n)
	}
	if n.DiskTotalMB != 1000 || n.DiskFreeMB != 300 || n.DiskMinFreeRatio != 0.30 {
		t.Fatalf("disk options = %+v", n)
	}
}

func TestAllJobOptions(t *testing.T) {
	j := MakeJob(WithJobID("job1"), Background, Auto, Hard, WithPreset("preset1"), WithModel("model1"))
	if j.ID != "job1" || j.Priority != domain.PriorityBackground || j.SpeedPref != domain.SpeedAuto {
		t.Fatalf("job options = %+v", j)
	}
	if j.Preemption != domain.PreemptHard || j.PresetID != "preset1" || j.Model != "model1" {
		t.Fatalf("job options = %+v", j)
	}
}

func TestAllPresetAndInstanceOptions(t *testing.T) {
	p := MakePreset(WithPresetID("preset1"), WithModelRef("model1"), WithAliases("alias1"), WithWeights(12), WithArtifactSize(13), WithKVPerToken(0.5), WithContextLength(4096), WithLaunchProfile("profile"), WithLaunchArgs("--x", "1"), WithPresetNode("node1"))
	if p.ID != "preset1" || p.ModelRef != "model1" || strings.Join(p.Aliases, ",") != "alias1" || p.EstWeightsMB != 12 || p.KVPerTokenMB != 0.5 || p.ContextLength != 4096 || p.NodeID != "node1" {
		t.Fatalf("preset options = %+v", p)
	}
	if p.ArtifactSizeMB != 13 {
		t.Fatalf("artifact size = %+v", p)
	}
	if p.LaunchProfile != "profile" || len(p.LaunchArgs) != 2 {
		t.Fatalf("launch options = %+v", p)
	}
	if MakeClaim(1, 2) != (domain.Claim{WeightsMB: 1, KVReservedMB: 2}) {
		t.Fatal("claim factory returned wrong value")
	}

	inst := MakeInstance(
		WithInstanceID("inst1"),
		OnNode("node1"),
		WithInstancePreset("preset1"),
		WithClaim(MakeClaim(3, 4)),
		WithInstancePriority(domain.PriorityInteractive),
		Loading,
	)
	if inst.ID != "inst1" || inst.NodeID != "node1" || inst.PresetID != "preset1" {
		t.Fatalf("instance options = %+v", inst)
	}
	if inst.Claim != (domain.Claim{WeightsMB: 3, KVReservedMB: 4}) || inst.Priority != domain.PriorityInteractive {
		t.Fatalf("instance options = %+v", inst)
	}
	if inst.State != domain.InstLoading || !inst.Loading {
		t.Fatalf("loading option = %+v", inst)
	}
}

func TestFederationFixtures(t *testing.T) {
	peer := MakePeer(WithPeerID("peer-a"), WithPeerAddress("127.0.0.1:9"), ComputeOff)
	if peer.ID != "peer-a" || peer.Compute || len(peer.Addresses) != 1 || peer.Addresses[0] != "127.0.0.1:9" {
		t.Fatalf("peer = %+v", peer)
	}

	offer := MakeLeaseOffer(WithOfferID("offer-a"), WithOfferFence(42))
	if offer.OfferID != "offer-a" || offer.Fence != 42 || offer.Claim != MakeClaim(1, 2) {
		t.Fatalf("offer = %+v", offer)
	}

	record := MakeJobRecord(WithRecordJobID("job-a"))
	if record.JobID != "job-a" || record.Status != domain.JobQueued || len(record.Request) == 0 {
		t.Fatalf("record = %+v", record)
	}
}
