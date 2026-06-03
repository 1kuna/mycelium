package lease

import (
	"testing"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
	"mycelium/test/contract"
	"mycelium/test/fixtures"
)

func TestAllocatorConformance(t *testing.T) {
	contract.RunAllocatorConformance(t, "lease",
		func() ports.Allocator { return NewAllocator() },
		fixtures.MakeNode(),
		fixtures.MakeClaim(1, 1))
}

func TestFitsAccountsForExistingClaimsAndNodeUsage(t *testing.T) {
	node := fixtures.MakeNode(fixtures.WithVRAM(1000), fixtures.WithUsedVRAM(100), fixtures.WithMaxUtil(0.90))
	existing := []domain.ModelInstance{
		fixtures.MakeInstance(fixtures.OnNode(node.ID), fixtures.WithClaim(fixtures.MakeClaim(300, 100))),
	}

	allocator := NewAllocator()
	if !allocator.Fits(node, []int{0}, existing, fixtures.MakeClaim(200, 100)) {
		t.Fatal("expected claim to fit under max_util")
	}
	if allocator.Fits(node, []int{0}, existing, fixtures.MakeClaim(300, 101)) {
		t.Fatal("expected claim to exceed max_util")
	}
}

func TestFitsRequiresSelectedAcceleratorAndValidInputs(t *testing.T) {
	allocator := NewAllocator()
	node := fixtures.MakeNode()

	if allocator.Fits(node, nil, nil, fixtures.MakeClaim(1, 1)) {
		t.Fatal("empty accelerator set should not fit")
	}
	if allocator.Fits(node, []int{99}, nil, fixtures.MakeClaim(1, 1)) {
		t.Fatal("unknown accelerator should not fit")
	}
	if allocator.Fits(node, []int{0, 0}, nil, fixtures.MakeClaim(1, 1)) {
		t.Fatal("duplicate accelerator set should not fit")
	}
	if allocator.Fits(fixtures.MakeNode(fixtures.WithVRAM(0)), []int{0}, nil, fixtures.MakeClaim(1, 1)) {
		t.Fatal("zero-capacity accelerator should not fit")
	}
	if allocator.Fits(fixtures.MakeNode(fixtures.WithMaxUtil(0)), []int{0}, nil, fixtures.MakeClaim(1, 1)) {
		t.Fatal("invalid max_util should not fit")
	}
	if allocator.Fits(node, []int{0}, nil, fixtures.MakeClaim(-1, 0)) {
		t.Fatal("negative claim should not fit")
	}
}

func TestFitsAccountsPerAcceleratorForMultiUnitClaims(t *testing.T) {
	node := fixtures.MakeNode(func(n *domain.Node) {
		n.MaxUtil = 0.90
		n.Accelerators = []domain.Accelerator{
			{Index: 0, Vendor: "nvidia", Kind: "gpu-a", VRAMTotalMB: 1000, VRAMUsedMB: 880},
			{Index: 1, Vendor: "nvidia", Kind: "gpu-b", VRAMTotalMB: 1000, VRAMUsedMB: 0},
		}
	})
	allocator := NewAllocator()

	if !allocator.Fits(node, []int{0, 1}, nil, fixtures.MakeClaim(40, 0)) {
		t.Fatal("40MB should split 20/20 and fit the tighter accelerator")
	}
	if allocator.Fits(node, []int{0, 1}, nil, fixtures.MakeClaim(42, 0)) {
		t.Fatal("42MB should split 21/21 and exceed accelerator 0")
	}
}

func TestFitsDistributesExistingClaimsByTheirOwnAcceleratorSet(t *testing.T) {
	node := fixtures.MakeNode(func(n *domain.Node) {
		n.MaxUtil = 0.90
		n.Accelerators = []domain.Accelerator{
			{Index: 0, Vendor: "nvidia", Kind: "gpu-a", VRAMTotalMB: 1000},
			{Index: 1, Vendor: "nvidia", Kind: "gpu-b", VRAMTotalMB: 1000},
		}
	})
	existing := []domain.ModelInstance{
		fixtures.MakeInstance(fixtures.OnNode(node.ID), fixtures.WithClaim(fixtures.MakeClaim(850, 0))),
		fixtures.MakeInstance(fixtures.OnNode("other-node"), fixtures.WithClaim(fixtures.MakeClaim(900, 0))),
	}
	allocator := NewAllocator()

	if !allocator.Fits(node, []int{0, 1}, existing, fixtures.MakeClaim(100, 0)) {
		t.Fatal("100MB should split 50/50 and exactly fill accelerator 0")
	}
	if allocator.Fits(node, []int{0, 1}, existing, fixtures.MakeClaim(102, 0)) {
		t.Fatal("102MB should split 51/51 and exceed accelerator 0")
	}

	badExisting := fixtures.MakeInstance(fixtures.OnNode(node.ID), fixtures.WithClaim(fixtures.MakeClaim(1, 0)))
	badExisting.AcceleratorSet = []int{0, 0}
	if allocator.Fits(node, []int{0}, []domain.ModelInstance{badExisting}, fixtures.MakeClaim(1, 0)) {
		t.Fatal("malformed existing accelerator set should not fit")
	}
}

func TestFitsRejectsInvalidDistributedClaims(t *testing.T) {
	node := fixtures.MakeNode(fixtures.WithVRAM(1000), fixtures.WithMaxUtil(1))
	if NewAllocator(WithReservedHeadroom(node.ID, domain.Claim{WeightsMB: -1})).Fits(node, []int{0}, nil, fixtures.MakeClaim(1, 0)) {
		t.Fatal("negative reserved headroom should not fit")
	}

	maxInt := int(^uint(0) >> 1)
	if NewAllocator().Fits(node, []int{0}, nil, domain.Claim{WeightsMB: maxInt, KVReservedMB: 1}) {
		t.Fatal("overflowed claim total should not fit")
	}
}

func TestFitsAppliesReservedHeadroom(t *testing.T) {
	node := fixtures.MakeNode(fixtures.WithVRAM(1000), fixtures.WithMaxUtil(0.90))
	allocator := NewAllocator(WithReservedHeadroom(node.ID, fixtures.MakeClaim(200, 0)))

	if !allocator.Fits(node, []int{0}, nil, fixtures.MakeClaim(700, 0)) {
		t.Fatal("700MB should fit after headroom")
	}
	if allocator.Fits(node, []int{0}, nil, fixtures.MakeClaim(701, 0)) {
		t.Fatal("701MB should exceed usable memory after headroom")
	}
}

func TestCatastrophicUnitsKeepExtraMargin(t *testing.T) {
	node := fixtures.MakeSparkNode(fixtures.WithVRAM(1000), fixtures.WithMaxUtil(0.90))
	allocator := NewAllocator()

	if !allocator.Fits(node, []int{0}, nil, fixtures.MakeClaim(850, 0)) {
		t.Fatal("850MB should fit with 5% catastrophic margin")
	}
	if allocator.Fits(node, []int{0}, nil, fixtures.MakeClaim(851, 0)) {
		t.Fatal("851MB should exceed catastrophic margin")
	}
}

func TestCanStackLoadRefusesConcurrentLoadOnCatastrophicUnit(t *testing.T) {
	node := fixtures.MakeSparkNode()
	existing := []domain.ModelInstance{
		fixtures.MakeInstance(fixtures.OnNode(node.ID), fixtures.Loading),
	}

	allocator := NewAllocator()
	if allocator.CanStackLoad(node, []int{0}, existing) {
		t.Fatal("catastrophic unit should not stack concurrent loads")
	}
	if !allocator.CanStackLoad(fixtures.Make4090Node(), []int{0}, existing) {
		t.Fatal("soft unit should allow stacked loads")
	}
	if !allocator.CanStackLoad(node, []int{0}, []domain.ModelInstance{
		fixtures.MakeInstance(fixtures.OnNode(node.ID), fixtures.WithClaim(fixtures.MakeClaim(1, 1))),
	}) {
		t.Fatal("catastrophic unit should allow load when no existing load is in flight")
	}
}

func TestSelectedCapacityAndOverlap(t *testing.T) {
	node := fixtures.MakeNode(func(n *domain.Node) {
		n.Accelerators = append(n.Accelerators, domain.Accelerator{Index: 1, VRAMTotalMB: 200, VRAMUsedMB: 20})
	})

	total, used, ok := selectedCapacity(node, []int{0, 1})
	if !ok || total != 24776 || used != 20 {
		t.Fatalf("selectedCapacity = %d/%d/%v", total, used, ok)
	}
	if _, _, ok := selectedCapacity(node, []int{0, 0}); ok {
		t.Fatal("duplicate selected capacity should fail")
	}
	if !overlaps([]int{0, 1}, []int{1, 2}) {
		t.Fatal("expected overlap")
	}
	if overlaps([]int{0}, []int{1}) {
		t.Fatal("unexpected overlap")
	}
	if overlaps(nil, []int{1}) {
		t.Fatal("empty left side should not overlap")
	}
	if got := reservationClaim(domain.Reservation{Kind: domain.ReservationPinned, Headroom: fixtures.MakeClaim(1, 1)}); got != (domain.Claim{}) {
		t.Fatalf("pinned reservation claim = %+v", got)
	}
	if got := reservationClaim(domain.Reservation{Kind: domain.ReservationHeadroom, Headroom: fixtures.MakeClaim(1, 1)}); got != fixtures.MakeClaim(1, 1) {
		t.Fatalf("headroom reservation claim = %+v", got)
	}
}

func TestSplitClaimBranches(t *testing.T) {
	if _, ok := splitClaim(-1, []int{0}); ok {
		t.Fatal("negative split should fail")
	}
	if _, ok := splitClaim(1, nil); ok {
		t.Fatal("empty split should fail")
	}
	if _, ok := splitClaim(1, []int{0, 0}); ok {
		t.Fatal("duplicate split should fail")
	}
	shares, ok := splitClaim(3, []int{1, 0})
	if !ok || shares[0] != 2 || shares[1] != 1 {
		t.Fatalf("uneven split = %+v ok=%v", shares, ok)
	}
	shares, ok = splitClaim(2, []int{0, 1})
	if !ok || shares[0] != 1 || shares[1] != 1 {
		t.Fatalf("even split = %+v ok=%v", shares, ok)
	}
	used := map[int]int{0: 0}
	if !addClaimShares(used, []int{0, 1}, fixtures.MakeClaim(2, 0)) || used[0] != 1 {
		t.Fatalf("partial selected add = %+v", used)
	}
}
