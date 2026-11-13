package myce

import (
	"strings"
	"testing"
)

// A dev session sizes its cache from need within the host's room above the
// disk floor, so a laptop with 15 GiB free runs it (#1540).
func TestDevEnvironmentCacheSizesFromNeed(t *testing.T) {
	original := workerDiskProbe
	t.Cleanup(func() { workerDiskProbe = original })
	free := uint64(15 << 30) // 1 TiB volume: 10.24 GB floor, about 4.8 GiB of room.
	workerDiskProbe = func(string) (DiskReading, error) {
		return DiskReading{Volume: "laptop", TotalBytes: 1 << 40, FreeBytes: free}, nil
	}
	c := EnvironmentCache{Root: t.TempDir(), growToNeed: true}
	if err := c.init(); err != nil || c.Limit != devEnvironmentCacheStart {
		t.Fatalf("dev cache on 15 GiB free: limit=%d err=%v", c.Limit, err)
	}
	if err := c.admit(strings.Repeat("a", 64), 4<<30); err != nil {
		t.Fatal("dev cache did not grow to a 4 GiB environment's need:", err)
	}
	err := c.admit(strings.Repeat("b", 64), 6<<30)
	for _, want := range []string{"environment " + strings.Repeat("b", 64) + " is ", "bytes short", "16106127360 bytes free", "10995116277-byte disk floor"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("refusal lacks %q: %v", want, err)
		}
	}
	free = 1 << 30
	empty := EnvironmentCache{Root: t.TempDir(), growToNeed: true}
	if err := empty.init(); err == nil || !strings.Contains(err.Error(), "has no room") {
		t.Fatal("dev cache below the disk floor started:", err)
	}
}
