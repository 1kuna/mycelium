package myce

import (
	"os"
	"testing"
)

func TestEnvironmentCacheAutoSizing(t *testing.T) {
	c := EnvironmentCache{Root: t.TempDir()}
	if err := c.init(); err != nil {
		t.Fatal("omitted cache limit must auto-size:", err)
	}
	if c.Limit != 0 {
		t.Fatalf("unbounded auto limit %d", c.Limit)
	}

	for _, tc := range []struct{ free, want int64 }{
		{1 << 20, (1 << 20) - int64(os.Getpagesize())}, {23 << 30, (23 << 30) - int64(os.Getpagesize())}, {1000 << 30, (1000 << 30) - int64(os.Getpagesize())},
	} {
		got, err := environmentCacheSize(tc.free)
		if got != tc.want || err != nil {
			t.Fatalf("free=%d limit=%d err=%v", tc.free, got, err)
		}
	}
	for _, tc := range []struct {
		saved    int64
		explicit bool
	}{
		{20 << 30, false}, {32 << 30, false}, {40 << 30, false}, {20 << 30, true},
	} {
		s := ServiceConfig{Role: "worker", EnvironmentCache: c.Root, EnvironmentCacheBytes: tc.saved}
		if err := resolveEnvironmentCacheSizing(&s, tc.explicit, func(string) (int64, error) { return 50 << 30, nil }); err != nil {
			t.Fatal(err)
		}
		want := tc.saved
		if tc.saved == 20<<30 && !tc.explicit {
			want = 0
		}
		if s.EnvironmentCacheBytes != want {
			t.Fatalf("saved %d explicit=%v became %d", tc.saved, tc.explicit, s.EnvironmentCacheBytes)
		}
	}
}
