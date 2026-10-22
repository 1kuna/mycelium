package myce

import "testing"

func TestEnvironmentCacheAutoSizing(t *testing.T) {
	c := EnvironmentCache{Root: t.TempDir()}
	if err := c.init(); err != nil {
		t.Fatal("omitted cache limit must auto-size:", err)
	}
	if c.Limit < 4<<30 || c.Limit > 80<<30 {
		t.Fatalf("unbounded auto limit %d", c.Limit)
	}

	for _, tc := range []struct{ free, want int64 }{
		{23 << 30, 0}, {24 << 30, 4 << 30}, {200 << 30, 50 << 30}, {1000 << 30, 80 << 30},
	} {
		got, err := environmentCacheSize(tc.free)
		if got != tc.want || (err != nil) != (tc.want == 0) {
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
			want = 50 << 30
		}
		if s.EnvironmentCacheBytes != want {
			t.Fatalf("saved %d explicit=%v became %d", tc.saved, tc.explicit, s.EnvironmentCacheBytes)
		}
	}
}
