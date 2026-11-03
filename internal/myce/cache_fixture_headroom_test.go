package myce

import "testing"

func TestExplicitFixtureCacheLimitUnderLowHeadroom(t *testing.T) {
	lowHeadroom := func(string) (int64, error) { return environmentCacheSize(23 << 30) }
	auto := EnvironmentCache{Root: t.TempDir(), sizeLimit: lowHeadroom}
	if err := auto.init(); err == nil {
		t.Fatal("automatic sizing accepted less than 24 GiB free")
	}
	explicit := EnvironmentCache{Root: t.TempDir(), Limit: 1 << 20, sizeLimit: lowHeadroom}
	if err := explicit.init(); err != nil {
		t.Fatalf("explicit fixture limit depends on disk headroom: %v", err)
	}
	config := ServiceConfig{Role: "worker", EnvironmentCache: t.TempDir(), EnvironmentCacheBytes: 1 << 20}
	if err := resolveEnvironmentCacheSizing(&config, true, lowHeadroom); err != nil || config.EnvironmentCacheBytes != 1<<20 {
		t.Fatalf("explicit installer fixture limit was not preserved: %+v, %v", config, err)
	}
}
