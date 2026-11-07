package myce

import (
	"os"
	"testing"
)

func TestStorageReopenLinuxRequestsCacheBypass(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "data-")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err = f.Write([]byte("read-only recovery")); err != nil {
		t.Fatal(err)
	}
	if err = f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err = storageCacheBypass(f, 0, 18); err != nil {
		t.Fatal(err)
	}
}
