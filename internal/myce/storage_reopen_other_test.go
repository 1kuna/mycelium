//go:build !darwin && !linux

package myce

import (
	"errors"
	"testing"
)

func TestStorageReopenOtherRecordsUnavailable(t *testing.T) {
	if err := storageCacheBypass(nil, 0, 1); !errors.Is(err, errStorageCacheBypass) {
		t.Fatal(err)
	}
}
