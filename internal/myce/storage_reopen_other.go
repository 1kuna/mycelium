//go:build !darwin && !linux

package myce

import "io/fs"

func storageCacheBypass(fs.File, int64, int64) error { return errStorageCacheBypass }
