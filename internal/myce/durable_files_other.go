//go:build !darwin

package myce

import "os"

func syncLocalFile(f *os.File) error { return f.Sync() }
