//go:build !darwin && !linux

package myce

import "os"

func cloneSharedFile(*os.File, string) bool { return false }
