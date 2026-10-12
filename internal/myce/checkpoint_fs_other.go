//go:build !linux

package myce

import "os"

// Other platforms keep their directory durability barrier.
func checkpointCIFS(*os.File) (bool, error) { return false, nil }
