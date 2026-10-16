package myce

import (
	"os"
	"path/filepath"
)

// A real null-device descriptor avoids both pipe backpressure and inheritance
// of the worker's stdout/stderr (which may be connected to journald).
func openCommandOutput(dir, name, policy string, flags int) (*os.File, error) {
	if policy == "none" {
		return os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	}
	return os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_WRONLY|flags, 0600)
}

func syncCommandOutput(output *os.File) error {
	if output.Name() == os.DevNull {
		return nil // fsync is not supported by the null device.
	}
	return output.Sync()
}
