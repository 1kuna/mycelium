//go:build !darwin

package myce

import "os"

func publishJobOutput(source, destination string) error {
	return os.Link(source, destination)
}
