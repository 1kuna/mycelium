//go:build !darwin && !linux

package myce

import "testing"

func assertSharedFileClone(t *testing.T, _, _ string) {
	t.Helper()
	t.Fatal("cloneSharedFile reported support without a platform clone implementation")
}
