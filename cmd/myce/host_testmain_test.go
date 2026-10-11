package main

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "myce-cli-host-tests-")
	if err != nil {
		panic(err)
	}
	if err = os.Setenv("MYCE_HOST_STATE_DIR", dir); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}
