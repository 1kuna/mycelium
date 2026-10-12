package main

import (
	"os"
	"path/filepath"
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
	// Keep ordinary laptop tests off any installed daemon; Docker fault fixtures override this CLI.
	if err = os.WriteFile(filepath.Join(dir, "docker"), []byte("#!/bin/sh\ncase \"$3\" in ps) exit 0;; *) exit 99;; esac\n"), 0700); err != nil {
		panic(err)
	}
	if err = os.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH")); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}
