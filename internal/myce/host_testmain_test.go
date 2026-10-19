package myce

import (
	"os"
	"path/filepath"
	"testing"
)

var dockerTestOriginalPath = os.Getenv("PATH")

func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == "_command-exec" {
		if CommandExec(os.Args[2:]) != nil {
			os.Exit(125)
		}
		return
	}
	// The crash/restart drill is another process on the same provisioned host.
	// Preserve its inherited operation rather than inventing a conflicting host.
	if os.Getenv("MYCE_RECOVERY_TEST_HELPER") == "1" {
		os.Exit(m.Run())
	}
	dir, err := os.MkdirTemp("", "myce-docker-tests-")
	if err != nil {
		panic(err)
	}

	// Keep ordinary laptop tests off any installed daemon; Docker fault fixtures override this CLI.
	if err = os.WriteFile(filepath.Join(dir, "docker"), []byte("#!/bin/sh\ncase \"$3:$4\" in ps:*|network:ls) exit 0;; *) exit 99;; esac\n"), 0700); err != nil {
		panic(err)
	}
	if err = os.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH")); err != nil {
		panic(err)
	}
	code := m.Run()
	_ = os.RemoveAll(dir) // Owned fake Docker executable; all tests and children have joined.
	os.Exit(code)
}
