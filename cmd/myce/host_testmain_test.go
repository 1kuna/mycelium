package main

import (
	"mycelium/internal/myce"
	"os"
	"path/filepath"
	"testing"
)

func TestMain(m *testing.M) {
	os.Exit(cliTestMain(m))
}

func cliTestMain(m *testing.M) int {
	if len(os.Args) > 1 && os.Args[1] == "_command-exec" {
		if myce.CommandExec(os.Args[2:]) != nil {
			return 125
		}
		return 0
	}
	dir, err := os.MkdirTemp("", "myce-cli-docker-tests-")
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
	var code int
	if os.Getenv("MYCE_TEST_BIND_STOP") == "1" {
		code = queueBindStopHelper()
	} else {
		code = m.Run()
	}
	if err := os.RemoveAll(dir); err != nil { // All tests and children have joined.
		code = 1
	}
	return code
}
