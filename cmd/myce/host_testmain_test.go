package main

import (
	"encoding/json"
	"mycelium/internal/myce"
	"os"
	"path/filepath"
	"testing"
)

func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == "_command-exec" {
		if myce.CommandExec(os.Args[2:]) != nil {
			os.Exit(125)
		}
		return
	}
	dir, err := os.MkdirTemp("", "myce-cli-host-tests-")
	if err != nil {
		panic(err)
	}
	if err = os.Setenv("MYCE_HOST_STATE_DIR", dir); err != nil {
		panic(err)
	}
	s, e := myce.InitializeHostIntent(dir, "test-host", myce.RealClock{})
	if e != nil {
		panic(e)
	}
	s.Desired = "open"
	s.HostRevision = myce.HostRevision{Revision: s.Revision + 1, Operation: myce.ID()}
	b, e := json.Marshal(s)
	if e != nil {
		panic(e)
	}
	if e = os.WriteFile(filepath.Join(dir, "host-state.json"), b, 0600); e != nil {
		panic(e)
	}

	// Keep ordinary laptop tests off any installed daemon; Docker fault fixtures override this CLI.
	if err = os.WriteFile(filepath.Join(dir, "docker"), []byte("#!/bin/sh\ncase \"$3:$4\" in ps:*|network:ls) exit 0;; *) exit 99;; esac\n"), 0700); err != nil {
		panic(err)
	}
	if err = os.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH")); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}
