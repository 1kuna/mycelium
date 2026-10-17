package myce

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

var dockerTestOriginalPath = os.Getenv("PATH")

func TestMain(m *testing.M) {
	if len(os.Args) == 2 && os.Args[1] == "_host-worker-mutation" {
		if HostWorkerChildEntry(context.Background(), os.Stdin, os.Stdout, LocalHostIdentityResolver(), hostProtocolTestHandler) != nil {
			os.Exit(125)
		}
		return
	}

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
	dir, err := os.MkdirTemp("", "myce-host-tests-")
	if err != nil {
		panic(err)
	}
	if err = os.Setenv("MYCE_HOST_STATE_DIR", dir); err != nil {
		panic(err)
	}
	s, e := InitializeHostIntent(dir, "test-host", RealClock{})
	if e != nil {
		panic(e)
	}
	s.Desired = "open"
	s.HostRevision = HostRevision{Revision: s.Revision + 1, Operation: ID()}
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
