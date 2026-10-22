package myce

import (
	"mycelium/internal/testenv"
	"mycelium/internal/testguard"
	"os"
	"path/filepath"
	"testing"
)

var dockerTestOriginalPath = os.Getenv("PATH")

func TestMain(m *testing.M) {
	os.Exit(coreTestMain(m))
}

func coreTestMain(m *testing.M) int {
	if len(os.Args) > 1 && os.Args[1] == "_command-exec" {
		if CommandExec(os.Args[2:]) != nil {
			return 125
		}
		return 0
	}
	installSyntheticWorkerHost()
	// The crash/restart drill is another process on the same provisioned host.
	// Preserve its inherited operation rather than inventing a conflicting host.
	if os.Getenv("MYCE_RECOVERY_TEST_HELPER") == "1" || os.Getenv("MYCE_TEST_DOCKER_BIN") != "" {
		cleanupHome := testguard.Install()
		defer cleanupHome()
		return m.Run()
	}
	dir, err := os.MkdirTemp("", "myce-docker-tests-")
	if err != nil {
		panic(err)
	}

	if err = testenv.Isolate(dir); err != nil {
		panic(err)
	}
	if err = os.Setenv("MYCE_TEST_ISOLATED_HOME", os.Getenv("HOME")); err != nil {
		panic(err)
	}
	cleanupHome := testguard.Install()
	defer cleanupHome()
	// Keep ordinary laptop tests off any installed daemon; Docker fault fixtures override this CLI.
	if err = os.WriteFile(filepath.Join(dir, "docker"), []byte("#!/bin/sh\ncase \"$3:$4\" in ps:*|network:ls) exit 0;; *) exit 99;; esac\n"), 0700); err != nil {
		panic(err)
	}
	if err = os.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH")); err != nil {
		panic(err)
	}
	// Helpers may call os.Exit or be deliberately killed. They reuse this
	// parent-owned shim instead of creating a temp root they cannot clean up.
	if err = os.Setenv("MYCE_TEST_DOCKER_BIN", dir); err != nil {
		panic(err)
	}
	code := m.Run()

	if err := testenv.Cleanup(dir); err != nil { // All tests and children have joined.
		code = 1
	}
	return code
}
