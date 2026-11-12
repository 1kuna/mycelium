package myce

import (
	"fmt"
	"mycelium/internal/testenv"
	"mycelium/internal/testguard"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	os.Exit(coreTestMain(m))
}

func coreTestMain(m *testing.M) int {
	if len(os.Args) > 1 && os.Args[1] == "_native-exec" {
		if err := NativeExec(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 125
		}
		return 0
	}
	if len(os.Args) > 1 && os.Args[1] == "_command-exec" {
		if CommandExec(os.Args[2:]) != nil {
			return 125
		}
		return 0
	}
	installSyntheticWorkerHost()
	// The crash/restart drill is another process on the same provisioned host.
	// Preserve its inherited operation rather than inventing a conflicting host.
	if os.Getenv("MYCE_RECOVERY_TEST_HELPER") == "1" || os.Getenv("MYCE_TEST_ROOT") != "" {
		cleanupHome := testguard.Install()
		defer cleanupHome()
		return m.Run()
	}
	dir, err := os.MkdirTemp("", "myce-core-tests-")
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
	// Helpers may call os.Exit or be deliberately killed. They reuse this
	// parent-owned root instead of creating a temp root they cannot clean up.
	if err = os.Setenv("MYCE_TEST_ROOT", dir); err != nil {
		panic(err)
	}
	before := packageDirEntries()
	code := m.Run()
	// A test must never write into the package directory (a stray lock there
	// dirtied checkouts). Any entry the run added fails the binary.
	for name := range packageDirEntries() {
		if !before[name] {
			fmt.Fprintf(os.Stderr, "test run left %s in the package directory\n", name)
			code = 1
		}
	}

	if err := testenv.Cleanup(dir); err != nil { // All tests and children have joined.
		code = 1
	}
	return code
}

func packageDirEntries() map[string]bool {
	entries, err := os.ReadDir(".")
	if err != nil {
		panic(err)
	}
	names := make(map[string]bool, len(entries))
	for _, e := range entries {
		names[e.Name()] = true
	}
	return names
}
