package main

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"testing/fstest"

	"mycelium/internal/myce"
	"mycelium/internal/probeguard"

	"mycelium/internal/testenv"
)

func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == "_command-exec" {
		if myce.CommandExec(os.Args[2:]) != nil {
			os.Exit(125)
		}
		return
	}
	installSyntheticHost()
	probeguard.Before = func() { panic("test invoked a real hardware probe; inject a fake") }
	dir, err := os.MkdirTemp("", "myce-cli-docker-tests-")
	if err != nil {
		panic(err)
	}

	if err = testenv.Isolate(dir); err != nil {
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
	if err := testenv.Cleanup(dir); err != nil {
		panic(err)
	} // All tests and children have joined.
	os.Exit(code)
}

// In-process dev workers need the same synthetic host as tagged CLI children.
func installSyntheticHost() {
	myce.SetWorkerProbeDefaults(myce.WorkerProbeDefaults{
		Profile: func() myce.ProfileProbe {
			return myce.ProfileProbe{Machine: myce.MachineProbe{OS: runtime.GOOS, Arch: runtime.GOARCH, Files: fstest.MapFS{}, Command: func(_ context.Context, name string, _ ...string) ([]byte, error) {
				if name == "/usr/sbin/sysctl" {
					return []byte("137438953472"), nil
				}
				return nil, myce.ErrNoGPU
			}}, ReadFile: func(string) ([]byte, error) { return []byte("MemTotal: 134217728 kB\n"), nil }, CPUs: func() int { return 20 }, Disk: func(string) (uint64, uint64, error) { return 1 << 40, 1 << 39, nil }}
		},
		HostMemory: func(reserve float64) (myce.MemorySource, error) {
			return func(context.Context) (float64, error) { return max(0, 128-reserve), nil }, nil
		},
		OwnerSignals: func(context.Context) myce.OwnerSignals { return myce.OwnerSignals{Power: "desktop"} },
		Identity: func() myce.MachineProbe {
			return myce.MachineProbe{Files: fstest.MapFS{"proc/cpuinfo": {Data: []byte("model name : synthetic CPU\n")}}, Command: func(context.Context, string, ...string) ([]byte, error) {
				return []byte("synthetic hardware identity"), nil
			}}
		},
	})
}
