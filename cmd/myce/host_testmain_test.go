package main

import (
	"context"
	"fmt"
	"mycelium/internal/myce"
	"mycelium/internal/probeguard"
	"mycelium/internal/testenv"
	"mycelium/internal/testguard"
	"os"
	"runtime"
	"testing"
	"testing/fstest"
)

func TestMain(m *testing.M) {
	os.Exit(cliTestMain(m))
}

func cliTestMain(m *testing.M) int {
	if len(os.Args) > 1 && os.Args[1] == "_native-exec" {
		if err := myce.NativeExec(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 125
		}
		return 0
	}
	// Fleet jobs inherit the hosting service's identity. These CLI fixtures
	// run outside its worker cgroup and own their simulated service startup.
	if err := os.Unsetenv("INVOCATION_ID"); err != nil {
		panic(err)
	}
	// Test submits must never wake the agent that runs this suite.
	for _, wake := range harnessWakes {
		for _, key := range wake.env {
			if err := os.Unsetenv(key); err != nil {
				panic(err)
			}
		}
	}
	if len(os.Args) > 1 && os.Args[1] == "_command-exec" {
		if myce.CommandExec(os.Args[2:]) != nil {
			return 125
		}
		return 0
	}
	installSyntheticHost()
	probeguard.Before = func() { panic("test invoked a real hardware probe; inject a fake") }
	if os.Getenv("MYCE_TEST_ROOT") != "" {
		cleanupHome := testguard.Install()
		defer cleanupHome()
		if os.Getenv("MYCE_TEST_BIND_STOP") == "1" {
			return queueBindStopHelper()
		}
		return m.Run()
	}
	dir, err := os.MkdirTemp("", "myce-cli-tests-")
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
	if err = os.Setenv("MYCE_TEST_ROOT", dir); err != nil {
		panic(err)
	}
	var code int
	if os.Getenv("MYCE_TEST_BIND_STOP") == "1" {
		code = queueBindStopHelper()
	} else {
		code = m.Run()
	}

	if err := stopCLIObserverFixtures(dir); err != nil {
		fmt.Fprintln(os.Stderr, "retain active observer fixture:", err)
		return 1
	}
	if err := testenv.Cleanup(dir); err != nil { // All tests and observers have joined.
		fmt.Fprintln(os.Stderr, "clean CLI fixture:", err)
		code = 1
	}
	return code
}

// In-process dev workers need the same synthetic host as tagged CLI children.
func installSyntheticHost() {
	myce.SetOnboardingMountCommand(func(context.Context, string, ...string) ([]byte, error) { return nil, nil })
	myce.SetWorkerProbeDefaults(syntheticWorkerProbes())
	myce.SetQueueHostMemorySource(syntheticWorkerProbes().HostMemory)
}

func syntheticWorkerProbes() myce.WorkerProbeDefaults {
	return myce.WorkerProbeDefaults{
		FaultHost:      func() (map[string]string, []string) { return nil, []string{"synthetic_host"} },
		SignalEvidence: func(myce.SignalCapture) *myce.SignalEvidence { return &myce.SignalEvidence{CoreCapture: "unavailable"} },
		IOPressure: func(clock myce.Clock) *myce.IOPressure {
			return &myce.IOPressure{SampledAt: clock.Now(), Some: &myce.PressureStall{TotalUS: 1234}, Full: &myce.PressureStall{}}
		},
		ResourceCPU:  func(context.Context, map[int]string, map[string]float64) (float64, error) { return 1.25, nil },
		MountRoots:   func(context.Context) ([]string, error) { return []string{"/"}, nil },
		DeviceFaults: func() myce.DeviceFaultSource { return nil },
		Disk: func(string) (myce.DiskReading, error) {
			return myce.DiskReading{Volume: "synthetic", TotalBytes: 1 << 40, FreeBytes: 1 << 39}, nil
		},
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
	}
}
