package myce

import (
	"context"
	"fmt"
	"mycelium/internal/probeguard"
	"os"
	"runtime"
	"sort"
	"testing"
	"time"
)

// Opt-in native proof; ordinary gates retain injected probes and laptop safety.
func TestDarwinRSSGuardNative(t *testing.T) {
	if os.Getenv("MYCE_NATIVE_RSS_TEST") != "1" {
		t.Skip("requires explicit bounded native RSS proof")
	}
	before := probeguard.Before
	probeguard.Before = nil
	defer func() { probeguard.Before = before }()
	w := recoveryWorker(t)
	w.Machine.GPU = "cpu"
	w.HostMemorySource = func(context.Context) (float64, error) { return 64, nil }
	w.processMemory = localProcessMemory()
	store, err := w.openStore()
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	j := recoveryJob(t, []string{"sh", "-c", `"$@" & wait`, "rss-parent", os.Args[0], "-test.run=^TestDarwinRSSGuardChild$"})
	j.Spec.Needs = ResourceNeeds{CPUCores: 1, HostMemoryGB: 64.0 / 1024, Platform: Platform{Required: []string{"cpu"}}}
	j.Spec.Run.Env = map[string]string{"MYCE_RSS_CHILD": "1"}
	p, err := startAfterFixtureInputs(t, w, store, j)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.stopDirective(w.Clock.Now(), 0, true, true) })
	// Measure the exact RSS+pressure probe path used by the observer, including
	// session inventory/birth checks. No threshold assertion depends on speed.
	var costs []time.Duration
	for i := 0; i < 20; i++ {
		at := time.Now()
		_, _, err := w.processMemory.OwnedResident(p.rssRoots())
		_, pressureErr := w.processMemory.Pressure(context.Background())
		if err != nil || pressureErr != nil {
			t.Fatalf("native probe: %v %v", err, pressureErr)
		}
		costs = append(costs, time.Since(at))
	}
	sort.Slice(costs, func(i, j int) bool { return costs[i] < costs[j] })
	t.Logf("20 complete RSS+pressure probes: median=%s p95=%s max=%s", costs[10], costs[18], costs[19])
	r := d5Finish(t, p)
	if r.Success || !r.OutOfMemory || r.ReleasePending || r.Interrupted || r.ExecutionCause == nil || r.ExecutionCause.Class != rssLimitExceeded {
		t.Fatalf("native guard: %+v", r)
	}
	if members, err := workerGroupMembers(p.cmd.Process.Pid); err != nil || len(members) != 0 {
		t.Fatalf("native release: %v %v", members, err)
	}
	t.Logf("native child stopped with %s; whole session released; log retained=%t", r.ExecutionCause.String(), store.state.Log != "")
	// Below-limit real command still succeeds through the same native runner.
	low := recoveryJob(t, []string{"sleep", "1"})
	low.Spec.Needs = j.Spec.Needs
	p, err = startAfterFixtureInputs(t, w, store, low)
	if err != nil {
		t.Fatal(err)
	}
	if r := d5Finish(t, p); !r.Success || r.ReleasePending {
		t.Fatalf("below-limit command failed: %+v", r)
	}
}

func TestDarwinRSSGuardChild(t *testing.T) {
	if os.Getenv("MYCE_RSS_CHILD") != "1" {
		return
	}
	data := make([]byte, 128<<20)
	for i := 0; i < len(data); i += 4096 {
		data[i] = 1
	}
	fmt.Println("partial diagnostic retained before RSS stop")
	timer := time.NewTimer(15 * time.Second)
	defer timer.Stop()
	<-timer.C
	runtime.KeepAlive(data)
}

func TestDarwinRSSGuardMemberBirthAndControlExclusion(t *testing.T) {
	// The control anchor alone is far above the payload threshold; payload and
	// a separate-PGID helper remain below it. Unrelated process 99 is excluded.
	const data = "7 1 1048576\n8 7 1024\n9 1 2048\n99 1 1048576\n"
	for _, mode := range []string{"complete", "reused member", "changed session"} {
		t.Run(mode, func(t *testing.T) {
			collected := false
			birth := func(pid int) (string, error) {
				if collected && pid == 9 && mode == "reused member" {
					return "replacement", nil
				}
				return fmt.Sprint(pid), nil
			}
			belongs := func(pid, root int) bool { return !(collected && pid == 9 && mode == "changed session") }
			bytes, members, err := sampleOwnedResident(map[int]string{7: "7"}, birth, func(int) ([]int, error) { return []int{7, 8, 9}, nil }, belongs, func() ([]byte, error) { collected = true; return []byte(data), nil })
			if mode != "complete" {
				if err == nil {
					t.Fatal("changed member became valid RSS")
				}
				return
			}
			if err != nil || bytes != 3<<20 || members != 2 {
				t.Fatalf("control/sibling charged or helper missing: %d %d %v", bytes, members, err)
			}
			g := rssGuard{limit: 4 << 20}
			for i := 0; i < 3; i++ {
				if g.observe(time.Unix(100+int64(i), 0), map[int]string{7: "7"}, bytes, members, err) != nil {
					t.Fatal("supervisor overhead stopped below-limit payload")
				}
			}
		})
	}
}
