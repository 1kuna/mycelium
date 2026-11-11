package myce

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"mycelium/internal/probeguard"
)

// Opt-in native measurement of the Darwin RSS guard's reaction time and
// overshoot under synthetic growth. It allocates at most growthCapMiB per
// scenario on the host running it; ordinary gates use injected probes.
//
//	MYCE_NATIVE_RSS_TEST=1 go test -run '^TestDarwinRSSGuardGrowthOvershoot$' -v ./internal/myce
func TestDarwinRSSGuardGrowthOvershoot(t *testing.T) {
	if os.Getenv("MYCE_NATIVE_RSS_TEST") != "1" {
		t.Skip("requires explicit bounded native RSS proof")
	}
	before := probeguard.Before
	probeguard.Before = nil
	defer func() { probeguard.Before = before }()
	const limitMiB, growthCapMiB = 256, 3072
	scenarios := []struct {
		name, mode string
		rateMiB    int
	}{
		{"steady 256MiB/s", "steady", 256},
		{"steady 1GiB/s", "steady", 1024},
		{"spawn 32MiB leaves 320MiB/s", "spawn", 320},
		{"steady 256MiB/s with process churn", "churn", 256},
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			w := recoveryWorker(t)
			w.Machine.GPU = "cpu"
			w.HostMemorySource = func(context.Context) (float64, error) { return 64, nil }
			w.processMemory = localProcessMemory()
			store, err := w.openStore()
			if err != nil {
				t.Fatal(err)
			}
			defer store.close()
			log := filepath.Join(t.TempDir(), "growth.log")
			j := recoveryJob(t, []string{"sh", "-c", `"$@" & wait`, "growth-parent", os.Args[0], "-test.run=^TestDarwinRSSGuardGrowthChild$"})
			j.Spec.Needs = ResourceNeeds{CPUCores: 1, HostMemoryGB: limitMiB / 1024.0, Platform: Platform{Required: []string{"cpu"}}}
			j.Spec.Run.Env = map[string]string{"MYCE_GROWTH_MODE": sc.mode, "MYCE_GROWTH_LOG": log,
				"MYCE_GROWTH_RATE_MIB": strconv.Itoa(sc.rateMiB), "MYCE_GROWTH_CAP_MIB": strconv.Itoa(growthCapMiB)}
			started := time.Now()
			p, err := startAfterFixtureInputs(t, w, store, j)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { p.stopDirective(w.Clock.Now(), 0, true, true) })
			var r Result
			select {
			case f := <-p.done:
				if f.err != nil {
					t.Fatal(f.err)
				}
				r = f.result
			case <-time.After(testWaitTimeout):
				t.Fatal("attempt did not finish")
			}
			finished := time.Now()
			m := readGrowthLog(t, log, limitMiB<<20)
			stopped := r.ExecutionCause != nil && r.ExecutionCause.Class == rssLimitExceeded
			observed := "none"
			if stopped {
				observed = r.ExecutionCause.String()
			}
			t.Logf("RESULT %s: stopped=%t crossed=+%s reaction(last write)=%s reaction(finish)=%s allocated at stop=%dMiB overshoot=%dMiB guard=%s",
				sc.name, stopped, m.crossed.Sub(started).Round(time.Millisecond), m.last.Sub(m.crossed).Round(time.Millisecond),
				finished.Sub(m.crossed).Round(time.Millisecond), m.total>>20, (int64(m.total)-limitMiB<<20)>>20, observed)
			if !stopped {
				t.Errorf("guard did not stop growth past %d MiB: %+v", limitMiB, r)
			}
		})
	}
}

type growthLog struct {
	crossed, last time.Time
	total         uint64
}

// Lines are "unixnano bytes" increments appended after the pages are touched.
func readGrowthLog(t *testing.T, path string, limit uint64) growthLog {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var g growthLog
	type entry struct {
		at    time.Time
		bytes uint64
	}
	var entries []entry
	s := bufio.NewScanner(f)
	for s.Scan() {
		fields := strings.Fields(s.Text())
		if len(fields) != 2 {
			continue
		}
		ns, e1 := strconv.ParseInt(fields[0], 10, 64)
		n, e2 := strconv.ParseUint(fields[1], 10, 64)
		if e1 == nil && e2 == nil {
			entries = append(entries, entry{time.Unix(0, ns), n})
		}
	}
	// Spawned leaves append concurrently; order by their own timestamps.
	for i := 1; i < len(entries); i++ {
		for k := i; k > 0 && entries[k].at.Before(entries[k-1].at); k-- {
			entries[k], entries[k-1] = entries[k-1], entries[k]
		}
	}
	for _, e := range entries {
		g.total += e.bytes
		g.last = e.at
		if g.crossed.IsZero() && g.total >= limit {
			g.crossed = e.at
		}
	}
	if g.crossed.IsZero() {
		t.Fatalf("growth never crossed the limit: %d bytes", g.total)
	}
	return g
}

func TestDarwinRSSGuardGrowthChild(t *testing.T) {
	mode := os.Getenv("MYCE_GROWTH_MODE")
	if mode == "" {
		return
	}
	deadline := time.Now().Add(20 * time.Second)
	rate, _ := strconv.Atoi(os.Getenv("MYCE_GROWTH_RATE_MIB"))
	capMiB, _ := strconv.Atoi(os.Getenv("MYCE_GROWTH_CAP_MIB"))
	log, err := os.OpenFile(os.Getenv("MYCE_GROWTH_LOG"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	const stepMiB = 16
	var held [][]byte
	grow := func(mib int) {
		data := make([]byte, mib<<20)
		for i := 0; i < len(data); i += 4096 {
			data[i] = 1
		}
		held = append(held, data)
		fmt.Fprintf(log, "%d %d\n", time.Now().UnixNano(), len(data))
	}
	switch mode {
	case "leaf":
		grow(32)
	case "steady", "churn":
		if mode == "churn" {
			go func() {
				for time.Now().Before(deadline) {
					_ = exec.Command("/usr/bin/true").Run()
				}
			}()
		}
		interval := time.Duration(float64(time.Second) * stepMiB / float64(rate))
		next := time.Now()
		for total := 0; total < capMiB && time.Now().Before(deadline); total += stepMiB {
			grow(stepMiB)
			next = next.Add(interval)
			time.Sleep(time.Until(next))
		}
	case "spawn":
		interval := time.Duration(float64(time.Second) * 32 / float64(rate))
		var leaves []*exec.Cmd
		for total := 0; total < capMiB && time.Now().Before(deadline); total += 32 {
			cmd := exec.Command(os.Args[0], "-test.run=^TestDarwinRSSGuardGrowthChild$")
			cmd.Env = append(os.Environ(), "MYCE_GROWTH_MODE=leaf")
			if cmd.Start() == nil {
				leaves = append(leaves, cmd)
			}
			time.Sleep(interval)
		}
		defer func() {
			for _, c := range leaves {
				_ = c.Process.Kill()
				_ = c.Wait()
			}
		}()
	}
	time.Sleep(time.Until(deadline))
	runtime.KeepAlive(held)
}
