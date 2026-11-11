//go:build darwin && cgo

package myce

import (
	"bufio"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"mycelium/internal/probeguard"
)

// The footprint probe reads the same tree, members and RSS as the resident
// probe, and adds the phys_footprint that Metal buffers are charged to.
func TestLibprocOwnedFootprintMatchesResident(t *testing.T) {
	before := probeguard.Before
	probeguard.Before = nil
	defer func() { probeguard.Before = before }()
	cmd := exec.Command("sh", "-c", "sleep 30 & sleep 30 & wait")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); _ = cmd.Wait() }()
	time.Sleep(200 * time.Millisecond)
	birth, err := processBirth(cmd.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	roots := map[int]string{cmd.Process.Pid: birth}
	rss, members, err, ok := darwinFastOwnedResident(roots)
	if err != nil || !ok {
		t.Fatal(err, ok)
	}
	footprint, resident, fpMembers, err := darwinOwnedFootprintProbe()(roots)
	if err != nil || fpMembers != members || members != 2 || footprint == 0 || max(rss, resident)-min(rss, resident) > 1<<20 {
		t.Fatalf("footprint %d rss %d/%d members %d/%d: %v", footprint, resident, rss, fpMembers, members, err)
	}
}

// Opt-in native measurement of the footprint guard's reaction time and
// overshoot for a Metal job: a compiled Swift child allocates private
// MTLBuffers, fills each with a GPU blit, and logs its own phys_footprint and
// RSS after every step. At most growthCapMiB per scenario on the host running
// it; requires swiftc and a Metal device.
//
//	MYCE_NATIVE_METAL_TEST=1 go test -run '^TestDarwinMetalFootprintGuardOvershoot$' -v ./internal/myce
func TestDarwinMetalFootprintGuardOvershoot(t *testing.T) {
	if os.Getenv("MYCE_NATIVE_METAL_TEST") != "1" {
		t.Skip("requires explicit bounded native Metal proof")
	}
	before := probeguard.Before
	probeguard.Before = nil
	defer func() { probeguard.Before = before }()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "grow.swift"), []byte(metalGrowthSource), 0o600); err != nil {
		t.Fatal(err)
	}
	child := filepath.Join(dir, "metal-grow")
	if out, err := exec.Command("swiftc", "-O", "-framework", "Metal", filepath.Join(dir, "grow.swift"), "-o", child).CombinedOutput(); err != nil {
		t.Fatalf("swiftc: %v\n%s", err, out)
	}
	const deviceMiB, hostMiB, growthCapMiB = 384, 128, 3072
	limit := int64(deviceMiB+hostMiB) << 20
	for _, rate := range []int{256, 1024} {
		t.Run(strconv.Itoa(rate)+"MiBps", func(t *testing.T) {
			w := recoveryWorker(t)
			w.Machine.GPU = "metal"
			w.HostMemorySource = func(context.Context) (float64, error) { return 64, nil }
			w.processMemory = localProcessMemory()
			store, err := w.openStore()
			if err != nil {
				t.Fatal(err)
			}
			defer store.close()
			log := filepath.Join(t.TempDir(), "growth.log")
			j := recoveryJob(t, []string{"sh", "-c", `"$@" & wait`, "metal-parent", child})
			j.Spec.Needs = ResourceNeeds{MemoryGB: deviceMiB / 1024.0, HostMemoryGB: hostMiB / 1024.0, Platform: Platform{Required: []string{"metal"}}}
			j.Spec.Run.Env = map[string]string{"MYCE_GROWTH_LOG": log, "MYCE_GROWTH_RATE_MIB": strconv.Itoa(rate), "MYCE_GROWTH_CAP_MIB": strconv.Itoa(growthCapMiB)}
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
			case <-time.After(60 * time.Second):
				t.Fatal("attempt did not finish")
			}
			finished := time.Now()
			m := readMetalGrowthLog(t, log, uint64(limit))
			stopped := r.ExecutionCause != nil && r.ExecutionCause.Class == rssLimitExceeded
			evidence := "none"
			if stopped {
				evidence = r.ExecutionCause.String()
			}
			t.Logf("RESULT %d MiB/s: stopped=%t limit=%dMiB reaction(last write)=%s reaction(finish)=%s buffers at stop=%dMiB footprint at stop=%dMiB overshoot=%dMiB rss at stop=%dMiB guard=%s",
				rate, stopped, limit>>20, m.last.Sub(m.crossed).Round(time.Millisecond), finished.Sub(m.crossed).Round(time.Millisecond),
				m.buffers>>20, m.footprint>>20, (int64(m.footprint)-limit)>>20, m.rss>>20, evidence)
			if !stopped || r.ExecutionCause.RSSLimit == nil || r.ExecutionCause.RSSLimit.Quality != footprintGuardQuality {
				t.Errorf("footprint guard did not stop Metal growth past %d MiB: %+v", limit>>20, r)
			}
		})
	}
}

type metalGrowthLog struct {
	crossed, last           time.Time
	buffers, footprint, rss uint64
}

// Lines are "unixnano buffer-bytes footprint-bytes rss-bytes" after each blit.
func readMetalGrowthLog(t *testing.T, path string, limit uint64) metalGrowthLog {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var g metalGrowthLog
	s := bufio.NewScanner(f)
	for s.Scan() {
		fields := strings.Fields(s.Text())
		if len(fields) != 4 {
			continue
		}
		var v [4]uint64
		for i := range v {
			v[i], err = strconv.ParseUint(fields[i], 10, 64)
			if err != nil {
				t.Fatal(err)
			}
		}
		at := time.Unix(0, int64(v[0]))
		g.last, g.buffers, g.footprint, g.rss = at, v[1], v[2], v[3]
		if g.crossed.IsZero() && v[2] >= limit {
			g.crossed = at
		}
	}
	if g.crossed.IsZero() {
		t.Fatalf("footprint never crossed the limit: %+v", g)
	}
	return g
}

const metalGrowthSource = `import Foundation
import Metal

func counters() -> (UInt64, UInt64) {
    var vm = task_vm_info_data_t()
    var count = mach_msg_type_number_t(MemoryLayout<task_vm_info_data_t>.size / MemoryLayout<integer_t>.size)
    let kr = withUnsafeMutablePointer(to: &vm) {
        $0.withMemoryRebound(to: integer_t.self, capacity: Int(count)) { task_info(mach_task_self_, task_flavor_t(TASK_VM_INFO), $0, &count) }
    }
    return kr == KERN_SUCCESS ? (vm.phys_footprint, UInt64(vm.resident_size)) : (0, 0)
}

let env = ProcessInfo.processInfo.environment
let rate = Double(env["MYCE_GROWTH_RATE_MIB"] ?? "256")!
let capMiB = Int(env["MYCE_GROWTH_CAP_MIB"] ?? "1024")!
guard let path = env["MYCE_GROWTH_LOG"], FileManager.default.createFile(atPath: path, contents: nil),
      let log = FileHandle(forWritingAtPath: path),
      let device = MTLCreateSystemDefaultDevice(), let queue = device.makeCommandQueue() else { exit(2) }
let stepMiB = 16
let step = stepMiB << 20
let interval = Double(stepMiB) / rate
var held: [MTLBuffer] = []
var next = Date()
let deadline = Date().addingTimeInterval(30)
while held.count * stepMiB < capMiB && Date() < deadline {
    guard let buffer = device.makeBuffer(length: step, options: .storageModePrivate),
          let command = queue.makeCommandBuffer(), let blit = command.makeBlitCommandEncoder() else { exit(3) }
    blit.fill(buffer: buffer, range: 0..<step, value: 0xA5)
    blit.endEncoding()
    command.commit()
    command.waitUntilCompleted()
    held.append(buffer)
    let (footprint, rss) = counters()
    let line = "\(UInt64(Date().timeIntervalSince1970 * 1e9)) \(held.count * step) \(footprint) \(rss)\n"
    log.write(line.data(using: .utf8)!)
    next = next.addingTimeInterval(interval)
    let wait = next.timeIntervalSinceNow
    if wait > 0 { Thread.sleep(forTimeInterval: wait) }
}
Thread.sleep(until: deadline)
withExtendedLifetime(held) {}
`
