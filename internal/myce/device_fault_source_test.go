package myce

import (
	"errors"
)

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// #595: a 313k-record boot journal (mostly P2P warnings) took 9.4 s to read in
// full. Startup reads from worker start and later scans follow the cursor.
func TestDeviceFaultSourceBoundedStartupRead(t *testing.T) {
	start := time.Date(2026, 11, 2, 4, 0, 0, 0, time.UTC)
	type record struct {
		at      time.Time
		message string
	}
	boot := make([]record, 313_000)
	for i := range boot {
		boot[i] = record{start.Add(-13*24*time.Hour + time.Duration(i)*3*time.Second), "pcieport 0000:00:01.0: P2P DMA warning"}
	}
	var requests [][]string
	journal := func(_ context.Context, args []string) ([]byte, error) {
		requests = append(requests, slices.Clone(args))
		from := 0
		for _, arg := range args {
			if v, ok := strings.CutPrefix(arg, "--since=@"); ok {
				seconds, _ := strconv.ParseInt(v, 10, 64)
				from = sort.Search(len(boot), func(i int) bool { return boot[i].at.Unix() >= seconds })
			}
			if v, ok := strings.CutPrefix(arg, "--after-cursor="); ok {
				from, _ = strconv.Atoi(v)
				from++
			}
		}
		var out bytes.Buffer
		for i := from; i < len(boot); i++ {
			fmt.Fprintf(&out, `{"__CURSOR":"%d","__MONOTONIC_TIMESTAMP":"%d","__REALTIME_TIMESTAMP":"%d","MESSAGE":%q}`+"\n", i, i, boot[i].at.UnixMicro(), boot[i].message)
		}
		return out.Bytes(), nil
	}
	clock := &fakeClock{now: start}
	m := m3Machine()
	m.Devices[0].pciAddress = "0000:03:00.0"
	w := Worker{Machine: m, Clock: clock, DeviceFaultMatchers: map[string]DeviceFaultMatcher{"fake": intelKernelFault}}
	w.DeviceFaultSource = kernelLogSource(func() ([]byte, error) { return []byte("boot\n"), nil }, journal, nil)
	root := &workerStore{dir: t.TempDir()}
	began := time.Now()
	if err := w.initDeviceFaults(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(began); elapsed > 2*time.Second || w.Machine.FaultMonitorError != "" {
		t.Fatalf("startup read took %s, monitor error %q", elapsed, w.Machine.FaultMonitorError)
	}
	if !slices.Contains(requests[0], "--since=@"+strconv.FormatInt(start.Unix(), 10)) {
		t.Fatalf("startup read is not bounded: %v", requests[0])
	}
	boot = append(boot, record{start.Add(10 * time.Second), "xe 0000:03:00.0: [drm] GPU HANG"}, record{start.Add(11 * time.Second), "pcieport 0000:00:01.0: P2P DMA warning"})
	clock.Advance(15 * time.Second)
	if err := w.refreshDeviceFaults(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	if w.Machine.Devices[0].Reason != "device_fault_xe" {
		t.Fatal("fault after startup missed", w.Machine.Devices)
	}
	if err := w.refreshDeviceFaults(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	if last := requests[len(requests)-1]; !slices.Contains(last, "--after-cursor=313001") {
		t.Fatalf("later scan does not follow the cursor: %v", last)
	}

	t.Run("DeviceFaultReadFailureReported", func(t *testing.T) {

		q, clock := fixture(t)
		w := Worker{Machine: m3Machine(), Clock: clock}
		unreadable := errors.New("kernel log not readable")
		failing := true
		w.DeviceFaultSource = kernelLogSource(func() ([]byte, error) { return []byte("boot\n"), nil }, func(context.Context, []string) ([]byte, error) {
			if failing {
				return nil, errors.New("journal denied")
			}
			return nil, nil
		}, func(context.Context, time.Time) ([]KernelLogLine, error) { return nil, unreadable })
		root := &workerStore{dir: t.TempDir()}
		if err := w.initDeviceFaults(context.Background(), root); err != nil {
			t.Fatal(err)
		}
		if err := w.refreshDeviceFaults(context.Background(), root); err != nil {
			t.Fatal(err)
		}
		if w.Machine.FaultMonitorError != "kernel log not readable" {
			t.Fatalf("read failure not reported: %q", w.Machine.FaultMonitorError)
		}
		machineCheck(t, q, w.Machine)
		snapshot, err := q.Snapshot()
		if err != nil || len(snapshot.Machines) != 1 || snapshot.Machines[0].FaultMonitorError != "kernel log not readable" {
			t.Fatalf("status lacks fault monitor error: %v %+v", err, snapshot.Machines)
		}
		failing = false
		if err := w.refreshDeviceFaults(context.Background(), root); err != nil {
			t.Fatal(err)
		}
		if w.Machine.FaultMonitorError != "" {
			t.Fatal("recovery not reported", w.Machine.FaultMonitorError)
		}

	})
}
