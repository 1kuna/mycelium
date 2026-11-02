package myce

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

func TestDeviceFaultSourceKernelInput(t *testing.T) {
	input := []byte(`{"__CURSOR":"a","__MONOTONIC_TIMESTAMP":"123","MESSAGE":"xe 0000:03:00.0: Engine memory CAT error. Fault response: Unsuccessful -EINVAL"}
{"__CURSOR":"b","__MONOTONIC_TIMESTAMP":"124","MESSAGE":"i915 0000:07:00.0: [drm] GPU HANG: ecode 12:0:85dffffb"}
{"__CURSOR":"c","__MONOTONIC_TIMESTAMP":"125","MESSAGE":"NVRM: Xid (PCI:0000:09:00): 79, GPU has fallen off the bus."}
`)
	lines, cursor, err := parseKernelJournal(input)
	if err != nil || cursor != "c" || len(lines) != 3 {
		t.Fatalf("kernel input: lines=%d cursor=%q err=%v", len(lines), cursor, err)
	}
	for i, tc := range []struct {
		pci, reason string
		match       DeviceFaultMatcher
	}{
		{"0000:03:00.0", "device_fault_xe", intelKernelFault},
		{"0000:07:00.0", "device_fault_i915", intelKernelFault},
		{"00000000:09:00.0", "device_fault_nvidia_xid", cudaKernelFault},
	} {
		if got := tc.match(MachineDevice{pciAddress: tc.pci}, lines[i].Message); got != tc.reason {
			t.Fatalf("family %d: %q", i, got)
		}
		if got := tc.match(MachineDevice{pciAddress: "0000:ff:00.0"}, lines[i].Message); got != "" {
			t.Fatalf("fault crossed PCI devices: %q", got)
		}
	}
}

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
}
