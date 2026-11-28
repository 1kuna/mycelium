package myce

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestXPUDeviceSamplingCoverage(t *testing.T) {
	// Replay the retained summary for two 18-sample attempts (#2185).
	// The original timestamps between these landmarks were not retained.
	const (
		first     = 10*time.Second + 2926252*time.Nanosecond
		peak      = 80*time.Second + 4366898*time.Nanosecond
		gap       = 10*time.Second + 1730010*time.Nanosecond
		end       = 102*time.Second + 510316422*time.Nanosecond
		lateFirst = 15*time.Second + 3100964*time.Nanosecond
		longGap   = 15*time.Second + 927913*time.Nanosecond
		longEnd   = 111*time.Second + 517314318*time.Nanosecond
	)
	const pci = "0000:01:00.0"
	proc := t.TempDir()
	for pid, fdinfo := range map[int]string{
		7: xeAttemptFD("7", pci, 192876, 0),
		8: xeAttemptFD("8", pci, 5175680, 0),
		9: "pos:\t0\n",
	} {
		dir := filepath.Join(proc, fmt.Sprint(pid), "fdinfo")
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "3"), []byte(fdinfo), 0600); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		name                             string
		first, peak, gap, lastCycle, end time.Duration
		want                             bool
	}{
		{"one missed cycle with jitter", first, peak, gap, 100 * time.Second, end, true},
		{"end with jitter", first, peak, gap, 100 * time.Second, 110*time.Second + 5*time.Millisecond, true},
		{"missing start and interior samples", lateFirst, 95*time.Second + 2540734*time.Nanosecond, longGap, 110 * time.Second, longEnd, false},
		{"stalled mid-run", first, peak, longGap, 105 * time.Second, 107 * time.Second, false},
		{"missing end samples", first, peak, gap, 100 * time.Second, 115 * time.Second, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			type reading struct {
				at  time.Duration
				pid int
			}
			// A poll at the first cycle finds no matching DRM client. A
			// poll is also missed between the peak and the next reading.
			polls := []reading{{5 * time.Second, 9}, {tc.first, 7}}
			for at := tc.first.Truncate(time.Second) + 5*time.Second; at < tc.peak.Truncate(time.Second); at += 5 * time.Second {
				polls = append(polls, reading{at + 3*time.Millisecond, 7})
			}
			polls = append(polls, reading{tc.peak, 8}, reading{tc.peak + 5*time.Second, 9}, reading{tc.peak + tc.gap, 7})
			for at := (tc.peak + tc.gap).Truncate(time.Second) + 5*time.Second; at <= tc.lastCycle; at += 5 * time.Second {
				polls = append(polls, reading{at + 3*time.Millisecond, 7})
			}
			start := time.Unix(100, 0)
			clock := &workerFakeClock{now: start}
			w := Worker{Machine: Machine{GPU: "xpu"}, selectedDevice: &MachineDevice{pciAddress: pci}}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			ticks := make(chan time.Time, len(polls)+1)
			for range cap(ticks) {
				ticks <- start
			}
			i := 0
			done := w.observeDeviceMemory(ctx, func() (map[int]bool, map[int]bool, error) {
				if i == len(polls) {
					clock.Advance(start.Add(tc.end).Sub(clock.Now()))
					cancel()
					return nil, nil, nil
				}
				p := polls[i]
				i++
				clock.Advance(start.Add(p.at).Sub(clock.Now()))
				return map[int]bool{p.pid: true}, nil, nil
			}, gpuHostFS{memoryFS: osMemoryFS{}, proc: proc}, deviceMemorySamplePeriod, clock, nil, ticks)
			select {
			case r := <-done:
				s := r.peak.DeviceSampling
				if s == nil || s.Samples != 18 || s.FirstAtSeconds != tc.first.Seconds() || s.PeakAtSeconds != tc.peak.Seconds() ||
					s.MaxIntervalSeconds != tc.gap.Seconds() || s.ElapsedSeconds != tc.end.Seconds() || s.Complete != tc.want {
					t.Fatalf("device coverage = %+v, want complete=%t with 18 samples", s, tc.want)
				}
				shadow := shadowFootprintSampler{platform: shadowLinuxDiscrete}
				shadow.state.Store(&shadowAttemptReading{})
				card := shadow.finishPeak(r.peak, clock.Now(), attemptMemorySamplePeriod).Learned.Card
				if card.Complete != tc.want || tc.want && card.PeakGB != s.MaxGB {
					t.Fatalf("terminal card peak = %+v, want complete=%t with peak %g", card, tc.want, s.MaxGB)
				}
			case <-time.After(testWaitTimeout):
				t.Fatal("device sampler did not finish")
			}
		})
	}
}
