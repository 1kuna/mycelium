package myce

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func TestPlacementUnknownCapacityReason(t *testing.T) {
	for _, missing := range []string{"system", "accelerator"} {
		t.Run(missing, func(t *testing.T) {
			q, _ := fixture(t)
			m := machine("worker", "xpu")
			m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 28
			want := "system RAM capacity unavailable"
			switch missing {
			case "system":
				m.CapabilityProfile.SystemMemoryBytes = 0
			case "accelerator":
				m.CapabilityProfile.Accelerators[0].MemoryBytes = 0
				want = "accelerator memory capacity unavailable"
			}
			s := spec("experiment")
			s.Resumable = false
			s.Needs.MemoryGB, s.Needs.HostMemoryGB = 1, 1
			s.Needs.Platform = Platform{Required: []string{"xpu"}}
			j := submit(t, q, s)
			if check(t, q, m, nil).Job != nil {
				t.Fatal("unknown capacity admitted")
			}
			if got := job(t, q, j.ID); got.Attempt != 0 || !strings.Contains(got.Reason, want) {
				t.Fatalf("unhelpful job reason: %+v", got)
			}
			if got := q.statusWaitingReason(q.s.Jobs[j.ID]); !strings.Contains(got, want) {
				t.Fatal("status hid missing capacity:", got)
			}
			// A healthy eligible sibling still places the job.
			m = machine("healthy", "xpu")
			m.HostMemoryMeasured, m.HostAvailableMemoryGB = true, 28
			if got := check(t, q, m, nil).Job; got == nil || got.ID != j.ID {
				t.Fatal("known-capacity worker did not place job", got)
			}
		})
	}
}

func TestPlacementLinuxFreshHostAllowance(t *testing.T) {
	q, clock := fixture(t)
	dir := filepath.Join(newTestStateRoot(t), "host")
	budget := 24.0
	if _, err := q.ChangeHostMaintenance("host", HostMaintenanceRequest{Action: "drain", MemoryBudgetGB: &budget}); err != nil {
		t.Fatal(err)
	}
	if err := setMaintenance(t, q, "host", "resume"); err != nil {
		t.Fatal(err)
	}
	probe := ProfileProbe{
		Machine: MachineProbe{OS: "linux", Arch: "amd64", Command: func(_ context.Context, name string, args ...string) ([]byte, error) {
			if name == "sycl-ls" {
				if len(args) > 0 && args[0] == "--verbose" {
					return []byte("global_mem_size: 34359738368\n"), nil
				}
				return []byte("[level_zero:gpu:0] Intel discrete GPU\n"), nil
			}
			return nil, errors.New("unavailable")
		}},
		ReadFile: func(path string) ([]byte, error) {
			if path != "/proc/meminfo" {
				t.Fatal("unexpected RAM source", path)
			}
			return []byte("MemTotal: 33554432 kB\nMemAvailable: 33554432 kB\n"), nil
		},
		CPUs: func() int { return 8 },
		Disk: func(string) (uint64, uint64, error) { return 100 << 30, 50 << 30, nil },
	}
	for i, hostNeed := range []float64{8, 12, 5} {
		m := machine([]string{"card-0", "card-1", "card-2"}[i], "xpu")
		m.HostID, m.Device, m.FreeMemoryGB = "host", "0", 32
		profile := probe.Detect(context.Background(), m, dir, clock.Now())
		m.CapabilityProfile = &profile
		if profile.SystemMemoryBytes != 32<<30 || len(profile.Unavailable) != 0 || availabilityBlocked(&m) {
			t.Fatal("Linux capacity probe failed", profile)
		}
		m.HostMemoryBudgetGB = 24
		source, err := newHostMemorySource(4, "linux", probe.ReadFile, nil)
		if err != nil {
			t.Fatal(err)
		}
		m.HostAvailableMemoryGB, err = source(context.Background())
		m.HostMemoryMeasured = true
		if err != nil || m.HostAvailableMemoryGB != 28 {
			t.Fatal("RAM reserve not applied", err)
		}
		s := spec("production")
		s.Resumable = false
		s.Needs.MemoryGB, s.Needs.HostMemoryGB = 1, hostNeed
		s.Needs.Platform = Platform{Required: []string{"xpu"}}
		j := submit(t, q, s)
		run := check(t, q, m, nil).Job
		if i < 2 {
			if run == nil || run.ID != j.ID || run.Attempt != 1 {
				t.Fatal("production did not place within 24 GiB", run)
			}
		} else if run != nil {
			t.Fatal("aggregate 25 GiB exceeded the 24 GiB allowance")
		}
	}
}
