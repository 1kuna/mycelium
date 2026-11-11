package myce

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestRepeatedResourcesSchedulingPressure(t *testing.T) {
	_, clock := fixture(t)
	a := resourceCgroupFixture(t)
	path := filepath.Join(a.path, "cpu.pressure")
	for _, value := range []string{"some avg10=0 avg60=0 avg300=0 total=0\n", "some avg10=0 avg60=0 avg300=0 total=456\n"} {
		if err := os.WriteFile(path, []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
		r := cgroupResources(a, clock.Now())
		if r.CPU.Pressure == nil || r.CPU.Pressure.Some == nil || r.CPU.Pressure.Unavailable != "" {
			t.Fatal("scheduler contention coverage missing", r.CPU.Pressure)
		}
		if bytes.Contains([]byte(value), []byte("456")) && r.CPU.Pressure.Some.TotalUS != 456 {
			t.Fatal("scheduler delay lost")
		}
		copy := safeAttemptResources(r)
		r.CPU.Pressure.Some.TotalUS++
		if copy.CPU.Pressure.Some.TotalUS == r.CPU.Pressure.Some.TotalUS {
			t.Fatal("ingress retained mutable CPU pressure")
		}
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if p := cgroupResources(a, clock.Now()).CPU.Pressure; p == nil || p.Some != nil || p.Unavailable == "" {
		t.Fatal("missing CPU sensor became zero contention", p)
	}
}

// Keep the prior CPU/I/O wire contract intact with a queue that supports v1 but
// has a strict decoder predating CPU pressure. Every worker delivery shape and
// a live downgrade preserve durable local evidence while stripping only fields
// unsupported by that queue.
