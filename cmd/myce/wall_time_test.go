package main

import (
	"strings"
	"testing"
)

func TestWallTimeAdapterLowering(t *testing.T) {
	data := strings.Replace(cpuAdapterYAML("source"), "    source:", "    wall_time: 10m\n    source:", 1)
	a, err := decodeAdapter([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	p, _, err := adapterProfile(a, "")
	if err != nil || p.WallTime != "10m" {
		t.Fatal("adapter lost wall_time", p.WallTime, err)
	}
}
