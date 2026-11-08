package myce

import (
	"testing"
)

func syntheticComponent(id, host string, seconds map[int]float64) batchComponent {
	m := batchComponent{id: id, host: host, cpuBudget: 16, hostBudget: 64}
	for _, c := range []int{1, 2, 4, 8} {
		if s := seconds[c]; s > 0 {
			m.points = append(m.points, batchPoint{count: c, rate: float64(c) / s, cpu: float64(c), host: float64(c)})
		}
	}
	return m
}

func TestBatchPlanSharedHostBudget(t *testing.T) {
	a := syntheticComponent("a", "shared", map[int]float64{1: 10, 4: 12, 8: 20})
	b := a
	b.id = "b"
	a.cpuBudget, b.cpuBudget = 4, 4
	a.hostBudget, b.hostBudget = 4, 4
	plan := planBatch([]batchComponent{a, b}, 8, 1)
	concurrent := 0
	for _, p := range plan {
		concurrent += p.count
	}
	if concurrent > 4 {
		t.Fatalf("shared resources double counted: %+v", plan)
	}
}
