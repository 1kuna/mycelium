package myce

import (
	"math"
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

func TestBatchPlanResearchArithmeticAndTail(t *testing.T) {
	mini := syntheticComponent("mini", "mini", map[int]float64{1: 400.0 / 25, 2: 400.0 / 22, 4: 400.0 / 18, 8: 400.0 / 12})
	spark := syntheticComponent("spark", "spark", map[int]float64{1: 8, 2: 10, 4: 400.0 / 30, 8: 20})
	b1 := syntheticComponent("b1", "b", map[int]float64{1: 400.0 / 35, 2: 400.0 / 28, 4: 400.0 / 18, 8: 40})
	b2 := b1
	b2.id = "b2"
	if got, _ := batchFinish(mini, 1, 40, 1); got != 640 {
		t.Fatal(got)
	}
	if got, _ := batchFinish(mini, 8, 40, 1); math.Abs(got-166.6666666667) > 1e-6 {
		t.Fatal(got)
	}
	naming := planBatch([]batchComponent{mini, spark, b1, b2}, 40, 1)
	namingPieces := 0
	for _, a := range naming {
		namingPieces += a.pieces
		if a.seconds > 40+1e-6 {
			t.Fatalf("research allocation regressed: %+v", a)
		}
	}
	if namingPieces != 40 {
		t.Fatalf("incomplete naming allocation: %+v", naming)
	}
	b := syntheticComponent("b", "b", map[int]float64{1: 10, 4: 11, 8: 16})
	s := syntheticComponent("s", "s", map[int]float64{1: 12, 4: 14, 8: 24})
	m := syntheticComponent("m", "m", map[int]float64{1: 15, 4: 18, 8: 40})
	if got, _ := batchFinish(m, 4, 48, 1); got != 216 {
		t.Fatal(got)
	}
	plan := planBatch([]batchComponent{b, s, m}, 48, 1)
	count := 0
	for _, a := range plan {
		count += a.pieces
		if a.seconds > 48+1e-6 || a.component == "m" && a.count > 4 {
			t.Fatalf("CPU curve ignored: %+v", plan)
		}
	}
	if count != 48 {
		t.Fatal(plan)
	}
	short := planBatch([]batchComponent{mini}, 1, 1)
	if len(short) != 1 || short[0].count != 1 || short[0].seconds != 16 {
		t.Fatal(short)
	}
	falling := planBatch([]batchComponent{m}, 8, 1)
	if len(falling) != 1 || falling[0].count != 4 || falling[0].seconds != 36 {
		t.Fatal(falling)
	}
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
