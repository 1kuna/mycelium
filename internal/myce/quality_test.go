package myce

import (
	"math"
	"strings"
	"testing"
	"time"
)

func qualityNumber(v float64) *float64 { return &v }

func qualityVariants(t *testing.T, observation *QualityMeasurement) (Extension, Machine, Machine) {
	t.Helper()
	e, a, b := variantsFixture(t, true)
	work := "extension:" + e.Name
	for i := range e.Variants {
		e.Variants[i].Quality = map[string]map[string]QualityMeasurement{work: {"score": {Reference: "frozen-corpus-v1-reference-recipe", Value: qualityNumber(10), ReferenceValue: qualityNumber(10), MarginOfError: qualityNumber(0.125)}}}
	}
	if observation == nil {
		e.Variants[1].Quality = nil
	} else {
		e.Variants[1].Quality[work]["score"] = *observation
	}
	e = freezeComposition(t, e)
	for _, m := range []*Machine{&a, &b} {
		r := newExtensionReport(e, m, work)
		r.Setup, r.Qualified, r.CheckID = true, true, "quality-native-check"
		m.ExtensionReports = []ExtensionReport{r}
	}
	return e, a, b
}

func TestQualityVariantEligibility(t *testing.T) {
	for _, tc := range []struct {
		name, class             string
		value                   float64
		tolerance               map[string]float64
		unknown, approved, want bool
	}{
		{name: "default measured parity", class: "production", value: 10.125, want: true},
		{name: "declared tolerance", class: "production", value: 10.25, tolerance: map[string]float64{"score": 0.25}, want: true},
		{name: "default excludes", class: "production", value: 10.25},
		{name: "approval cannot override failure", class: "production", value: 10.25, approved: true},
		{name: "experiment respects tolerance", class: "experiment", value: 10.25},
		{name: "explicit zero is exact", class: "production", value: 10.125, tolerance: map[string]float64{"score": 0}},
		{name: "unmeasured holds production", class: "production", unknown: true},
		{name: "unmeasured uses approval", class: "production", unknown: true, approved: true, want: true},
		{name: "unmeasured permits experiments", class: "experiment", unknown: true, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, clock := fixture(t)
			observation := &QualityMeasurement{Reference: "frozen-corpus-v1-reference-recipe", Value: qualityNumber(tc.value), ReferenceValue: qualityNumber(10), MarginOfError: qualityNumber(0.125)}
			if tc.unknown {
				observation = nil
			}
			e, a, b := qualityVariants(t, observation)
			if err := q.AddExtension(e); err != nil {
				t.Fatal(err)
			}
			check(t, q, a, nil)
			check(t, q, b, nil)
			if tc.approved {
				setParityHosts(t, q, "test", "extension:"+e.Name, hostID(&b))
			}
			spec := variantSpec(e, tc.class)
			spec.QualityTolerance = tc.tolerance
			j := submit(t, q, spec)
			clock.Advance(time.Second)
			got, err := q.jobSnapshot(j.ID)
			if err != nil {
				t.Fatal(err)
			}
			for metric, limit := range tc.tolerance {
				if got.Spec.QualityTolerance[metric] != limit {
					t.Fatal("status lost declared tolerance")
				}
			}
			found := false
			for _, row := range got.VariantEligibility {
				if row.Host == hostID(&b) && row.Variant == machineTarget(&b) {
					found = true
					if row.Eligible != tc.want {
						t.Fatalf("eligibility: %+v", row)
					}
					if !tc.want {
						reason := "out of tolerance"
						if tc.unknown {
							reason = "quality measurements unknown"
						}
						if !strings.Contains(strings.Join(row.Reasons, ";"), reason) {
							t.Fatalf("missing quality reason: %+v", row)
						}
					}
				}
			}
			if !found {
				t.Fatal("missing variant status")
			}
			q = reopenVariants(t, q, clock)
			assigned := check(t, q, b, nil).Job
			if (assigned != nil) != tc.want {
				t.Fatalf("placement want %v, got %+v", tc.want, assigned)
			}
			if assigned != nil && assigned.ID != j.ID {
				t.Fatal("wrong assignment")
			}
		})
	}
}

func TestQualityUnknownUncertainty(t *testing.T) {
	q, _ := fixture(t)
	observation := &QualityMeasurement{Reference: "frozen-reference", Value: qualityNumber(10), ReferenceValue: qualityNumber(10)}
	e, a, b := qualityVariants(t, observation)
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	check(t, q, a, nil)
	check(t, q, b, nil)
	spec := variantSpec(e, "production")
	spec.QualityTolerance = map[string]float64{"score": 1}
	j := submit(t, q, spec)
	// Admission evidence may read the same job while these private helpers
	// inspect parity. Keep both assertions on one synchronized queue state.
	q.mu.Lock()
	defer q.mu.Unlock()
	if reason := q.variantParityReason(q.s.Jobs[j.ID], &b); !strings.Contains(reason, "unknown") {
		t.Fatal("absent uncertainty treated as zero:", reason)
	}
	if q.productionParityKnown(q.s.Jobs[j.ID], &b) {
		t.Fatal("unknown uncertainty permits retargeting")
	}
}

func TestQualityValidation(t *testing.T) {
	e, _, _ := qualityVariants(t, nil)
	for _, invalid := range []float64{-1, math.NaN(), math.Inf(1)} {
		spec := variantSpec(e, "production")
		spec.QualityTolerance = map[string]float64{"score": invalid}
		if err := spec.Validate(); err == nil {
			t.Fatal("accepted invalid tolerance", invalid)
		}
	}
	e.Variants[0].Quality["extension:"+e.Name]["score"] = QualityMeasurement{Reference: "frozen", MarginOfError: qualityNumber(-1)}
	if err := e.validate(); err == nil {
		t.Fatal("accepted negative uncertainty")
	}
}

func TestQualityProductionUsesMeasuredParityHost(t *testing.T) {
	q, clock := fixture(t)
	e, a, b := qualityVariants(t, &QualityMeasurement{Reference: "frozen-reference", Value: qualityNumber(10.125), ReferenceValue: qualityNumber(10), MarginOfError: qualityNumber(0.125)})
	if err := q.AddExtension(e); err != nil {
		t.Fatal(err)
	}
	if err := q.ConfigureHosts(map[string]HostPolicy{hostID(&a): {ProductionHold: true, ProductionHoldScope: "device"}, hostID(&b): {ProductionHold: true, ProductionHoldScope: "device"}}); err != nil {
		t.Fatal(err)
	}
	check(t, q, a, nil)
	first := submit(t, q, variantSpec(e, "production"))
	running := check(t, q, a, nil).Job
	if running == nil || running.ID != first.ID {
		t.Fatal("first production did not start")
	}
	if err := q.Started(result(running, true)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Millisecond)
	queued := submit(t, q, variantSpec(e, "production"))
	assigned := check(t, q, b, nil).Job
	if assigned == nil || assigned.ID != queued.ID {
		t.Fatal("measured parity did not permit the second host", assigned)
	}
}
