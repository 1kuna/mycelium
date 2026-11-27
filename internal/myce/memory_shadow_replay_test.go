package myce

import (
	"encoding/csv"
	"encoding/json"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestMemoryShadowReplay(t *testing.T) {
	var rows []struct {
		Case, Pool                                                  string
		At                                                          int
		Capacity, LegacyCharge                                      float64
		Free, Slack, Reach                                          *float64
		Short, Fits                                                 bool
		Request, RaisedTo, Head, HeadStart, SmallJob, SmallDuration float64
		Attempts                                                    []struct {
			Expected, Peak, Growth, Ends float64
			Used                         *float64
			Lent                         bool
		}
	}
	data, err := os.ReadFile("testdata/memory-shadow-cases.json")
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&rows); err != nil {
		t.Fatal(err)
	}
	clock := &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	start := clock.Now()
	for _, r := range rows {
		t.Run(r.Case, func(t *testing.T) {
			clock.Advance(start.Add(time.Duration(r.At) * time.Second).Sub(clock.Now()))
			p := MemoryShadowPool{Name: r.Pool, CapacityGB: r.Capacity, FreeGB: r.Free}
			if p.Name == "" {
				p.Name = "ram"
			}
			for i, a := range r.Attempts {
				p.Attempts = append(p.Attempts, MemoryShadowAttempt{
					ID: strconv.Itoa(i), Attempt: 1, ExpectedGB: a.Expected, FootprintGB: a.Used, HighWaterGB: a.Peak, Growth: a.Growth, Lent: a.Lent,
				})
			}
			p.sum()
			if !shadowReplayEqual(p.SlackGB, r.Slack) || !shadowReplayEqual(p.ReachGB, r.Reach) {
				t.Fatalf("sample at %s produced S %v, reach %v", clock.Now(), p.SlackGB, p.ReachGB)
			}
			if r.Slack == nil {
				return
			}
			prior := p
			p.forecast(&prior, 1, 11)
			if (*p.ForecastFreeGB < 0) != r.Short {
				t.Fatalf("forecast %g", *p.ForecastFreeGB)
			}
			if r.Request > 0 && (r.Request <= *p.SlackGB) != r.Fits {
				t.Fatalf("fits with S %g", *p.SlackGB)
			}
			if r.RaisedTo > 0 {
				a := p.Attempts[0]
				if raised := min(*p.ReachGB, 2*max(a.ExpectedGB, a.HighWaterGB)); raised != r.RaisedTo {
					t.Fatalf("raised E %g", raised)
				}
			}
			if r.LegacyCharge > 0 && r.LegacyCharge <= p.CommitmentGB {
				t.Fatalf("comparison did not recover any charge")
			}
			if r.Head > 0 {
				// Feed the R3 example with stage 0 commitments and synthetic
				// completion times. This does not call a placement path.
				room, earliest := *p.SlackGB, 0.0
				for i, a := range p.Attempts {
					room += *a.CommitmentGB
					if room >= r.Head {
						earliest = r.Attempts[i].Ends
						break
					}
				}
				if earliest != r.HeadStart || r.SmallJob > *p.SlackGB || float64(r.At)+r.SmallDuration > earliest {
					t.Fatalf("head fits at %g, small completion at %g", earliest, float64(r.At)+r.SmallDuration)
				}
			}
		})
	}
}

func shadowReplayEqual(got, want *float64) bool {
	return got == nil && want == nil || got != nil && want != nil && math.Abs(*got-*want) < 1e-9
}

func TestMemoryShadowAppendixAReplay(t *testing.T) {
	data, err := os.Open("testdata/memory-shadow-history.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer data.Close()
	rows, err := csv.NewReader(data).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	clean := map[string][]float64{}
	type replay struct {
		compared, overshoot        int
		expectedRatio, legacyRatio float64
	}
	results := map[string]replay{}
	for _, r := range rows[1:] {
		read := func(s string) *float64 {
			if s == "" {
				return nil
			}
			v, err := strconv.ParseFloat(s, 64)
			if err != nil {
				t.Fatal(err)
			}
			return &v
		}
		platform := r[6]
		// The fixture's discrete host values already exclude driver system
		// memory and the synthetic driver reports an explicit zero.
		host, _ := shadowFootprint(platform, shadowMemoryCounters{read(r[7]), read(r[8]), shadowGB(0)})
		if host == nil {
			continue
		}
		key := strings.Join(r[:7], "\x00")
		prior := clean[key]
		if len(prior) > 0 {
			e := slices.Max(prior)
			got := results[platform]
			got.compared++
			got.expectedRatio += e / *host
			got.legacyRatio += *read(r[9]) / *host
			if *host > e {
				got.overshoot++
			}
			results[platform] = got
		}
		if len(prior) == 20 {
			prior = prior[1:]
		}
		clean[key] = append(prior, *host)
	}
	cpu := results[shadowLinuxCPU]
	if cpu.compared != 21 || cpu.overshoot != 1 || math.Abs(cpu.expectedRatio-(19*20.0/7+20.0/8+8.0/9)) > 1e-9 || cpu.expectedRatio >= cpu.legacyRatio {
		t.Fatalf("last-20 replay %+v", cpu)
	}
	for _, p := range []string{shadowLinuxUnified, shadowLinuxDiscrete, shadowDarwin} {
		got := results[p]
		if got.compared != 1 || got.overshoot != 1 || got.expectedRatio >= got.legacyRatio {
			t.Fatalf("%s replay %+v", p, got)
		}
	}
}
