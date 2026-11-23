package myce

import (
	"compress/gzip"
	"encoding/csv"
	"io"
	"os"
	"regexp"
	"strconv"
	"testing"
)

func TestMemoryEstimatorRetainedEvidenceReplay(t *testing.T) {
	f, err := os.Open("testdata/memory-evidence-replay.csv.gz")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	z, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer z.Close()
	r := csv.NewReader(z)
	if _, err := r.Read(); err != nil {
		t.Fatal(err)
	}
	anonymous := regexp.MustCompile(`^[a-f0-9]{24}$`)
	platform := regexp.MustCompile(`^(linux|darwin)/(cpu|cuda|xpu|metal)/(host|unified|discrete)$`)
	for _, confidence := range []float64{0.90, 0.95} {
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			t.Fatal(err)
		}
		if err := z.Reset(f); err != nil {
			t.Fatal(err)
		}
		r = csv.NewReader(z)
		if _, err := r.Read(); err != nil {
			t.Fatal(err)
		}
		evidence := newMemoryEvidence()
		var total, estimated, covered int
		var charged, used float64
		for {
			row, err := r.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(row) != 4 || !platform.MatchString(row[0]) || !anonymous.MatchString(row[1]) || !anonymous.MatchString(row[2]) {
				t.Fatalf("unexpected fixture record at row %d", total+2)
			}
			peak, err := strconv.ParseFloat(row[3], 64)
			if err != nil || !validRate(peak) {
				t.Fatalf("invalid peak at row %d", total+2)
			}
			key := memoryEvidenceKey{row[0], "ram", row[1], row[2]}
			guess := evidence.estimate(key, 0, confidence)
			total++
			if !guess.Alone {
				estimated++
				charged += guess.ChargeGB
				used += peak
				if guess.ChargeGB >= peak {
					covered++
				}
			}
			evidence.observe(memoryEvidencePoint{Key: key, Peak: peak})
		}
		coverage := float64(covered) / float64(estimated)
		t.Logf("target %.2f: %d observations, %d estimated, %d alone, %.1f%% covered, %.2fx charged/used", confidence, total, estimated, total-estimated, 100*coverage, charged/used)
		if total < 4000 || estimated < total*9/10 || coverage < confidence-0.05 || charged/used > 3.5 {
			t.Fatal("historical estimator coverage or charge outside the replay's guardrails")
		}
	}
}

func TestMemoryEstimatorColdPriorAndLimitedFloor(t *testing.T) {
	e := newMemoryEvidence()
	k := memoryEvidenceKey{"linux/cpu/host", "ram", "new-family", "new-exact"}
	for i := 1; i <= 9; i++ {
		before := e.estimate(k, 0, initialMemoryConfidence)
		if !before.Alone {
			t.Fatalf("shared with only %d previous families", i-1)
		}
		key := k
		key.Family, key.Exact = strconv.Itoa(i), strconv.Itoa(i)
		e.observe(memoryEvidencePoint{Key: key, Peak: float64(i)})
	}
	if got := e.estimate(k, 0, initialMemoryConfidence); got != (memoryPoolEstimate{TypicalGB: 5, ChargeGB: 9, Evidence: memoryEstimateEvidence{Level: "platform", Runs: 9}}) {
		t.Fatalf("cold prior = %+v", got)
	}
	e.observe(memoryEvidencePoint{Key: k, Peak: 8, Limited: true})
	if got := e.estimate(k, 0, initialMemoryConfidence); got.TypicalGB != 8 || got.ChargeGB != 16 || got.Evidence.Runs != 9 {
		t.Fatalf("limited attempt changed the platform prior or failed to set a floor: %+v", got)
	}
	other := k
	other.Exact = "other"
	if got := e.estimate(other, 0, initialMemoryConfidence); got.TypicalGB != 5 || got.ChargeGB != 9 {
		t.Fatalf("limited attempt changed other work's estimate: %+v", got)
	}
}
