package myce

import (
	"bytes"
	"fmt"
	"log"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestJobGetVolume(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 11, 4, 0, 0, 0, 0, time.UTC)}
	v := newJobGetVolume(clock)
	var output bytes.Buffer
	old := log.Writer()
	log.SetOutput(&output)
	defer log.SetOutput(old)
	v.record("myce/s072", false)
	v.record("myce/s072", true)
	v.record("mycelium-python/0.1.0", true)
	for _, ua := range []string{"", "Go-http-client/1.1", "Python-urllib/3.12", "myce/private/path", "myce/s072\nsecret"} {
		v.record(ua, false)
	}
	if output.Len() != 0 {
		t.Fatal("per-request logging", output.String())
	}
	clock.Advance(time.Minute)
	v.record("myce/s072", true)
	line := output.String()
	for _, want := range []string{`event=job_get_totals`, `"kind":"cli","version":"s072","full":1,"wait":2`, `"kind":"python","version":"0.1.0","full":0,"wait":1`, `"kind":"unknown","version":"unknown","full":5,"wait":0`} {
		if !strings.Contains(line, want) {
			t.Fatalf("missing %s in %s", want, line)
		}
	}
	output.Reset()
	v.record("myce/s072", false)
	if output.Len() != 0 {
		t.Fatal("logged before next minute")
	}
	clock.Advance(time.Minute)
	v.record("myce/s072", false)
	if !strings.Contains(output.String(), `"full":3,"wait":2`) {
		t.Fatal("totals were reset", output.String())
	}
}

func TestJobGetVolumeBoundedConcurrent(t *testing.T) {
	v := newJobGetVolume(&fakeClock{now: time.Unix(1, 0)})
	var wg sync.WaitGroup
	for i := range 1000 {
		wg.Go(func() { v.record(fmt.Sprintf("myce/v%d", i), i%2 == 0) })
	}
	wg.Wait()
	v.record("", false)
	if len(v.totals) > jobGetIdentityLimit+3 {
		t.Fatal("unbounded identities", len(v.totals))
	}
	var full, wait uint64
	for _, total := range v.totals {
		full += total.Full
		wait += total.Wait
	}
	if full != 501 || wait != 500 {
		t.Fatal("lost counts", full, wait)
	}
	if v.totals[jobGetIdentity{"unknown", "unknown"}].Full != 1 {
		t.Fatal("missing unknown bucket")
	}
}
