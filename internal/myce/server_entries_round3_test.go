package myce

import (
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func entryHoldFixture(t *testing.T, fence string) (*Queue, *fakeClock, Machine, Machine) {
	t.Helper()
	q, clock, a := modelFixture(t, "ha")
	b := a
	b.ID, b.Session, b.HostID = "two", "session-two", "hb"
	model := &q.models.Models[0]
	model.Project = "owner"
	switch fence {
	case "entry":
		model.AllowedMachines = []string{b.ID}
	case "owner":
		if err := q.ConfigureProjects(map[string]ProjectPolicy{"owner": {Weight: 1, AllowedMachines: []string{b.ID}}}); err != nil {
			t.Fatal(err)
		}
	}
	a.ModelConfigHash, b.ModelConfigHash = modelConfigHash(q.models), modelConfigHash(q.models)
	if err := q.ConfigureHosts(map[string]HostPolicy{"ha": {ProductionHold: true, ProductionHoldScope: "device"}, "hb": {ProductionHold: true, ProductionHoldScope: "device"}}); err != nil {
		t.Fatal(err)
	}
	check(t, q, a, nil)
	check(t, q, b, nil)
	return q, clock, a, b
}

func TestEntryProductionBatchHoldRespectsModelFences(t *testing.T) {
	for _, fence := range []string{"entry", "owner"} {
		t.Run(fence, func(t *testing.T) {
			q, clock, a, b := entryHoldFixture(t, fence)
			s := spec("production")
			s.Needs.Model = "test"
			j := submit(t, q, s)
			clock.Advance(time.Second)
			if err := q.Tick(); err != nil {
				t.Fatal(err)
			}
			if check(t, q, a, nil).Job != nil {
				t.Fatal("excluded worker received model batch")
			}
			got := check(t, q, b, nil).Job
			if got == nil || got.ID != j.ID {
				t.Fatal("eligible worker left idle")
			}
		})
	}
}

type entryProbeReader func([]byte) (int, error)

func (f entryProbeReader) Read(p []byte) (int, error) { return f(p) }

func TestEntryGenericHeadersBeforeSpooling(t *testing.T) {
	q, _ := fixture(t)
	dir := t.TempDir()
	if err := q.ConfigureUploadSpool(dir, 1024); err != nil {
		t.Fatal(err)
	}
	for _, headers := range []http.Header{{}, {"X-Myce-Project": {"test"}}, {"X-Myce-Project": {"test"}, "X-Myce-Priority": {"bad"}}} {
		read, spooled := false, false
		body := entryProbeReader(func([]byte) (int, error) {
			read = true
			files, err := filepath.Glob(filepath.Join(dir, "myce-model-request-*"))
			if err != nil {
				t.Fatal(err)
			}
			spooled = len(files) > 0
			return 0, io.EOF
		})
		r := httptest.NewRequest("POST", "/models/test/upload", body)
		r.Header = headers
		r.Header.Set("Authorization", "Bearer test")
		w := httptest.NewRecorder()
		(API{Queue: q, Token: "test"}).Handler().ServeHTTP(w, r)
		files, err := filepath.Glob(filepath.Join(dir, "myce-model-request-*"))
		if w.Code != 400 || read || spooled || err != nil || len(files) != 0 || q.uploadSpool.used != 0 {
			t.Fatalf("status=%d read=%t spooled=%t files=%d err=%v", w.Code, read, spooled, len(files), err)
		}
		if !strings.Contains(w.Body.String(), "X-Myce-") {
			t.Fatal("routing error lacks actionable header names")
		}
		// With neither header, the message names both and suggests no unsupported defaults.
		if body := w.Body.String(); headers.Get("X-Myce-Project") == "" && headers.Get("X-Myce-Priority") == "" && (!strings.Contains(body, "X-Myce-Project and X-Myce-Priority are required") || strings.Contains(body, "default_call_priority") || strings.Contains(body, "entry")) {
			t.Fatal("generic route suggests unsupported defaults:", body)
		}
	}
}
