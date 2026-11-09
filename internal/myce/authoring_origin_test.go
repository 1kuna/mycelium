package myce

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAuthoringOriginRetentionReplayAndPrivacy(t *testing.T) {
	path, root := filepath.Join(t.TempDir(), "queue.db"), t.TempDir()
	clock := &fakeClock{now: time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)}
	q, err := Open(path, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { q.Close() }()
	ids := map[string]string{}
	for _, origin := range []string{OriginProfile, OriginAdapter, OriginJobSpec, OriginAPI, OriginCommand, OriginExtension} {
		s := spec("experiment")
		s.SubmissionKey, s.AuthoringOrigin = origin, origin
		s.Run.Command = []string{"echo", "private-payload-sentinel"}
		legacy := s
		legacy.AuthoringOrigin = ""
		before, _ := submissionFingerprint(legacy)
		after, _ := submissionFingerprint(s)
		if before != after {
			t.Fatal("diagnostic changed historical fingerprint")
		}
		first := submit(t, q, s)
		ids[first.ID] = origin
		s.AuthoringOrigin = OriginCommand
		replay := submit(t, q, s)
		if replay.ID != first.ID || !replay.SubmissionReused || replay.Spec.AuthoringOrigin != origin {
			t.Fatal("replay replaced first accepted origin")
		}
	}
	// Model an old retained record, with no origin bytes. Do not infer a reader
	// from its name or accepted spec when it is read or replayed after restart.
	s := spec("experiment")
	s.SubmissionKey = "historical"
	historical := submit(t, q, s)
	if historical.Spec.AuthoringOrigin != OriginAPI {
		t.Fatal("new unmarked submission did not record API origin")
	}
	if err := q.update(func() error { q.touchJob(historical.ID).Spec.AuthoringOrigin = ""; return nil }); err != nil {
		t.Fatal(err)
	}
	ids[historical.ID] = OriginUnknown
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = Open(path, root, clock)
	if err != nil {
		t.Fatal(err)
	}
	s.AuthoringOrigin = OriginProfile
	if replay := submit(t, q, s); replay.ID != historical.ID || replay.Spec.AuthoringOrigin != "" {
		t.Fatal("historical origin was backfilled")
	}
	api := API{Queue: q, Token: "fixture"}.Handler()
	read := func(path string) []byte {
		t.Helper()
		r := httptest.NewRequest("GET", path, nil)
		r.Header.Set("Authorization", "Bearer fixture")
		w := httptest.NewRecorder()
		api.ServeHTTP(w, r)
		if w.Code != 200 || strings.Contains(w.Body.String(), "private-payload-sentinel") {
			t.Fatalf("read status/privacy: %d %s", w.Code, w.Body.String())
		}
		return w.Body.Bytes()
	}
	for id, want := range ids {
		var got Job
		if err := json.Unmarshal(read("/api/jobs/"+id), &got); err != nil || got.Spec.AuthoringOrigin != want || len(got.Spec.Run.Command) != 0 {
			t.Fatalf("job read origin/privacy: %+v %v", got, err)
		}
	}
	for _, query := range []string{"?jobs=all", "?jobs=all&detail=full"} {
		var got Snapshot
		seen := map[string]bool{}
		// Page sizes follow host capacity; the cursor, rather than a single
		// response's row count, promises complete retained-history coverage.
		for before := ""; ; {
			var page Snapshot
			if err := json.Unmarshal(read("/api/status"+query+before), &page); err != nil {
				t.Fatal(err)
			}
			for _, j := range page.Jobs {
				if seen[j.ID] {
					t.Fatal("duplicate status origin", j.ID)
				}
				seen[j.ID] = true
			}
			got.Jobs = append(got.Jobs, page.Jobs...)
			if page.JobsPage.NextBefore == "" {
				break
			}
			before = "&before=" + url.QueryEscape(page.JobsPage.NextBefore)
		}
		if len(got.Jobs) != len(ids) {
			t.Fatalf("status count changed: %d", len(got.Jobs))
		}
		for _, j := range got.Jobs {
			if j.Spec.AuthoringOrigin != ids[j.ID] {
				t.Fatalf("status lost origin: %+v", j.Spec)
			}
		}
		var page bytes.Buffer
		if err := statusPage.Execute(&page, got); err != nil || !strings.Contains(page.String(), "authoring origin: profile") || !strings.Contains(page.String(), "authoring origin: unknown") {
			t.Fatalf("page lost origin: %v", err)
		}
	}
	bad := spec("experiment")
	bad.AuthoringOrigin = "private-origin-sentinel"
	if _, err := q.Submit(bad); err == nil || strings.Contains(err.Error(), bad.AuthoringOrigin) {
		t.Fatalf("invalid origin accepted or exposed: %v", err)
	}
	// Cancellation reservations intentionally precede normal spec validation.
	// Even that acceptance path must persist only a fixed diagnostic label.
	bad.SubmissionKey = "cancelled-before-submit"
	reserveSubmission(t, q, bad.Project, bad.SubmissionKey)
	if cancelled := submit(t, q, bad); cancelled.State != "cancelled" || cancelled.Spec.AuthoringOrigin != OriginUnknown {
		t.Fatal("reservation retained unvalidated origin text")
	}
}
