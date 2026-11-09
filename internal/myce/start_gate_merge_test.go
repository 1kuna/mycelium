package myce

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestProductionStartGateMixedVersions(t *testing.T) {
	q, _ := fixture(t)
	handler := API{Queue: q, Token: "test"}.Handler()
	req := httptest.NewRequest("POST", "/api/workers/checkin", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer test")
	rw := httptest.NewRecorder()
	handler.ServeHTTP(rw, req)
	if rw.Code != http.StatusUpgradeRequired || !strings.Contains(rw.Body.String(), "upgrade queue and workers together") {
		t.Fatalf("old worker accepted: %d %s", rw.Code, rw.Body.String())
	}
	for _, body := range []string{`{"ok":true}`, `{"ok":false,"start_gate":"durable-start-v1"}`} {
		t.Run(body, func(t *testing.T) {
			server := workerTestServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
				rw.Header().Set("Content-Type", "application/json")
				_, _ = rw.Write([]byte(body))
			}))
			defer server.Close()
			w := recoveryWorker(t)

			w.Client = Client{URL: server.URL}
			now := time.Now()
			j := Job{ID: "test", Spec: spec("production")}
			j.Spec.StartBy = &now
			if err := w.authorizeStart(j, "test"); err == nil || !strings.Contains(err.Error(), "protocol mismatch") {
				t.Fatalf("old queue accepted: %v", err)
			}
		})
	}
	w := Worker{}
	if _, err := w.start(nil, Job{Spec: spec("production")}); err == nil || !strings.Contains(err.Error(), "no start_by") {
		t.Fatalf("legacy assignment accepted: %v", err)
	}
}
