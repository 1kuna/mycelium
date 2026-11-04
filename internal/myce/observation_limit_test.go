package myce

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"
)

func TestObservationLimitClockAndWorkerExemption(t *testing.T) {
	q, clock := fixture(t)
	job := submit(t, q, spec("experiment"))
	observationPath := "/api/jobs/" + job.ID + "?view=wait"
	handler := (API{Queue: q, Token: "fixture"}).handler("", clock)
	request := func(path, method, peer string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, nil)
		r.RemoteAddr = peer
		r.Header.Set("Authorization", "Bearer fixture")
		r.Header.Set("X-Myce-Start-Gate", startGateProtocol)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	for range 10 {
		if w := request(observationPath, "GET", "192.0.2.1:1000"); w.Code != 200 {
			t.Fatal(w.Code)
		}
	}
	w := request(observationPath, "GET", "192.0.2.1:2000")
	var api map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &api)
	if w.Code != 429 || api["code"] != "client_rate_limited" || w.Header().Get("Retry-After") != "1" {
		t.Fatal(w.Code, api, w.Header())
	}
	for _, path := range []string{"/api/workers/checkin", "/api/workers/launch", "/api/workers/started", "/api/files/checkpoint"} {
		if w := request(path, "POST", "192.0.2.1:1000"); w.Code == 429 {
			t.Fatal("worker throttled", path)
		}
	}
	for _, path := range []string{"/api/instructions", "/api/files/fixture", "/api/environments/fixture/reference"} {
		if observationRequest(httptest.NewRequest("GET", path, nil)) {
			t.Fatal("worker dependency throttled", path)
		}
	}
	if w := request(observationPath, "GET", "192.0.2.2:1000"); w.Code != 200 {
		t.Fatal("peer isolation", w.Code)
	}
	clock.Advance(time.Second)
	for range 5 {
		if w := request(observationPath, "GET", "192.0.2.1:1000"); w.Code != 200 {
			t.Fatal(w.Code)
		}
	}
	if w := request(observationPath, "GET", "192.0.2.1:1000"); w.Code != 429 {
		t.Fatal("refill exceeded rate", w.Code)
	}
}
