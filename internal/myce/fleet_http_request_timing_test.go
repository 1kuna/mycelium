package myce

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestFleetHTTPRequestTiming(t *testing.T) {
	q, _ := fixture(t)
	mux := http.NewServeMux()
	API{Queue: q}.registerFleetUpgrade(mux)
	for _, path := range []string{"/api/fleet/publisher", "/api/fleet/instruction", "/api/fleet/artifact"} {
		q.measureRequests(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mux.ServeHTTP(w, r)
			s := spanFrom(r.Context())
			if !s.measured || s.hold <= 0 {
				t.Errorf("missing queue phases for %s", path)
			}
		})).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", path, nil))
	}
}
