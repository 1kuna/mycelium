package myce

import (
	"net/http"
	"net/http/httptest"
)

func workerTestServer(handler http.Handler) *httptest.Server { return httptest.NewServer(handler) }
func workerTestHandler(handler http.Handler) http.Handler    { return handler }
