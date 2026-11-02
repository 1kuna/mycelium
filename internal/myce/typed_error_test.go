package myce

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
)

// #593: every typed code reaches HTTP callers with its server-written reason,
// while untyped internal errors stay request_failed with no private cause.
func TestHTTPPassesEveryTypedCode(t *testing.T) {
	q, _ := fixture(t)
	api := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	t.Cleanup(api.Close)
	c := Client{URL: api.URL, Token: "test"}
	s := spec("experiment")
	s.ProfileMemory = strings.Repeat("a", 64) // no profile archive
	var he *HTTPError
	if _, err := c.Submit(context.Background(), s); !errors.As(err, &he) || he.Status != 400 || he.Code != "job_profile_invalid" || he.Message == "" || he.Message == "request failed" {
		t.Fatalf("want 400 job_profile_invalid with its reason, got %v", err)
	}
	unlisted := &HTTPError{Status: 409, Code: "some_future_refusal", Message: "server-written reason"}
	for _, tc := range []struct {
		err          error
		code, reason string
	}{
		{unlisted, "some_future_refusal", "server-written reason"},
		{errors.New("open /private/path: permission denied"), "request_failed", "request failed"},
		{&HTTPError{Status: 409, Message: "untyped detail"}, "request_failed", "request failed"},
	} {
		w := httptest.NewRecorder()
		respond(w, nil, tc.err)
		var body struct{ Code, Error string }
		if err := json.NewDecoder(w.Body).Decode(&body); err != nil || body.Code != tc.code || body.Error != tc.reason {
			t.Fatalf("%v: got %+v %v", tc.err, body, err)
		}
	}
}
