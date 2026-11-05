package myce

import (
	"fmt"
	"net/url"
	"syscall"
	"testing"
)

func TestClientErrorCauseSanitizesUntrustedDetails(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want string
	}{
		{&url.Error{Op: "POST", URL: "https://secret.invalid/token", Err: syscall.ECONNREFUSED}, "ECONNREFUSED"},
		{fmt.Errorf("private local path: %w", syscall.ECONNRESET), "ECONNRESET"},
		{&HTTPError{Status: 503, Code: "input_blob_unavailable", Message: "secret"}, "HTTP 503 code=input_blob_unavailable"},
		{&HTTPError{Status: 429, Code: "secret\ncode", Message: "secret"}, "HTTP 429 code=request_failed"},
		{fmt.Errorf("secret"), "io_error"},
	} {
		if got := ClientErrorCause(tc.err); got != tc.want {
			t.Errorf("got %q want %q", got, tc.want)
		}
	}
}
