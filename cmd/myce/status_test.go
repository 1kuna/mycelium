package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestStatusFullIDsAndOneBasedAttempts(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"machines":[{"id":"b70-gpu1","enable_pending":true}],"jobs":[{"id":"12345678abcdef","state":"running","attempt":1,"spec":{"name":"work","priority":"production"}},{"id":"abcdefgh123456","state":"queued","attempt":0,"spec":{"name":"waiting","priority":"experiment"}}]}`)
	}))
	defer server.Close()
	saved := os.Stdout
	defer func() { os.Stdout = saved }()
	for _, full := range []bool{false, true} {
		f, e := os.CreateTemp(t.TempDir(), "status")
		if e != nil {
			t.Fatal(e)
		}
		os.Stdout = f
		args := []string{"status", "--queue", server.URL, "--token", "test-only"}
		if full {
			args = append(args, "--full")
		}
		if e = run(args); e != nil {
			t.Fatal(e)
		}
		f.Close()
		data, e := os.ReadFile(f.Name())
		if e != nil {
			t.Fatal(e)
		}
		output := string(data)
		if strings.Contains(output, "12345678abcdef") != full {
			t.Fatal(output)
		}
		if !strings.Contains(output, "held: enable not yet acknowledged (retrying)") {
			t.Fatal(output)
		}
		if !strings.Contains(output, "running · attempt 1") || strings.Contains(output, "attempt 0") {
			t.Fatal(output)
		}
	}
}
