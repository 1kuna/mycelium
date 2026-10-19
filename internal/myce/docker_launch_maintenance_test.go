package myce

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMaintenanceDelayedDockerGroupLaunch(t *testing.T) {
	for _, boundary := range []struct {
		name   string
		call   int
		denied string
	}{{"network", 1, "network create"}, {"sidecar-create", 2, "create --restart=no"}, {"sidecar-start", 3, "start sidecar-sortformer"}, {"main-create", 6, "--network-alias main"}, {"main-start", 7, "start id1"}} {
		for _, action := range []string{"drain", "disable", "disable-stop"} {
			t.Run(boundary.name+"/"+action, func(t *testing.T) {
				w, store, _, root := dockerFixture(t)
				q, _ := fixture(t)
				w.Machine.HostID = "host"
				w.Machine.HostMemoryMeasured, w.Machine.HostAvailableMemoryGB = true, 32
				check(t, q, w.Machine, nil)
				group := dockerGroupJob(t)
				submit(t, q, group.Spec)
				j := check(t, q, w.Machine, nil).Job
				if j == nil {
					t.Fatal("missing group assignment")
				}
				entered, release := make(chan struct{}), make(chan struct{})
				calls := 0
				handler := API{q, "test"}.Handler()
				server := httptest.NewServer(http.HandlerFunc(func(out http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/api/workers/launch" {
						calls++
						if calls == boundary.call {
							close(entered)
							select {
							case <-release:
							case <-r.Context().Done():
								return
							}
						}
					}
					handler.ServeHTTP(out, r)
				}))
				defer server.Close()
				w.Client = Client{URL: server.URL, Token: "test"}
				p, err := startDockerFixture(w, store, *j)
				if err != nil {
					t.Fatal(err)
				}
				defer p.docker.close()
				select {
				case <-entered:
				case <-time.After(5 * time.Second):
					t.Fatal("missing per-operation queue authorization")
				}
				name := action
				if action == "disable-stop" {
					name = "disable"
				}
				maintenanceChange(t, q, name, action == "disable-stop")
				close(release)
				select {
				case f := <-p.done:
					if f.result.Success || f.result.ReleasePending {
						t.Fatal("denied launch did not clean up", f.result)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("denied group did not settle")
				}
				data, _ := os.ReadFile(filepath.Join(root, "calls"))
				if strings.Contains(string(data), boundary.denied) {
					t.Fatal("Docker launch reached daemon after maintenance", boundary.denied)
				}
			})
		}
	}
}
