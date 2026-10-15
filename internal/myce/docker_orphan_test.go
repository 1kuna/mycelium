package myce

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestDockerOrphanStopProtocol(t *testing.T) {
	for _, mode := range []string{"term", "stop-error", "delivered-stop-error"} {
		t.Run(mode, func(t *testing.T) {
			_, s, clock, root := dockerFixture(t)
			for name, value := range map[string]string{"created": "id1", "running": "yes", "label": "owned", "mode": mode} {
				if err := os.WriteFile(filepath.Join(root, name), []byte(value), 0600); err != nil {
					t.Fatal(err)
				}
			}
			endpoint := "fake-" + ID()
			newOrphan := func() *dockerAttempt {
				d := &dockerAttempt{binary: filepath.Join(root, "docker"), clock: clock, timeout: time.Second, env: withoutToken(os.Environ()), state: dockerExecution{Identity: "owned", Endpoint: endpoint, Container: "id1"}}
				d.stop(clock.Now(), 5*time.Second, false)
				return d
			}
			d := newOrphan()
			// Reconstructing the cleanup object in the same process must not
			// reissue stop, even after uncertain delivery or a longer new grace.
			for i := 0; i < 3; i++ {
				_, _ = d.reconcile()
				if mode == "term" {
					// Even a restart of the same exact container must not cause
					// another graceful signal during this process lifetime.
					if err := os.WriteFile(filepath.Join(root, "running"), nil, 0600); err != nil {
						t.Fatal(err)
					}
				}
				d = newOrphan()
			}
			if _, err := os.Stat(filepath.Join(root, "stop-delivered")); (err == nil) != (mode == "delivered-stop-error") {
				t.Fatal("delivery uncertainty fixture", err)
			}
			calls := func() string {
				b, err := os.ReadFile(filepath.Join(root, "calls"))
				if err != nil {
					t.Fatal(err)
				}
				return string(b)
			}
			if strings.Count(calls(), "stop --time 5 id1\n") != 1 || strings.Contains(calls(), "kill ") {
				t.Fatalf("before grace: %s", calls())
			}
			clock.advance(5*time.Second - time.Nanosecond)
			_, _ = d.reconcile()
			if strings.Contains(calls(), "kill ") {
				t.Fatal("premature KILL")
			}
			clock.advance(time.Nanosecond)
			if mode != "term" {
				if err := os.WriteFile(filepath.Join(root, "mode"), []byte("refuse-kill"), 0600); err != nil {
					t.Fatal(err)
				}
				for i := 0; i < 2; i++ {
					if _, err := d.reconcile(); err == nil {
						t.Fatal("missing KILL error")
					}
				}
				if strings.Count(calls(), "kill --signal=KILL id1\n") != 2 {
					t.Fatalf("KILL not retried: %s", calls())
				}
				if err := os.WriteFile(filepath.Join(root, "mode"), nil, 0600); err != nil {
					t.Fatal(err)
				}
			}
			for i := 0; i < 3 && !d.state.Released; i++ {
				if _, err := d.reconcile(); err != nil {
					t.Fatal(err)
				}
			}
			if !d.state.Released || d.state.EvidenceSaved {
				t.Fatalf("untruthful orphan state: %+v", d.state)
			}
			if strings.Count(calls(), "stop --time ") != 1 || strings.Contains(calls(), "--signal=TERM") || strings.Contains(calls(), "logs ") {
				t.Fatalf("orphan signal/evidence calls: %s", calls())
			}
			for _, suffix := range []string{".docker.json", ".docker-evidence.json"} {
				if _, err := os.Stat(filepath.Join(s.dir, "owned"+suffix)); !os.IsNotExist(err) {
					t.Fatal("invented orphan attempt/evidence record", err)
				}
			}
		})
	}
}

func TestDockerEvidenceWithoutStore(t *testing.T) {
	d := &dockerAttempt{state: dockerExecution{Identity: "orphan"}}
	if err := d.retainEvidence(nil); err != nil {
		t.Fatal(err)
	}
	if d.state.EvidenceSaved {
		t.Fatal("evidence saved without a durable write")
	}
}

// Enumerate lifecycle dispatch sites so another cleanup path cannot quietly
// introduce a TERM loop. Behavioral tests exercise both permitted dispatchers.
func TestDockerLifecycleCallSites(t *testing.T) {
	want := map[string]int{
		"docker.go/signalContainer/kill/--signal=TERM":   1,
		"docker.go/signalContainer/kill/--signal=KILL":   1,
		"docker_orphan.go/stopOrphan/kill/--signal=KILL": 2,
		"docker_orphan.go/stopOrphan/stop/--time":        1,
		"docker_retention.go/pruneStoppedDocker/rm":      1,
		"docker_network.go/removeNetworks/network/rm":    1,
	}
	seen := map[string]int{}
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				if ident, ok := call.Fun.(*ast.Ident); ok && ident.Name == "append" {
					return true
				}
				var literals []string
				for _, arg := range call.Args {
					ast.Inspect(arg, func(n ast.Node) bool {
						if lit, ok := n.(*ast.BasicLit); ok && lit.Kind == token.STRING {
							s, err := strconv.Unquote(lit.Value)
							if err != nil {
								t.Fatal(err)
							}
							literals = append(literals, s)
						}
						return true
					})
				}
				// Systemd stops units, not containers; it has no Docker TERM call.
				if path == "host_systemd.go" {
					return true
				}
				key := path + "/" + fn.Name.Name + "/" + strings.Join(literals, "/")
				values := "/" + strings.Join(literals, "/")
				if strings.Contains(values, "--signal=TERM") || strings.Contains(values, "--signal=KILL") || strings.Contains(values, "/stop/") || strings.HasSuffix(values, "/rm") {
					seen[key]++
					if want[key] == 0 {
						t.Errorf("unaudited lifecycle call site %s", key)
					}
				}
				return true
			})
		}
	}
	for key, count := range want {
		if seen[key] != count {
			t.Errorf("%s count=%d want %d", key, seen[key], count)
		}
	}
}
