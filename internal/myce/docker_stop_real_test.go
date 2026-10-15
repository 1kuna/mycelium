package myce

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Explicitly opted in on an isolated Docker Desktop host; never pulls an image.
func TestDockerRealSidecarStopProtocol(t *testing.T) {
	if os.Getenv("MYCE_REAL_STOP_TEST") != "1" {
		t.Skip("isolated real Docker opt-in disabled")
	}
	image := os.Getenv("MYCE_REAL_DOCKER_IMAGE")
	if image == "" {
		t.Fatal("local digest-pinned image required")
	}
	t.Setenv("PATH", dockerTestOriginalPath)
	binary, err := exec.LookPath("docker")
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("MYCE_TOKEN", "throwaway-stop-protocol-test-token")
	t.Setenv("DOCKER_CONTEXT", "")
	for _, ignore := range []bool{false, true} {
		t.Run(fmt.Sprintf("ignore-term=%v", ignore), func(t *testing.T) {
			root := t.TempDir()
			script := "#!/bin/sh\nif [ \"$3\" = kill ]; then printf '%s %s\\n' \"$4\" \"$5\" >> '" + filepath.Join(root, "signals") + "'; fi\nexec '" + binary + "' \"$@\"\n"
			if err := os.WriteFile(filepath.Join(root, "docker"), []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PATH", root+string(os.PathListSeparator)+os.Getenv("PATH"))
			w := Worker{Machine: machine("isolated-stop-test", "fake"), StateDir: t.TempDir(), HostStateDir: t.TempDir(), Clock: RealClock{}, HostMemorySource: func(context.Context) (float64, error) { return 8, nil }}
			s, err := w.openStore()
			if err != nil {
				t.Fatal(err)
			}
			defer s.close()
			s.state.Session = w.Machine.Session
			j := dockerJob()
			j.Checkpoint = t.TempDir()
			j.Spec.Run.DockerNetwork = "private"
			j.Spec.Run.DockerMemoryGB = .125
			j.Spec.Needs.HostMemoryGB = .25
			j.Spec.Run.Command = []string{"docker", "run", image, "python3", "-c", "import time; time.sleep(2)"}
			code := `import signal,time,sys,os
seen=False
def stop(sig,frame):
 global seen
 if seen: os._exit(134)
 seen=True
 print('first TERM',file=sys.stderr,flush=True)
 time.sleep(1)
 sys.exit(143)
signal.signal(signal.SIGTERM,stop)
print('ready',file=sys.stderr,flush=True)
while True: time.sleep(.05)
`
			if ignore {
				code = `import signal,time,sys
signal.signal(signal.SIGTERM,signal.SIG_IGN)
print('ignoring TERM',file=sys.stderr,flush=True)
while True: time.sleep(.05)
`
			}
			j.Spec.Run.DockerSidecars = []DockerSidecar{{Name: "signal-test", MemoryGB: .125, Command: []string{"docker", "run", image, "python3", "-c", code}}}
			p, err := startDockerFixture(w, s, j)
			if err != nil {
				t.Fatal(err)
			}
			defer p.docker.close()
			defer func() {
				cs, e := p.docker.containers()
				if e != nil {
					t.Errorf("cleanup inventory: %v", e)
					return
				}
				for _, c := range cs {
					if *c.State.Running {
						t.Errorf("test container still running")
						continue
					}
					if _, e = p.docker.call("rm", c.ID); e != nil {
						t.Error(e)
					}
				}
			}()
			await(t, func() bool { p.docker.mu.Lock(); defer p.docker.mu.Unlock(); return p.docker.state.MainFinished })
			// Shorten only this test's stop request; production retains StopGrace.
			grace := 3 * time.Second
			if ignore {
				grace = time.Second
			}
			p.docker.stop(w.Clock.Now(), grace, false)
			select {
			case f := <-p.done:
				if !f.result.Success {
					t.Fatalf("main result: %+v", f.result)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("stop timeout")
			}
			data, err := os.ReadFile(filepath.Join(s.dir, s.state.Identity+".docker-evidence.json"))
			if err != nil {
				t.Fatal(err)
			}
			var evidence map[string]dockerMember
			if err = json.Unmarshal(data, &evidence); err != nil {
				t.Fatal(err)
			}
			signals, _ := os.ReadFile(filepath.Join(root, "signals"))
			want := 143
			kills := 0
			if ignore {
				want = 137
				kills = 1
			}
			for id, m := range evidence {
				if m.Name != "signal-test" {
					continue
				}
				if m.ExitCode == nil || *m.ExitCode != want {
					t.Fatalf("sidecar exit != %d", want)
				}
				term := strings.Count(string(signals), "--signal=TERM "+id+"\n")
				kill := strings.Count(string(signals), "--signal=KILL "+id+"\n")
				if term != 1 || kill != kills || m.StderrTail == "" {
					t.Fatalf("TERM=%d KILL=%d stderr-bytes=%d", term, kill, len(m.StderrTail))
				}
				t.Logf("Linux container: main=0 sidecar=%d TERM=%d KILL=%d private-stderr=true", *m.ExitCode, term, kill)
			}
		})
	}
}
