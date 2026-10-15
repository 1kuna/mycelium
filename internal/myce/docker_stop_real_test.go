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
			if err := validateRealStopEvidence(evidence, filepath.Join(root, "signals"), ignore); err != nil {
				t.Fatal(err)
			}
			t.Logf("Linux containers: main=0 sidecar evidence and bounded stderr verified; ignore-term=%v", ignore)

		})
	}
}

// Return an error so the same acceptance assertions can be tested negatively
// without requiring Docker or weakening the opt-in runtime test.
func validateRealStopEvidence(evidence map[string]dockerMember, signalPath string, ignore bool) error {
	signals, err := os.ReadFile(signalPath)
	if err != nil {
		return fmt.Errorf("signal ledger: %w", err)
	}
	if len(evidence) != 2 {
		return fmt.Errorf("container evidence count=%d, want main and sidecar", len(evidence))
	}
	mainCount, sidecarCount := 0, 0
	for id, m := range evidence {
		switch m.Name {
		case "main":
			mainCount++
			if m.ExitCode == nil || *m.ExitCode != 0 {
				return fmt.Errorf("main exit evidence missing or nonzero")
			}
		case "signal-test":
			sidecarCount++
			if !m.TermDispatched || m.TermOutcome != "sent" {
				return fmt.Errorf("sidecar TERM outcome missing or not sent")
			}
			wantExit, wantKills, wantTail := 143, 0, "ready\nfirst TERM\n"
			if ignore {
				wantExit, wantKills, wantTail = 137, 1, "ignoring TERM\n"
			}
			if m.ExitCode == nil || *m.ExitCode != wantExit {
				return fmt.Errorf("sidecar exit evidence != %d", wantExit)
			}
			if len(m.StderrTail) > dockerStderrTailBytes || m.StderrTail != wantTail {
				return fmt.Errorf("sidecar bounded stderr content mismatch (bytes=%d)", len(m.StderrTail))
			}
			term := strings.Count(string(signals), "--signal=TERM "+id+"\n")
			kill := strings.Count(string(signals), "--signal=KILL "+id+"\n")
			if term != 1 || kill != wantKills {
				return fmt.Errorf("sidecar TERM=%d KILL=%d, want 1/%d", term, kill, wantKills)
			}
		default:
			return fmt.Errorf("unexpected container evidence name")
		}
	}
	if mainCount != 1 || sidecarCount != 1 {
		return fmt.Errorf("main evidence count=%d sidecar=%d, want one each", mainCount, sidecarCount)
	}
	return nil
}

func TestDockerRealStopEvidenceRequired(t *testing.T) {
	for _, ignore := range []bool{false, true} {
		for _, fault := range []string{"none", "empty", "missing-sidecar", "missing-main", "duplicate-sidecar", "missing-exit", "wrong-exit", "missing-main-exit", "wrong-main-exit", "missing-stderr", "wrong-stderr", "oversized-stderr", "missing-ledger", "unreadable-ledger", "wrong-signals", "missing-term-outcome", "error-term-outcome"} {
			t.Run(fmt.Sprintf("ignore=%v/%s", ignore, fault), func(t *testing.T) {
				zero, code := 0, 143
				tail, signals := "ready\nfirst TERM\n", "--signal=TERM sidecar-id\n"
				if ignore {
					code, tail = 137, "ignoring TERM\n"
					signals += "--signal=KILL sidecar-id\n"
				}
				main := dockerMember{Name: "main", ExitCode: &zero}
				sidecar := dockerMember{Name: "signal-test", ExitCode: &code, StderrTail: tail, TermDispatched: true, TermOutcome: "sent"}
				switch fault {
				case "missing-term-outcome":
					sidecar.TermOutcome = ""
				case "error-term-outcome":
					sidecar.TermOutcome = "error"
				case "missing-exit":
					sidecar.ExitCode = nil
				case "wrong-exit":
					sidecar.ExitCode = &zero
				case "missing-main-exit":
					main.ExitCode = nil
				case "wrong-main-exit":
					main.ExitCode = &code
				case "missing-stderr":
					sidecar.StderrTail = ""
				case "wrong-stderr":
					sidecar.StderrTail = "unrelated stderr\n"
				case "oversized-stderr":
					sidecar.StderrTail = strings.Repeat("x", dockerStderrTailBytes+1)
				case "wrong-signals":
					signals = ""
				}
				evidence := map[string]dockerMember{"main-id": main, "sidecar-id": sidecar}
				switch fault {
				case "empty":
					evidence = nil
				case "missing-sidecar":
					delete(evidence, "sidecar-id")
				case "missing-main":
					delete(evidence, "main-id")
				case "duplicate-sidecar":
					evidence["main-id"] = sidecar
				}
				path := filepath.Join(t.TempDir(), "signals")
				if fault == "unreadable-ledger" {
					if err := os.Mkdir(path, 0700); err != nil {
						t.Fatal(err)
					}
				} else if fault != "missing-ledger" {
					if err := os.WriteFile(path, []byte(signals), 0600); err != nil {
						t.Fatal(err)
					}
				}
				err := validateRealStopEvidence(evidence, path, ignore)
				if (err == nil) != (fault == "none") {
					t.Fatalf("fault=%s validation=%v", fault, err)
				}
			})
		}
	}
}
