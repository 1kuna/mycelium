package myce

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Retains the round 2 review reproduction: an earlier consumer must restart
// with the HTTP helper when the helper loses its port at final exec approval.
func TestNativeGroupEarlierConsumerEndpoint(t *testing.T) {
	q, initial := fixture(t)
	clock := &workerFakeClock{now: initial.Now()}
	q.clock = clock
	s := nativeSpec(t)
	s.Priority = "production"
	s.SubmissionKey = "round2-earlier-consumer"
	helper := s.Run.Sidecars[0]
	helper.Command = append(append([]string{}, helper.Command...), "--", "{port}")
	consumer := NativeSidecar{Name: "consumer", MemoryGB: 0.5, Command: []string{"/bin/sh", "-c", `printf '%s' "$UPSTREAM" > "$ARTIFACTS/consumer-url"; while [ ! -f "$ARTIFACTS/main-ready" ]; do sleep 0.01; done; /usr/bin/curl --silent --max-time 0.3 "$UPSTREAM/ready" > /dev/null; code=$?; printf '%s' "$code" > "$ARTIFACTS/consumer-code"; if [ "$code" != 0 ]; then exit 71; fi; touch "$ARTIFACTS/finish"; exec /bin/sleep 600`}, Env: map[string]string{"UPSTREAM": "{sidecars.helper.url}", "ARTIFACTS": "{work}"}}
	s.Run.Sidecars = []NativeSidecar{consumer, helper}
	j := submit(t, q, s)
	m := machine("native", "fake", "native-host")
	m.HostMemoryMeasured = true
	m.FreeHostMemoryGB = 64
	m.HostAvailableMemoryGB = 64
	reply := check(t, q, m, nil)
	if reply.Job == nil {
		t.Fatal("no assignment")
	}
	stateDir := t.TempDir()
	var n atomic.Int32
	var stolen net.Listener
	var stealErr error
	var oldURL string
	var oldGroup int
	api := API{Queue: q, Token: "review-synthetic"}.Handler()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call := int32(0)
		if r.URL.Path == "/api/workers/launch" {
			call = n.Add(1)
		}
		if call == 6 {
			left, err := workerGroupMembers(oldGroup)
			if err != nil || len(left) != 0 {
				t.Errorf("relaunch before old group cleanup: %v %v", left, err)
			}
		}
		if call == 5 {
			dir := nativeWorkerWork(t, stateDir)
			nativeFile(t, dir, "consumer-url")
			data, err := os.ReadFile(filepath.Join(dir, "consumer-url"))
			if err != nil {
				t.Error(err)
			}
			oldURL = string(data)
			b, _ := readSingleAttemptState(filepath.Join(stateDir, "state.json"))
			var st workerState
			_ = json.Unmarshal(b, &st)
			oldGroup = st.PID
			b, _ = os.ReadFile(filepath.Join(stateDir, "native-"+st.Identity+".json"))
			var v struct{ Members []nativeMember }
			_ = json.Unmarshal(b, &v)
			if len(v.Members) > 1 {
				out, e := exec.Command("ps", "-p", strconv.Itoa(v.Members[1].PID), "-o", "command=").Output()
				stealErr = e
				fields := strings.Fields(string(out))
				if len(fields) > 0 {
					stolen, stealErr = net.Listen("tcp4", "127.0.0.1:"+fields[len(fields)-1])
				}
			}
		}
		api.ServeHTTP(w, r)
	})
	defer func() {
		if stolen != nil {
			stolen.Close()
		}
	}()
	worker := Worker{Clock: clock, StateDir: stateDir, Machine: m, Client: nativeFixtureClient(handler), HostMemorySource: func(context.Context) (float64, error) { return 64, nil }}
	worker.nativeListenerProbe = func(port string, _ int) error {
		if stolen != nil && port == strconv.Itoa(stolen.Addr().(*net.TCPAddr).Port) {
			return errNativeForeignListener
		}
		return nil
	}
	store, e := worker.openStore()
	if e != nil {
		t.Fatal(e)
	}
	defer store.close()
	store.state.Session = m.Session
	p, e := startAfterFixtureInputs(t, worker, store, *reply.Job)
	if e != nil {
		t.Fatal(e)
	}
	defer func() { p.stop(worker.Clock.Now(), 0, true) }()
	dir := p.job.Spec.Run.Workdir
	select {
	case finish := <-p.done:
		if n.Load() != 12 || stolen == nil || stealErr != nil {
			t.Fatalf("group restart not exercised: launches=%d collision=%v", n.Load(), stealErr)
		}
		if e = q.Complete(finish.result); e != nil {
			t.Fatal(e)
		}
		main := nativeEnv(t, dir, "main")
		early, e := os.ReadFile(filepath.Join(dir, "consumer-url"))
		if e != nil {
			t.Fatal(e)
		}
		code, e := os.ReadFile(filepath.Join(dir, "consumer-code"))
		if e != nil {
			t.Fatal(e)
		}
		cur := job(t, q, j.ID)
		t.Logf("consumer URL=%s main helper URL=%s consumer curl=%s state=%s infrastructure_retries=%d", early, main["MYCE_SIDECAR_HELPER_URL"], code, cur.State, cur.InfrastructureRetries)
		nativeNoSurvivors(t, &nativeTestRun{p: p, dir: dir})
		if len(p.native.custody.Groups) != 2 {
			t.Fatalf("expected two group attempts, got %d", len(p.native.custody.Groups))
		}
		for group := range p.native.custody.Groups {
			left, err := workerGroupMembers(group)
			if err != nil || len(left) != 0 {
				t.Errorf("surviving attempt group: %v %v", left, err)
			}
		}
		if string(code) != "0" || !finish.result.Success || cur.State != "succeeded" || cur.InfrastructureRetries != 0 {
			t.Fatalf("consumer endpoint did not work after group restart: %s %+v", code, finish.result)
		}
		if string(early) == oldURL || string(early) != main["MYCE_SIDECAR_HELPER_URL"] {
			t.Fatalf("retry left earlier consumer on stolen endpoint; healthy helper and main use replacement endpoint")
		}
	case <-time.After(15 * time.Second):
		p.stop(worker.Clock.Now(), 0, true)
		<-p.done
		t.Fatal("timeout")
	}
}
