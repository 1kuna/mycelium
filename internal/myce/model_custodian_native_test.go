package myce

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func configureNativeModel(t *testing.T, q *Queue, c *ServiceConfig) {
	t.Helper()
	m := serverTestModel(t, "ready")
	m.Name, m.Device, m.DeviceEnv, m.HostMemoryGB = "test", "", nil, 0.25
	m.Path = filepath.Join(filepath.Dir(c.Config), "weights")
	if err := os.WriteFile(m.Path, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	m.SizeBytes, m.DirectNAS = 1, true
	var err error
	m.SHA256, err = FileSHA256(m.Path)
	if err != nil {
		t.Fatal(err)
	}
	config := ModelConfig{Models: []Model{m}, Idle: time.Hour}
	if err := q.ConfigureModels(config); err != nil {
		t.Fatal(err)
	}
	c.ModelCatalog = filepath.Join(filepath.Dir(c.Config), "models.yaml")
	data, err := yaml.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(c.ModelCatalog, data, 0600); err != nil {
		t.Fatal(err)
	}
}

// This mode shares the existing native-manager harness and its two real worker
// images. The queue HTTP response stays open while the manager stops the old
// service and starts the replacement. Engine processes and service membership
// are real; GPU inventory is synthetic.
func custodianNativeModelReplacement(t *testing.T, q *Queue, c ServiceConfig, queue, next string, start, stop func(), writeImage func(string)) {
	t.Helper()
	dir := custodianDir(c.State)
	defer func() {
		paths, _ := filepath.Glob(filepath.Join(dir, "*.model-request.json"))
		for _, path := range paths {
			var r modelCustodianRequest
			if readCustodianJSON(dir, filepath.Base(path), &r) != nil {
				t.Error("cannot read owned model cleanup identity")
				continue
			}
			g := &modelCustodian{dir: dir, request: r, clock: RealClock{}, interval: time.Second, verify: modelCustodianMembership}
			if err := g.Unload(); err != nil {
				t.Error(err)
			}
			if runtime.GOOS == "linux" {
				_ = exec.Command("systemctl", "--user", "stop", custodianUnit(r.ID)).Run()
			} else {
				_ = exec.Command("launchctl", "bootout", fmt.Sprintf("gui/%d/local.mycelium.attempt.%s", os.Getuid(), r.ID)).Run()
			}
		}
	}()
	await(t, func() bool {
		q.mu.Lock()
		defer q.mu.Unlock()
		return q.s.Machines[c.Name] != nil
	})
	t.Log("model worker checked in; assigning pin")
	if err := q.Pin(ModelPin{Project: "test", Model: "test", Machine: c.Name}); err != nil {
		t.Fatal(err)
	}
	var before modelCustodianSnapshot
	await(t, func() bool {
		paths, _ := filepath.Glob(filepath.Join(dir, "*.model-request.json"))
		if len(paths) != 1 {
			return false
		}
		id := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(paths[0]), "custodian-"), ".model-request.json")
		return readCustodianJSON(dir, custodianName(id, "snapshot"), &before) == nil && before.Report.State == "ready"
	})
	t.Log("detached model ready; verifying native membership")
	if err := verifyCustodianIsolation(before.ID, before.PID, before.Cgroup); err != nil {
		t.Fatal(err)
	}
	paths, _ := filepath.Glob(filepath.Join(dir, "model-"+before.ID, "engine-*.json"))
	if len(paths) != 1 {
		t.Fatal("expected one engine launch record", paths)
	}
	var engine workerState
	if err := readCustodianJSON(filepath.Dir(paths[0]), filepath.Base(paths[0]), &engine); err != nil {
		t.Fatal(err)
	}
	birth, err := processBirth(engine.PID)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), testWaitTimeout)
	defer cancel()
	request := func(target, session, attempt, endpoint string) *http.Response {
		t.Helper()
		r, err := http.NewRequestWithContext(ctx, "POST", target+"/models/test"+endpoint, strings.NewReader("input"))
		if err != nil {
			t.Fatal(err)
		}
		r.Header.Set("Authorization", "Bearer custodian-test")
		r.Header.Set("X-Myce-Project", "test")
		r.Header.Set("X-Myce-Priority", "experiment")
		r.Header.Set("X-Myce-Session", session)
		r.Header.Set("X-Myce-Engine-Attempt", attempt)
		res, err := http.DefaultClient.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		return res
	}
	response := request(queue, "", "", "/stream")
	defer response.Body.Close()
	stream := bufio.NewReader(response.Body)
	if first, err := stream.ReadString('\n'); response.StatusCode != 200 || err != nil || first != "first\n" {
		t.Fatalf("stream did not start: HTTP %d %q %v", response.StatusCode, first, err)
	}
	t.Log("queue-routed model stream open; replacing worker image")
	stop()
	stoppedAt := q.clock.Now()
	writeImage(next)
	start()
	await(t, func() bool {
		image, _ := os.ReadFile(filepath.Join(c.State, "worker-image"))
		q.mu.Lock()
		defer q.mu.Unlock()
		s := q.s.ModelSlots[slotKey(c.Name, "test")]
		worker := q.s.Machines[c.Name]
		return string(image) == "my55-next" && worker != nil && worker.LastSeen.After(stoppedAt) && s != nil && s.State == "ready" && s.Attempt == before.Execution.Attempt && s.Session == before.Execution.Session && s.URL == before.URL && validModelCustody(s.Custody) && s.Custody.ID == before.ID
	})
	var after modelCustodianSnapshot
	if err := readCustodianJSON(dir, custodianName(before.ID, "snapshot"), &after); err != nil || after.PID != before.PID || after.Birth != before.Birth || after.Execution != before.Execution {
		t.Fatalf("replacement changed model custodian identity: %+v %v", after, err)
	}
	if current, err := processBirth(engine.PID); err != nil || current != birth {
		t.Fatalf("binary replacement restarted engine: %v", err)
	}
	for _, identity := range [][2]string{{"stale-session", before.Execution.Attempt}, {before.Execution.Session, "stale-attempt"}} {
		stale := request(before.URL, identity[0], identity[1], "/echo")
		stale.Body.Close()
		if stale.StatusCode != 401 && stale.StatusCode != 503 {
			t.Fatal("stale identity reached adopted engine", stale.StatusCode)
		}
	}
	release := request(before.URL, before.Execution.Session, before.Execution.Attempt, "/release-stream")
	release.Body.Close()
	if last, err := io.ReadAll(stream); err != nil || string(last) != "last\n" {
		t.Fatalf("replacement interrupted or duplicated stream: %q %v", last, err)
	}
	await(t, func() bool {
		q.mu.Lock()
		defer q.mu.Unlock()
		s := q.s.ModelSlots[slotKey(c.Name, "test")]
		return len(q.calls) == 0 && s != nil && s.InFlight == 0 && len(q.s.Pins) == 1
	})
	paths, _ = filepath.Glob(filepath.Join(dir, "*.model-request.json"))
	if len(paths) != 1 {
		t.Fatal("replacement loaded another engine", paths)
	}
	t.Log("native manager replacement retained model PID/birth, engine attempt/session, one load and pin; admitted stream completed exactly once")
}
