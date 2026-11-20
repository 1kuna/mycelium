package myce

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func configureNativeModel(t *testing.T, q *Queue, c *ServiceConfig) {
	t.Helper()
	identity, err := json.Marshal(map[string]string{"service": c.Service, "slice": c.workerSlice(), "jobs": c.custodianSlice()})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("native fixture cleanup identity: %s", identity)
	m := serverTestModel(t, "ready")
	m.Name, m.Device, m.DeviceEnv, m.HostMemoryGB = "test", "", nil, 0.25
	m.Path = filepath.Join(filepath.Dir(c.Config), "weights")
	if err := os.WriteFile(m.Path, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	m.SizeBytes, m.DirectNAS = 1, true
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
		if t.Failed() {
			// Capture before Unload can replace the failed/loading observation.
			q.mu.Lock()
			var slots []ModelSlot
			for _, slot := range q.s.ModelSlots {
				if slot.Machine == c.Name {
					slots = append(slots, *slot)
				}
			}
			q.mu.Unlock()
			t.Logf("native model queue observations: %+v", slots)
			for _, path := range paths {
				var request modelCustodianRequest
				if err := readCustodianJSON(dir, filepath.Base(path), &request); err != nil {
					t.Logf("native model request observation: %v", err)
					continue
				}
				t.Logf("native model launch observation: execution=%+v oom_score=%d host_budget_gb=%g slice=%s", request.Execution, request.OOMScore, request.Machine.HostMemoryBudgetGB, c.workerSlice())
				for _, kind := range []string{"snapshot", "gate", "gate-receipt", "model-policy"} {
					b, err := os.ReadFile(filepath.Join(dir, custodianName(request.ID, kind)))
					t.Logf("native model %s observation: %s (read=%v)", kind, b, err)
				}
				files, _ := filepath.Glob(filepath.Join(dir, "model-"+request.ID, "engine-*"))
				for _, file := range files {
					if strings.HasSuffix(file, ".json") || strings.HasSuffix(file, ".log") {
						b, err := os.ReadFile(file)
						t.Logf("native model engine observation %s: %s (read=%v)", filepath.Base(file), b, err)
					}
				}
			}
		}
		for _, path := range paths {
			var r modelCustodianRequest
			if readCustodianJSON(dir, filepath.Base(path), &r) != nil {
				t.Error("cannot read owned model cleanup identity")
				continue
			}
			t.Logf("native owned model scope: %s", custodianUnit(r.ID))
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
	identity, err := json.Marshal(map[string]any{"scope": custodianUnit(before.ID), "cgroup": before.Cgroup, "helper_pid": before.PID, "helper_birth": before.Birth, "engine_pid": engine.PID, "engine_birth": birth})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("native model cleanup identity: %s", identity)
	if runtime.GOOS == "linux" {
		verifyNativeInheritedOOM(t, q, c, before, engine.PID)
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
	var adoptionObservation string
	defer func() {
		if t.Failed() {
			t.Log("replacement adoption observation:", adoptionObservation)
		}
	}()
	await(t, func() bool {
		image, _ := os.ReadFile(filepath.Join(c.State, "worker-image"))
		q.mu.Lock()
		defer q.mu.Unlock()
		s := q.s.ModelSlots[slotKey(c.Name, "test", before.Execution.Device)]
		worker := q.s.Machines[c.Name]
		adoptionObservation = fmt.Sprintf("image=%q worker_present=%t slot=%+v expected_execution=%+v expected_url=%q expected_custodian=%s", image, worker != nil, s, before.Execution, before.URL, before.ID)
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
		s := q.s.ModelSlots[slotKey(c.Name, "test", before.Execution.Device)]
		return len(q.calls) == 0 && s != nil && s.InFlight == 0 && len(q.s.Pins) == 1
	})
	paths, _ = filepath.Glob(filepath.Join(dir, "*.model-request.json"))
	if len(paths) != 1 {
		t.Fatal("replacement loaded another engine", paths)
	}
	t.Log("native manager replacement retained model PID/birth, engine attempt/session, one load and pin; admitted stream completed exactly once")
}

// The same isolated native run also proves the inherited minimum on an actual
// highest-tier batch launch. No installed service or OOM policy is changed.
func verifyNativeInheritedOOM(t *testing.T, q *Queue, c ServiceConfig, model modelCustodianSnapshot, enginePID int) {
	t.Helper()
	score := func(pid int) int {
		t.Helper()
		b, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "oom_score_adj"))
		if err != nil {
			t.Fatal(err)
		}
		v, err := strconv.Atoi(strings.TrimSpace(string(b)))
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	b, err := os.ReadFile(filepath.Join(c.State, "worker-pid"))
	if err != nil {
		t.Fatal(err)
	}
	workerPID, err := strconv.Atoi(string(b))
	if err != nil {
		t.Fatal(err)
	}
	inherited := score(workerPID)
	modelScore, helperScore := score(enginePID), score(model.PID)
	if modelScore != inherited || helperScore != inherited {
		t.Fatalf("model inherited score: worker=%d helper=%d engine=%d", inherited, helperScore, modelScore)
	}
	release := ""
	defer func() {
		if release != "" {
			if err := os.WriteFile(release, nil, 0600); err != nil {
				t.Error(err)
			}
		}
	}()
	s := cpuSpec(1, 0.125)
	s.Priority, s.Resumable = "production", false
	s.Targets = []ExecutionTarget{{OS: runtime.GOOS, Arch: runtime.GOARCH, Platform: "cpu"}}
	s.Run.Command = []string{"/bin/sh", "-c", `echo $$ > "$MYCE_CHECKPOINT_DIR/native-batch-pid"; while [ ! -f "$MYCE_CHECKPOINT_DIR/native-batch-finish" ]; do sleep 0.01; done`}
	j := submit(t, q, s)
	dir := custodianDir(c.State)
	var batch custodianSnapshot
	await(t, func() bool {
		paths, _ := filepath.Glob(filepath.Join(dir, "*.request.json"))
		for _, path := range paths {
			var r custodianRequest
			if readCustodianJSON(dir, filepath.Base(path), &r) == nil && r.State.Job != nil && r.State.Job.ID == j.ID {
				return readCustodianJSON(dir, custodianName(r.ID, "snapshot"), &batch) == nil && batch.Started
			}
		}
		return false
	})
	if err := verifyCustodianIsolation(batch.ID, batch.PID, batch.Cgroup); err != nil {
		t.Fatal(err)
	}
	checkpoint := filepath.Join(c.State, "checkpoints", j.ID, fmt.Sprintf("attempt-%d", batch.State.Job.Attempt))
	release = filepath.Join(checkpoint, "native-batch-finish")
	var payload int
	await(t, func() bool {
		b, err := os.ReadFile(filepath.Join(checkpoint, "native-batch-pid"))
		if err != nil {
			return false
		}
		_, err = fmt.Sscan(string(b), &payload)
		return err == nil
	})
	birth, err := processBirth(payload)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := json.Marshal(map[string]any{"scope": custodianUnit(batch.ID), "cgroup": batch.Cgroup, "helper_pid": batch.PID, "helper_birth": batch.Birth, "payload_pid": payload, "payload_birth": birth})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("native batch cleanup identity: %s", identity)
	var gate custodianGate
	var receipt custodianGateReceipt
	for kind, value := range map[string]any{"gate": &gate, "gate-receipt": &receipt} {
		if err := readCustodianJSON(dir, custodianName(batch.ID, kind), value); err != nil {
			t.Fatal(err)
		}
	}
	if gate.Nonce == "" || receipt.Nonce != gate.Nonce || receipt.Error != "" {
		t.Fatalf("batch launch gate: %+v %+v", gate, receipt)
	}
	actual := score(payload)
	t.Logf("native inherited OOM evidence: worker_pid=%d inherited=%d model_helper=%d engine=%d batch_pid=%d actual=%d; exact batch gate receipt confirmed", workerPID, inherited, helperScore, modelScore, payload, actual)
	if actual != inherited {
		t.Fatalf("highest-tier batch score=%d inherited=%d", actual, inherited)
	}
	if err := os.WriteFile(release, nil, 0600); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { v := job(t, q, j.ID); return v.State == "succeeded" && v.ReleaseConfirmed })
}
