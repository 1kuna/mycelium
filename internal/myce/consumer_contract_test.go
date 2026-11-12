package myce

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	validate "github.com/santhosh-tekuri/jsonschema/v6"
)

// A consumer pins the v1 job reply schema it was built against and rejects any
// reply that no longer satisfies it. #1439 and the prediction cut dropped
// required fields (checkpoint, timing.samples) without a release of notice
// (MY-42), failing every reply. Real HTTP replies must keep validating against
// that pinned copy and against the previous release's published schema.
func TestJobRepliesSatisfyPinnedConsumerSchemas(t *testing.T) {
	schemas := map[string]*validate.Schema{}
	for _, name := range []string{"consumer-pinned-job-v1.schema.json", "previous-release-job-v1.schema.json"} {
		data, err := os.ReadFile(filepath.Join("testdata", name))
		if err != nil {
			t.Fatal(err)
		}
		var schema any
		if err = json.Unmarshal(data, &schema); err != nil {
			t.Fatal(err)
		}
		compiler := validate.NewCompiler()
		compiler.AssertFormat()
		if err = compiler.AddResource("https://schemas.example/"+name, schema); err != nil {
			t.Fatal(err)
		}
		if schemas[name], err = compiler.Compile("https://schemas.example/" + name); err != nil {
			t.Fatal(err)
		}
	}
	assertReply := func(t *testing.T, label string, raw []byte, wantTiming bool) map[string]any {
		t.Helper()
		var reply map[string]any
		if err := json.Unmarshal(raw, &reply); err != nil {
			t.Fatalf("%s: %v: %s", label, err, raw)
		}
		for name, schema := range schemas {
			if err := schema.Validate(reply); err != nil {
				t.Fatalf("%s violates %s: %v\n%s", label, name, err, raw)
			}
		}
		if _, ok := reply["checkpoint"]; !ok {
			t.Fatalf("%s omits checkpoint: %s", label, raw)
		}
		timing, ok := reply["timing"].(map[string]any)
		if ok != wantTiming {
			t.Fatalf("%s timing present=%v, want %v: %s", label, ok, wantTiming, raw)
		}
		if ok {
			if _, ok := timing["samples"]; !ok {
				t.Fatalf("%s timing omits samples: %s", label, raw)
			}
		}
		return reply
	}

	q, c := fixture(t)
	server := httptest.NewServer(API{Queue: q, Token: "test"}.Handler())
	defer server.Close()
	call := func(t *testing.T, method, path string, body any) []byte {
		t.Helper()
		var payload io.Reader
		if body != nil {
			data, err := json.Marshal(body)
			if err != nil {
				t.Fatal(err)
			}
			payload = bytes.NewReader(data)
		}
		req, err := http.NewRequest(method, server.URL+path, payload)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer test")
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		data, err := io.ReadAll(resp.Body)
		if err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("%s %s: %d %v %s", method, path, resp.StatusCode, err, data)
		}
		return data
	}
	get := func(t *testing.T, id string) []byte {
		t.Helper()
		return call(t, "GET", "/api/jobs/"+id, nil)
	}
	m := cpuMachine("one", "host-one", "cpu", 8, 32)
	m.ExtensionFingerprint = "cpu-fingerprint"
	start := func(t *testing.T, busy *Job) *Job {
		t.Helper()
		j := check(t, q, m, busy).Job
		if j == nil {
			t.Fatal("nothing assigned")
		}
		report := AttemptReport{Execution: Execution{ID: j.ID, Attempt: j.Attempt}, Session: j.Session, Started: true}
		if _, err := q.Checkin(Checkin{Machine: m, AttemptProtocol: keyedAttemptProtocol, Attempts: []AttemptReport{report}}); err != nil {
			t.Fatal(err)
		}
		c.Advance(5 * time.Second)
		return j
	}
	keyed := func(key string) Spec {
		s := cpuSpec(1, 2)
		s.Resumable = true
		s.SubmissionKey = key
		return s
	}

	// First run of a kind of work: queued without timing, running and
	// succeeded with timing but no accepted history (samples zero).
	first := assertReply(t, "submit reply", call(t, "POST", "/api/jobs", keyed("first")), false)
	id := first["id"].(string)
	if first["placement_intent"] == nil {
		t.Fatal("submit reply lacks placement intent", first)
	}
	assertReply(t, "queued", get(t, id), false)
	firstRun := start(t, nil)
	assertReply(t, "running without history", get(t, id), true)
	finish(t, q, firstRun, true)
	assertReply(t, "succeeded", get(t, id), true)

	// Accepted history for this kind of work, as memory learning records it.
	drainAdmissionWait(t, q)
	q.mu.Lock()
	key := q.s.Jobs[id].Timing.Key
	if key == nil {
		q.mu.Unlock()
		t.Fatal("plain command timing has no observation key")
	}
	if q.s.WorkObservations == nil {
		q.s.WorkObservations = map[string]WorkObservation{}
	}
	q.s.WorkObservations[identityHash(key)] = WorkObservation{Key: *key, Samples: 2, LastSeconds: 5, RecordedAt: c.Now()}
	q.mu.Unlock()
	second := assertReply(t, "submit reply", call(t, "POST", "/api/jobs", keyed("second")), false)
	running := start(t, nil)
	reply := assertReply(t, "running with history", get(t, running.ID), true)
	timing, runtime := reply["timing"].(map[string]any), reply["runtime"].(map[string]any)
	if reply["id"] != second["id"] || timing["last_accepted_seconds"] != 5.0 || timing["samples"] != 2.0 || runtime["samples"] != 2.0 || runtime["remaining_work_unknown"] != true {
		q.mu.Lock()
		obs := q.s.WorkObservations[identityHash(key)]
		q.mu.Unlock()
		t.Fatalf("second run lacks accepted history: %v %v obs=%+v", timing, runtime, obs)
	}

	// Cancelled before start, and an older record without placement intent or
	// timing, both through the same reply wrapper.
	cancelled := assertReply(t, "submit reply", call(t, "POST", "/api/jobs", keyed("cancelled")), false)
	call(t, "POST", "/api/jobs/"+cancelled["id"].(string)+"/cancel", map[string]any{})
	assertReply(t, "cancelled", get(t, cancelled["id"].(string)), false)
	bare := cloneJob(running)
	bare.PlacementIntent, bare.Timing, bare.Checkpoint = nil, nil, ""
	assertReply(t, "without placement intent", wireJSON(t, &bare), false)
}
