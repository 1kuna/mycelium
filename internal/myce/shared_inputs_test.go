package myce

import "testing"

func TestSharedInputsRequireWorkerCapability(t *testing.T) {
	q, _ := fixture(t)
	s := spec("experiment")
	s.SharedInputs = map[string]SharedInput{"original": {NAS: "media/original", SHA256: "0000000000000000000000000000000000000000000000000000000000000000", SizeBytes: 10}}
	j := submit(t, q, s)
	old := machine("worker", "fake")
	if r := check(t, q, old, nil); r.Job != nil {
		t.Fatal("old worker assigned shared inputs")
	}
	upgraded := old
	upgraded.Capabilities = append([]string{"shared_inputs_v1"}, old.Capabilities...)
	r := check(t, q, upgraded, nil)
	if r.Job == nil {
		t.Fatal("capable worker was not assigned input")
	}
	if r = check(t, q, old, nil); r.Job != nil {
		t.Fatal("assignment redelivered to incompatible worker")
	}
	current := job(t, q, j.ID)
	if err := q.Started(Result{Machine: old.ID, Session: old.Session, Execution: Execution{ID: j.ID, Attempt: current.Attempt}}); err == nil {
		t.Fatal("old worker authorized to start shared input job")
	}
}
