package peer

import (
	"strings"
	"testing"

	"mycelium/test/fixtures"
)

func TestRescuePayloadRoundTripClonesBody(t *testing.T) {
	job := fixtures.MakeJob(fixtures.WithJobID("job-a"))
	body := []byte(`{"model":"tiny"}`)
	encoded, err := EncodeRescuePayload(job, body)
	if err != nil {
		t.Fatalf("EncodeRescuePayload: %v", err)
	}
	body[0] = '['
	gotJob, gotBody, err := DecodeRescuePayload(encoded)
	if err != nil {
		t.Fatalf("DecodeRescuePayload: %v", err)
	}
	if gotJob.ID != job.ID || string(gotBody) != `{"model":"tiny"}` {
		t.Fatalf("decoded job=%+v body=%s", gotJob, gotBody)
	}
	gotBody[0] = '['
	_, reread, err := DecodeRescuePayload(encoded)
	if err != nil {
		t.Fatalf("DecodeRescuePayload reread: %v", err)
	}
	if string(reread) != `{"model":"tiny"}` {
		t.Fatalf("payload body was not cloned: %s", reread)
	}
}

func TestRescuePayloadErrors(t *testing.T) {
	if _, err := EncodeRescuePayload(fixtures.MakeJob(fixtures.WithJobID("")), []byte(`{}`)); err == nil || !strings.Contains(err.Error(), "job id") {
		t.Fatalf("missing job id encode err = %v", err)
	}
	if _, err := EncodeRescuePayload(fixtures.MakeJob(fixtures.WithJobID("job-a")), nil); err == nil || !strings.Contains(err.Error(), "body") {
		t.Fatalf("missing body encode err = %v", err)
	}
	if _, _, err := DecodeRescuePayload(nil); err == nil || !strings.Contains(err.Error(), "required") {
		t.Fatalf("empty decode err = %v", err)
	}
	if _, _, err := DecodeRescuePayload([]byte(`{`)); err == nil {
		t.Fatal("bad json decode accepted")
	}
	if _, _, err := DecodeRescuePayload([]byte(`{"job":{},"body":"e30="}`)); err == nil || !strings.Contains(err.Error(), "job id") {
		t.Fatalf("missing job id decode err = %v", err)
	}
	if _, _, err := DecodeRescuePayload([]byte(`{"job":{"id":"job-a"}}`)); err == nil || !strings.Contains(err.Error(), "body") {
		t.Fatalf("missing body decode err = %v", err)
	}
}
