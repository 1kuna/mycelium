package peer

import (
	"errors"
	"strings"
	"testing"

	"mycelium/internal/domain"
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

func TestPrivateRescuePayloadIsEncryptedAndRequiresKey(t *testing.T) {
	job := fixtures.MakeJob(fixtures.WithJobID("job-private"))
	job.Handling = domain.HandlingPrivate
	key := []byte("0123456789abcdef0123456789abcdef")
	encoded, err := EncodeRescuePayloadWithKey(job, []byte(`{"secret":true}`), key)
	if err != nil {
		t.Fatalf("Encode private: %v", err)
	}
	if strings.Contains(string(encoded), "secret") || strings.Contains(string(encoded), "job-private") {
		t.Fatalf("private payload leaked plaintext: %s", encoded)
	}
	if _, _, err := DecodeRescuePayload(encoded); err == nil || !strings.Contains(err.Error(), "key") {
		t.Fatalf("decode without key err = %v", err)
	}
	gotJob, body, err := DecodeRescuePayloadWithKey(encoded, key)
	if err != nil {
		t.Fatalf("Decode private: %v", err)
	}
	if gotJob.ID != job.ID || gotJob.Handling != domain.HandlingPrivate || string(body) != `{"secret":true}` {
		t.Fatalf("decoded job=%+v body=%s", gotJob, body)
	}
}

func TestRescuePayloadErrors(t *testing.T) {
	if _, err := EncodeRescuePayload(fixtures.MakeJob(fixtures.WithJobID("")), []byte(`{}`)); err == nil || !strings.Contains(err.Error(), "job id") {
		t.Fatalf("missing job id encode err = %v", err)
	}
	if _, err := EncodeRescuePayload(fixtures.MakeJob(fixtures.WithJobID("job-a")), nil); err == nil || !strings.Contains(err.Error(), "body") {
		t.Fatalf("missing body encode err = %v", err)
	}
	privateJob := fixtures.MakeJob(fixtures.WithJobID("job-private"))
	privateJob.Handling = domain.HandlingPrivate
	if _, err := EncodeRescuePayload(privateJob, []byte(`{}`)); err == nil || !strings.Contains(err.Error(), "key") {
		t.Fatalf("missing private key err = %v", err)
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
	key := []byte("0123456789abcdef0123456789abcdef")
	for name, data := range map[string][]byte{
		"unsupported": []byte(`{"encrypted":"rot13","nonce":"","ciphertext":""}`),
		"bad nonce":   []byte(`{"encrypted":"aes-256-gcm","nonce":"%%%","ciphertext":""}`),
		"bad body":    []byte(`{"encrypted":"aes-256-gcm","nonce":"AAAAAAAAAAAAAAAA","ciphertext":"%%%"}`),
		"bad seal":    []byte(`{"encrypted":"aes-256-gcm","nonce":"AAAAAAAAAAAAAAAA","ciphertext":"AAAA"}`),
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := DecodeRescuePayloadWithKey(data, key); err == nil {
				t.Fatal("encrypted decode error was not surfaced")
			}
		})
	}
}

func TestRescuePayloadCryptoHelpersPanicOnImpossibleSetupFailure(t *testing.T) {
	assertPanic(t, func() {
		_ = aesGCM([]byte("short"))
	})
	assertPanic(t, func() {
		fillRandom(make([]byte, 1), errReader{})
	})
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) {
	return 0, errors.New("entropy")
}

func assertPanic(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	fn()
}
