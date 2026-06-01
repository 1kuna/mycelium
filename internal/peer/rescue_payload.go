package peer

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"

	"mycelium/internal/domain"
)

type RescuePayload struct {
	Job  domain.Job `json:"job"`
	Body []byte     `json:"body"`
}

type encryptedRescuePayload struct {
	Encrypted  string `json:"encrypted"`
	Nonce      string `json:"nonce"`
	Ciphertext string `json:"ciphertext"`
}

func EncodeRescuePayload(job domain.Job, body []byte) ([]byte, error) {
	return EncodeRescuePayloadWithKey(job, body, nil)
}

func EncodeRescuePayloadWithKey(job domain.Job, body, key []byte) ([]byte, error) {
	if job.ID == "" {
		return nil, fmt.Errorf("rescue payload job id is required")
	}
	if len(body) == 0 {
		return nil, fmt.Errorf("rescue payload body is required")
	}
	data, _ := json.Marshal(RescuePayload{Job: job, Body: append([]byte(nil), body...)})
	if job.Handling != domain.HandlingPrivate {
		return data, nil
	}
	if len(key) != 32 {
		return nil, fmt.Errorf("private rescue payload key must be 32 bytes")
	}
	gcm := aesGCM(key)
	nonce := make([]byte, gcm.NonceSize())
	fillRandom(nonce, rand.Reader)
	sealed := gcm.Seal(nil, nonce, data, nil)
	return json.Marshal(encryptedRescuePayload{
		Encrypted:  "aes-256-gcm",
		Nonce:      base64.StdEncoding.EncodeToString(nonce),
		Ciphertext: base64.StdEncoding.EncodeToString(sealed),
	})
}

func DecodeRescuePayload(data []byte) (domain.Job, []byte, error) {
	return DecodeRescuePayloadWithKey(data, nil)
}

func DecodeRescuePayloadWithKey(data, key []byte) (domain.Job, []byte, error) {
	if len(data) == 0 {
		return domain.Job{}, nil, fmt.Errorf("rescue payload is required")
	}
	var encrypted encryptedRescuePayload
	if err := json.Unmarshal(data, &encrypted); err == nil && encrypted.Encrypted != "" {
		plain, err := decryptRescuePayload(encrypted, key)
		if err != nil {
			return domain.Job{}, nil, err
		}
		data = plain
	}
	var payload RescuePayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return domain.Job{}, nil, err
	}
	if payload.Job.ID == "" {
		return domain.Job{}, nil, fmt.Errorf("rescue payload job id is required")
	}
	if len(payload.Body) == 0 {
		return domain.Job{}, nil, fmt.Errorf("rescue payload body is required")
	}
	return payload.Job, append([]byte(nil), payload.Body...), nil
}

func decryptRescuePayload(payload encryptedRescuePayload, key []byte) ([]byte, error) {
	if payload.Encrypted != "aes-256-gcm" {
		return nil, fmt.Errorf("unsupported rescue payload encryption %q", payload.Encrypted)
	}
	if len(key) != 32 {
		return nil, fmt.Errorf("private rescue payload key must be 32 bytes")
	}
	nonce, err := base64.StdEncoding.DecodeString(payload.Nonce)
	if err != nil {
		return nil, err
	}
	ciphertext, err := base64.StdEncoding.DecodeString(payload.Ciphertext)
	if err != nil {
		return nil, err
	}
	gcm := aesGCM(key)
	return gcm.Open(nil, nonce, ciphertext, nil)
}

func aesGCM(key []byte) cipher.AEAD {
	block, err := aes.NewCipher(key)
	if err != nil {
		panic(err)
	}
	gcm, _ := cipher.NewGCM(block)
	return gcm
}

func fillRandom(dst []byte, reader io.Reader) {
	if _, err := io.ReadFull(reader, dst); err != nil {
		panic(err)
	}
}
