package myce

import (
	"crypto/ed25519"
	"errors"
	"net/http"
)

// The queue is the approval authority already trusted by the exact TLS pin.
// It verifies the publisher's signature using its provisioned key, then serves
// public approved metadata to inviters. No credential or signing key is served.
type EnrollmentRelease struct {
	Manifest ReleaseManifest `json:"manifest"`
	Origin   string          `json:"origin"`
}

func (q *Queue) ConfigureEnrollmentRelease(data []byte, key ed25519.PublicKey, origin string) error {
	m, err := VerifyRelease(data, key)
	if err != nil {
		return err
	}
	if err = releaseOrigin(origin); err != nil {
		return err
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.enrollmentRelease = &EnrollmentRelease{m, origin}
	return nil
}
func (a API) registerEnrollmentRelease(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/invites/release", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		a.Queue.mu.Lock()
		release := a.Queue.enrollmentRelease
		a.Queue.mu.Unlock()
		if release == nil {
			respond(w, nil, &HTTPError{Status: 503, Code: "release_unavailable", Message: "queue administrator must configure a signed release and publisher URL before inviting"})
			return
		}
		respond(w, release, nil)
	})
}
func (r EnrollmentRelease) Validate() error {
	if err := r.Manifest.Validate(); err != nil {
		return err
	}
	if err := releaseOrigin(r.Origin); err != nil {
		return errors.New("queue approved release origin invalid")
	}
	return nil
}
