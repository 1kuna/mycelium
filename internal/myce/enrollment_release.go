package myce

import (
	"crypto/ed25519"
	"errors"
	"net/http"
	"reflect"

	"golang.org/x/mod/semver"
)

// The queue is the approval authority already trusted by the exact TLS pin.
// It verifies the publisher's signature using its provisioned key, then serves
// public approved metadata to inviters. No credential or signing key is served.
type EnrollmentRelease struct {
	Manifest    ReleaseManifest `json:"manifest"`
	Origin      string          `json:"origin"`
	CACert      string          `json:"ca_cert,omitempty"`
	NASIdentity NASIdentity     `json:"nas_identity,omitempty"`
}

// EnrollmentReleaseApproval carries signed publisher bytes and a public key
// independently provisioned by the authenticated administrator, never a key
// supplied by the manifest. It changes only the queue's default invitation.
type EnrollmentReleaseApproval struct {
	SignedRelease  []byte            `json:"signed_release"`
	PublicKey      ed25519.PublicKey `json:"public_key"`
	Origin         string            `json:"origin"`
	CACert         string            `json:"ca_cert,omitempty"`
	AllowDowngrade bool              `json:"allow_downgrade,omitempty"`
}

func (q *Queue) ConfigureEnrollmentRelease(data []byte, key ed25519.PublicKey, origin string, allowDowngrade ...bool) error {
	return q.ConfigureEnrollmentReleaseWithCA(data, key, origin, "", allowDowngrade...)
}

// Publisher trust is explicitly provisioned by the administrator, then delivered
// to inviters over their already pinned, authenticated queue connection.
func (q *Queue) ConfigureEnrollmentReleaseWithCA(data []byte, key ed25519.PublicKey, origin, caCert string, allowDowngrade ...bool) error {
	m, err := VerifyRelease(data, key)
	if err != nil {
		return err
	}
	if err = releaseOrigin(origin); err != nil {
		return err
	}
	if err = validateReleaseCA(caCert); err != nil {
		return err
	}
	return q.update(func() error {
		approved := &EnrollmentRelease{Manifest: m, Origin: origin, CACert: caCert}
		if reflect.DeepEqual(q.s.EnrollmentRelease, approved) {
			return nil // Retry of an already committed, verified approval.
		}
		if current := q.s.EnrollmentRelease; current != nil && semver.Compare(m.Version, current.Manifest.Version) <= 0 && !(len(allowDowngrade) > 0 && allowDowngrade[0]) {
			return reject("release_downgrade_refused", nil)
		}
		q.s.EnrollmentRelease = approved
		return nil
	})
}
func (a API) registerEnrollmentRelease(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/invites/release", func(w http.ResponseWriter, r *http.Request) {
		var approval EnrollmentReleaseApproval
		if !decode(w, r, &approval) {
			return
		}
		err := a.Queue.ConfigureEnrollmentReleaseWithCA(approval.SignedRelease, approval.PublicKey, approval.Origin, approval.CACert, approval.AllowDowngrade)
		respondOperation(w, map[string]bool{"ok": true}, err, "release_approval_invalid")
	})
	mux.HandleFunc("GET /api/invites/release", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		a.Queue.mu.Lock()
		release := a.Queue.s.EnrollmentRelease
		if release != nil {
			current := *release
			current.NASIdentity = a.Queue.onboardingNAS
			release = &current
		}
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
	return validateReleaseCA(r.CACert)
}
