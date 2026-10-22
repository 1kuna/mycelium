package myce

import (
	"crypto/ed25519"
	"errors"
	"net/http"

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
		if current := q.s.EnrollmentRelease; current != nil && semver.Compare(m.Version, current.Manifest.Version) <= 0 && !(len(allowDowngrade) > 0 && allowDowngrade[0]) {
			return errors.New("release_downgrade_refused: queue approval requires a newer version; use --allow-downgrade explicitly to re-approve or downgrade")
		}
		q.s.EnrollmentRelease = &EnrollmentRelease{Manifest: m, Origin: origin, CACert: caCert}
		return nil
	})
}
func (a API) registerEnrollmentRelease(mux *http.ServeMux) {
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
