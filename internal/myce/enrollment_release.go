package myce

import (
	"bytes"
	"crypto/ed25519"
	"errors"
	"log"
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

// EnrollmentReleaseApproval cannot provision trust. PublicKey must exactly
// match the publisher key provisioned locally on the queue host.
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

// ConfigureEnrollmentReleaseWithCA is local queue-host provisioning, including
// publisher key rotation. Never call it from an HTTP handler.
func (q *Queue) ConfigureEnrollmentReleaseWithCA(data []byte, key ed25519.PublicKey, origin, caCert string, allowDowngrade ...bool) error {
	return q.configureEnrollmentRelease(data, key, origin, caCert, len(allowDowngrade) > 0 && allowDowngrade[0], false)
}

// ConfigureStartupEnrollmentRelease preserves a newer/equal persisted default
// when old service flags are replayed, while provisioning the local publisher key.
func (q *Queue) ConfigureStartupEnrollmentRelease(data []byte, key ed25519.PublicKey, origin, caCert string, allowDowngrade bool) error {
	return q.configureEnrollmentRelease(data, key, origin, caCert, allowDowngrade, true)
}

func verifiedEnrollmentRelease(data []byte, key ed25519.PublicKey, origin, caCert string) (*EnrollmentRelease, error) {
	m, err := VerifyRelease(data, key)
	if err != nil {
		return nil, reject("release_approval_invalid", err)
	}
	if err = releaseOrigin(origin); err != nil {
		return nil, reject("release_approval_invalid", err)
	}
	if err = validateReleaseCA(caCert); err != nil {
		return nil, reject("release_approval_invalid", err)
	}
	return &EnrollmentRelease{Manifest: m, Origin: origin, CACert: caCert}, nil
}

func (q *Queue) configureEnrollmentRelease(data []byte, key ed25519.PublicKey, origin, caCert string, allowDowngrade, startup bool) error {
	key = bytes.Clone(key)
	approved, err := verifiedEnrollmentRelease(data, key, origin, caCert)
	if err != nil {
		return err
	}
	var keptVersion string
	err = q.update(func() error {
		if current := q.s.EnrollmentRelease; startup && current != nil && semver.Compare(approved.Manifest.Version, current.Manifest.Version) <= 0 && !allowDowngrade {
			keptVersion = current.Manifest.Version
		} else if err := q.applyEnrollmentRelease(approved, allowDowngrade); err != nil {
			return err
		}
		q.s.EnrollmentReleaseKey = key
		return nil
	})
	if err == nil && keptVersion != "" {
		log.Printf("startup release %s skipped: retaining persisted approval %s; use --allow-downgrade explicitly to replace it", approved.Manifest.Version, keptVersion)
	}
	return err
}

// applyEnrollmentRelease runs inside the committing queue transaction.
func (q *Queue) applyEnrollmentRelease(approved *EnrollmentRelease, allowDowngrade bool) error {
	if reflect.DeepEqual(q.s.EnrollmentRelease, approved) {
		return nil
	}
	if current := q.s.EnrollmentRelease; current != nil && semver.Compare(approved.Manifest.Version, current.Manifest.Version) <= 0 && !allowDowngrade {
		return reject("release_downgrade_refused", nil)
	}
	q.s.EnrollmentRelease = approved
	return nil
}

func (q *Queue) approveEnrollmentRelease(approval EnrollmentReleaseApproval) error {
	q.mu.RLock()
	key := ed25519.PublicKey(bytes.Clone(q.s.EnrollmentReleaseKey))
	q.mu.RUnlock()
	if len(key) != ed25519.PublicKeySize {
		return &HTTPError{Status: http.StatusServiceUnavailable, Code: "release_publisher_key_unavailable", Message: rejectionMessages["release_publisher_key_unavailable"]}
	}
	if !bytes.Equal(key, approval.PublicKey) {
		return reject("release_publisher_key_mismatch", nil)
	}
	approved, err := verifiedEnrollmentRelease(approval.SignedRelease, key, approval.Origin, approval.CACert)
	if err != nil {
		return err
	}
	return q.update(func() error {
		// Verification is outside q.mu; local rotation must invalidate the snapshot.
		if !bytes.Equal(key, q.s.EnrollmentReleaseKey) {
			return reject("release_publisher_key_mismatch", nil)
		}
		return q.applyEnrollmentRelease(approved, approval.AllowDowngrade)
	})
}
func (a API) registerEnrollmentRelease(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/invites/release", func(w http.ResponseWriter, r *http.Request) {
		var approval EnrollmentReleaseApproval
		if !decode(w, r, &approval) {
			return
		}
		err := a.Queue.approveEnrollmentRelease(approval)
		var rule *rejectionError
		var apiError *HTTPError
		if err != nil && !errors.As(err, &rule) && !errors.As(err, &apiError) {
			err = &HTTPError{Status: http.StatusInternalServerError, Code: "release_approval_failed", Message: rejectionMessages["release_approval_failed"]}
		}
		respond(w, map[string]bool{"ok": true}, err)
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
