package myce

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func (a API) registerFleetUpgrade(mux *requestMux) {
	mux.HandleFunc("POST /api/fleet/upgrade", func(w http.ResponseWriter, r *http.Request) {
		var request FleetUpgradeRequest
		if !decode(w, r, &request) {
			return
		}
		out, err := a.Queue.StartFleetUpgrade(request, r.Context())
		respond(w, out, err)
	})
	mux.HandleFunc("GET /api/fleet/publisher", func(w http.ResponseWriter, r *http.Request) {
		requestUnlock1 := requestReadLock(r.Context(), &a.Queue.mu)
		key := bytes.Clone(a.Queue.s.EnrollmentReleaseKey)
		requestUnlock1()
		w.Header().Set("Cache-Control", "no-store")
		respond(w, struct {
			PublicKey []byte `json:"public_key"`
		}{key}, nil)
	})
	mux.HandleFunc("GET /api/fleet/instruction", func(w http.ResponseWriter, r *http.Request) {
		v := r.URL.Query()
		var out FleetUpdateInstruction
		unlock := requestReadLock(r.Context(), &a.Queue.mu)
		// No instruction can be issued without a running upgrade. Ordinary
		// maintenance belongs to Tick, not these otherwise read-only polls.
		inactive := a.Queue.s.FleetUpgrade == nil || a.Queue.s.FleetUpgrade.State != "running"
		stopping := a.Queue.stopping
		unlock()
		w.Header().Set("Cache-Control", "no-store")
		if inactive && !stopping {
			respond(w, out, nil)
			return
		}
		// Active upgrades retain the atomic maintenance/selection transaction,
		// which rechecks policy, custody and session fences under the lock.
		err := a.Queue.updateRequest(r.Context(), func() error {
			a.Queue.maintain()
			var e error
			out, e = a.Queue.fleetInstruction(v.Get("host"), v.Get("machine"), v.Get("session"))
			return e
		}, &queueUpdateTiming{measureWriter: true})
		respond(w, out, err)
	})
	mux.HandleFunc("GET /api/fleet/authorize", func(w http.ResponseWriter, r *http.Request) {
		v := r.URL.Query()
		revision, _ := strconv.Atoi(v.Get("revision"))
		var proof *FleetRuntimeProof
		err := a.Queue.updateRequest(r.Context(), func() error {
			if e := a.Queue.authorizeFleetUpdate(v.Get("operation"), v.Get("host"), v.Get("machine"), v.Get("session"), revision); e != nil {
				return e
			}
			if v.Get("machine") == "@queue" {
				proof = cloneFleetProof(a.Queue.fleetQueue.Proof)
			} else if m := a.Queue.s.Machines[v.Get("machine")]; m != nil {
				proof = cloneFleetProof(m.Fleet)
			}
			return nil
		}, nil)
		respond(w, proof, err)
	})
	mux.HandleFunc("POST /api/fleet/ack", func(w http.ResponseWriter, r *http.Request) {
		var ack FleetUpgradeAck
		if !decode(w, r, &ack) {
			return
		}
		respond(w, map[string]bool{"ok": true}, a.Queue.AckFleetUpgrade(ack, r.Context()))
	})
	mux.HandleFunc("GET /api/fleet/artifact", a.fleetArtifact)
}

// Authenticated control-path proxy. The worker independently verifies the frozen
// Ed25519 envelope and streamed hash/size. No fleet credential leaves the queue.
func (a API) fleetArtifact(w http.ResponseWriter, r *http.Request) {
	q := a.Queue
	unlockRequest := requestReadLock(r.Context(), &q.mu)
	u := q.s.FleetUpgrade
	var release EnrollmentRelease
	var artifact ReleaseArtifact
	valid := false
	if u != nil && u.State == "running" && u.Operation == r.URL.Query().Get("operation") {
		release = cloneFleetRelease(u.Release)
		for _, v := range release.Manifest.Artifacts {
			if v.File == r.URL.Query().Get("target") {
				artifact = v
				valid = true
				break
			}
		}
	}
	unlockRequest()
	if !valid {
		respond(w, nil, fleetError("fleet_artifact_stale"))
		return
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	defer transport.CloseIdleConnections()
	if release.CACert != "" {
		roots := x509.NewCertPool()
		roots.AppendCertsFromPEM([]byte(release.CACert))
		transport.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	}
	client := &http.Client{Transport: transport, Timeout: 10 * time.Minute, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", strings.TrimRight(release.Origin, "/")+"/"+artifact.File, nil)
	if err != nil {
		respond(w, nil, fleetError("fleet_artifact_unavailable"))
		return
	}
	response, err := client.Do(req)
	if err != nil {
		respond(w, nil, fleetError("fleet_artifact_unavailable"))
		return
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		respond(w, nil, fleetError("fleet_artifact_unavailable"))
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = io.Copy(w, io.LimitReader(response.Body, artifact.Size+1))
}
