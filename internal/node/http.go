package node

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"mycelium/internal/domain"
	"mycelium/internal/ports"
)

type HTTPServer struct {
	Agent     ports.NodeAgent
	Admission ports.AdmissionController
}

func (s HTTPServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.Agent == nil && !strings.HasPrefix(r.URL.Path, "/admission/") {
		writeError(w, http.StatusInternalServerError, "node agent is not configured")
		return
	}
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/snapshot":
		s.snapshot(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/load":
		s.load(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/unload":
		s.unload(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/inspect":
		s.inspect(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/begin-request":
		s.beginRequest(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/end-request":
		s.endRequest(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/admission/offer":
		s.offer(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/admission/commit":
		s.commit(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/admission/release":
		s.release(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/admission/preempt":
		s.preempt(w, r)
	case r.Method == http.MethodGet && r.URL.Path == "/admission/lease":
		s.leaseForJob(w, r)
	default:
		writeError(w, http.StatusNotFound, "not found")
	}
}

func (s HTTPServer) snapshot(w http.ResponseWriter, r *http.Request) {
	snap, err := s.Agent.Snapshot(r.Context())
	writeJSON(w, snap, err)
}

func (s HTTPServer) load(w http.ResponseWriter, r *http.Request) {
	var preset domain.Preset
	if err := json.NewDecoder(r.Body).Decode(&preset); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	inst, err := s.Agent.Load(r.Context(), preset)
	if errors.Is(err, domain.ErrNoFit) {
		writeDomainError(w, http.StatusTooManyRequests, err)
		return
	}
	writeJSON(w, inst, err)
}

func (s HTTPServer) unload(w http.ResponseWriter, r *http.Request) {
	var req struct {
		InstanceID string `json:"instance_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.InstanceID == "" {
		writeError(w, http.StatusBadRequest, "instance_id is required")
		return
	}
	writeJSON(w, map[string]string{"status": "ok"}, s.Agent.Unload(r.Context(), req.InstanceID))
}

func (s HTTPServer) inspect(w http.ResponseWriter, r *http.Request) {
	var preset domain.Preset
	if err := json.NewDecoder(r.Body).Decode(&preset); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	metadata, err := s.Agent.InspectModel(r.Context(), preset)
	writeJSON(w, metadata, err)
}

func (s HTTPServer) beginRequest(w http.ResponseWriter, r *http.Request) {
	instanceID, ok := decodeInstanceID(w, r)
	if !ok {
		return
	}
	writeJSON(w, map[string]string{"status": "ok"}, s.Agent.BeginRequest(r.Context(), instanceID))
}

func (s HTTPServer) endRequest(w http.ResponseWriter, r *http.Request) {
	instanceID, ok := decodeInstanceID(w, r)
	if !ok {
		return
	}
	writeJSON(w, map[string]string{"status": "ok"}, s.Agent.EndRequest(r.Context(), instanceID))
}

func (s HTTPServer) offer(w http.ResponseWriter, r *http.Request) {
	if s.Admission == nil {
		writeError(w, http.StatusInternalServerError, "admission controller is not configured")
		return
	}
	var req admissionOfferRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	offer, err := s.Admission.Offer(r.Context(), req.Job, req.Claim)
	if errors.Is(err, domain.ErrNoFit) {
		writeDomainError(w, http.StatusTooManyRequests, err)
		return
	}
	writeJSON(w, offer, err)
}

func (s HTTPServer) commit(w http.ResponseWriter, r *http.Request) {
	if s.Admission == nil {
		writeError(w, http.StatusInternalServerError, "admission controller is not configured")
		return
	}
	var req admissionCommitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.OfferID == "" {
		writeError(w, http.StatusBadRequest, "offer_id is required")
		return
	}
	lease, err := s.Admission.Commit(r.Context(), req.OfferID, req.Fence)
	if errors.Is(err, domain.ErrStaleFence) {
		writeDomainError(w, http.StatusConflict, err)
		return
	}
	if errors.Is(err, domain.ErrNoFit) {
		writeDomainError(w, http.StatusTooManyRequests, err)
		return
	}
	writeJSON(w, lease, err)
}

func (s HTTPServer) release(w http.ResponseWriter, r *http.Request) {
	if s.Admission == nil {
		writeError(w, http.StatusInternalServerError, "admission controller is not configured")
		return
	}
	leaseID, ok := decodeLeaseID(w, r)
	if !ok {
		return
	}
	writeJSON(w, map[string]string{"status": "ok"}, s.Admission.Release(r.Context(), leaseID))
}

func (s HTTPServer) preempt(w http.ResponseWriter, r *http.Request) {
	if s.Admission == nil {
		writeError(w, http.StatusInternalServerError, "admission controller is not configured")
		return
	}
	var req admissionPreemptRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.LeaseID == "" {
		writeError(w, http.StatusBadRequest, "lease_id is required")
		return
	}
	writeJSON(w, map[string]string{"status": "ok"}, s.Admission.Preempt(r.Context(), req.LeaseID, req.Reason))
}

func (s HTTPServer) leaseForJob(w http.ResponseWriter, r *http.Request) {
	if s.Admission == nil {
		writeError(w, http.StatusInternalServerError, "admission controller is not configured")
		return
	}
	inspector, ok := s.Admission.(ports.LeaseInspector)
	if !ok {
		writeError(w, http.StatusNotImplemented, "admission controller does not expose lease inspection")
		return
	}
	jobID := r.URL.Query().Get("job_id")
	if jobID == "" {
		writeError(w, http.StatusBadRequest, "job_id is required")
		return
	}
	lease, found, err := inspector.LeaseForJob(r.Context(), jobID)
	writeJSON(w, admissionLeaseForJobResponse{Found: found, Lease: lease}, err)
}

func decodeInstanceID(w http.ResponseWriter, r *http.Request) (string, bool) {
	var req struct {
		InstanceID string `json:"instance_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return "", false
	}
	if req.InstanceID == "" {
		writeError(w, http.StatusBadRequest, "instance_id is required")
		return "", false
	}
	return req.InstanceID, true
}

func decodeLeaseID(w http.ResponseWriter, r *http.Request) (string, bool) {
	var req struct {
		LeaseID string `json:"lease_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return "", false
	}
	if req.LeaseID == "" {
		writeError(w, http.StatusBadRequest, "lease_id is required")
		return "", false
	}
	return req.LeaseID, true
}

type admissionOfferRequest struct {
	Job   domain.Job   `json:"job"`
	Claim domain.Claim `json:"claim"`
}

type admissionCommitRequest struct {
	OfferID string `json:"offer_id"`
	Fence   uint64 `json:"fence"`
}

type admissionPreemptRequest struct {
	LeaseID string `json:"lease_id"`
	Reason  string `json:"reason"`
}

type admissionLeaseForJobResponse struct {
	Found bool         `json:"found"`
	Lease domain.Lease `json:"lease,omitempty"`
}

type HTTPClient struct {
	BaseURL string
	Client  *http.Client
}

func NewHTTPClient(baseURL string) *HTTPClient {
	return &HTTPClient{BaseURL: strings.TrimRight(baseURL, "/"), Client: http.DefaultClient}
}

func (c *HTTPClient) Snapshot(ctx context.Context) (domain.NodeSnapshot, error) {
	var snap domain.NodeSnapshot
	err := c.do(ctx, http.MethodGet, "/snapshot", nil, &snap)
	return snap, err
}

func (c *HTTPClient) Load(ctx context.Context, p domain.Preset) (domain.ModelInstance, error) {
	var inst domain.ModelInstance
	err := c.do(ctx, http.MethodPost, "/load", p, &inst)
	return inst, err
}

func (c *HTTPClient) Unload(ctx context.Context, instanceID string) error {
	return c.do(ctx, http.MethodPost, "/unload", map[string]string{"instance_id": instanceID}, nil)
}

func (c *HTTPClient) InspectModel(ctx context.Context, p domain.Preset) (domain.ModelMetadata, error) {
	var metadata domain.ModelMetadata
	err := c.do(ctx, http.MethodPost, "/inspect", p, &metadata)
	return metadata, err
}

func (c *HTTPClient) BeginRequest(ctx context.Context, instanceID string) error {
	return c.do(ctx, http.MethodPost, "/begin-request", map[string]string{"instance_id": instanceID}, nil)
}

func (c *HTTPClient) EndRequest(ctx context.Context, instanceID string) error {
	return c.do(ctx, http.MethodPost, "/end-request", map[string]string{"instance_id": instanceID}, nil)
}

func (c *HTTPClient) Offer(ctx context.Context, job domain.Job, claim domain.Claim) (domain.LeaseOffer, error) {
	var offer domain.LeaseOffer
	err := c.do(ctx, http.MethodPost, "/admission/offer", admissionOfferRequest{Job: job, Claim: claim}, &offer)
	return offer, err
}

func (c *HTTPClient) Commit(ctx context.Context, offerID string, fence uint64) (domain.Lease, error) {
	var lease domain.Lease
	err := c.do(ctx, http.MethodPost, "/admission/commit", admissionCommitRequest{OfferID: offerID, Fence: fence}, &lease)
	return lease, err
}

func (c *HTTPClient) Release(ctx context.Context, leaseID string) error {
	return c.do(ctx, http.MethodPost, "/admission/release", map[string]string{"lease_id": leaseID}, nil)
}

func (c *HTTPClient) Preempt(ctx context.Context, leaseID, reason string) error {
	return c.do(ctx, http.MethodPost, "/admission/preempt", admissionPreemptRequest{LeaseID: leaseID, Reason: reason}, nil)
}

func (c *HTTPClient) LeaseForJob(ctx context.Context, jobID string) (domain.Lease, bool, error) {
	var out admissionLeaseForJobResponse
	err := c.do(ctx, http.MethodGet, "/admission/lease?job_id="+url.QueryEscape(jobID), nil, &out)
	return out.Lease, out.Found, err
}

func (c *HTTPClient) do(ctx context.Context, method, path string, in, out any) error {
	var body *bytes.Reader
	if in == nil {
		body = bytes.NewReader(nil)
	} else {
		data, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var e wireError
		if err := json.NewDecoder(resp.Body).Decode(&e); err != nil {
			return fmt.Errorf("node http %s: %s", path, resp.Status)
		}
		switch e.Code {
		case "no_fit":
			return fmt.Errorf("%w: node http %s: %s", domain.ErrNoFit, path, e.Error)
		case "stale_fence":
			return fmt.Errorf("%w: node http %s: %s", domain.ErrStaleFence, path, e.Error)
		}
		return fmt.Errorf("node http %s: %s", path, e.Error)
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

type wireError struct {
	Error string `json:"error"`
	Code  string `json:"code,omitempty"`
}

func writeJSON(w http.ResponseWriter, v any, err error) {
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		panic(err)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeWireError(w, status, msg, "")
}

func writeDomainError(w http.ResponseWriter, status int, err error) {
	code := ""
	switch {
	case errors.Is(err, domain.ErrNoFit):
		code = "no_fit"
	case errors.Is(err, domain.ErrStaleFence):
		code = "stale_fence"
	}
	writeWireError(w, status, err.Error(), code)
}

func writeWireError(w http.ResponseWriter, status int, msg, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(wireError{Error: msg, Code: code})
}

var _ ports.NodeAgent = (*HTTPClient)(nil)
var _ ports.AdmissionController = (*HTTPClient)(nil)
var _ ports.LeaseInspector = (*HTTPClient)(nil)
